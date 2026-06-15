"""Phase 7 - Global priority dispatch (the core engine).

ONE global priority queue across all SKUs and a single shared machine model.
Lots are processed CONSUMER-FIRST (a producer becomes ready only once its
consumer is placed). Each ready lot is popped by the criticality key
  (LST asc, slack asc, is_bottleneck desc, drum_deadline asc, sku, lot_id)
placed As-Late-As-Possible on the latest-feasible eligible machine, and
committed ONLY IF the correctness commit-test passes:
  aging_min <= gap <= aging_max  AND  no overlap  AND  start >= supplier ready.
Otherwise a reason code is emitted and the lot is left unplaced. Deterministic.
"""
from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import networkx as nx
import pandas as pd

from . import config as C
from .dag import SkuDag
from .pin import get_timeline
from .timeline import MachineTimeline


@dataclass
class DispatchResult:
    schedule_rows: List[dict] = field(default_factory=list)
    violations: List[dict] = field(default_factory=list)
    placed_starts: Dict[str, float] = field(default_factory=dict)  # lot_id -> start
    # FIX-1: folded op-195 carcasses are NOT schedule rows (they charge no machine
    # minutes and have 0 duration / blank machine, so an external validator reads
    # them as malformed op-195 producers). They are collected HERE instead and
    # emitted as outputs/produced_components.csv for BOM mass-balance / pegging.
    produced_component_rows: List[dict] = field(default_factory=list)


def _eligible_machines(lot: dict) -> List[str]:
    return [m.strip() for m in str(lot["machines"]).split(",") if m.strip()]


def dispatch(lots: pd.DataFrame,
             dags: Dict[str, SkuDag],
             timelines: Dict[str, MachineTimeline],
             block_starts: Dict[str, float],
             bottleneck_stage: str) -> DispatchResult:
    """lots: all windowed lots (all SKUs). timelines pre-loaded with curing
    anchors. block_starts: source_curing_block -> drum start (epoch min)."""
    res = DispatchResult()
    if lots.empty:
        return res

    lots = lots.copy().reset_index(drop=True)
    by_id = {r["lot_id"]: r for r in lots.to_dict("records")}

    # Build the per-block consumer graph among lots: a lot's consumer is the
    # lot in the SAME block whose item == consumer_item. FG's consumer is the
    # curing block (already pinned -> consumer_start = block start).
    consumer_lot: Dict[str, Optional[str]] = {}
    by_block_item: Dict[Tuple[str, str], str] = {}
    # (sku, item) -> [(deadline, lot_id)] sorted, for campaign-merge fallback
    by_sku_item: Dict[Tuple[str, str], List[Tuple[float, str]]] = {}
    # FIX-5: item -> [(deadline, lot_id)] sorted ACROSS all SKUs, so a POOLED
    # cross-SKU producer (one batch serving many SKUs' consumers) can resolve a
    # consumer of the same item CODE in ANY sku/block, not just its anchor sku.
    by_item: Dict[str, List[Tuple[float, str]]] = {}
    for lid, r in by_id.items():
        by_block_item[(r["source_curing_block"], r["item"])] = lid
        by_sku_item.setdefault((r["sku"], r["item"]), []).append(
            (float(r["curing_deadline"]), lid))
        by_item.setdefault(r["item"], []).append(
            (float(r["curing_deadline"]), lid))
    for k in by_sku_item:
        by_sku_item[k].sort()
    for k in by_item:
        by_item[k].sort()

    def _is_pooled(lid: str) -> bool:
        """A cross-SKU pooled compound lot (sizing.merge_cross_sku_campaigns).
        Identified by lot_id prefix POOL- (deterministic, no data lookup)."""
        return str(lid).startswith("POOL-")

    def _merged_consumer(sku: str, cons_item: str, deadline: float,
                         pooled: bool = False):
        """Resolve the consumer lot when campaign-merge anchored it on a DIFFERENT
        (earlier) block than the producer. Pick the consumer lot of the same item
        whose deadline is the latest <= this producer's deadline (the campaign
        that absorbed this block). Deterministic (sorted). Returns lot_id|None.

        FIX-5: a POOLED cross-SKU producer is ANCHORED at sizing time to its
        earliest-deadline (tightest) consumer's sku+block, so the same-sku lookup
        below already finds the correct anchor consumer (its anchor block is that
        sku's block). Only if NO same-sku consumer of that item exists at all (a
        genuinely orphaned pooled edge) do we fall back to the cross-SKU `by_item`
        index so the shared batch can still bind to a real consumer rather than go
        UNREACHED. Anchoring stays bound to the tightest puller (C2)."""
        cands = by_sku_item.get((sku, cons_item))
        if not cands and pooled:
            cands = by_item.get(cons_item)        # cross-SKU last resort
        if not cands:
            return None
        chosen = None
        for d, clid in cands:  # ascending deadline
            if d <= deadline + 1e-6:
                chosen = clid
            else:
                break
        # if none at/below, fall back to the earliest (tightest) consumer lot
        return chosen if chosen is not None else cands[0][1]

    # producers of a lot: lots whose consumer_item == this item, matched 1:1 in
    # the same block, else to the campaign-merged consumer covering this block.
    producers: Dict[str, List[str]] = {lid: [] for lid in by_id}
    for lid, r in by_id.items():
        cons_item = r["consumer_item"]
        block = r["source_curing_block"]
        if not cons_item:
            consumer_lot[lid] = None       # FG -> consumed by the drum
            continue
        pooled = _is_pooled(lid)
        cl = by_block_item.get((block, cons_item))
        if cl is None:
            cl = _merged_consumer(r["sku"], cons_item,
                                  float(r["curing_deadline"]), pooled=pooled)
        if cl is not None and cl != lid:
            consumer_lot[lid] = cl
            producers[cl].append(lid)
        else:
            # genuinely no consumer lot exists -> honest dangling producer; it
            # will be reported (UNREACHED) rather than silently anchored to drum.
            consumer_lot[lid] = None

    # ready = consumer placed (or consumer is the drum). placed start known.
    consumer_start_of: Dict[str, float] = {}
    placed: set = set()
    unplaced_reason: Dict[str, str] = {}
    # BUG-03 recourse bookkeeping: how each placed lot landed, so we can lift a
    # consumer later (within its own slack) to rescue an over-aged producer.
    placed_machine: Dict[str, str] = {}   # lot_id -> machine it sits on
    retries: Dict[str, int] = {}          # lot_id -> #recourse lifts so far
    reanchored: Dict[str, bool] = {}      # FIX-3: lot_id -> stock re-anchored once
    # BUG-N3 backstop: total recourse re-pushes a single producer lid may trigger
    # (lift OR re-anchor combined). Recourse already terminates - per-consumer
    # lifts are bounded by retries[c] < RECOURSE_MAX_LIFTS and the re-anchor is
    # one-shot (reanchored[lid]), and a lid transits at most 2 consumers, so the
    # natural max is 2*RECOURSE_MAX_LIFTS + 1 re-pushes. This hard per-lid cap is
    # set safely ABOVE that reachable max so it never changes the current schedule;
    # it exists purely to make per-lot termination provable independent of the
    # lift/re-anchor bookkeeping (a deterministic guard, no RNG).
    recourse_count: Dict[str, int] = {}   # lot_id -> #recourse re-pushes so far
    RECOURSE_CAP = 2 * C.RECOURSE_MAX_LIFTS + 4

    def criticality_key(lid: str):
        r = by_id[lid]
        return (
            r["lst"] if pd.notna(r["lst"]) else float("inf"),
            r["slack_min"] if pd.notna(r["slack_min"]) else float("inf"),
            0 if r["is_bottleneck"] or r["stage"] == bottleneck_stage else 1,
            r["curing_deadline"],
            r["sku"], lid,
        )

    # seed queue with lots whose consumer is the drum (FG lots)
    heap: List[Tuple] = []
    seeded: set = set()

    def push(lid: str):
        if lid in seeded:
            return
        seeded.add(lid)
        heapq.heappush(heap, (criticality_key(lid), lid))

    for lid, cl in consumer_lot.items():
        if cl is None:
            consumer_start_of[lid] = block_starts.get(
                by_id[lid]["source_curing_block"], by_id[lid]["curing_deadline"])
            push(lid)

    while heap:
        _, lid = heapq.heappop(heap)
        if lid in placed or lid in unplaced_reason:
            continue
        r = by_id[lid]
        cs = consumer_start_of.get(lid, block_starts.get(
            r["source_curing_block"], r["curing_deadline"]))

        elig = _eligible_machines(r)
        if not elig:
            unplaced_reason[lid] = "NO_ELIGIBLE_MACHINE"
            res.violations.append(_vrow(lid, "", "NO_ELIGIBLE_MACHINE",
                r["item"], "ERROR", r.get("source_curing_block", "")))
            continue

        transfer = r["transfer_min"]
        amin = r["aging_min_h"] * 60.0
        amax = r["aging_max_h"] * 60.0
        buf = r["buffer_h"] * 60.0
        proc = r["proc_eff_min"]
        # ALAP target: end so consumer can pull just in time. The placement
        # target governs the AVAILABILITY (aging-MIN / C1) gap, which is ALWAYS
        # transfer-netted (gap = cs - E - transfer below), exactly like the LST
        # in windows.py (LST = cs - transfer - proc - amin - buf). So the target
        # END must ALWAYS subtract transfer; otherwise the ALAP placement aims
        # for E = cs - amin while the commit-test measures gap = cs - E -
        # transfer = amin - transfer < amin and every lot fails INFEASIBLE_AGING
        # by exactly `transfer` whenever AGING_SUBTRACTS_TRANSFER is False.
        # REGRESSION FIX: the prior `(transfer if AGING_SUBTRACTS_TRANSFER else
        # 0)` conflated the MIN-availability target with the MAX-wall-clock
        # convention; transfer is unconditional on the MIN side. The aging-MAX /
        # cure-by MAX test below independently uses gap_aging (which honours
        # AGING_SUBTRACTS_TRANSFER), so the wall-clock convention is preserved.
        target_end = cs - amin - buf - transfer
        target_start = target_end - proc

        # supplier (producer) ready-time lower bound: producers placed AFTER
        # this lot, so we don't yet know them; AND-join is enforced when each
        # producer is later placed (its own consumer_start = this start).
        is_drum_fed = consumer_lot.get(lid) is None

        # FIX-A: BUILD LEVEL-LOADER. The green-tyre BUILD (op-200, drum-fed,
        # stage Building) is the OTIF bottleneck under ALAP: every build is pulled
        # to the last feasible instant before its press cures, collapsing a 2h
        # slice's worth of builds onto one moment (297% spikes) on a pool that is
        # only 75% loaded across the cure-by window. Instead of latest-feasible,
        # we place the build at the EARLIEST feasible slot within its cure-by
        # window on the LEAST-LOADED eligible TBM, so builds SPREAD across both the
        # window and the 11-TBM pool. The cure DRUM stays the fixed anchor (cs is
        # the real pinned press start); the commit-test below still binds the
        # cure-by band on BOTH edges (0 too-fresh / 0 over-aged - C2).
        #
        # WINDOW (start axis):
        #   earliest start = cs - amax - proc       (any earlier over-ages, C2 max)
        #     clamped up to inputs-ready; in this consumer-first model producers
        #     are placed AFTER the build, so the window-start IS the inputs floor.
        #   latest   start = target_start            (= cs - amin - buf - transfer
        #                     - proc; any later is too-fresh, aging-MIN / C1).
        is_leveled_build = (C.BUILD_LEVELING_ENABLED
                            and is_drum_fed
                            and int(r["op_seq"]) in C.BUILD_OPS)

        cands = []  # (machine, S, changeover, load)
        if is_leveled_build:
            # LEVEL TARGET: the early edge of the cure-by window (cs - amax - proc)
            # de-collides the ALAP spike, but pulling EVERY build to the extreme
            # early edge forces its compound/component INPUTS even earlier, where
            # their own (shorter) shelf windows over-age (C2). So we clamp the
            # early edge CONSERVATIVELY by BUILD_LEVEL_BACKOFF_FRAC of the window
            # toward the ALAP ceiling: builds still spread across the window + the
            # 11 TBMs, but stay close enough to JIT that their inputs survive.
            early_edge = cs - amax - proc
            span = target_start - early_edge                  # >= 0 window width
            window_start = early_edge + C.BUILD_LEVEL_BACKOFF_FRAC * max(span, 0.0)
            for m in sorted(elig):
                tl = get_timeline(timelines, m)
                # earliest feasible slot at/after window_start, not later than the
                # ALAP ceiling target_start (else too-fresh on the aging-MIN edge).
                S = tl.earliest_feasible_start(window_start, proc, r["sku"],
                                               latest=target_start)
                if S is None:
                    # window full at the early edge - fall back to the ALAP slot so
                    # a build still places when its window is congested.
                    S = tl.latest_feasible_start(target_start, proc, r["sku"])
                if S is None:
                    continue
                co = tl.changeover_at(S, proc, r["sku"])
                cands.append((m, S, co, tl.load_minutes()))
            if not cands:
                unplaced_reason[lid] = "NO_FEASIBLE_SLOT"
                res.violations.append(_vrow(lid, "", "NO_FEASIBLE_SLOT",
                    f"level window_start={window_start:.1f} "
                    f"target_start={target_start:.1f}", "ERROR",
                    r.get("source_curing_block", "")))
                continue
            # DETERMINISTIC total order: least machine busy-min -> lowest TBM id
            # -> earliest feasible slot -> (lot_id breaks ties at the queue level).
            # We balance load first (the whole point), then prefer the earliest
            # slot, then the lowest machine id. Changeover is a soft secondary so a
            # same-SKU run is preferred only among equally-loaded earliest builders.
            chosen = min(cands, key=lambda c: (c[3], c[0], c[1], c[2]))
            best = (None, chosen[0], chosen[1])
        else:
            # FIX-2: building ops share a multi-TBM pool and must SPREAD across
            # eligible builders rather than pin onto one. Collect every eligible
            # machine's latest-feasible (ALAP) start, then choose:
            #   * non-building op -> spec key (changeover, latest-start, load, id)
            #   * building op     -> among machines whose latest start is within
            #     BUILD_SPREAD_TOL_MIN of the best, pick the LEAST-LOADED.
            for m in sorted(elig):
                tl = get_timeline(timelines, m)
                S = tl.latest_feasible_start(target_start, proc, r["sku"])
                if S is None:
                    continue
                co = tl.changeover_at(S, proc, r["sku"])
                cands.append((m, S, co, tl.load_minutes()))
            if not cands:
                unplaced_reason[lid] = "NO_FEASIBLE_SLOT"
                res.violations.append(_vrow(lid, "", "NO_FEASIBLE_SLOT",
                    f"target_start={target_start:.1f}", "ERROR",
                    r.get("source_curing_block", "")))
                continue

            is_building = (C.BUILD_SPREAD_ENABLED
                           and int(r["op_seq"]) in C.BUILD_OPS
                           and len(cands) > 1)
            if is_building:
                best_S = max(c[1] for c in cands)        # latest available start
                tol = C.BUILD_SPREAD_TOL_MIN
                near = [c for c in cands if best_S - c[1] <= tol]
                # least-loaded first, then lower changeover, then latest start,
                # then machine id (determinism).
                chosen = min(near, key=lambda c: (c[3], c[2], -c[1], c[0]))
            else:
                # spec Phase 7 key: no/lower changeover, latest start, load, id
                chosen = min(cands, key=lambda c: (c[2], -c[1], c[3], c[0]))
            best = (None, chosen[0], chosen[1])

        _, m, S = best
        E = S + proc
        # Two gap conventions (Round-2 aging-convention decision, see config):
        #   gap          = consumer_start - end - transfer  (AVAILABILITY: C1 /
        #                  aging-MIN; material in transit can't yet be consumed)
        #   gap_aging    = consumer_start - end             (WALL-CLOCK material
        #                  ageing: aging-MAX & GT cure-by max; the compound keeps
        #                  ageing during transfer). Flag-controlled so the plant
        #                  can revert to the pause-during-handling convention.
        gap = cs - E - transfer
        gap_aging = gap if C.AGING_SUBTRACTS_TRANSFER else (cs - E)
        eps = C.AGING_EPS_MIN

        # D2: green-tyre build -> op-210 cure-by gate. For a lot whose consumer
        # IS the drum (consumer_lot is None), `cs` is the REAL pinned op-210
        # block start, so this band is enforced against the placed press slot,
        # not a date. C1/min side uses transfer-netted gap; the 360-min cure-by
        # MAX is wall-clock (the green tyre scorches during transfer too).
        # is_drum_fed was resolved above (FIX-A build-leveling gate).
        if is_drum_fed:
            cure_min = C.GREEN_TYRE_CUREBY_MIN_H * 60.0
            cure_max = C.GREEN_TYRE_CUREBY_MAX_H * 60.0
            if gap < cure_min - eps:
                unplaced_reason[lid] = "CUREBY_TOO_EARLY"
                res.violations.append(_vrow(lid, m, "CUREBY_TOO_EARLY",
                    f"build->cure gap={gap:.1f} < {cure_min:.0f} (drum slot "
                    f"{r['source_curing_block']})", "ERROR",
                    r.get("source_curing_block", "")))
                continue
            if gap_aging > cure_max + eps:
                unplaced_reason[lid] = "CUREBY_EXPIRED"
                # ERROR-6 FIX: interpolate the ACTUAL GT cure-by max
                # (GREEN_TYRE_CUREBY_MAX_H*60), not the hardcoded "360".
                res.violations.append(_vrow(lid, m, "CUREBY_EXPIRED",
                    f"build->cure wall-clock gap={gap_aging:.1f} > "
                    f"{cure_max:.0f} GT cure-by "
                    f"(drum slot {r['source_curing_block']})", "ERROR",
                    r.get("source_curing_block", "")))
                continue

        # commit-test (both-sided aging on the consumer edge)
        if gap < amin - eps:
            unplaced_reason[lid] = "INFEASIBLE_AGING"
            res.violations.append(_vrow(lid, m, "INFEASIBLE_AGING",
                f"gap={gap:.1f} < amin={amin:.1f}", "ERROR",
                r.get("source_curing_block", "")))
            continue
        if amax > 0 and gap_aging > amax + eps:
            # BUG-03 RECOURSE (a): the latest-feasible slot still over-ages on
            # wall-clock. Before dropping, try to LIFT this lot's consumer to a
            # later start (within the consumer's OWN slack) so this producer can
            # be placed late enough to fit its aging-max. Bounded & deterministic:
            # each consumer may be lifted at most RECOURSE_MAX_LIFTS times; a lift
            # only proceeds if it strictly helps and keeps the consumer feasible.
            c = consumer_lot.get(lid)
            lifted = False
            if (c is not None and c in placed
                    and retries.get(c, 0) < C.RECOURSE_MAX_LIFTS
                    and recourse_count.get(lid, 0) < RECOURSE_CAP):
                # the latest start S we COULD use is the global latest-feasible on
                # the best machine; to fit aging-max we need the consumer to start
                # at >= E + transfer + amin (avail) AND E_wallclock within amax,
                # i.e. consumer_start >= S + proc + (amin*... ). Required minimum
                # consumer start so THIS producer (placed at its latest S) fits:
                need_cs = S + proc + transfer + amin  # availability lower bound
                # cap by wall-clock max too: cs - (S+proc) <= amax  -> cs<=S+proc+amax
                cs_hi = S + proc + (amax if amax > 0 else float("inf"))
                if need_cs <= cs_hi + eps:
                    new_cs = _lift_consumer(
                        c, by_id, timelines, placed, placed_machine,
                        consumer_start_of, res, need_cs, consumer_lot,
                        producers)
                    if new_cs is not None:
                        retries[c] = retries.get(c, 0) + 1
                        recourse_count[lid] = recourse_count.get(lid, 0) + 1
                        consumer_start_of[lid] = new_cs
                        seeded.discard(lid)
                        push(lid)              # retry this producer with later cs
                        lifted = True
            if lifted:
                continue
            # FIX-3: NON-BINDING UPSTREAM EARLY-BUILD (build-ahead-as-stock).
            # The latest-feasible slot over-ages against THIS (far) consumer, but
            # the producer is non-binding (not the bottleneck stage) and is made
            # to STOCK. Re-anchor it to the NEAREST already-placed consumer lot of
            # the same consumer-item whose start it can still feed WITHIN the
            # aging band [amin, amax] (avail: cs' >= E + transfer + amin ; max:
            # cs' - E <= amax). Deterministic (consumers sorted by start); bounded
            # (one re-anchor try per lot). Respects C2 on both sides - it only
            # changes WHICH consumer edge the stock lot is matched to.
            if (C.FIX3_EARLY_BUILD_ENABLED
                    and not is_drum_fed
                    and r["stage"] != bottleneck_stage
                    and not r["is_bottleneck"]
                    and not reanchored.get(lid)
                    and recourse_count.get(lid, 0) < RECOURSE_CAP):
                anchor = _nearest_stock_consumer(
                    r, E, transfer, amin, amax, by_sku_item,
                    res.placed_starts, placed, C.AGING_SUBTRACTS_TRANSFER,
                    by_item=(by_item if _is_pooled(lid) else None))
                if anchor is not None:
                    new_clid, new_cs = anchor
                    reanchored[lid] = True
                    recourse_count[lid] = recourse_count.get(lid, 0) + 1
                    # Re-point BOTH the consumer-edge and the start so the
                    # committed row's consumer_lot_id matches the gap Phase-8
                    # re-derives (no phantom violation), and the producer-release
                    # AND-join uses the real new consumer.
                    old_c = consumer_lot.get(lid)
                    if old_c is not None and lid in producers.get(old_c, []):
                        producers[old_c].remove(lid)
                    consumer_lot[lid] = new_clid
                    producers.setdefault(new_clid, [])
                    if lid not in producers[new_clid]:
                        producers[new_clid].append(lid)
                    consumer_start_of[lid] = new_cs
                    seeded.discard(lid)
                    push(lid)      # retry against the nearer (feasible) consumer
                    continue
            # ALAP should keep gap small; if still over-aged on WALL-CLOCK time,
            # infeasible (do not place a lot that scorches before consumption).
            unplaced_reason[lid] = "INFEASIBLE_AGING"
            res.violations.append(_vrow(lid, m, "INFEASIBLE_AGING",
                f"wall-clock gap={gap_aging:.1f} > amax={amax:.1f}", "ERROR",
                r.get("source_curing_block", "")))
            continue

        # COMMIT
        tl = get_timeline(timelines, m)
        co_reserved = tl.changeover_at(S, proc, r["sku"])  # actual neighbour C/O
        tl.commit(S, proc, lid, r["sku"])
        placed.add(lid)
        placed_machine[lid] = m
        consumer_start_of[lid] = cs
        res.placed_starts[lid] = S
        res.schedule_rows.append({
            "lot_id": lid, "sku": r["sku"], "item": r["item"],
            "item_type": r["item_type"], "stage": r["stage"],
            "op_seq": r["op_seq"], "machine": m, "qty": r["qty"], "uom": r["uom"],
            "start": S, "end": E, "duration_min": proc,
            "changeover_min": co_reserved, "is_curing": False,
            # MASTERS: carry the EXACT transfer minutes this lot was committed
            # with (per-item-type from the Transfer master), so Phase-8 validate
            # re-derives C1/precedence + aging-min with the SAME transfer the
            # commit-test used - not a flat default that would phantom-fail a lot
            # whose master transfer differs from C.TRANSFER_MIN (e.g. bead=7).
            "transfer_min": transfer,
            "consumer_item": r["consumer_item"],
            "aging_min_h": r["aging_min_h"], "aging_max_h": r["aging_max_h"],
            "gap_to_consumer_min": gap, "status": "PLACED",
            # D2/D3/D4: carry the real op-210 block this lot's chain feeds so
            # Phase 8 can re-derive C1/C2 from the placed drum time and the
            # handoff report can cite the actual cure_start.
            "source_curing_block": r["source_curing_block"],
            # D3 (consistency): the EXACT consumer this lot was committed against.
            # Empty when consumed directly by the drum (FG/green-tyre). Phase 8
            # re-derives the gap from this same edge's real placed times instead
            # of independently re-guessing the merged consumer (which diverged
            # and produced phantom AGING/PRECEDENCE breaches on committed lots).
            "consumer_lot_id": consumer_lot.get(lid) or "",
        })
        # BUG-2 (traceability): if this committed lot FOLDED a carcass (op-195)
        # into its GT (op-200) build, emit a companion CARCASS "produced" row so
        # mass-balance / pegging counts the carcass as produced (it showed 0%
        # before, since the fold made it a non-dispatched node). The companion is
        # CHARGED NO MACHINE MINUTES: duration_min=0, machine="" (blank), tagged
        # is_folded=True, co-located at the GT build END. A 0-duration, blank-
        # machine row never trips C4/C7 non-overlap or TBM capacity (the build was
        # already charged ONCE inside this GT lot's proc), but its qty IS included
        # in produced-quantity sums. Deterministic (derived from the committed GT).
        # FIX-1: the folded carcass (op-195) is NOT appended to the schedule. A
        # 0-duration, blank-machine schedule row is read by EXTERNAL validators as
        # a real op-195 producer (failing Duration validity, Aging integrity and
        # Precedence). The carcass build was ALREADY charged ONCE inside this GT
        # lot's proc (the fold), so it must not appear as a second machine row.
        # Instead we record it as a PRODUCED COMPONENT (mass-balance / pegging
        # only): qty, the consumer lot it folded into, and the fold provenance.
        carcass_item = str(r.get("carcass_item", "") or "")
        if carcass_item:
            res.produced_component_rows.append({
                "sku": r["sku"], "item": carcass_item, "item_type": "Carcass",
                "qty": r["qty"], "consumer_lot_id": lid,
                "source": "carcass_fold",
            })
        # release producers of this lot: their consumer_start = this lot's start
        for p in producers.get(lid, []):
            consumer_start_of[p] = S
            push(p)

    # any lot never seeded (orphans) -> report
    for lid in by_id:
        if lid not in placed and lid not in unplaced_reason:
            unplaced_reason[lid] = "UNREACHED"
            res.violations.append(_vrow(lid, "", "UNREACHED",
                "consumer never placed", "ERROR",
                by_id[lid].get("source_curing_block", "")))

    # BUG-09 FIX: recompute changeover_min in a final deterministic pass over
    # each machine's PLACED sequence. The value stamped at commit time is the
    # changeover vs the neighbours that existed THEN; if a later lot is inserted
    # between two existing lots, the earlier stored value drifts (it no longer
    # matches the real left neighbour). Here we re-derive C/O = CHANGEOVER_MIN
    # iff the immediately-preceding placed lot on the same machine is a DIFFERENT
    # SKU (0 for same SKU / first lot), so the stored value is stable and matches
    # the committed sequence exactly. Curing/reserved anchors carry their own
    # C/O (0) and are not dispatch rows, so only dispatch rows are touched.
    _recompute_changeover(res.schedule_rows)
    return res


def _recompute_changeover(rows: List[dict]) -> None:
    """Stabilise changeover_min over the final placed sequence per machine."""
    by_machine: Dict[str, List[dict]] = {}
    for r in rows:
        by_machine.setdefault(r["machine"], []).append(r)
    for m in sorted(by_machine):
        seq = sorted(by_machine[m], key=lambda r: (r["start"], r["lot_id"]))
        prev_sku = None
        for r in seq:
            if prev_sku is None or r["sku"] == prev_sku:
                r["changeover_min"] = 0.0
            else:
                r["changeover_min"] = C.CHANGEOVER_MIN
            prev_sku = r["sku"]


def _vrow(lot_id, machine, check_type, detail, severity,
          source_curing_block="") -> dict:
    return {"lot_id": lot_id, "machine": machine, "check_type": check_type,
            "detail": detail, "severity": severity,
            "source_curing_block": source_curing_block}


def _nearest_stock_consumer(r, E, transfer, amin, amax, by_sku_item,
                            placed_starts, placed, subtract_transfer,
                            by_item=None):
    """FIX-3: choose the START of the nearest already-placed consumer lot of this
    producer's consumer-item that this producer (fixed end E) can feed WITHIN its
    aging band, treating the producer as build-ahead stock.

    Feasible consumer start cs' must satisfy:
        availability: cs' - E - transfer >= amin          (C1 / aging-min)
        wall-clock  : cs' - E (or -transfer) <= amax       (C2 / aging-max)
    Among feasible placed consumers we pick the one with the SMALLEST cs' (the
    nearest = smallest gap), so the stock lot is drawn as soon as it is legally
    available. Deterministic (candidates sorted by (deadline, lot_id)); returns
    the chosen consumer start or None."""
    cons_item = r["consumer_item"]
    if not cons_item:
        return None
    cands = by_sku_item.get((r["sku"], cons_item))
    # FIX-5: a pooled producer with no same-sku stock consumer may draw against
    # the cross-SKU `by_item` index (the shared batch feeds any sku's consumer).
    if not cands and by_item is not None:
        cands = by_item.get(cons_item)
    if not cands:
        return None
    best = None
    best_lid = None
    for _deadline, clid in cands:               # ascending deadline (sorted)
        if clid not in placed:
            continue
        csp = placed_starts.get(clid)
        if csp is None:
            continue
        gap_avail = csp - E - transfer
        gap_max = gap_avail if subtract_transfer else (csp - E)
        if gap_avail >= amin - C.AGING_EPS_MIN and (
                amax <= 0 or gap_max <= amax + C.AGING_EPS_MIN):
            if best is None or csp < best:
                best = csp
                best_lid = clid
    if best is None:
        return None
    return (best_lid, best)


def _remove_interval(tl: MachineTimeline, lot_id: str) -> None:
    """Deterministically drop one committed interval by lot_id and resync caches."""
    for i, iv in enumerate(tl.intervals):
        if iv.lot_id == lot_id:
            tl._load -= (iv.end - iv.start)
            del tl.intervals[i]
            if i < len(tl._start_cache):
                del tl._start_cache[i]
            else:
                tl._start_cache = [v.start for v in tl.intervals]
            return


def _restore_committed(tl: MachineTimeline, lot_id: str, sku: str,
                       cur_start: float, proc: float, res=None,
                       transfer: float = 0.0, own_cs: float = None) -> None:
    """Re-commit a lot freed by a REFUSED lift, at a CHANGEOVER-SAFE start at/
    below its original cur_start (C4/C7).

    A lift first removes the lot, then may refuse. Between the lot's original
    placement and the refusal, a different-SKU neighbour may have been committed
    within a changeover of cur_start, so blindly re-committing at cur_start would
    create a 0-gap diff-SKU abutment (a CHANGEOVER_GAP violation Phase-8 catches).
    latest_feasible_start(cur_start, ...) returns cur_start itself when still
    clear (the common no-op case), else the nearest earlier changeover-feasible
    slot. As an absolute floor (should never trigger) we fall back to cur_start so
    the lot is never dropped from the schedule.

    If the safe restore start differs from cur_start, the lot's recorded start
    (res.placed_starts + its schedule row) is refreshed so the schedule and the
    timeline never desync. The lot can only move EARLIER (<= cur_start), which
    only WIDENS the gap to its own consumer, so its consumer-edge feasibility is
    preserved. Deterministic (pure bisect)."""
    S = tl.latest_feasible_start(cur_start, proc, sku)
    if S is None:
        S = cur_start
    tl.commit(S, proc, lot_id, sku)
    if res is not None and abs(S - cur_start) > 1e-9:
        res.placed_starts[lot_id] = S
        new_end = S + proc
        for row in res.schedule_rows:
            if row["lot_id"] == lot_id:
                row["start"] = S
                row["end"] = new_end
                if own_cs is not None:
                    row["gap_to_consumer_min"] = own_cs - new_end - transfer
                break


def _lift_consumer(c: str, by_id, timelines, placed, placed_machine,
                   consumer_start_of, res, need_cs: float,
                   consumer_lot, producers=None) -> Optional[float]:
    """BUG-03 RECOURSE: move already-placed lot ``c`` to the LATEST feasible start
    that is >= need_cs while still feeding ITS OWN consumer (consumer_start_of[c]).
    Returns the new start, or None if it cannot be lifted (e.g. drum-fed/green
    tyre whose press is immutable, or no later slot exists). Deterministic.

    L13 (sibling re-release): lifting c to a LATER start widens the gap between c
    and the producers ALREADY committed against c's OLD start, which can push one
    of those committed siblings OVER its own aging_max (C2). After tentatively
    re-committing c, we re-derive each committed producer's wall-clock gap to c's
    NEW start; if ANY would exceed its aging_max we REFUSE the lift (restore c at
    its original slot, return None). Bounded (one pass over c's producers) and
    deterministic."""
    if consumer_lot.get(c) is None:
        return None                       # drum-fed: press is fixed, cannot lift
    rc = by_id[c]
    cur_start = res.placed_starts.get(c)
    if cur_start is None or need_cs <= cur_start + C.AGING_EPS_MIN:
        return None                       # nothing to gain
    m = placed_machine.get(c)
    if m is None:
        return None
    proc_c = rc["proc_eff_min"]
    transfer_c = rc["transfer_min"]
    amin_c = rc["aging_min_h"] * 60.0
    buf_c = rc["buffer_h"] * 60.0
    own_cs = consumer_start_of.get(c)
    if own_cs is None:
        return None
    # c's own ALAP ceiling so IT stays feasible against its consumer
    target_end_c = own_cs - transfer_c - amin_c - buf_c
    target_start_c = target_end_c - proc_c
    if target_start_c < need_cs - C.AGING_EPS_MIN:
        return None                       # even c's latest can't reach need_cs
    tl = get_timeline(timelines, m)
    _remove_interval(tl, c)               # free c's current slot
    S = tl.latest_feasible_start(target_start_c, proc_c, rc["sku"],
                                 earliest=need_cs)
    if S is None or S < need_cs - C.AGING_EPS_MIN:
        # could not lift: restore c and bail. CHANGEOVER-SAFE RESTORE (C4/C7):
        # while c was placed, a DIFFERENT-SKU neighbour may have committed within
        # a changeover of c's original cur_start, so re-committing blindly at
        # cur_start would abut it (0-gap diff-SKU = CHANGEOVER_GAP). Re-query the
        # latest changeover-feasible start at/below cur_start; cur_start itself is
        # returned when still clear, so the no-op case is unchanged.
        _restore_committed(tl, c, rc["sku"], cur_start, proc_c,
                           res=res, transfer=transfer_c, own_cs=own_cs)
        return None
    new_start_c = S
    # L13: would lifting c to new_start_c over-age any producer ALREADY committed
    # against c? The producer p (fixed end res.placed_starts[p]+proc_p) now sees a
    # wider gap = new_start_c - p_end (wall-clock, per AGING_SUBTRACTS_TRANSFER).
    # If gap > p.aging_max for any committed p, REFUSE the lift (C2 protection).
    if producers and C.RECOURSE_SIBLING_GUARD:
        for p in producers.get(c, []):
            if p not in placed:
                continue
            pr = by_id.get(p)
            ps = res.placed_starts.get(p)
            if pr is None or ps is None:
                continue
            p_end = ps + pr["proc_eff_min"]
            p_amax = pr["aging_max_h"] * 60.0
            if p_amax <= 0:
                continue
            p_transfer = pr["transfer_min"]
            gap_avail = new_start_c - p_end - p_transfer
            gap_max = (gap_avail if C.AGING_SUBTRACTS_TRANSFER
                       else (new_start_c - p_end))
            if gap_max > p_amax + C.AGING_EPS_MIN:
                # this sibling would over-age -> refuse, restore c's old slot
                # (changeover-safe; see _restore_committed rationale above).
                _restore_committed(tl, c, rc["sku"], cur_start, proc_c,
                                   res=res, transfer=transfer_c, own_cs=own_cs)
                return None
    tl.commit(new_start_c, proc_c, c, rc["sku"])
    res.placed_starts[c] = new_start_c
    consumer_start_of[c] = own_cs
    # update c's committed schedule row (start/end/gap) in place
    new_end = new_start_c + proc_c
    for row in res.schedule_rows:
        if row["lot_id"] == c:
            row["start"] = new_start_c
            row["end"] = new_end
            row["gap_to_consumer_min"] = own_cs - new_end - transfer_c
            break
    # L13: c moved later, so its committed producers now have a WIDER (still
    # in-band, verified above) gap. Refresh each committed producer's recorded
    # consumer_start + its schedule-row gap so Phase-8 re-derivation and the
    # handoff stay consistent with the new consumer start.
    if producers:
        for p in producers.get(c, []):
            if p not in placed:
                # BUG-N4 FIX: p was released against c's OLD (earlier) start when c
                # first committed, and is still QUEUED (seeded, not yet popped).
                # If we leave its cached consumer_start at the old earlier value, p
                # will ALAP-target too early and over-age (wall-clock) against c's
                # real, now-later start -> a spurious INFEASIBLE_AGING/UNREACHED. So
                # refresh it to c's NEW start; when p is popped it reads the
                # corrected `cs` (line 175). No re-push needed: the heap entry's
                # criticality key (lst/slack/...) is static and unaffected by cs.
                if p in consumer_start_of:
                    consumer_start_of[p] = new_start_c
                continue
            consumer_start_of[p] = new_start_c
            pr = by_id.get(p)
            ps = res.placed_starts.get(p)
            if pr is None or ps is None:
                continue
            p_end = ps + pr["proc_eff_min"]
            for row in res.schedule_rows:
                if row["lot_id"] == p:
                    row["gap_to_consumer_min"] = (
                        new_start_c - p_end - pr["transfer_min"])
                    break
    return new_start_c
