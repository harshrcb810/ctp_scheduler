"""Phase 9 - Outputs, KPIs & traceability.

Builds the human-readable schedule (with datetimes), KPI report, handoff
(build->cure aging) report, and the plan-health verdict.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

import pandas as pd

from . import config as C
from .ingest import from_minutes_epoch


# FIX-4: ROOT vs CASCADE infeasibility classification.
# A ROOT failure is a HEAD constraint breach on the lot itself (its compound
# scorched, its green tyre missed cure-by, its SKU was quarantined). A CASCADE
# failure is a producer that was only UNREACHED because the consumer it would
# feed was itself dropped (orphaned upstream). Reporting them flat over-states
# independent demand loss; this label lets the dashboard say "X root failures
# cascading to Y". The HARD counts are unchanged - this is a label only.
INFEASIBLE_ROOT_CODES = frozenset({
    "CUREBY_EXPIRED", "CUREBY_TOO_EARLY", "INFEASIBLE_AGING",
    "QUARANTINED_SKU", "NO_ELIGIBLE_MACHINE", "NO_FEASIBLE_SLOT",
})


def classify_infeasibility(infeasibility: pd.DataFrame) -> pd.DataFrame:
    """FIX-4: add an ``infeasible_class`` column (ROOT | CASCADE).

    ROOT  = a head constraint breach on the lot (CUREBY_*, INFEASIBLE_AGING,
            QUARANTINED_SKU, or a no-machine/no-slot placement failure).
    CASCADE = an UNREACHED producer, orphaned because its consumer was itself
            dropped (it never got the chance to fail on its own merits).
    Deterministic; does NOT change row counts (label only)."""
    if infeasibility is None or infeasibility.empty \
            or "check_type" not in infeasibility.columns:
        return infeasibility
    df = infeasibility.copy()
    df["infeasible_class"] = df["check_type"].astype(str).map(
        lambda ct: "ROOT" if ct in INFEASIBLE_ROOT_CODES else "CASCADE")
    return df


# FIX-1: produced-components artefact schema (folded carcasses, mass-balance only)
PRODUCED_COMPONENTS_COLUMNS = [
    "sku", "item", "item_type", "qty", "consumer_lot_id", "source",
]


def produced_components(rows: List[dict]) -> pd.DataFrame:
    """FIX-1: BOM mass-balance / pegging artefact for components that are PRODUCED
    but folded into a consumer's build (e.g. the op-195 carcass folded into its
    op-200 GT). These carry NO machine minutes and are NOT schedule rows; they are
    recorded here so pegging counts the carcass as produced (~= demand) without an
    external validator reading a 0-duration / blank-machine op-195 row. Deterministic
    (sorted by sku, item, consumer_lot_id)."""
    if not rows:
        return pd.DataFrame(columns=PRODUCED_COMPONENTS_COLUMNS)
    df = pd.DataFrame(rows, columns=PRODUCED_COMPONENTS_COLUMNS)
    return df.sort_values(
        ["sku", "item", "consumer_lot_id"]).reset_index(drop=True)


def humanise_schedule(schedule: pd.DataFrame) -> pd.DataFrame:
    if schedule.empty:
        return schedule
    df = schedule.copy()
    # FIX-2: CHANGEOVER / MOULD_CLEAN are press-occupancy reservations (L6), NOT
    # feedable curing lots - they carry no SKU/tyre and can never be "fed". They
    # remain on the machine timeline (committed in pin_nonproduction_occupancy) and
    # in schedule_raw for the C4/C7 non-overlap re-proof, but they must NOT appear
    # in the exported schedule.csv as curing rows (an external validator would read
    # them as demanded-but-unfed presses). Excluded here from the human export.
    if "sku" in df.columns:
        df = df[~df["sku"].astype(str).str.strip().isin(C.NON_PRODUCTION_SKUS)]
        if df.empty:
            return df
    df["start_dt"] = df["start"].map(lambda m: from_minutes_epoch(m))
    df["end_dt"] = df["end"].map(lambda m: from_minutes_epoch(m))
    df = df.sort_values(["start", "machine", "lot_id"]).reset_index(drop=True)
    return df


@dataclass
class Kpis:
    lots_total: int = 0
    lots_placed: int = 0
    lots_unplaced: int = 0
    otif_pct: float = 0.0               # BUG-01: TRUE OTIF (presses fulfilled)
    lot_placement_pct: float = 0.0      # BUG-01: old placed/total lot ratio
    presses_total: int = 0              # demanded (built) drum blocks (in-scope)
    presses_fulfilled: int = 0          # blocks with a delivered green tyre
    otif_demand_pct: float = 0.0        # DEMAND-QTY OTIF (headline): GT qty on
                                        # fulfilled blocks / GT qty demanded
    gt_demand_total: float = 0.0        # sum of green-tyre qty demanded (drum)
    gt_demand_fulfilled: float = 0.0    # sum of green-tyre qty on fulfilled blocks
    presses_total_drum: int = 0         # R4: FULL drum (in-scope + missing-recipe)
    plant_fulfilment_pct: float = 0.0   # R4: fulfilled / full-drum (honest plant)
    scope: str = ""                     # R4: N_of_M SKUs scheduled / total
    bottleneck_stage: str = ""
    bottleneck_peak_pct: float = 0.0
    bottleneck_planning_peak_pct: float = 0.0
    prebuild_days: float = 0.0          # BUG-06: median build->cure lead (days)
    prebuild_max_days: float = 0.0      # BUG-06: max build->cure lead (days)
    aging_ok_pct: float = 0.0
    aging_expired: int = 0
    estimate_exposure_pct: float = 0.0
    violations: int = 0
    infeasible_root: int = 0        # FIX-4: head constraint breaches (CUREBY/AGING/QUARANTINE...)
    infeasible_cascade: int = 0     # FIX-4: UNREACHED producers orphaned by a dropped consumer
    plan_health: str = ""
    health_driver: str = ""             # ERROR-5: why NOT-CLEAN (self-explaining)
    extra: Dict = field(default_factory=dict)


def _all_block_ids(all_blocks) -> set:
    """ERROR-5: the authoritative FULL drum-demand block-id universe, derived
    from the CuringBlock list threaded from pipeline.py. Empty set when no block
    list is supplied (callers then fall back to the schedule-derived demand)."""
    if not all_blocks:
        return set()
    out = set()
    for cb in all_blocks:
        bid = getattr(cb, "block_id", None)
        if bid is None and isinstance(cb, dict):
            bid = cb.get("block_id")
        if bid:
            out.add(str(bid))
    return out


def _pool_blocks(tags: str) -> set:
    """Block ids carried in a pooled lot's parent_lot_ids (``sku|block|consumer``)."""
    out = set()
    if not tags:
        return out
    for tag in str(tags).split(";"):
        parts = tag.split("|")
        if len(parts) >= 2 and parts[1]:
            out.add(str(parts[1]))
    return out


def _pooled_broken_blocks(infeasibility, lots, placed_lot_ids=None) -> set:
    """R14: a POOLED (cross-SKU) lot's infeasibility row stamps only its ANCHOR
    block, but the pooled batch feeds EVERY consuming (sku, block) carried in its
    parent_lot_ids (``sku|block|consumer`` tags). If the shared batch is dropped,
    those blocks are short - not just the anchor. Expand a dropped pooled lot's
    broken-block set to every block in its parent_lot_ids so OTIF is not optimistic.

    BUT a pooled item is made as MANY campaign lots sharing overlapping membership
    tags; a block listed by a dropped lot may still be FED by a SURVIVING (placed)
    pooled lot of the SAME item. So a block is broken only if NO surviving pooled
    lot of the same item still covers it - we subtract blocks served by survivors
    (per item) to avoid over-attributing OTIF loss. Deterministic."""
    out = set()
    if (infeasibility is None or infeasibility.empty
            or lots is None or lots.empty
            or "lot_id" not in infeasibility.columns
            or "parent_lot_ids" not in lots.columns):
        return out
    placed = placed_lot_ids or set()
    parent_of = dict(zip(lots["lot_id"].astype(str),
                         lots["parent_lot_ids"].astype(str)))
    item_of = dict(zip(lots["lot_id"].astype(str), lots["item"].astype(str)))
    dropped = {str(l) for l in infeasibility["lot_id"].astype(str)
               if str(l).startswith("POOL-")}
    # per item: blocks still served by a SURVIVING (placed) pooled lot
    served_by_item: Dict[str, set] = {}
    for lid, tags in parent_of.items():
        if not lid.startswith("POOL-"):
            continue
        if lid in dropped or (placed and lid not in placed):
            continue                       # not a surviving/placed lot
        it = item_of.get(lid, "")
        served_by_item.setdefault(it, set()).update(_pool_blocks(tags))
    for lid in dropped:
        it = item_of.get(lid, "")
        blocks = _pool_blocks(parent_of.get(lid, ""))
        # only blocks NOT still covered by a surviving pooled lot of this item
        out |= (blocks - served_by_item.get(it, set()))
    return out


def _pooled_broken_reasons(infeasibility, lots, placed_lot_ids=None) -> Dict[str, str]:
    """Per-block REAL binding reason when a dropped cross-SKU POOLED lot poisoned
    the block (the true cause behind the old hardcoded 'calender 901' string).

    Mirror of _pooled_broken_blocks, but returns block -> human reason naming the
    dropped POOL lot id + its item code, e.g.
        'shared compound batch dropped: POOL-B278-... (item B278)'.
    Deterministic: blocks are attributed to the FIRST dropped pooled lot (sorted by
    lot_id) that covers them and has no surviving sibling for that block."""
    reasons: Dict[str, str] = {}
    if (infeasibility is None or infeasibility.empty
            or lots is None or lots.empty
            or "lot_id" not in infeasibility.columns
            or "parent_lot_ids" not in lots.columns):
        return reasons
    placed = placed_lot_ids or set()
    parent_of = dict(zip(lots["lot_id"].astype(str),
                         lots["parent_lot_ids"].astype(str)))
    item_of = dict(zip(lots["lot_id"].astype(str), lots["item"].astype(str)))
    dropped = sorted(str(l) for l in infeasibility["lot_id"].astype(str)
                     if str(l).startswith("POOL-"))
    served_by_item: Dict[str, set] = {}
    for lid, tags in parent_of.items():
        if not lid.startswith("POOL-"):
            continue
        if lid in dropped or (placed and lid not in placed):
            continue
        it = item_of.get(lid, "")
        served_by_item.setdefault(it, set()).update(_pool_blocks(tags))
    for lid in dropped:                       # sorted -> deterministic attribution
        it = item_of.get(lid, "")
        blocks = _pool_blocks(parent_of.get(lid, "")) - served_by_item.get(it, set())
        for blk in blocks:
            reasons.setdefault(
                blk, f"shared compound batch dropped: {lid} (item {it})")
    return reasons


def fulfilment(schedule: pd.DataFrame, infeasibility=None, all_blocks=None,
               lots=None):
    """BUG-01/BUG-03 core: per demanded drum block, was a FULLY-BUILT green tyre
    delivered by its cure-by? A block is FULFILLED iff:
      (1) a PLACED (committed) green-tyre lot exists for the block, AND
      (2) NO upstream lot feeding that block was dropped (unplaced) - i.e. the
          green tyre's whole feeding chain (calender roll, belts, compounds...)
          was actually placed.
    Condition (2) is the honest part: placing a green tyre whose calendered roll
    could not be made in time is NOT a real delivery. Dropped lots stamp their
    ``source_curing_block`` in the infeasibility report, so any block appearing
    there is NOT fully built.

    Returns (fulfilled_block_ids:set, demanded_block_ids:set, unfulfilled_rows:list).
    Demanded blocks = real op-210 curing anchors (status PINNED, is_curing) - NOT
    the RESERVED occupancy-only rows for un-scheduled SKUs. Deterministic."""
    demanded = {}    # block_id -> (sku, machine, start)
    if schedule is not None and not schedule.empty:
        for r in schedule.to_dict("records"):
            if r.get("is_curing") and str(r.get("status")) == "PINNED":
                demanded[r.get("source_curing_block", "")] = (
                    r.get("sku"), r.get("machine"), r.get("start"))
    # blocks with ANY dropped (unplaced) upstream lot -> not fully built
    broken_blocks = set()
    if infeasibility is not None and not infeasibility.empty \
            and "source_curing_block" in infeasibility.columns:
        for b in infeasibility["source_curing_block"].dropna().tolist():
            if b:
                broken_blocks.add(str(b))
    # R14: expand a dropped POOLED lot to its consuming blocks (parent_lot_ids),
    # minus any block still served by a SURVIVING placed pooled lot of that item.
    if C.R14_POOL_BROKEN_EXPANSION:
        placed_lot_ids = set()
        if schedule is not None and not schedule.empty:
            work = schedule[schedule.get("is_curing", False) != True]  # noqa: E712
            placed_lot_ids = set(work["lot_id"].astype(str))
        broken_blocks |= _pooled_broken_blocks(infeasibility, lots, placed_lot_ids)
    gt_placed = set()
    if schedule is not None and not schedule.empty:
        gt = schedule[
            (~schedule.get("is_curing", False)) &
            (schedule["item_type"].astype(str).str.lower().isin(
                ["green tyre", "green tyres"]))]
        for r in gt.to_dict("records"):
            blk = r.get("source_curing_block", "")
            if blk in demanded:
                gt_placed.add(blk)
    fulfilled = {b for b in gt_placed if b not in broken_blocks}
    unfulfilled = []
    for blk in sorted(demanded):
        if blk not in fulfilled:
            sku, machine, start = demanded[blk]
            reason = ("GREEN_TYRE_PLACED_BUT_CHAIN_INCOMPLETE"
                      if blk in gt_placed else "NO_GREEN_TYRE_PLACED")
            unfulfilled.append({
                "source_curing_block": blk, "sku": sku, "press": machine,
                "press_start": from_minutes_epoch(start), "fill_state": reason,
            })
    return fulfilled, set(demanded), unfulfilled


def unfulfilled_presses_report(schedule: pd.DataFrame, infeasibility,
                               all_blocks=None, lots=None) -> pd.DataFrame:
    """BUG-03 honest report: every demanded press/block that ends with NO green
    tyre, plus the binding reason (the dominant dispatch reason code for that
    block's green-tyre lots, if any).

    FIX-3: the quarantined-stub SKUs' RESERVED curing presses are NOT building-
    infeasible curing-feed misses (they have no real BOM to build); they are routed
    to uncovered_demand.csv with reason QUARANTINED_SKU instead, so they are
    counted ONCE there and never double-counted in the unfed-press denominator."""
    cols = ["source_curing_block", "sku", "press", "press_start",
            "fill_state", "binding_reason"]
    _, _, unfulfilled = fulfilment(schedule, infeasibility, all_blocks, lots)

    if not unfulfilled:
        return pd.DataFrame(columns=cols)
    # map block -> dominant dispatch reason code (the binding constraint)
    reason_of_block: Dict[str, str] = {}
    if infeasibility is not None and not infeasibility.empty \
            and "source_curing_block" in infeasibility.columns:
        order = {"CUREBY_EXPIRED": 0, "INFEASIBLE_AGING": 1, "NO_FEASIBLE_SLOT": 2,
                 "CUREBY_TOO_EARLY": 3, "NO_ELIGIBLE_MACHINE": 4, "UNREACHED": 5}
        for r in infeasibility.to_dict("records"):
            blk = str(r.get("source_curing_block", "") or "")
            ct = str(r.get("check_type", ""))
            if not blk:
                continue
            cur = reason_of_block.get(blk)
            if cur is None or order.get(ct, 9) < order.get(cur, 9):
                reason_of_block[blk] = ct
    # REAL binding reason for blocks poisoned by a dropped cross-SKU POOLED batch
    # (replaces the old hardcoded "calender 901" guess, which was FALSE - the
    # calender verdicts CLEAN and TBM builds are balanced). Names the dropped POOL
    # lot id + item code so the dashboard explains the true cause. Computed over
    # the SAME placed-lot set fulfilment() uses, so it agrees with the broken-block
    # attribution. Determinism: _pooled_broken_reasons is sorted/deterministic.
    placed_lot_ids = set()
    if schedule is not None and not schedule.empty:
        _work = schedule[schedule.get("is_curing", False) != True]  # noqa: E712
        placed_lot_ids = set(_work["lot_id"].astype(str))
    pool_reason_of_block = _pooled_broken_reasons(
        infeasibility, lots, placed_lot_ids)
    rows = []
    for u in unfulfilled:
        blk = u["source_curing_block"]
        u = dict(u)
        # priority for the explanation: an explicit per-block dispatch reason code
        # (from a lot stamped with this block) wins; else the shared-compound pool
        # cause (the real driver of the multi-SKU regression); else a neutral
        # upstream-chain fallback (NOT the bogus "calender 901" string).
        u["binding_reason"] = (
            reason_of_block.get(blk)
            or pool_reason_of_block.get(blk)
            or "UPSTREAM_CHAIN_UNPLACED (no producer lot reached this block)")
        rows.append(u)
    df = pd.DataFrame(rows, columns=cols)
    return df.sort_values(["sku", "source_curing_block"]).reset_index(drop=True)


def compute_kpis(schedule: pd.DataFrame, lots: pd.DataFrame,
                 cap_report, n_unplaced: int, violations: int,
                 drum_starts: List[float],
                 infeasibility: "pd.DataFrame" = None,
                 all_blocks=None) -> Kpis:
    k = Kpis()
    k.lots_total = len(lots)
    # FIX-1: folded carcasses are no longer in the schedule (emitted as
    # produced_components), so no is_folded exclusion is needed here.
    work = schedule[~schedule.get("is_curing", False)] if not schedule.empty else schedule
    k.lots_placed = len(work)
    k.lots_unplaced = n_unplaced
    k.violations = violations

    # FIX-4: ROOT vs CASCADE split of the infeasible set. root + cascade == total
    # infeasible (label only; hard counts unchanged). Uses the infeasible_class
    # column when present, else derives it from the check_type code.
    if infeasibility is not None and not infeasibility.empty:
        if "infeasible_class" in infeasibility.columns:
            cls = infeasibility["infeasible_class"].astype(str)
        elif "check_type" in infeasibility.columns:
            cls = infeasibility["check_type"].astype(str).map(
                lambda ct: "ROOT" if ct in INFEASIBLE_ROOT_CODES else "CASCADE")
        else:
            cls = pd.Series(dtype=str)
        k.infeasible_root = int((cls == "ROOT").sum())
        k.infeasible_cascade = int((cls == "CASCADE").sum())

    # BUG-01: TRUE OTIF = demanded drum blocks delivered a green tyre by cure-by
    # / total demanded drum blocks. The old metric (placed lots / total lots) is
    # kept SEPARATELY as lot_placement_pct - it is a planning-completeness ratio,
    # not on-time-in-full delivery.
    # R14: pass the lot frame so a dropped POOLED lot marks ALL its consuming
    # blocks broken (not just the anchor) - keeps OTIF honest, never optimistic.
    fulfilled, demanded, _ = fulfilment(schedule, infeasibility, all_blocks, lots)
    # ERROR-5 FIX (KPI honesty): the presses denominator must be the FULL drum
    # demand (the authoritative CuringBlock universe threaded from pipeline.py),
    # NOT the derived `demanded` set (PINNED curing rows in the schedule), so
    # OTIF/placement reflect TOTAL drum demand - including blocks whose chain was
    # never built and so never produced a PINNED anchor in the schedule.
    full_demand = _all_block_ids(all_blocks)
    presses_total = len(full_demand) if full_demand else len(demanded)
    k.presses_total = presses_total
    k.presses_fulfilled = len(fulfilled)
    if k.presses_total:
        k.otif_pct = 100.0 * k.presses_fulfilled / k.presses_total

    # HEADLINE OTIF (demand-quantity based): of all green tyres the curing
    # schedule needs, the share we can fully build & cure on time. We weight each
    # demanded curing block by its green-tyre qty (the PINNED op-210 drum row) and
    # count a block's qty as fulfilled iff it is in the `fulfilled` set (a fully
    # built green tyre delivered by its cure-by - i.e. NOT in broken_blocks).
    # Deterministic: derived from the same fulfilment() block sets + drum qty.
    gt_demand_total = 0.0
    gt_demand_fulfilled = 0.0
    if schedule is not None and not schedule.empty:
        for r in schedule.to_dict("records"):
            if r.get("is_curing") and str(r.get("status")) == "PINNED":
                blk = r.get("source_curing_block", "")
                qty = float(r.get("qty", 0.0) or 0.0)
                gt_demand_total += qty
                if blk in fulfilled:
                    gt_demand_fulfilled += qty
    k.gt_demand_total = gt_demand_total
    k.gt_demand_fulfilled = gt_demand_fulfilled
    if gt_demand_total > 0:
        k.otif_demand_pct = 100.0 * gt_demand_fulfilled / gt_demand_total
    if k.lots_total:
        k.lot_placement_pct = 100.0 * k.lots_placed / k.lots_total

    if cap_report and cap_report.stages:
        k.bottleneck_stage = cap_report.bottleneck
        top = cap_report.stages[0]
        k.bottleneck_peak_pct = top.peak_util * 100.0
        k.bottleneck_planning_peak_pct = top.planning_peak_util * 100.0

    # BUG-06: prebuild lead = per-green-tyre (earliest-upstream-start -> real
    # cure-start) lead per drum block, reported as MEDIAN and MAX (days). The old
    # metric (latest drum start - earliest upstream start) was the whole horizon
    # span, not a per-block lead time.
    leads = _prebuild_leads(schedule)
    if leads:
        leads_sorted = sorted(leads)
        n = len(leads_sorted)
        median = (leads_sorted[n // 2] if n % 2 else
                  0.5 * (leads_sorted[n // 2 - 1] + leads_sorted[n // 2]))
        k.prebuild_days = median / 1440.0
        k.prebuild_max_days = max(leads_sorted) / 1440.0

    # estimate exposure (share of lots whose proc came from a proc==22 placeholder)
    if not lots.empty:
        est = lots["estimated_proc"].mean() if "estimated_proc" in lots else 0.0
        k.estimate_exposure_pct = 100.0 * float(est)

    # BUG-07: aging health on committed green-tyre handoffs. The enforced cure-by
    # MAX is the WALL-CLOCK gap (cs - build_end), because AGING_SUBTRACTS_TRANSFER
    # is False. The stored gap_to_consumer_min = cs - end - transfer, so compare
    # against (stored_gap + transfer) to match what dispatch/validate enforce.
    if not work.empty:
        gt = work[work["item_type"].astype(str).str.lower().isin(
            ["green tyre", "green tyres"])]
        n_gt = len(gt)
        if n_gt:
            transfer = gt.get("transfer_min", C.TRANSFER_MIN)
            gap_wall_h = (gt["gap_to_consumer_min"] + transfer) / 60.0
            amin = gt["aging_min_h"]
            # MIN side stays availability (transfer-netted); MAX side wall-clock.
            gap_avail_h = gt["gap_to_consumer_min"] / 60.0
            expired = int((gap_wall_h > gt["aging_max_h"] + 1e-6).sum())
            early = int((gap_avail_h < amin - 1e-6).sum())
            ok = n_gt - expired - early
            k.aging_ok_pct = 100.0 * ok / n_gt
            k.aging_expired = expired

    plan_ok = (violations == 0)
    cap_v = cap_report.verdict if cap_report else "CLEAN"
    # BUG-03: plan health must reflect HONEST fulfilment. Unfulfilled presses or
    # any post-condition violation => NOT-CLEAN; a fully-feasible but tight pool
    # => TIGHT; otherwise CLEAN.
    presses_short = k.presses_total - k.presses_fulfilled
    if not plan_ok or presses_short > 0:
        k.plan_health = "NOT-CLEAN"
        # ERROR-5: name the driver so a NOT-CLEAN run with an EMPTY violations.csv
        # is self-explaining (presses short of the full drum demand vs an actual
        # post-condition breach). Post-condition violations dominate the label.
        if not plan_ok and presses_short > 0:
            k.health_driver = "POSTCOND_VIOLATION+PRESSES_SHORT"
        elif not plan_ok:
            k.health_driver = "POSTCOND_VIOLATION"
        else:
            k.health_driver = "PRESSES_SHORT"
    elif cap_v in ("TIGHT", "OVER-CAPACITY"):
        k.plan_health = "TIGHT"
        k.health_driver = "CAPACITY_TIGHT"
    else:
        k.plan_health = "CLEAN"
        k.health_driver = "NONE"
    return k


def _prebuild_leads(schedule: pd.DataFrame) -> List[float]:
    """BUG-06: per drum block, (real cure-start) - (earliest upstream start that
    feeds that block) in minutes. Upstream = any placed (non-curing) lot stamped
    with that ``source_curing_block``."""
    if schedule is None or schedule.empty:
        return []
    cure_start: Dict[str, float] = {}
    earliest_up: Dict[str, float] = {}
    for r in schedule.to_dict("records"):
        blk = r.get("source_curing_block", "")
        if r.get("is_curing"):
            if str(r.get("status")) == "PINNED":
                cure_start[blk] = r["start"]
        else:
            cur = earliest_up.get(blk)
            if cur is None or r["start"] < cur:
                earliest_up[blk] = r["start"]
    leads = []
    for blk, cs in cure_start.items():
        up = earliest_up.get(blk)
        if up is not None:
            leads.append(cs - up)
    return leads


def handoff_report(schedule: pd.DataFrame) -> pd.DataFrame:
    """Per green tyre: build-complete, REAL cure-start (the placed op-210 drum
    slot), aging gap vs the 6h window.

    D4 FIX: cure_start is read from the actual curing (op-210) row pinned on the
    drum - matched 1:1 to the green-tyre build on ``source_curing_block`` - NOT
    derived as ``build_end + stored_gap``. The reported gap is the real
    cure_start - build_end - transfer, so the report describes placed reality."""
    cols = ["lot_id", "sku", "source_curing_block", "build_end", "cure_start",
            "transfer_min", "aging_min_h", "window_h", "gap_h", "pct_consumed",
            "status"]
    if schedule.empty:
        return pd.DataFrame(columns=cols)

    # real op-210 start per drum block (1:1 with the drum)
    cure_start_of_block = {}
    for r in schedule.to_dict("records"):
        if r.get("is_curing"):
            cure_start_of_block[r.get("source_curing_block", "")] = r["start"]

    gt = schedule[
        (~schedule.get("is_curing", False)) &
        (schedule["item_type"].astype(str).str.lower().isin(
            ["green tyre", "green tyres"]))
    ].copy()
    rows = []
    for r in gt.to_dict("records"):
        block = r.get("source_curing_block", "")
        cure_start_min = cure_start_of_block.get(block)
        build_end_min = r["end"]
        transfer = float(r.get("transfer_min", C.TRANSFER_MIN))
        window = r["aging_max_h"]
        amin = r["aging_min_h"]
        if cure_start_min is None:
            # no matching drum anchor: honest EXPIRED/MISSING, never fabricated
            rows.append({
                "lot_id": r["lot_id"], "sku": r["sku"],
                "source_curing_block": block,
                "build_end": from_minutes_epoch(build_end_min),
                "cure_start": pd.NaT, "transfer_min": transfer,
                "aging_min_h": amin, "window_h": window, "gap_h": float("nan"),
                "pct_consumed": float("nan"), "status": "NO_DRUM_ANCHOR",
            })
            continue
        gap_min = cure_start_min - build_end_min - transfer
        gap_h = gap_min / 60.0
        pct = (gap_h / window * 100.0) if window else 0.0
        if gap_min < -1e-6:
            status = "LATE"        # build finishes after the press starts (C1 break)
        elif gap_h < amin - 1e-9:
            status = "EARLY"
        elif gap_h > window + 1e-9:
            status = "EXPIRED"
        elif pct > 90:
            status = "LATE"
        else:
            status = "OK"
        rows.append({
            "lot_id": r["lot_id"], "sku": r["sku"],
            "source_curing_block": block,
            "build_end": from_minutes_epoch(build_end_min),
            "cure_start": from_minutes_epoch(cure_start_min),
            "transfer_min": transfer,
            "aging_min_h": amin, "window_h": window, "gap_h": gap_h,
            "pct_consumed": pct, "status": status,
        })
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df
    # worst first: missing anchor / expired / late, then early, then ok
    order = {"NO_DRUM_ANCHOR": 0, "EXPIRED": 1, "LATE": 2, "EARLY": 3, "OK": 4}
    df["_o"] = df["status"].map(lambda s: order.get(s, 9))
    df = df.sort_values(["_o", "lot_id"]).drop(columns=["_o"]).reset_index(drop=True)
    return df


def kpi_dataframe(k: Kpis) -> pd.DataFrame:
    rows = [
        ("plan_health", k.plan_health),
        ("health_driver", k.health_driver),                 # ERROR-5 (self-explaining)
        ("otif_demand_pct", round(k.otif_demand_pct, 2)),   # HEADLINE: GT-qty OTIF
        ("gt_demand_fulfilled", round(k.gt_demand_fulfilled, 0)),
        ("gt_demand_total", round(k.gt_demand_total, 0)),
        ("otif_pct", round(k.otif_pct, 2)),                 # in-scope OTIF (BUG-01)
        ("scope", k.scope),                                  # R4: N_of_M SKUs
        ("presses_fulfilled", k.presses_fulfilled),          # BUG-03 honest count
        ("presses_total", k.presses_total),                  # in-scope demand
        ("presses_fulfilled_of_total",
         f"{k.presses_fulfilled} of {k.presses_total}"),
        ("presses_total_drum", k.presses_total_drum),        # R4: full drum
        ("plant_fulfilment_pct", round(k.plant_fulfilment_pct, 2)),  # R4 honest
        ("lot_placement_pct", round(k.lot_placement_pct, 2)),  # old ratio (BUG-01)
        ("lots_total", k.lots_total),
        ("lots_placed", k.lots_placed),
        ("lots_unplaced", k.lots_unplaced),
        ("bottleneck_stage", k.bottleneck_stage),
        ("bottleneck_peak_pct", round(k.bottleneck_peak_pct, 1)),       # PLACED
        ("bottleneck_planning_peak_pct",
         round(k.bottleneck_planning_peak_pct, 1)),                      # labelled
        ("prebuild_median_days", round(k.prebuild_days, 2)),           # BUG-06
        ("prebuild_max_days", round(k.prebuild_max_days, 2)),
        ("aging_ok_pct", round(k.aging_ok_pct, 2)),
        ("aging_expired", k.aging_expired),
        ("estimate_exposure_pct", round(k.estimate_exposure_pct, 1)),
        ("violations", k.violations),
        ("infeasible_root", k.infeasible_root),         # FIX-4: head failures
        ("infeasible_cascade", k.infeasible_cascade),   # FIX-4: orphaned producers
    ]
    return pd.DataFrame(rows, columns=["kpi", "value"])
