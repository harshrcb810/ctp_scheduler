"""Phase 8 - Validation (post-conditions), RE-DERIVED from placed reality.

D3 FIX: this module independently re-proves the hard constraints by reading the
ACTUAL placed ``start``/``end`` on the committed schedule. It NEVER trusts the
``gap_to_consumer_min`` column the dispatcher wrote about itself. The consumer of
each lot is resolved on the committed rows themselves:

  - a green-tyre / FG build (consumer_item == "") is consumed by its op-210 drum
    block (matched 1:1 on ``source_curing_block``); cure_start = that curing
    row's real ``start``.
  - every other lot is consumed by the lot in the SAME block whose
    ``item == consumer_item``; consumer_start = that lot's real ``start``.

Checks (all from placed times):
  C4 non-overlap per machine
  C3 eligibility (machine in lot's eligible pool)
  C1 build->cure precedence: build_end + transfer <= cure_start
  C2 aging both-sides on each producer->consumer edge: amin <= gap <= amax,
     where gap = consumer_start - producer_end (real placed times).
  Green-tyre cure-by (6h/360 min) band re-checked against the real drum start.

On a correct-by-construction run this returns ZERO violations. When the placed
reality breaks a constraint, the breach is surfaced here (and lands in
violations.csv) - no silent pass.
"""
from __future__ import annotations

import hashlib
import inspect
import math
from typing import Dict, List, Optional

import pandas as pd

from . import config as C
from .ingest import to_minutes_epoch
from .sizing import _mpq_for, _mpq_source, mpq_in_lot_uom


def validate(schedule: pd.DataFrame, lots: pd.DataFrame) -> List[dict]:
    violations: List[dict] = []
    if schedule.empty:
        return violations

    recs = schedule.to_dict("records")

    # FIX-1: folded carcasses are no longer emitted into the schedule (they are
    # emitted as produced_components), so every row here is a real machine lot or
    # a curing anchor - no is_folded exclusion is needed any more.
    sched_phys = schedule

    # C4 non-overlap per machine (from placed start/end)
    for machine, g in sched_phys.groupby("machine"):
        s = g.sort_values(["start", "lot_id"]).reset_index(drop=True)
        for i in range(1, len(s)):
            if s.loc[i, "start"] < s.loc[i - 1, "end"] - 1e-6:
                violations.append(_v(s.loc[i, "lot_id"], machine, "OVERLAP",
                                     f"overlaps {s.loc[i-1,'lot_id']} "
                                     f"(start {s.loc[i,'start']:.1f} < end "
                                     f"{s.loc[i-1,'end']:.1f})", "ERROR"))

    # C4 (changeover GAP) - BUG-N5 FIX: the dispatcher RESERVES CHANGEOVER_MIN
    # between two consecutive DIFFERENT-SKU lots on an upstream machine
    # (timeline.latest_feasible_start). The old post-condition re-proved only raw
    # non-overlap, so a changeover UNDER-reservation would land in the schedule
    # with violations.csv still empty (C4 was only half-proven). Re-derive the gap
    # here. Curing presses carry their changeover as separate drum MOULD_CLEAN/
    # CHANGEOVER occupancy blocks (timeline changeover 0), so this check is scoped
    # to dispatched (non-curing) rows only - exactly where the 15-min reservation
    # applies.
    co_min = C.CHANGEOVER_MIN
    if co_min > 0:
        up = sched_phys[sched_phys.get("is_curing") != True]  # noqa: E712
        for machine, g in up.groupby("machine"):
            s = g.sort_values(["start", "lot_id"]).reset_index(drop=True)
            for i in range(1, len(s)):
                if str(s.loc[i, "sku"]) == str(s.loc[i - 1, "sku"]):
                    continue
                gap = s.loc[i, "start"] - s.loc[i - 1, "end"]
                if gap < co_min - 1e-6:
                    violations.append(_v(
                        s.loc[i, "lot_id"], machine, "CHANGEOVER_GAP",
                        f"diff-SKU gap {gap:.1f} < changeover {co_min:.0f} "
                        f"after {s.loc[i-1,'lot_id']} "
                        f"({s.loc[i-1,'sku']} -> {s.loc[i,'sku']})", "ERROR"))

    # C3 eligibility: machine in lot's eligible pool
    lot_map = {r["lot_id"]: r for r in lots.to_dict("records")} if not lots.empty else {}
    for r in recs:
        if r.get("is_curing"):
            continue
        lot = lot_map.get(r["lot_id"])
        if lot is None:
            continue
        elig = [m.strip() for m in str(lot["machines"]).split(",") if m.strip()]
        if elig and r["machine"] not in elig:
            violations.append(_v(r["lot_id"], r["machine"], "ELIGIBILITY",
                                 f"machine not in {elig}", "ERROR"))

    # ---- Build the REAL consumer-start map from placed rows -----------------
    # curing anchors: (block) -> real op-210 start
    cure_start_of_block: Dict[str, float] = {}
    # EXACT placed start per committed lot_id (the edge dispatch actually used)
    start_of_lot: Dict[str, float] = {}
    # producer lots: (block, item) -> real start  (legacy same-block fallback)
    start_of_block_item: Dict[tuple, float] = {}
    # campaign-merge fallback: (sku, item) -> [(start, start)] for any row whose
    # explicit consumer edge is missing (mirrors dispatch._merged_consumer).
    starts_of_sku_item: Dict[tuple, List[tuple]] = {}
    for r in recs:
        block = r.get("source_curing_block", "")
        if r.get("is_curing"):
            cure_start_of_block[block] = r["start"]
        else:
            start_of_lot[r["lot_id"]] = r["start"]
            start_of_block_item[(block, r["item"])] = r["start"]
            starts_of_sku_item.setdefault((r.get("sku"), r["item"]), []).append(
                (r["start"], r["start"]))
    for k in starts_of_sku_item:
        starts_of_sku_item[k].sort()

    def _consumer_start(sku, block, cons_item, prod_end, consumer_lot_id):
        """Real placed start of the consumer. PRIMARY: the EXACT consumer lot
        dispatch committed this edge against (``consumer_lot_id``) - re-read from
        its actual placed start, so the re-derivation is both truthful (real
        time) AND on the same edge the dispatcher used (no phantom mismatch).
        FALLBACK (legacy rows without a stamped edge): same block, then the
        merged consumer of the same (sku,item) that can still pull this producer."""
        if consumer_lot_id:
            cs = start_of_lot.get(consumer_lot_id)
            if cs is not None:
                return cs
        cs = start_of_block_item.get((block, cons_item))
        if cs is not None:
            return cs
        cands = starts_of_sku_item.get((sku, cons_item))
        if not cands:
            return None
        # BUG-05 FIX: when no explicit consumer_lot_id was stamped, pick the
        # EARLIEST consumer start that can still pull this producer (start>=end).
        # The old fallback returned cands[-1][0] (the LATEST consumer start),
        # which silently satisfied precedence/aging against a far-future consumer
        # and MASKED a real PRECEDENCE break. cands is sorted ascending, so the
        # first qualifying start is the earliest valid consumer.
        for s0, _ in cands:
            if s0 >= prod_end - 1e-6:
                return s0
        # No consumer can legitimately pull this producer (all start before the
        # producer finishes): return the EARLIEST start so the genuine
        # precedence/aging breach is surfaced, never the latest (which hides it).
        return cands[0][0]

    # ---- C1 precedence + C2 aging + cure-by, all from placed times ----------
    for r in recs:
        if r.get("is_curing"):
            continue
        block = r.get("source_curing_block", "")
        cons_item = str(r.get("consumer_item", "") or "")
        _tv = r.get("transfer_min", None)
        try:
            transfer = float(_tv)
            if math.isnan(transfer):
                transfer = C.TRANSFER_MIN
        except (TypeError, ValueError):
            transfer = C.TRANSFER_MIN
        prod_end = r["end"]
        amin = r["aging_min_h"] * 60.0
        amax = r["aging_max_h"] * 60.0

        if cons_item == "":
            # consumed by the drum: cure_start is the REAL pinned op-210 start
            cs = cure_start_of_block.get(block)
            if cs is None:
                violations.append(_v(r["lot_id"], r["machine"], "NO_DRUM_ANCHOR",
                    f"no curing row for block {block}", "ERROR"))
                continue
            # C1: build must finish + transfer before the press starts
            if prod_end + transfer > cs + C.AGING_EPS_MIN:
                violations.append(_v(r["lot_id"], r["machine"], "PRECEDENCE",
                    f"build_end+transfer {prod_end+transfer:.1f} > cure_start "
                    f"{cs:.1f}", "ERROR"))
            gap = cs - prod_end - transfer
            # green-tyre cure-by MAX is WALL-CLOCK (scorches in transit too);
            # see config.AGING_SUBTRACTS_TRANSFER. C1/min side keeps transfer.
            gap_aging = gap if C.AGING_SUBTRACTS_TRANSFER else (cs - prod_end)
            it = str(r.get("item_type", "")).strip().lower()
            if it in ("green tyre", "green tyres"):
                cby = C.GREEN_TYRE_CUREBY_MAX_H * 60.0
                if gap_aging > cby + C.AGING_EPS_MIN:
                    # L16: interpolate the REAL cure-by max (cby = 480 at the
                    # current 8h GT cure-by), not the stale hardcoded "360".
                    violations.append(_v(r["lot_id"], r["machine"], "CUREBY",
                        f"wall-clock gap {gap_aging:.1f} > {cby:.0f} cure-by",
                        "ERROR"))
        else:
            cons_lot_id = str(r.get("consumer_lot_id", "") or "")
            cs = _consumer_start(r.get("sku"), block, cons_item, prod_end,
                                 cons_lot_id)
            if cs is None:
                violations.append(_v(r["lot_id"], r["machine"], "NO_CONSUMER",
                    f"consumer {cons_item} not placed for sku {r.get('sku')}",
                    "ERROR"))
                continue
            # C1: producer must finish + transfer before its consumer starts
            if prod_end + transfer > cs + C.AGING_EPS_MIN:
                violations.append(_v(r["lot_id"], r["machine"], "PRECEDENCE",
                    f"end+transfer {prod_end+transfer:.1f} > consumer_start "
                    f"{cs:.1f}", "ERROR"))
            gap = cs - prod_end - transfer
            gap_aging = gap if C.AGING_SUBTRACTS_TRANSFER else (cs - prod_end)

        # C2 aging both-sides, computed from PLACED times (not stored gap).
        # MIN side = availability (transfer-netted); MAX side = wall-clock
        # material ageing (transfer NOT removed) per the explicit convention.
        eps = C.AGING_EPS_MIN
        if gap < amin - eps:
            violations.append(_v(r["lot_id"], r["machine"], "AGING_MIN",
                                 f"placed gap {gap:.1f} < {amin:.1f}", "ERROR"))
        if amax > 0 and gap_aging > amax + eps:
            violations.append(_v(r["lot_id"], r["machine"], "AGING_MAX",
                f"placed wall-clock gap {gap_aging:.1f} > {amax:.1f}", "ERROR"))

    # dedupe by (lot_id, check_type)
    seen = set()
    deduped = []
    for v in violations:
        k = (v["lot_id"], v["check_type"])
        if k in seen:
            continue
        seen.add(k)
        deduped.append(v)
    return deduped


def _v(lot_id, machine, check_type, detail, severity) -> dict:
    return {"lot_id": lot_id, "machine": machine, "check_type": check_type,
            "detail": detail, "severity": severity}


# ===========================================================================
# INDEPENDENT re-proof of C5-C8 (D3b / TASK A)
# ---------------------------------------------------------------------------
# C1-C4 + green-tyre cure-by are re-proven above from placed times. The four
# below were previously asserted "by-construction" and NOT independently scored.
# Each function here RE-DERIVES the constraint from the produced artefacts (the
# placed schedule + masters + the real drum), NEVER trusting an engine column
# that describes the engine's own output. Each returns a structured result:
#   {"id","name","status":"PASS"|"FAIL","violations":int,"basis":str,
#    "rows":[<offending detail dicts>]}
# so app.py can render them exactly like C1-C4 (validate.py / PASS|FAIL / count).
# ===========================================================================

def _result(cid: str, name: str, n: int, basis: str,
            rows: Optional[List[dict]] = None) -> dict:
    return {"id": cid, "name": name,
            "status": "PASS" if n == 0 else "FAIL",
            "violations": int(n), "basis": basis,
            "rows": rows or []}


def check_c5_mpq(schedule: pd.DataFrame, lots: pd.DataFrame,
                 sku_mpq: Dict[str, Dict[str, tuple]],
                 sku_mpq_uom: Optional[Dict[str, Dict[str, str]]] = None) -> dict:
    """C5 MPQ - every PRODUCED lot's qty within [MPQ_min, MPQ_max] for its
    item-type, bounds RE-RESOLVED from masters (the SKU's MPQ sheet, then the
    estimated/default fallback - identical resolution to sizing._mpq_for) rather
    than trusting any bound the sizer stamped on the lot. Counts below-min and
    above-max. Curing rows / folded carcass are not dispatchable lots and are
    excluded (carcass is folded into its GT lot - C7 proves it is charged once).
    """
    rows: List[dict] = []
    if lots is None or lots.empty:
        return _result("C5", "MPQ sizing", 0,
                       "no dispatchable lots produced")
    pool_types = {t.strip().lower() for t in C.CALENDER_POOL_ITEM_TYPES}
    sku_mpq_uom = sku_mpq_uom or {}
    n_pool = 0
    for r in lots.to_dict("records"):
        it = r.get("item_type", "")
        itl = str(it).strip().lower()
        sku = str(r.get("sku"))
        mpq = sku_mpq.get(sku, {})
        lot_uom = r.get("uom", "")
        # FIX B: re-resolve the MPQ bounds IN THE LOT'S UOM (canonical compare),
        # exactly as the sizer floors them, so C5 neither misses a real below-min
        # breach nor reports a spurious above-max from a unit mismatch.
        # MASTERS: re-resolve from the plant MPQ master (per line, item-type) when
        # ON - item_code + the lot's mixing machine pool feed compound/roll min.
        item_code = str(r.get("item", ""))
        line = str(r.get("line_class", "PCR")) or "PCR"
        mach = [m.strip() for m in str(r.get("machines", "")).split(",")
                if m.strip()]
        mn, mx = mpq_in_lot_uom(it, lot_uom, mpq, sku_mpq_uom.get(sku, {}),
                                item_code=item_code, line=line,
                                machine_names=mach)
        qty = float(r.get("qty", 0.0))
        eps = 1e-6
        # DOCUMENTED EXCEPTION (config.CALENDER_POOL_ITEM_TYPES / FIX-1): a pooled
        # wide-sheet calender MOTHER ROLL is ONE physical roll feeding MANY tyres,
        # so the per-tyre MPQ_max (e.g. cap strip 214 MTR) is a NARROW-component
        # cap, NOT the roll-size cap. The binding upper bound for these is the
        # pooled cap CALENDER_POOL_MAX_QTY (0 = span-bounded, unbounded qty). The
        # MIN floor still binds. This is a real engineering exception, not a hole:
        # it is logged in the basis so the scorecard stays honest.
        # FIX-5: an intentional pooled mother-roll lot is stamped pooling_exempt
        # by the sizer; honour that flag (authoritative) OR the item-type match
        # (legacy). Bead Apex (NOS piece) is NOT pooling_exempt, so a real
        # bead-apex MPQ breach is NEVER masked by this exception.
        if itl in pool_types or bool(r.get("pooling_exempt", False)):
            n_pool += 1
            mx = C.CALENDER_POOL_MAX_QTY  # 0 -> no qty cap (span-bounded run)
        if mn and mn > 0 and qty < mn - eps:
            rows.append({"lot_id": r["lot_id"], "machine": "",
                         "check_type": "MPQ_BELOW_MIN",
                         "detail": f"qty {qty:g} < MPQ_min {mn:g} ({it})",
                         "severity": "ERROR"})
        if mx and mx > 0 and qty > mx + eps:
            rows.append({"lot_id": r["lot_id"], "machine": "",
                         "check_type": "MPQ_ABOVE_MAX",
                         "detail": f"qty {qty:g} > MPQ_max {mx:g} ({it})",
                         "severity": "ERROR"})
    n_lots = len(lots)
    n_master = sum(1 for r in lots.to_dict("records")
                   if _mpq_source(
                       r.get("item_type", ""), sku_mpq.get(str(r.get("sku")), {}),
                       item_code=str(r.get("item", "")),
                       line=str(r.get("line_class", "PCR")) or "PCR",
                       machine_names=[m.strip() for m in
                                      str(r.get("machines", "")).split(",")
                                      if m.strip()]) == "master")
    n_est = sum(1 for r in lots.to_dict("records")
                if _mpq_source(r.get("item_type", ""),
                               sku_mpq.get(str(r.get("sku")), {})) == "estimated")
    src = ("plant MPQ master (per line/item-type, MAX unbounded)"
           if C.USE_MPQ_TRANSFER_MASTERS else "per-SKU MPQ sheets")
    basis = (f"{n_lots} produced lots checked against {src} "
             f"(re-resolved); {n_master} resolved from the MPQ master; "
             f"{n_est} used bounded ESTIMATED MPQ_TYPE_DEFAULTS (legacy path); "
             f"{n_pool} pooled / unbounded-max lots bound by the span-cap not a "
             f"per-tyre MPQ_max; MIN floor enforced on all")
    return _result("C5", "MPQ sizing", len(rows), basis, rows)


def check_c6_drum(schedule: pd.DataFrame,
                  drum: Optional[pd.DataFrame]) -> dict:
    """C6 drum-pinned curing - every committed op-210 curing row must match a
    REAL drum row (sku + machine + start + end) 1:1. Re-derived against the drum
    CSV directly (Curing_Sch_PCR.csv), not the engine's pinned copy. Counts:
      - synthetic / orphan: a curing row with no matching (sku,machine,start,end)
        drum row (a fabricated press slot);
      - over-pinned: two+ curing rows claiming the SAME drum slot (drum is 1:1).
    RESERVED occupancy rows (un-scheduled SKUs) are real drum rows too and are
    matched the same way.
    """
    if schedule is None or schedule.empty or "is_curing" not in schedule.columns:
        return _result("C6", "drum-pinned curing", 0, "no curing rows committed")
    cure = schedule[schedule["is_curing"] == True]  # noqa: E712
    if cure.empty:
        return _result("C6", "drum-pinned curing", 0, "no curing rows committed")
    if drum is None or drum.empty:
        # no drum to match against -> cannot independently prove; surface as FAIL
        rows = [{"lot_id": r["lot_id"], "machine": r["machine"],
                 "check_type": "DRUM_UNVERIFIABLE",
                 "detail": "no drum frame supplied for 1:1 match",
                 "severity": "ERROR"} for r in cure.to_dict("records")]
        return _result("C6", "drum-pinned curing", len(rows),
                       "drum frame unavailable", rows)

    # multiset of real drum slots keyed (sku, machine, start_min, end_min)
    drum_slots: Dict[tuple, int] = {}
    for d in drum.to_dict("records"):
        s = to_minutes_epoch(d.get("StartTime"))
        e = to_minutes_epoch(d.get("EndTime"))
        if pd.isna(s) or pd.isna(e):
            continue
        key = (str(d.get("SKUCode")).strip(), str(d.get("Machine")).strip(),
               round(float(s), 3), round(float(e), 3))
        drum_slots[key] = drum_slots.get(key, 0) + 1

    rows: List[dict] = []
    used: Dict[tuple, int] = {}
    for r in cure.to_dict("records"):
        key = (str(r.get("sku")).strip(), str(r.get("machine")).strip(),
               round(float(r.get("start")), 3), round(float(r.get("end")), 3))
        avail = drum_slots.get(key, 0)
        taken = used.get(key, 0)
        if avail == 0:
            rows.append({"lot_id": r["lot_id"], "machine": r["machine"],
                         "check_type": "DRUM_ORPHAN",
                         "detail": f"curing slot {key[1]}@{key[2]:.0f}-{key[3]:.0f} "
                                   f"sku {key[0]} has no matching drum row",
                         "severity": "ERROR"})
        elif taken >= avail:
            rows.append({"lot_id": r["lot_id"], "machine": r["machine"],
                         "check_type": "DRUM_OVERPINNED",
                         "detail": f"drum slot {key[1]}@{key[2]:.0f} pinned "
                                   f"{taken+1}x (drum has {avail})",
                         "severity": "ERROR"})
        used[key] = taken + 1
    basis = (f"{len(cure)} committed op-210 curing rows matched 1:1 against "
             f"{len(drum_slots)} real drum slots (Curing_Sch_PCR.csv)")
    return _result("C6", "drum-pinned curing", len(rows), basis, rows)


def check_c7_machine(schedule: pd.DataFrame, lots: pd.DataFrame) -> dict:
    """C7 single shared machine model -
      (a) NO overlapping committed intervals on ANY machine (re-derived from
          placed start/end across the WHOLE schedule - curing + builds + every
          stage share one timeline per machine);
      (b) every build op resolves into the single 11-TBM PCR pool, and the
          carcass (op-195) is charged ONCE - it must NOT appear as a separately
          dispatched lot (it is folded into its op-200 GT lot). Count residual
          op-195 dispatched lots (double-charges).
    """
    rows: List[dict] = []
    if schedule is None or schedule.empty:
        return _result("C7", "single shared machine model", 0,
                       "empty schedule")

    # FIX-1: folded carcasses are no longer in the schedule (emitted as
    # produced_components), so every row here is a real machine lot. Any remaining
    # op-195 row therefore IS a genuine double-charge (the carcass should fold into
    # its op-200 GT lot on the shared TBM pool) and is correctly flagged below.
    sched_phys = schedule

    # (a) non-overlap on EVERY machine (all stages on one shared timeline)
    n_iv = 0
    for machine, g in sched_phys.groupby("machine"):
        s = g.sort_values(["start", "lot_id"]).reset_index(drop=True)
        n_iv += len(s)
        for i in range(1, len(s)):
            if s.loc[i, "start"] < s.loc[i - 1, "end"] - 1e-6:
                rows.append({"lot_id": s.loc[i, "lot_id"], "machine": machine,
                             "check_type": "MACHINE_OVERLAP",
                             "detail": f"overlaps {s.loc[i-1,'lot_id']} on "
                                       f"{machine}", "severity": "ERROR"})

    # (b) carcass charged once: no op-195 should be a dispatched (non-curing) row
    # that CONSUMES MACHINE TIME. Folded carcasses are PRODUCED-component records
    # (outputs/produced_components.csv), not schedule rows, so the carcass is
    # counted for mass-balance without re-charging the TBM pool. Any op-195 row
    # remaining in the schedule here is therefore a genuine double-charge.
    if "op_seq" in schedule.columns:
        carc = sched_phys[(sched_phys.get("is_curing") != True) &  # noqa: E712
                          (sched_phys["op_seq"] == C.OP_CARCASS_BUILD)]
        for r in carc.to_dict("records"):
            rows.append({"lot_id": r["lot_id"], "machine": r.get("machine", ""),
                         "check_type": "CARCASS_DOUBLE_CHARGE",
                         "detail": "op-195 dispatched separately (should fold "
                                   "into op-200 GT lot on the shared TBM pool)",
                         "severity": "ERROR"})

    basis = (f"{n_iv} committed intervals checked for per-machine non-overlap; "
             f"op-195 carcass folded into op-200 (charged once on the 11-TBM pool)")
    return _result("C7", "single shared machine model", len(rows), basis, rows)


def check_c8_determinism(lots: pd.DataFrame,
                         schedule: pd.DataFrame) -> dict:
    """C8 determinism - STRUCTURAL proof, not a runtime diff:
      1. the engine modules contain no RNG / wall-clock entropy source;
      2. the dispatch heap key is fully specified (every sort ends in a unique
         lot_id / machine_id tie-break - see dispatch._pq_key);
      3. a stable content hash of the produced lot frame is reported as the
         determinism BASIS (identical inputs -> identical hash -> identical
         schedule.csv). PASS unless an entropy source is detected.
    """
    rows: List[dict] = []

    # 1. scan engine source for entropy primitives
    from . import dispatch as _dispatch  # local import (avoid cycle at top)
    from . import sizing as _sizing, pin as _pin
    banned = ("random.", "np.random", "numpy.random", "time.time(",
              "datetime.now", "datetime.today", "perf_counter", "uuid.")
    src_all = ""
    for mod in (_dispatch, _sizing, _pin):
        try:
            src_all += inspect.getsource(mod)
        except (OSError, TypeError):
            continue
    for tok in banned:
        if tok in src_all:
            rows.append({"lot_id": "", "machine": "",
                         "check_type": "NONDETERMINISM",
                         "detail": f"entropy source '{tok}' in engine source",
                         "severity": "ERROR"})

    # 2. confirm the dispatch heap key ends in a UNIQUE lot_id tie-break (the
    # spec PQ order: LST, slack, is_bottleneck, drum_deadline, sku, lot_id) and
    # heappush carries lid as the final secondary tie-break.
    try:
        dsrc = inspect.getsource(_dispatch)
    except (OSError, TypeError):
        dsrc = ""
    if "criticality_key" not in dsrc or "r[\"sku\"], lid," not in dsrc \
            or "heapq.heappush(heap, (criticality_key(lid), lid))" not in dsrc:
        rows.append({"lot_id": "", "machine": "",
                     "check_type": "NONDETERMINISM",
                     "detail": "dispatch heap key not the fully tie-broken "
                               "(criticality_key, lot_id) tuple",
                     "severity": "ERROR"})

    # 3. stable content hash of the lot frame (the determinism basis)
    h = "0" * 12
    if lots is not None and not lots.empty:
        key_cols = [c for c in ("lot_id", "sku", "item", "qty", "machines",
                                "proc_eff_min", "curing_deadline") if c in lots.columns]
        canon = lots[key_cols].sort_values("lot_id").to_csv(index=False)
        h = hashlib.sha256(canon.encode("utf-8")).hexdigest()[:12]
    basis = (f"no RNG/wall-clock in engine; dispatch heap key "
             f"(criticality_key, lot_id) fully tie-broken; lot-frame content "
             f"hash {h}")
    return _result("C8", "determinism (no RNG)", len(rows), basis, rows)


def independent_checks(schedule: pd.DataFrame, lots: pd.DataFrame,
                       sku_mpq: Dict[str, Dict[str, tuple]],
                       drum: Optional[pd.DataFrame] = None,
                       sku_mpq_uom: Optional[Dict[str, Dict[str, str]]] = None
                       ) -> List[dict]:
    """Run the four independent C5-C8 re-derivations and return their results
    in fixed order (deterministic). Each entry is consumed by app.py's scorecard.
    """
    return [
        check_c5_mpq(schedule, lots, sku_mpq, sku_mpq_uom),
        check_c6_drum(schedule, drum),
        check_c7_machine(schedule, lots),
        check_c8_determinism(lots, schedule),
    ]
