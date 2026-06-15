"""Phase 5 - Bottleneck identification & rough-cut capacity.

Per-stage rated capacity vs peak-day load, the bottleneck stage, and a
CLEAN / TIGHT / OVER-CAPACITY verdict. Final Mixing (2 mixers) and Building
(11 TBMs) are the structural candidates.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

import pandas as pd

from . import config as C
from .ingest import from_minutes_epoch


# machines per stage (resolved plant pools)
STAGE_MACHINE_COUNT = {
    C.STAGE_MASTER_MIX: 4,
    C.STAGE_FINAL_MIX: 2,
    C.STAGE_BUILDING: 11,
    C.STAGE_CURING: 86,
}


@dataclass
class StageLoad:
    stage: str
    machines: int
    total_min: float
    peak_day_min: float
    peak_util: float            # PLACED peak (real) when a schedule is given
    avg_util: float             # PLACED average over the active horizon
    verdict: str
    planning_peak_util: float = 0.0   # legacy LST day-bucket peak (labelled)


@dataclass
class CapacityReport:
    stages: List[StageLoad]
    bottleneck: str
    verdict: str            # CLEAN / TIGHT / OVER-CAPACITY
    reasons: List[str] = field(default_factory=list)


def _machines_for(stage: str, lots: pd.DataFrame, overrides=None) -> int:
    base = STAGE_MACHINE_COUNT.get(stage)
    if base is None:
        # single-unit stages: count distinct machine ids seen
        sub = lots[lots["stage"] == stage]
        ms = set()
        for cell in sub["machines"]:
            for m in str(cell).split(","):
                m = m.strip()
                if m:
                    ms.add(m)
        base = max(1, len(ms))
    # WHAT-IF: add scenario machines to this stage's effective pool. Curing is
    # never expanded (the drum is fixed); normalise_overrides already strips it.
    if overrides:
        base += int(overrides.get("machine_adds", {}).get(stage, 0))
    return max(1, base)


def _placed_peak_avg(intervals: List[tuple], machines: int) -> tuple:
    """BUG-02 FIX: rolling 1440-min (1-day) window utilisation from the PLACED
    schedule. ``intervals`` = list of (start, end) for one stage's machine pool.

    For every active 1440-min window we sum the busy-minutes overlapping it; the
    pool can supply ``machines * 1440`` busy-minutes per window. The PEAK window
    util is the max over all windows; AVG is total busy-minutes / (pool capacity
    over the active span). This measures REAL contention on actual start/end
    times - not the planning artefact of bucketing each lot's LST into a day,
    which double-counted lots planned to the same date and reported 339.9% for a
    single calender that is physically ~80% loaded.

    Deterministic: candidate window starts are the sorted distinct interval
    starts (peak load can only change at an interval boundary)."""
    if not intervals or machines <= 0:
        return 0.0, 0.0
    W = C.SHIFT_MINUTES
    cap_window = machines * W
    starts = sorted({s for s, _ in intervals})
    horizon_lo = min(s for s, _ in intervals)
    horizon_hi = max(e for _, e in intervals)
    total_busy = sum(max(0.0, e - s) for s, e in intervals)
    peak_busy = 0.0
    for w0 in starts:
        w1 = w0 + W
        busy = 0.0
        for s, e in intervals:
            ov = min(e, w1) - max(s, w0)
            if ov > 0:
                busy += ov
        if busy > peak_busy:
            peak_busy = busy
    peak_util = peak_busy / cap_window if cap_window else 0.0
    span = max(W, horizon_hi - horizon_lo)
    avg_util = total_busy / (machines * span) if (machines * span) else 0.0
    return peak_util, avg_util


def analyse(lots: pd.DataFrame, schedule: pd.DataFrame = None,
            overrides=None) -> CapacityReport:
    if lots.empty:
        return CapacityReport([], "n/a", "CLEAN", ["no lots"])
    lots = lots.copy()
    # day bucket from LST (planned start) - PLANNING peak only (clearly labelled)
    lots["_day"] = lots["lst"].map(
        lambda m: from_minutes_epoch(m).date() if pd.notna(m) else None
    )
    # placed intervals per stage (real start/end), excluding curing/reserved
    placed_by_stage: Dict[str, List[tuple]] = {}
    if schedule is not None and not schedule.empty:
        # FIX-1: folded carcasses are no longer in the schedule (they are emitted
        # as produced_components), so no is_folded filter is needed here.
        work = schedule[~schedule.get("is_curing", False)]
        for r in work.to_dict("records"):
            placed_by_stage.setdefault(r["stage"], []).append(
                (float(r["start"]), float(r["end"])))
    # building: ops 195 & 200 share the pool -> count both but on 11 machines
    stage_loads: List[StageLoad] = []
    for stage in sorted(lots["stage"].unique()):
        sub = lots[lots["stage"] == stage]
        machines = _machines_for(stage, lots, overrides)
        total = float(sub["proc_eff_min"].sum())
        per_day = sub.groupby("_day")["proc_eff_min"].sum()
        peak = float(per_day.max()) if len(per_day) else 0.0
        cap_day = machines * C.SHIFT_MINUTES
        planning_peak = peak / cap_day if cap_day else 0.0
        # PLACED utilisation (the real number) when a committed schedule exists
        placed_intervals = placed_by_stage.get(stage, [])
        if placed_intervals:
            peak_util, avg_util = _placed_peak_avg(placed_intervals, machines)
        else:
            # no placed rows for this stage (e.g. all unplaced): fall back to the
            # planning peak so the stage is still reported, clearly as planning.
            peak_util = planning_peak
            n_days = max(1, per_day.shape[0])
            avg_util = (total / n_days) / cap_day if cap_day else 0.0
        if peak_util > 1.0:
            v = "OVER-CAPACITY"
        elif peak_util > 0.8:
            v = "TIGHT"
        else:
            v = "CLEAN"
        stage_loads.append(StageLoad(stage, machines, total, peak,
                                     peak_util, avg_util, v, planning_peak))
    # bottleneck = max PLACED peak util
    stage_loads.sort(key=lambda s: (-s.peak_util, s.stage))
    bottleneck = stage_loads[0].stage if stage_loads else "n/a"
    worst = stage_loads[0].peak_util if stage_loads else 0.0
    if worst > 1.0:
        verdict = "OVER-CAPACITY"
    elif worst > 0.8:
        verdict = "TIGHT"
    else:
        verdict = "CLEAN"
    reasons = [
        f"{s.stage}: placed peak {s.peak_util*100:.0f}% (planning {s.planning_peak_util*100:.0f}%) "
        f"of {s.machines} machines ({s.verdict})"
        for s in stage_loads[:3]
    ]
    # G2: per-MACHINE hot-spot. The per-stage peak averages a multi-machine pool,
    # so a single saturated machine inside a pool (e.g. one belt cutter / extruder
    # at 100% while the pool sits at ~30%) is invisible. Surface the busiest single
    # machine by its own 1-machine rolling-window peak. DIAGNOSTIC ONLY - it does
    # not change the bottleneck stage or the dispatch priority. Deterministic
    # (candidates sorted by -peak then machine id).
    if schedule is not None and not schedule.empty:
        per_machine: Dict[str, List[tuple]] = {}
        work = schedule[~schedule.get("is_curing", False)]
        for r in work.to_dict("records"):
            per_machine.setdefault(str(r["machine"]), []).append(
                (float(r["start"]), float(r["end"])))
        hot = []
        for mid, ivs in per_machine.items():
            pk, _ = _placed_peak_avg(ivs, 1)      # single-machine capacity
            hot.append((pk, mid))
        hot.sort(key=lambda t: (-t[0], t[1]))
        if hot and hot[0][0] > 0.8:
            pk, mid = hot[0]
            reasons.append(
                f"hottest single machine {mid}: placed peak {pk*100:.0f}% "
                f"(masked by its pooled-stage average)")
    return CapacityReport(stage_loads, bottleneck, verdict, reasons)
