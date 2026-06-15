"""End-to-end pipeline orchestrator (Phases 0-9).

run_pipeline(drum_df, skus) -> PipelineResult with every intermediate artefact,
so both run.py (CLI) and app.py (Streamlit) share one code path. Deterministic.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

from . import config as C
from . import capacity, demand, outputs, validate
from .dag import SkuDag, build_dag
from .demand import CuringBlock, curing_blocks_for_sku, explode_demand
from .dispatch import dispatch
from .ingest import (SkuData, available_recipe_skus, load_sku, to_minutes_epoch,
                     validate_drum, DrumSummary)
from .pin import (build_shared_timelines, pin_curing,
                  pin_nonproduction_occupancy, pin_press_occupancy)
from .sizing import make_lots, merge_cross_sku_campaigns
from .waste import WASTE_COLUMNS, pooled_waste_matrix, waste_matrix
from .windows import compute_windows


@dataclass
class PipelineResult:
    drum_summary: DrumSummary
    schedule: pd.DataFrame          # humanised
    schedule_raw: pd.DataFrame
    lots: pd.DataFrame
    demand: pd.DataFrame
    violations: pd.DataFrame          # Phase-8 post-condition breaches (clean => empty)
    independent: List[dict]           # C5-C8 independently re-proven (PASS/FAIL + counts)
    infeasibility: pd.DataFrame       # Phase-7 dispatch reason codes (unplaced lots)
    kpis: outputs.Kpis
    kpi_df: pd.DataFrame
    handoff: pd.DataFrame
    unfulfilled: pd.DataFrame          # BUG-03: per-press fulfilment shortfall
    waste: pd.DataFrame                # per-component WASTE/YIELD matrix (analytics)
    capacity: capacity.CapacityReport
    skus_used: List[str]
    n_unplaced: int
    uncovered_demand: pd.DataFrame = field(default_factory=pd.DataFrame)  # R4
    produced_components: pd.DataFrame = field(default_factory=pd.DataFrame)  # FIX-1
    data_audit: pd.DataFrame = field(default_factory=pd.DataFrame)        # R4 audit lines
    notes: List[str] = field(default_factory=list)
    overrides: Optional[dict] = None       # normalised what-if scenario (or None)


UNCOVERED_COLUMNS = ["sku", "curing_blocks", "gt_qty", "reason"]


def _uncovered_demand(drum_df: pd.DataFrame, corrected_dir: str) -> pd.DataFrame:
    """R4: real drum production SKUs that have NO recipe and are dropped silently.

    One row per missing-recipe drum SKU: sku, curing_blocks (count of positive-qty
    production drum rows), gt_qty (summed green-tyre/press demand), reason=
    MISSING_RECIPE. Deterministic (sorted by sku). These presses are genuinely
    demanded by the plant but cannot be scheduled here (no BOM/routing), so they
    are surfaced honestly rather than vanishing from every report."""
    prod = drum_df[~drum_df["SKUCode"].isin(C.NON_PRODUCTION_SKUS)]
    drum_skus = set(prod["SKUCode"].astype(str).str.strip().unique())
    recipes = set(available_recipe_skus(corrected_dir))
    missing = sorted(drum_skus - recipes)
    rows: List[dict] = []
    for sku in missing:
        sub = prod[prod["SKUCode"].astype(str).str.strip() == sku]
        sub = sub[sub["Qty"] > 0]
        rows.append({
            "sku": sku,
            "curing_blocks": int(len(sub)),
            "gt_qty": float(sub["Qty"].sum()),
            "reason": "MISSING_RECIPE",
        })
    return (pd.DataFrame(rows, columns=UNCOVERED_COLUMNS) if rows
            else pd.DataFrame(columns=UNCOVERED_COLUMNS))


def _quarantine_reason(sku: str, data, dag) -> Optional[str]:
    """D5: explicit quarantine gate for degenerate-stub SKUs.

    A schedulable PCR recipe must carry a real BOM tree (FG -> components) and a
    build chain feeding op-210. SKU-D (1325221318095HURL0) is a certified stub:
    BOM = 1 row, so its FG has no real component tree to explode/build. Such a
    stub MUST be QUARANTINED (reason-coded), never built - otherwise it mints
    fake demand against real press slots. The gate is structural (BOM degeneracy
    + no build op), so it also catches any future stub, not just this id."""
    n_bom = len(data.bom)
    has_build_op = any(op.op_seq in C.BUILD_OPS for op in dag.ops.values())
    if n_bom <= 1:
        return f"DEGENERATE_STUB (BOM rows={n_bom})"
    if not has_build_op:
        return "NO_BUILD_OP (no op-195/200 in routing)"
    return None


def _apply_overrides_to_lots(lots_df: pd.DataFrame, overrides: dict) -> pd.DataFrame:
    """WHAT-IF: deterministically perturb the lot frame for a scenario run.

    * cycle_multiplier / oee_factor scale every DISPATCHABLE lot's effective
      run-time (proc_eff_min). proc_eff_min /= oee_factor (better OEE = faster),
      then *= cycle_multiplier. Re-ceiled to whole minutes if the engine rounds,
      so the perturbed run uses the same numeric convention as the baseline.
    * machine_adds appends synthetic eligible machine ids (SCN-<stage>-<k>) to
      every lot of that stage, so dispatch can SPREAD load onto the extra units.
      Curing lots never reach here (they are pinned, not in lots_df). Identity
      when overrides is falsy. Determinism preserved (no RNG, sorted ids).
    """
    if lots_df.empty or not overrides:
        return lots_df
    df = lots_df.copy()
    oee = float(overrides.get("oee_factor", 1.0) or 1.0)
    cyc = float(overrides.get("cycle_multiplier", 1.0) or 1.0)
    if abs(oee - 1.0) > 1e-9 or abs(cyc - 1.0) > 1e-9:
        scale = cyc / oee
        new = df["proc_eff_min"].astype(float) * scale
        if C.ROUND_PROC_TO_WHOLE_MIN:
            new = new.map(lambda v: float(math.ceil(v - 1e-9)) if v > 0 else v)
        df["proc_eff_min"] = new
    adds = overrides.get("machine_adds") or {}
    if adds:
        def _augment(row):
            n = int(adds.get(row["stage"], 0))
            if n <= 0:
                return row["machines"]
            extra = [f"{C.SCENARIO_STAGE_PREFIX}-{row['op_seq']}-{k+1}"
                     for k in range(n)]
            cur = [m.strip() for m in str(row["machines"]).split(",") if m.strip()]
            return ",".join(cur + extra)
        df["machines"] = df.apply(_augment, axis=1)
    return df


def run_pipeline(drum_df: pd.DataFrame, skus: List[str],
                 corrected_dir: Optional[str] = None,
                 config_overrides: Optional[dict] = None) -> PipelineResult:
    cd = corrected_dir or C.CORRECTED_DIR
    overrides = C.normalise_overrides(config_overrides)
    drum_summary = validate_drum(drum_df)
    notes: List[str] = []
    quarantined: List[dict] = []
    if overrides:
        notes.append(f"SCENARIO overrides applied: {overrides}")

    timelines = build_shared_timelines()
    per_sku_lots: Dict[str, pd.DataFrame] = {}   # sku -> unwindowed make_lots frame
    all_demand: List[pd.DataFrame] = []
    all_blocks: List[CuringBlock] = []
    block_starts: Dict[str, float] = {}
    dags: Dict[str, SkuDag] = {}
    sku_data: Dict[str, SkuData] = {}   # retained SkuData per scheduled SKU (waste matrix)
    scheduled_skus: List[str] = []   # SKUs that produced lots (actually built)

    for sku in skus:
        try:
            data = load_sku(sku, corrected_dir=cd)
            dag = build_dag(data)
        except Exception as e:  # pragma: no cover
            notes.append(f"{sku}: load/dag failed ({e})")
            continue
        # D5: explicit quarantine gate (BEFORE any demand/lot is created)
        qr = _quarantine_reason(sku, data, dag)
        if qr is not None:
            quarantined.append({
                "lot_id": f"{sku}-QUARANTINE", "machine": "",
                "check_type": "QUARANTINED_SKU",
                "detail": f"{sku}: {qr}", "severity": "ERROR"})
            notes.append(f"{sku}: QUARANTINED - {qr}")
            continue
        dags[sku] = dag
        blocks = curing_blocks_for_sku(drum_df, sku)
        if not blocks:
            notes.append(f"{sku}: no curing blocks in drum")
            continue
        all_blocks.extend(blocks)
        for cb in blocks:
            block_starts[cb.block_id] = cb.start_min
        dem = explode_demand(data, dag, blocks)
        all_demand.append(dem)
        # make_lots SKIPS pooled bulk-compound types (C.POOLED_CROSS_SKU_TYPES);
        # those are emitted once, cross-SKU, by merge_cross_sku_campaigns below.
        lots = make_lots(data, dag, dem)
        if lots.empty:
            continue
        per_sku_lots[sku] = lots
        sku_data[sku] = data
        scheduled_skus.append(sku)

    demand_df = (pd.concat(all_demand, ignore_index=True)
                 if all_demand else pd.DataFrame(columns=C.SCHEMA_DEMAND))

    # FIX-5: CROSS-SKU COMPOUND POOLING. Pool the bulk compound demand
    # (final/master/small-chemical) across ALL scheduled SKUs by item code and
    # emit one campaign stream per (item, shelf-window) - one MPQ-floored batch
    # serves every consuming SKU instead of one per SKU. Each pooled lot is
    # ANCHORED to its earliest-deadline consumer's (sku, block) so it joins that
    # anchor SKU's window/consumer chain; its cross-SKU consumer edges ride in
    # parent_lot_ids for handoff + dispatch matching. Deterministic.
    pooled_lots = merge_cross_sku_campaigns(demand_df, dags, sku_data)

    # Window pass (Phase 4). Pooled lots are folded into their ANCHOR SKU's frame
    # so the consumer-start propagation (compute_windows' per-block planned map)
    # resolves the compound's real consumer LST within that SKU's block - never
    # the drum fallback. The anchor item is a node in the anchor SKU's dag (the
    # compound is in that SKU's BOM), so its topo rank/consumer edge are known.
    all_lots: List[pd.DataFrame] = []
    pooled_by_anchor: Dict[str, List[pd.DataFrame]] = {}
    if not pooled_lots.empty:
        for a_sku, grp in pooled_lots.groupby("sku", sort=True):
            pooled_by_anchor.setdefault(str(a_sku), []).append(grp)
    for sku in scheduled_skus:
        frames = [per_sku_lots[sku]]
        frames.extend(pooled_by_anchor.get(sku, []))
        combined = (pd.concat(frames, ignore_index=True)
                    if len(frames) > 1 else frames[0])
        windowed = compute_windows(combined, dags[sku], block_starts)
        all_lots.append(windowed)
    # Any pooled lot whose anchor SKU produced no per-SKU lots (defensive; should
    # not occur since the anchor SKU always consumes the compound) is windowed on
    # its own anchor dag so it is never silently dropped.
    handled = set(scheduled_skus)
    if not pooled_lots.empty:
        orphan = pooled_lots[~pooled_lots["sku"].isin(handled)]
        if not orphan.empty:
            for a_sku, grp in orphan.groupby("sku", sort=True):
                if a_sku in dags:
                    all_lots.append(compute_windows(grp, dags[a_sku], block_starts))

    # Phase 6: pin curing anchors on the shared timeline (built SKUs)
    curing_rows = pin_curing(timelines, all_blocks)
    # BUG-10: reserve press time for un-scheduled/quarantined SKU drum rows
    # (e.g. SKU-D's 441 blocks) so those presses are NOT falsely free. Limited
    # to the SKUs in this run's scope; occupancy-only, never built.
    in_scope = [s for s in skus if s not in scheduled_skus]
    reserve_rows = pin_press_occupancy(
        timelines, drum_df[drum_df["SKUCode"].isin(set(in_scope))],
        scheduled_skus)
    # L6: reserve press time for CHANGEOVER (360 min) + MOULD_CLEAN drum rows so
    # no build is validated as feeding a press mid-changeover. Occupancy-only;
    # excluded from demand and the GT-fulfilment denominator.
    nonprod_rows = (pin_nonproduction_occupancy(timelines, drum_df)
                    if C.L6_NONPROD_OCCUPANCY else [])

    lots_df = (pd.concat(all_lots, ignore_index=True)
               if all_lots else pd.DataFrame(columns=C.SCHEMA_LOT))
    # demand_df already concatenated above (needed for the cross-SKU pool pass).

    # WHAT-IF: perturb capacity deterministically (proc scaling + extra machines)
    # BEFORE bottleneck/dispatch so the scenario actually re-schedules. Identity
    # when overrides is None (no regression on any baseline run).
    if overrides:
        lots_df = _apply_overrides_to_lots(lots_df, overrides)

    # Phase 5: bottleneck (pre-dispatch, lots-only). Used ONLY to name the stage
    # that dispatch prioritises in its criticality key (is_bottleneck already
    # flags Final Mixing lots independently). The real PLACED utilisation is
    # recomputed post-dispatch below (BUG-02).
    pre_cap = capacity.analyse(lots_df, overrides=overrides)

    # Phase 7: global dispatch
    disp = dispatch(lots_df, dags, timelines, block_starts, pre_cap.bottleneck)

    sched_rows = (list(curing_rows) + list(reserve_rows) + list(nonprod_rows)
                  + list(disp.schedule_rows))
    schedule_raw = (pd.DataFrame(sched_rows) if sched_rows
                    else pd.DataFrame(columns=C.SCHEMA_SCHEDULE))

    # Phase 7 dispatch reason codes = INFEASIBILITY (unplaced lots), kept SEPARATE
    # from Phase 8 post-condition violations so a clean run yields empty
    # violations.csv (the contract) while infeasibility is surfaced honestly.
    infeas_rows = list(quarantined) + list(disp.violations)
    infeas_df = (pd.DataFrame(infeas_rows) if infeas_rows
                 else pd.DataFrame(columns=C.SCHEMA_VIOLATION))
    # FIX-4: label each infeasible lot ROOT (head constraint breach) vs CASCADE
    # (UNREACHED producer orphaned by a dropped consumer). Counts unchanged.
    infeas_df = outputs.classify_infeasibility(infeas_df)

    # Phase 8: independently re-prove C1-C6 on the COMMITTED schedule.
    post_violations = validate.validate(schedule_raw, lots_df)
    seen = set()
    deduped = []
    for v in post_violations:
        key = (v["lot_id"], v["check_type"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(v)
    violations_df = (pd.DataFrame(deduped) if deduped
                     else pd.DataFrame(columns=C.SCHEMA_VIOLATION))

    # Phase 8 (TASK A): independently RE-PROVE C5-C8 from the produced artefacts
    # (placed schedule + per-SKU MPQ masters + the real drum), not by-construction.
    sku_mpq = {s: d.mpq for s, d in sku_data.items()}
    # FIX B: per-SKU MPQ-bound UOMs so C5 reduces bound + lot to one canonical
    # unit before comparing (the floor binds for MTR/KG length/mass items).
    sku_mpq_uom = {s: d.mpq_uom for s, d in sku_data.items()}
    independent_results = validate.independent_checks(
        schedule_raw, lots_df, sku_mpq, drum=drum_df, sku_mpq_uom=sku_mpq_uom)
    # any independent FAIL with offending rows folds into violations.csv so a
    # clean run still yields the empty-violations contract while a real C5-C8
    # breach is surfaced honestly (never silently passed).
    for res_chk in independent_results:
        for vr in res_chk.get("rows", []):
            key = (vr["lot_id"], vr["check_type"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(vr)
    violations_df = (pd.DataFrame(deduped) if deduped
                     else pd.DataFrame(columns=C.SCHEMA_VIOLATION))

    # Phase 5 (final): PLACED utilisation from the committed schedule (BUG-02).
    cap_report = capacity.analyse(lots_df, schedule=schedule_raw,
                                  overrides=overrides)

    # Phase 9: KPIs / handoff / humanise
    # FIX-1: folded carcasses are no longer in disp.schedule_rows (they are in
    # disp.produced_component_rows), so every schedule row is a real placed lot.
    n_placed_lots = len(disp.schedule_rows)
    n_unplaced = len(lots_df) - n_placed_lots
    drum_starts = [cb.start_min for cb in all_blocks]
    # R4: uncovered demand - drum production SKUs with NO recipe (dropped silently
    # before). Surfaced as its own report + a data_audit line per missing SKU, and
    # folded into a HONEST plant-level fulfilment number below.
    uncovered_df = _uncovered_demand(drum_df, cd)
    # FIX-3: quarantined-stub SKUs reserve real drum presses (occupancy-only) but
    # have a degenerate BOM so they can never be built. They must be reported ONCE
    # as uncovered_demand (reason QUARANTINED_SKU), NOT double-counted as building-
    # infeasible curing-feed misses in the unfed-press report. Append their press
    # counts (from the real drum) to uncovered_demand so they appear exactly once.
    quarantined_skus = sorted({
        str(q["lot_id"]).replace("-QUARANTINE", "") for q in quarantined
        if str(q.get("check_type")) == "QUARANTINED_SKU"})
    q_uncovered_rows: List[dict] = []
    if quarantined_skus:
        qprod = drum_df[
            (~drum_df["SKUCode"].isin(C.NON_PRODUCTION_SKUS)) &
            (drum_df["SKUCode"].astype(str).str.strip().isin(quarantined_skus))]
        for sku in quarantined_skus:
            sub = qprod[qprod["SKUCode"].astype(str).str.strip() == sku]
            sub = sub[sub["Qty"] > 0]
            if len(sub) == 0:
                continue
            q_uncovered_rows.append({
                "sku": sku, "curing_blocks": int(len(sub)),
                "gt_qty": float(sub["Qty"].sum()), "reason": "QUARANTINED_SKU",
            })
    if q_uncovered_rows:
        q_df = pd.DataFrame(q_uncovered_rows, columns=UNCOVERED_COLUMNS)
        uncovered_df = (pd.concat([uncovered_df, q_df], ignore_index=True)
                        if not uncovered_df.empty else q_df)
        uncovered_df = uncovered_df.sort_values(
            ["reason", "sku"]).reset_index(drop=True)
    for _, ur in uncovered_df.iterrows():
        notes.append(
            f"{ur['sku']}: UNCOVERED_DEMAND - MISSING_RECIPE "
            f"({int(ur['curing_blocks'])} curing blocks, gt_qty {ur['gt_qty']:.0f})")
    audit_rows = [{
        "sku": ur["sku"], "event": "UNCOVERED_DEMAND",
        "reason": "MISSING_RECIPE",
        "detail": (f"{int(ur['curing_blocks'])} drum curing blocks, "
                   f"gt_qty {ur['gt_qty']:.0f} - no recipe, not scheduled"),
    } for _, ur in uncovered_df.iterrows()]
    data_audit_df = (pd.DataFrame(audit_rows,
                                  columns=["sku", "event", "reason", "detail"])
                     if audit_rows
                     else pd.DataFrame(columns=["sku", "event", "reason", "detail"]))

    # R4 / FIX-3: FULL-DRUM denominator = in-scope curing blocks + uncovered blocks
    # (missing-recipe + quarantined-SKU presses). Quarantined SKUs are NOT in
    # presses_total (they never reach all_blocks), so adding their uncovered rows
    # here counts them exactly ONCE in the plant-level number.
    uncovered_blocks = int(uncovered_df["curing_blocks"].sum()) if not \
        uncovered_df.empty else 0

    kpis = outputs.compute_kpis(schedule_raw, lots_df, cap_report,
                                n_unplaced, len(violations_df), drum_starts,
                                infeasibility=infeas_df, all_blocks=all_blocks)
    # R4: plant-level fulfilment alongside the (unchanged) in-scope OTIF.
    n_scope = len(scheduled_skus)
    n_total_skus = len(set(skus)) + len(uncovered_df)
    kpis.presses_total_drum = kpis.presses_total + uncovered_blocks
    kpis.plant_fulfilment_pct = (
        100.0 * kpis.presses_fulfilled / kpis.presses_total_drum
        if kpis.presses_total_drum else 0.0)
    kpis.scope = f"{n_scope}_of_{n_total_skus}"
    kpi_df = outputs.kpi_dataframe(kpis)
    handoff = outputs.handoff_report(schedule_raw)
    unfulfilled = outputs.unfulfilled_presses_report(
        schedule_raw, infeas_df, all_blocks=all_blocks, lots=lots_df)
    schedule_h = outputs.humanise_schedule(schedule_raw)
    # FIX-1: folded carcasses as a produced-components artefact (mass-balance /
    # pegging), NOT schedule rows. Built from the dispatch result so the carcass
    # qty is counted as produced without an external validator reading a
    # 0-duration / blank-machine op-195 row in schedule.csv.
    produced_components_df = outputs.produced_components(disp.produced_component_rows)

    # WASTE / YIELD matrix (analytics, read-only). One frame per scheduled SKU,
    # concatenated. Cheap (groupby on already-built lots/demand) and deterministic.
    # infeas_df supplies the optional aging_scrap_qty (lots that expired).
    # FIX-6: non-pooled (per-tyre / per-block) components keep PER-SKU rows
    # (waste_matrix excludes pooled bulk compounds). Cross-SKU pooled bulk
    # compounds are reported ONCE at the plant/component level (required &
    # produced summed across all SKUs), so they never show inflated-positive
    # anchor waste or negative ghost-SKU waste.
    waste_frames: List[pd.DataFrame] = []
    for sku in scheduled_skus:
        wm = waste_matrix(sku_data[sku], dags[sku], demand_df, lots_df,
                          dispatch_df=infeas_df, sku=sku)
        if not wm.empty:
            waste_frames.append(wm)
    pooled_wm = pooled_waste_matrix(demand_df, lots_df, dispatch_df=infeas_df)
    if not pooled_wm.empty:
        waste_frames.append(pooled_wm)
    waste_df = (pd.concat(waste_frames, ignore_index=True) if waste_frames
                else pd.DataFrame(columns=WASTE_COLUMNS))
    if not waste_df.empty:
        waste_df = waste_df.sort_values(
            ["waste_pct", "component", "sku"],
            ascending=[False, True, True]).reset_index(drop=True)

    return PipelineResult(
        drum_summary=drum_summary, schedule=schedule_h, schedule_raw=schedule_raw,
        lots=lots_df, demand=demand_df, violations=violations_df,
        independent=independent_results,
        infeasibility=infeas_df, kpis=kpis,
        kpi_df=kpi_df, handoff=handoff, unfulfilled=unfulfilled,
        waste=waste_df,
        capacity=cap_report, skus_used=skus,
        n_unplaced=n_unplaced,
        uncovered_demand=uncovered_df,
        produced_components=produced_components_df,
        data_audit=data_audit_df,
        notes=notes, overrides=overrides,
    )
