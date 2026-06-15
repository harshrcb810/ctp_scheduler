#!/usr/bin/env python
"""CTP Production Scheduler -- command-line entry point.

Loads the curing schedule (the drum) + master data, runs the full pipeline,
and writes the schedule + KPI / violation / infeasibility reports to --outdir.

Examples
--------
  python run.py --drum-skus                 # schedule every SKU in the drum
  python run.py --skus ALL                   # same as --drum-skus
  python run.py --skus <SKU1> <SKU2>          # a specific subset
  python run.py --drum data/CuringSchedule.csv --skus ALL --outdir outputs
"""
from __future__ import annotations

import argparse
import os
import sys

from ctp_scheduler import config as C
from ctp_scheduler.ingest import load_drum
from ctp_scheduler.pipeline import run_pipeline


def _drum_skus(drum_df):
    """Distinct production SKUs in the drum (excludes changeover / mould-clean)."""
    col = "SKUCode" if "SKUCode" in drum_df.columns else drum_df.columns[3]
    skus = [str(s).strip() for s in drum_df[col].dropna().unique()]
    nonprod = {x.upper() for x in C.NON_PRODUCTION_SKUS}
    return sorted(s for s in skus if s and s.upper() not in nonprod)


def _write(df, outdir, name):
    if df is None:
        return
    path = os.path.join(outdir, name)
    try:
        df.to_csv(path, index=False)
    except Exception as e:  # pragma: no cover
        print(f"[warn] could not write {name}: {e}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="CTP production scheduler")
    ap.add_argument("--drum", default=None,
                    help="path to the curing schedule CSV (default: config DEFAULT_DRUM_CSV)")
    ap.add_argument("--skus", nargs="*", default=None,
                    help="SKU codes to schedule, or ALL")
    ap.add_argument("--drum-skus", action="store_true",
                    help="schedule every production SKU present in the drum")
    ap.add_argument("--outdir", default=C.OUTPUT_DIR,
                    help="output directory (default: outputs/)")
    args = ap.parse_args(argv)

    os.makedirs(args.outdir, exist_ok=True)

    drum = load_drum(args.drum)
    print(f"[ingest] drum rows={len(drum)}")

    all_drum_skus = _drum_skus(drum)
    if args.drum_skus or args.skus in (None, ["ALL"], ["all"]):
        skus = all_drum_skus
    else:
        skus = [s.strip() for s in args.skus]
    print(f"[ingest] scheduling {len(skus)} SKU(s)")

    res = run_pipeline(drum, skus)

    # --- write all artefacts -------------------------------------------------
    _write(res.schedule,             args.outdir, "schedule.csv")
    _write(res.violations,           args.outdir, "violations.csv")
    _write(res.infeasibility,        args.outdir, "infeasibility.csv")
    _write(res.kpi_df,               args.outdir, "kpi_report.csv")
    _write(res.handoff,              args.outdir, "handoff_report.csv")
    _write(res.unfulfilled,          args.outdir, "unfulfilled_presses.csv")
    _write(res.waste,                args.outdir, "waste_matrix.csv")
    _write(res.uncovered_demand,     args.outdir, "uncovered_demand.csv")
    _write(res.data_audit,           args.outdir, "data_audit.csv")
    _write(res.produced_components,  args.outdir, "produced_components.csv")

    # --- summary -------------------------------------------------------------
    n_viol = 0 if res.violations is None else len(res.violations)
    print(f"[kpi]    SKUs used={len(res.skus_used)}  "
          f"lots placed={len(res.lots) - res.n_unplaced}/{len(res.lots)}  "
          f"unplaced={res.n_unplaced}  violations={n_viol}")
    try:
        kdf = res.kpi_df
        for key in ("otif_pct", "otif_demand_pct", "plant_fulfilment_pct",
                    "lot_placement_pct", "bottleneck_stage"):
            row = kdf[kdf.iloc[:, 0] == key]
            if len(row):
                print(f"[kpi]    {key} = {row.iloc[0, 1]}")
    except Exception:
        pass
    for note in (res.notes or [])[:20]:
        print(f"[note]   {note}")
    print(f"[out]    wrote schedule + reports to {args.outdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
