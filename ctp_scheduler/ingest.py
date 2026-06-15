"""Phase 0 - Ingestion, normalisation & validation gate.

DB-SOURCE MIGRATION (CONFIG_VERSION 1.17.0): the per-SKU CTP_Routing_<SKU>.xlsx
workbooks are GONE. The plant now exports a small set of CONSOLIDATED database
CSVs into data/from_db/. When C.USE_DB_SOURCE is True (default), this module
reads exclusively from those CSVs:

  * jkt_routing.csv         - all SKUs, keyed finished_product. Its columns
                              already match the old per-SKU Routing sheet, so a
                              slice (finished_product == sku) IS the routing df.
  * jkt_bom.csv             - all SKUs, keyed Super_parent. REMAPPED to the
                              legacy flat BOM schema the engine expects:
                                Output       <- Parent
                                output qty   <- Parent_qty
                                unit         <- Parent_unit
                                input code   <- child
                                qty          <- child_quantity
                                unit.1       <- child_Unit
                                Input ItemType <- child_description
  * jkt_aging_master.csv    - GLOBAL aging master (old "Aging Master" sheet).
  * jkt_mpq.csv             - GLOBAL MPQ (old "MPQ" sheet; underscore headers).
  * jkt_buffer_master.csv   - GLOBAL buffer master.
  * jkt_itemType_master.csv - GLOBAL item-type master.
  * CuringSchedule.csv      - the DRUM (mapped to the legacy drum schema).
  * jkt_demand.csv          - NEW real demand (load_demand()).

The 7.6MB BOM + 6.6MB routing are read ONCE, grouped, and cached at module scope
(NOT re-read per SKU). The 4 global masters load ONCE as shared dicts. load_sku()
slices the cache + injects the shared masters, producing the SAME SkuData the
xlsx path produced, so every downstream phase is unchanged.

Normalises proc_time to minutes; builds aging / mpq / buffer / item_type /
machine-pool lookups; runs a hard validation gate. Pure function of file bytes
(determinism C8): sorted groupby, no RNG, no wall-clock.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from . import config as C


# ---------------------------------------------------------------------------
# Epoch axis: all times -> float minutes from a fixed epoch (no wall-clock).
# ---------------------------------------------------------------------------
EPOCH = datetime(2000, 1, 1)


def to_minutes_epoch(ts) -> float:
    if pd.isna(ts):
        return float("nan")
    if isinstance(ts, str):
        ts = pd.to_datetime(ts)
    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()
    if getattr(ts, "tzinfo", None) is not None:
        # ingest runs on ONE naive wall-clock axis (EPOCH is naive). A tz-aware
        # timestamp (e.g. an ISO +05:30 offset in a drum/master CSV) would raise
        # "can't subtract offset-naive and offset-aware" below and abort Phase 0.
        # Strip the zone to the naive local wall-clock so ingest is tz-robust.
        ts = ts.replace(tzinfo=None)
    return (ts - EPOCH).total_seconds() / 60.0


def from_minutes_epoch(m: float):
    if m is None or pd.isna(m):
        return pd.NaT
    return EPOCH + pd.Timedelta(minutes=float(m))


# ---------------------------------------------------------------------------
# Consolidated DB-CSV cache (the new data source). All read ONCE, cached at
# module scope, keyed by file path so a test can point at an alternate export.
# The grouped frames are kept as dict-of-DataFrame so a per-SKU slice is O(1).
# ---------------------------------------------------------------------------
@dataclass
class _DbCache:
    """All consolidated DB inputs, parsed once.

    routing_by_sku / bom_by_sku map a SKU (finished_product / Super_parent) to
    its already-sliced, legacy-schema DataFrame. aging/mpq/buffer/item_type are
    the GLOBAL master dicts shared by every SKU.
    """
    routing_by_sku: Dict[str, pd.DataFrame]
    bom_by_sku: Dict[str, pd.DataFrame]
    aging: Dict[str, Tuple[float, float]]
    mpq: Dict[str, Tuple[float, float]]
    mpq_uom: Dict[str, str]
    buffer: Dict[str, float]
    item_type: Dict[str, str]
    skus: List[str]


_DB_CACHE: Optional[_DbCache] = None
_DB_CACHE_KEY: Optional[Tuple[str, str, str, str, str, str]] = None


# BOM column remap: DB jkt_bom.csv -> legacy flat BOM schema the engine reads.
# The duplicate "unit" column name is preserved as the pandas-style "unit.1" the
# old xlsx parse produced (dag.py reads r["unit.1"] for the child's demand UOM).
_BOM_RENAME = {
    "Parent": "Output",
    "Parent_qty": "output qty",
    "Parent_unit": "unit",
    "child": "input code",
    "child_quantity": "qty",
    "child_Unit": "unit.1",
    "child_description": "Input ItemType",
}
_BOM_OUT_COLS = ["Output", "output qty", "unit", "input code", "qty",
                 "unit.1", "Input ItemType"]


def _global_aging_dict(aging_df: pd.DataFrame) -> Dict[str, Tuple[float, float]]:
    """ItemCode -> (min_h, max_h) from the global aging master."""
    out: Dict[str, Tuple[float, float]] = {}
    for _, r in aging_df.iterrows():
        code = str(r["ItemCode"]).strip()
        try:
            amin = _hours(r["MinAging"], r.get("MinAgingUnit"))
            amax = _hours(r["MaxAging"], r.get("MaxAgingUnit"))
        except (TypeError, ValueError):
            continue
        out[code] = (amin, amax)
    return out


def _global_mpq_dicts(mpq_df: pd.DataFrame
                      ) -> Tuple[Dict[str, Tuple[float, float]], Dict[str, str]]:
    """Item-type(casefold) -> (min,max) and -> bound UOM, from the global MPQ
    master. Note the DB headers use underscores (Item_Type/Minimum_Run_Qty/
    Maximum_Run_Qty) vs the old xlsx spaces."""
    mpq: Dict[str, Tuple[float, float]] = {}
    mpq_uom: Dict[str, str] = {}
    type_col = "Item_Type" if "Item_Type" in mpq_df.columns else "Item Type"
    min_col = "Minimum_Run_Qty" if "Minimum_Run_Qty" in mpq_df.columns else "Minimum Run Qty"
    max_col = "Maximum_Run_Qty" if "Maximum_Run_Qty" in mpq_df.columns else "Maximum Run Qty"
    has_uom = "UOM" in mpq_df.columns
    for _, r in mpq_df.iterrows():
        it = str(r[type_col]).strip().lower()
        mn = float(r[min_col]) if pd.notna(r[min_col]) else C.DEFAULT_MPQ_MIN
        mx = float(r[max_col]) if pd.notna(r[max_col]) else C.DEFAULT_MPQ_MAX
        mpq[it] = (mn, mx)
        if has_uom and pd.notna(r.get("UOM")):
            mpq_uom[it] = str(r["UOM"]).strip()
    return mpq, mpq_uom


def _global_buffer_dict(buf_df: pd.DataFrame) -> Dict[str, float]:
    """Item-type(casefold, base-folded) -> buffer hours."""
    out: Dict[str, float] = {}
    for _, r in buf_df.iterrows():
        label = str(r["Item type"]).strip()
        key = C.BUFFER_BASE.get(label.lower(), label).lower()
        out[key] = float(r["Buffer Level (Hrs)"]) if pd.notna(r["Buffer Level (Hrs)"]) else 0.0
    return out


def _global_item_type_dict(it_df: pd.DataFrame) -> Dict[str, str]:
    """ItemCode -> ItemType from the global item-type master."""
    out: Dict[str, str] = {}
    for _, r in it_df.iterrows():
        out[str(r["ItemCode"]).strip()] = str(r["ItemType"]).strip()
    return out


def _remap_bom_slice(g: pd.DataFrame) -> pd.DataFrame:
    """Remap a Super_parent BOM slice to the legacy flat BOM schema."""
    cols = {src: dst for src, dst in _BOM_RENAME.items() if src in g.columns}
    out = g.rename(columns=cols)
    # Guarantee every expected column exists (defensive against a missing field).
    for c in _BOM_OUT_COLS:
        if c not in out.columns:
            out[c] = pd.NA
    return out[_BOM_OUT_COLS].reset_index(drop=True)


def _load_db_cache() -> _DbCache:
    """Read + group + cache the consolidated DB CSVs. Idempotent; keyed on the
    file paths so an alternate export busts the cache deterministically."""
    global _DB_CACHE, _DB_CACHE_KEY
    key = (C.DB_ROUTING_CSV, C.DB_BOM_CSV, C.DB_AGING_CSV, C.DB_MPQ_CSV,
           C.DB_BUFFER_CSV, C.DB_ITEMTYPE_CSV)
    if _DB_CACHE is not None and _DB_CACHE_KEY == key:
        return _DB_CACHE

    routing_all = pd.read_csv(C.DB_ROUTING_CSV, dtype={"finished_product": str},
                              low_memory=False)
    routing_all.columns = [str(c).strip() for c in routing_all.columns]
    routing_all["finished_product"] = routing_all["finished_product"].astype(str).str.strip()

    bom_all = pd.read_csv(C.DB_BOM_CSV, dtype={"Super_parent": str}, low_memory=False)
    bom_all.columns = [str(c).strip() for c in bom_all.columns]
    bom_all["Super_parent"] = bom_all["Super_parent"].astype(str).str.strip()

    # Group once. groupby(sort=True) gives a deterministic key order (C8).
    routing_by_sku = {
        str(k): g.reset_index(drop=True)
        for k, g in routing_all.groupby("finished_product", sort=True)
    }
    bom_by_sku = {
        str(k): _remap_bom_slice(g)
        for k, g in bom_all.groupby("Super_parent", sort=True)
    }

    aging = _global_aging_dict(pd.read_csv(C.DB_AGING_CSV, dtype={"ItemCode": str}))
    mpq, mpq_uom = _global_mpq_dicts(pd.read_csv(C.DB_MPQ_CSV))
    buffer = _global_buffer_dict(pd.read_csv(C.DB_BUFFER_CSV))
    item_type = _global_item_type_dict(pd.read_csv(C.DB_ITEMTYPE_CSV, dtype={"ItemCode": str}))

    skus = sorted(routing_by_sku.keys())
    _DB_CACHE = _DbCache(
        routing_by_sku=routing_by_sku, bom_by_sku=bom_by_sku,
        aging=aging, mpq=mpq, mpq_uom=mpq_uom, buffer=buffer,
        item_type=item_type, skus=skus,
    )
    _DB_CACHE_KEY = key
    return _DB_CACHE


# ---------------------------------------------------------------------------
# Workbook discovery (legacy xlsx path; only used when USE_DB_SOURCE is False)
# ---------------------------------------------------------------------------
def sku_to_path(sku: str) -> str:
    return os.path.join(
        C.CORRECTED_DIR, f"{C.ROUTING_FILE_PREFIX}{sku}{C.ROUTING_FILE_SUFFIX}"
    )


_BUILD_CYCLE_MASTER: Optional[Dict[str, float]] = None


def load_build_cycle_master(path: str = None) -> Dict[str, float]:
    """Authoritative per-SKU TBM building cycle time (seconds), product_code ->
    build_cycle_sec, sourced from GTCT TBR PCR 1.xlsx via data/build_cycle_master.csv.
    Cached (pure function of file bytes; safe for determinism C8)."""
    global _BUILD_CYCLE_MASTER
    if _BUILD_CYCLE_MASTER is not None and path is None:
        return _BUILD_CYCLE_MASTER
    p = path or C.BUILD_CYCLE_MASTER_CSV
    out: Dict[str, float] = {}
    if os.path.exists(p):
        m = pd.read_csv(p, dtype={"product_code": str})
        for _, r in m.iterrows():
            try:
                out[str(r["product_code"]).strip()] = float(r["build_cycle_sec"])
            except (TypeError, ValueError):
                continue
    if path is None:
        _BUILD_CYCLE_MASTER = out
    return out


def available_recipe_skus(corrected_dir: str = None) -> List[str]:
    """Distinct SKUs that have a routing recipe.

    DB source: distinct finished_product from jkt_routing (~250 SKUs). Legacy:
    scan the corrected_dir for CTP_Routing_<SKU> workbooks.
    """
    if C.USE_DB_SOURCE:
        return list(_load_db_cache().skus)
    corrected_dir = corrected_dir or C.CORRECTED_DIR
    skus = []
    for fn in os.listdir(corrected_dir):
        if fn.startswith(C.ROUTING_FILE_PREFIX) and fn.endswith(C.ROUTING_FILE_SUFFIX):
            sku = fn[len(C.ROUTING_FILE_PREFIX):-len(C.ROUTING_FILE_SUFFIX)]
            skus.append(sku)
    return sorted(skus)


# ---------------------------------------------------------------------------
# Drum (curing schedule) ingest + validation
# ---------------------------------------------------------------------------
@dataclass
class DrumSummary:
    rows: int
    production_rows: int
    horizon_from: Optional[datetime]
    horizon_to: Optional[datetime]
    presses: List[str]
    total_gt_demand: float
    distinct_skus: List[str]
    verdict: str            # DRUM OK / GAPS / INVALID
    issues: List[str] = field(default_factory=list)


# CuringSchedule.csv (DB drum) -> legacy drum schema. A row is detected as the
# DB format by the presence of the DB-only columns (sizeCode / machineName).
_CURING_DB_RENAME = {
    "sizeCode": "SKUCode",
    "machineName": "Machine",
    "startTime": "StartTime",
    "endTime": "EndTime",
    "scheduleQuantity": "Qty",
    "description": "SKU_Description",
    "runTime": "CycleTime_min",
    "remarks": "Remarks",
}


def _is_curing_db_format(cols) -> bool:
    cset = {str(c).strip() for c in cols}
    return "sizeCode" in cset and ("machineName" in cset or "machineId" in cset)


def load_drum(path: str = None) -> pd.DataFrame:
    """Load the curing-schedule DRUM.

    Detects the DB CuringSchedule.csv format (by columns) and maps it to the
    legacy drum schema (SKUCode/Machine/StartTime/EndTime/Qty/SKU_Description/
    CycleTime_min/Remarks); otherwise reads the legacy Curing_Sch_PCR.csv as-is.

    CHANGEOVER / MOULD_CLEAN admin rows: the DB encodes non-production via the
    remarks/changeOverStartTime fields. Any row whose sizeCode is already
    CHANGEOVER/MOULD_CLEAN is preserved; additionally a row that carries a
    changeover window but a blank sizeCode is normalised to SKUCode=CHANGEOVER so
    the existing NON_PRODUCTION_SKUS handling (press occupancy, C6) still fires.
    """
    path = path or C.DEFAULT_DRUM_CSV
    df = pd.read_csv(path, low_memory=False)
    df.columns = [str(c).strip() for c in df.columns]

    if _is_curing_db_format(df.columns):
        # Prefer machineName (the press id e.g. 4801); fall back to machineId.
        if "machineName" not in df.columns and "machineId" in df.columns:
            df = df.rename(columns={"machineId": "machineName"})
        cols = {src: dst for src, dst in _CURING_DB_RENAME.items() if src in df.columns}
        df = df.rename(columns=cols)
        # Normalise CHANGEOVER/MOULD_CLEAN admin rows: a row with a changeover
        # window but no sizeCode is a press-occupancy admin block.
        sku_blank = df["SKUCode"].isna() | (df["SKUCode"].astype(str).str.strip() == "")
        if "changeOverStartTime" in df.columns:
            has_co = df["changeOverStartTime"].notna() & (
                df["changeOverStartTime"].astype(str).str.strip() != "")
            df.loc[sku_blank & has_co, "SKUCode"] = "CHANGEOVER"

    # Required legacy columns; create empty if a source lacked them.
    for col in ("SKUCode", "Machine", "StartTime", "EndTime", "Qty"):
        if col not in df.columns:
            df[col] = pd.NA
    df["SKUCode"] = df["SKUCode"].astype(str).str.strip()
    df["Machine"] = df["Machine"].astype(str).str.strip()
    df["StartTime"] = pd.to_datetime(df["StartTime"], errors="coerce")
    df["EndTime"] = pd.to_datetime(df["EndTime"], errors="coerce")
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
    return df


def validate_drum(df: pd.DataFrame) -> DrumSummary:
    issues: List[str] = []
    prod = df[~df["SKUCode"].isin(C.NON_PRODUCTION_SKUS)].copy()
    if df["StartTime"].isna().any() or df["EndTime"].isna().any():
        issues.append("Some rows have unparseable Start/EndTime")
    bad_order = (df["EndTime"] <= df["StartTime"]).sum()
    if bad_order:
        issues.append(f"{bad_order} rows have EndTime <= StartTime")
    horizon_from = df["StartTime"].min()
    horizon_to = df["EndTime"].max()
    presses = sorted(df["Machine"].dropna().unique().tolist())
    distinct = sorted(prod["SKUCode"].unique().tolist())
    total_gt = float(prod["Qty"].sum())
    if total_gt <= 0:
        issues.append("Total GT demand is zero")
        verdict = "INVALID"
    elif issues:
        verdict = "GAPS"
    else:
        verdict = "DRUM OK"
    return DrumSummary(
        rows=len(df), production_rows=len(prod),
        horizon_from=horizon_from.to_pydatetime() if pd.notna(horizon_from) else None,
        horizon_to=horizon_to.to_pydatetime() if pd.notna(horizon_to) else None,
        presses=presses, total_gt_demand=total_gt, distinct_skus=distinct,
        verdict=verdict, issues=issues,
    )


# ---------------------------------------------------------------------------
# proc_time -> minutes (the 4 UOM cases)
# ---------------------------------------------------------------------------
def proc_to_minutes(proc, uom, batch_size, order_qty: float,
                    bom_len_mm: float = None,
                    qty_uom: str = "NOS") -> Tuple[float, bool]:
    """Return (proc_minutes_for_whole_order, estimated_flag).

    order_qty is the LOT quantity in its demand UOM (qty_uom): KG for compounds,
    MTR for length items, NOS for piece items.

    SEC/BATCH -> minutes_per_batch = proc/60 ; n_batches = ceil(qty/batch_size)
    M/MIN     -> minutes = required_length_m / proc. If lot UOM is already a
                 length (MTR/M) the qty IS the metres; else length = mm/pc x pcs.
    SEC       -> proc/60 per piece * count(order_qty)
    M / None  -> default
    estimated_flag set when proc == placeholder 22.
    """
    estimated = (proc == C.PROC_PLACEHOLDER)
    u = str(uom).strip().upper() if uom is not None else ""
    qu = str(qty_uom).strip().upper() if qty_uom is not None else "NOS"
    try:
        proc = float(proc)
    except (TypeError, ValueError):
        # Parse failure: use a 30-min default RATE but FALL THROUGH to the UOM
        # dispatch below, so a length (M/MIN) or batch (SEC/BATCH) lot still
        # scales by qty. The old early `return (30.0, True)` gave a flat, qty-
        # INDEPENDENT 30 min - the exact scale error (a 73,000 MTR roll == a
        # 1-piece lot) that BUG-08 fixed for blank UOMs, reintroduced here for
        # blank proc. (The `* 0.0` in the old expression was dead arithmetic.)
        proc = 30.0
        estimated = True
    if proc <= 0:
        proc = 30.0
        estimated = True

    if u == "SEC/BATCH":
        bs = float(batch_size) if batch_size and not _isnan(batch_size) else 1.0
        if bs <= 0:
            bs = 1.0
        n_batches = math.ceil(max(order_qty, 1e-9) / bs)
        return (proc / 60.0 * n_batches, estimated)
    if u == "M/MIN":
        # rate: minutes = length_m / proc.
        if qu in ("MTR", "MTRS", "M"):
            length_m = max(order_qty, 1e-9)              # qty already in metres
        elif bom_len_mm and bom_len_mm > 0:
            length_m = bom_len_mm / 1000.0 * max(order_qty, 1.0)  # mm/pc x pcs
        else:
            length_m = max(order_qty, 1.0)
        return (length_m / proc if proc > 0 else length_m, estimated)
    if u == "SEC":
        return (proc / 60.0 * max(order_qty, 1.0), estimated)
    if u == "M":
        # BUG-08 FIX: bare "M" is a length/rate UOM (metres-per-minute), exactly
        # like "M/MIN". A length item's run-time MUST scale with order_qty -
        # returning a flat (proc, True) ignored quantity and under/over-stated
        # run time for every length lot. Treat identically to M/MIN:
        #   minutes = required_length_m / rate.
        if qu in ("MTR", "MTRS", "M"):
            length_m = max(order_qty, 1e-9)              # qty already in metres
        elif bom_len_mm and bom_len_mm > 0:
            length_m = bom_len_mm / 1000.0 * max(order_qty, 1.0)  # mm/pc x pcs
        else:
            length_m = max(order_qty, 1.0)
        return (length_m / proc if proc > 0 else length_m, estimated)
    # Truly unknown / None UOM: we cannot derive a quantity-correct duration, so
    # flag it ESTIMATED explicitly (never silently return a qty-independent flat
    # block for what may be a length/weight item). proc is the per-order best
    # guess; the estimated flag surfaces the exposure in the KPI.
    return (proc, True)


def _isnan(x) -> bool:
    try:
        return math.isnan(float(x))
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Per-SKU workbook ingest
# ---------------------------------------------------------------------------
@dataclass
class SkuData:
    sku: str
    bom: pd.DataFrame
    routing: pd.DataFrame
    aging: Dict[str, Tuple[float, float]]      # item -> (min_h, max_h)
    mpq: Dict[str, Tuple[float, float]]        # item_type -> (min, max)
    # FIX B: the UOM the MPQ bounds are STATED in (from the MPQ sheet's UOM
    # column), keyed by item-type, so the canonical-unit compare can reduce the
    # bound and the lot qty to the same dimension before flooring/checking.
    mpq_uom: Dict[str, str]                    # item_type -> bound UOM (e.g. MTR, KG, NOS)
    buffer: Dict[str, float]                   # item_type -> hours
    item_type: Dict[str, str]                  # item -> item_type
    fg_code: str
    audit: List[str] = field(default_factory=list)


def _hours(val, unit) -> float:
    v = float(val)
    u = str(unit).strip().lower() if unit is not None else "hours"
    if u.startswith("min"):
        return v / 60.0
    if u.startswith("day"):
        return v * 24.0
    return v  # hours


def _load_sku_db(sku: str) -> Tuple[pd.DataFrame, pd.DataFrame,
                                    Dict[str, Tuple[float, float]],
                                    Dict[str, Tuple[float, float]],
                                    Dict[str, str], Dict[str, float],
                                    Dict[str, str]]:
    """Slice the cached consolidated DB CSVs for one SKU.

    Returns (bom, routing, aging, mpq, mpq_uom, buffer, item_type). The bom is
    already remapped to the legacy flat schema; the routing slice's columns
    already match the old Routing sheet. The 4 masters are the GLOBAL shared
    dicts (copied so the per-SKU GT_CUREBY override below cannot mutate the
    cache that every other SKU reads)."""
    cache = _load_db_cache()
    key = str(sku).strip()
    routing = cache.routing_by_sku.get(key, pd.DataFrame()).copy()
    bom = cache.bom_by_sku.get(key, pd.DataFrame(columns=_BOM_OUT_COLS)).copy()
    # aging/item_type get a per-SKU override stamped below -> copy to isolate.
    aging = dict(cache.aging)
    item_type = dict(cache.item_type)
    return (bom, routing, aging, cache.mpq, cache.mpq_uom, cache.buffer, item_type)


def load_sku(sku: str, corrected_dir: str = None) -> SkuData:
    audit: List[str] = []

    if C.USE_DB_SOURCE:
        (bom, routing, aging, mpq, mpq_uom, buffer, item_type) = _load_sku_db(sku)
        for df in (bom, routing):
            df.columns = [str(c).strip() for c in df.columns]
        return _finalise_sku(sku, bom, routing, aging, mpq, mpq_uom,
                             buffer, item_type, audit)

    # ---- legacy per-SKU xlsx path (kept for USE_DB_SOURCE=False) ----
    path = (os.path.join(corrected_dir, f"{C.ROUTING_FILE_PREFIX}{sku}{C.ROUTING_FILE_SUFFIX}")
            if corrected_dir else sku_to_path(sku))
    xl = pd.ExcelFile(path)
    bom_sheet = next(s for s in xl.sheet_names if s.startswith("BOM"))
    rt_sheet = next(s for s in xl.sheet_names if s.startswith("Routing"))
    bom = xl.parse(bom_sheet)
    routing = xl.parse(rt_sheet)
    aging_df = xl.parse("Aging Master")
    mpq_df = xl.parse("MPQ")
    buf_df = xl.parse("Buffer Master")
    it_df = xl.parse("ItemType Master")

    # canonicalise
    for df in (bom, routing):
        df.columns = [str(c).strip() for c in df.columns]

    aging = _global_aging_dict(aging_df)
    mpq, mpq_uom = _global_mpq_dicts(mpq_df)
    buffer = _global_buffer_dict(buf_df)
    item_type = _global_item_type_dict(it_df)
    return _finalise_sku(sku, bom, routing, aging, mpq, mpq_uom,
                         buffer, item_type, audit)


def _finalise_sku(sku, bom, routing, aging, mpq, mpq_uom, buffer, item_type,
                  audit) -> SkuData:
    """Shared SKU post-processing: build-cycle override, BOM canonicalisation,
    GT cure-by override. Identical for the DB and xlsx paths so determinism and
    the C2/C5 invariants hold regardless of source."""
    # BUILD CYCLE-TIME OVERRIDE: stamp the authoritative GTCT building cycle
    # (seconds, UOM=SEC) onto every building op (195 & 200) so a stale/placeholder
    # building proc_time in the workbook (legacy "22") can never re-surface. Self-
    # healing for ANY sku, present or future. Logged to audit.
    if C.BUILD_CYCLE_OVERRIDE_ENABLED and "operation_seq" in routing.columns:
        gtct = load_build_cycle_master()
        real = gtct.get(str(sku).strip())
        if real is not None and "proc_time" in routing.columns:
            mask = pd.to_numeric(routing["operation_seq"], errors="coerce").isin(C.BUILD_OPS)
            old = pd.to_numeric(routing.loc[mask, "proc_time"], errors="coerce")
            changed = int((old.fillna(-1).round(2) != round(real, 2)).sum())
            routing.loc[mask, "proc_time"] = real
            if "proc_time_UOM" in routing.columns:
                routing.loc[mask, "proc_time_UOM"] = "SEC"
            if changed:
                audit.append(
                    f"BUILD_CYCLE_OVERRIDE: {changed} build op row(s) set to GTCT "
                    f"cycle {real:.3f}s/tyre (was stale/placeholder)")

    if "Output" in bom.columns:
        bom["Output"] = bom["Output"].astype(str).str.strip()
    if "input code" in bom.columns:
        bom["input code"] = bom["input code"].astype(str).str.strip()

    # FIX-4: GT CURE-BY OVERRIDE (config-driven, self-healing). Stamp the config
    # cure-by band onto every Green-Tyre item's aging row, overriding the per-file
    # 6h/8h. Done at ingest so sizing._aging_for() (which reads data.aging first)
    # sees the corrected band for ALL 137 SKUs, present and future. Deterministic;
    # logged to audit. The GT item-type is the BOM/ItemType label, not the op.
    if C.GT_CUREBY_OVERRIDE_ENABLED:
        gt_band = (C.GREEN_TYRE_CUREBY_MIN_H, C.GREEN_TYRE_CUREBY_MAX_H)
        gt_codes = sorted(
            code for code, it in item_type.items()
            if str(it).strip().lower() in C.GREEN_TYRE_ITEM_TYPES)
        n_over = 0
        for code in gt_codes:
            prev = aging.get(code)
            if prev is None or (round(prev[0], 4), round(prev[1], 4)) != (
                    round(gt_band[0], 4), round(gt_band[1], 4)):
                n_over += 1
            aging[code] = gt_band
        if n_over:
            audit.append(
                f"GT_CUREBY_OVERRIDE: {n_over} green-tyre item(s) stamped to "
                f"cure-by ({gt_band[0]:.1f}h,{gt_band[1]:.1f}h) (was per-file)")

    return SkuData(
        sku=sku, bom=bom, routing=routing, aging=aging, mpq=mpq,
        mpq_uom=mpq_uom, buffer=buffer, item_type=item_type, fg_code=sku,
        audit=audit,
    )


# ---------------------------------------------------------------------------
# Machine pool parsing (Machine Master "Machine Id" -> individual machines)
# ---------------------------------------------------------------------------
def parse_machine_cell(cell) -> List[str]:
    if cell is None or _isnan_str(cell):
        return []
    s = str(cell).replace('"', "")
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p and p.lower() != "nan"]


def _isnan_str(cell) -> bool:
    return str(cell).strip().lower() in ("", "nan", "none")


# ---------------------------------------------------------------------------
# SKU coverage check (for the input gate)
# ---------------------------------------------------------------------------
@dataclass
class SkuCoverage:
    sku: str
    has_routing: bool
    has_bom: bool
    has_recipe: bool        # workbook file present
    eligible_tbm: bool      # has a build op with an eligible machine
    schedulable: bool
    reason: str = ""


def check_coverage(sku: str, corrected_dir: str = None) -> SkuCoverage:
    if C.USE_DB_SOURCE:
        if str(sku).strip() not in _load_db_cache().routing_by_sku:
            return SkuCoverage(sku, False, False, False, False, False, "MISSING_RECIPE")
    else:
        cd = corrected_dir or C.CORRECTED_DIR
        path = os.path.join(cd, f"{C.ROUTING_FILE_PREFIX}{sku}{C.ROUTING_FILE_SUFFIX}")
        if not os.path.exists(path):
            return SkuCoverage(sku, False, False, False, False, False, "MISSING_RECIPE")
    try:
        data = load_sku(sku, corrected_dir=corrected_dir)
    except Exception as e:  # pragma: no cover - defensive
        return SkuCoverage(sku, False, False, True, False, False, f"BAD_RECIPE:{e}")
    has_bom = len(data.bom) > 0
    has_routing = len(data.routing) > 0
    rt = data.routing
    build = (rt[rt["operation_seq"].isin(C.BUILD_OPS)]
             if "operation_seq" in rt.columns else rt.iloc[0:0])
    eligible = False
    for _, r in build.iterrows():
        if parse_machine_cell(r.get("machines")):
            eligible = True
            break
    schedulable = has_bom and has_routing and eligible
    reason = ""
    if not has_bom:
        reason = "MISSING_BOM"
    elif not has_routing:
        reason = "MISSING_ROUTING"
    elif not eligible:
        reason = "NO_ELIGIBLE_MACHINE"
    return SkuCoverage(sku, has_routing, has_bom, True, eligible, schedulable, reason)


# ---------------------------------------------------------------------------
# Demand loader (NEW DB export: jkt_demand.csv). The drum (curing schedule)
# remains the scheduling anchor for now; this surfaces the real plant demand
# for reporting / future demand-anchored scheduling. Pure function of bytes.
# ---------------------------------------------------------------------------
def load_demand(path: str = None) -> pd.DataFrame:
    """Load the real demand frame from jkt_demand.csv.

    Columns: plan_id, skuCode, skuDescription, requirement, market,
    deliveryDate (+ createdAt/createdBy). skuCode is normalised to str and
    requirement to numeric; deliveryDate parsed to datetime.
    """
    path = path or C.DB_DEMAND_CSV
    if not os.path.exists(path):
        return pd.DataFrame(columns=[
            "plan_id", "skuCode", "skuDescription", "requirement",
            "market", "deliveryDate"])
    df = pd.read_csv(path)
    df.columns = [str(c).strip() for c in df.columns]
    if "skuCode" in df.columns:
        df["skuCode"] = df["skuCode"].astype(str).str.strip()
    if "requirement" in df.columns:
        df["requirement"] = pd.to_numeric(df["requirement"], errors="coerce").fillna(0)
    if "deliveryDate" in df.columns:
        df["deliveryDate"] = pd.to_datetime(df["deliveryDate"], errors="coerce")
    return df
