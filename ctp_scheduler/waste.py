"""Per-component WASTE / YIELD matrix (analytics, read-only).

"Waste" here is *material over-produced* beyond what the cured tyres actually
consume, forced by lot-sizing. For each MADE component of an SKU:

    required(used)  = sum(explode_demand.required_qty)   # what the tyres consume
    produced(run)   = sum(make_lots.qty)                  # what the engine runs
    waste_qty       = produced - required
    waste_pct       = waste / produced                    # share of the run wasted

The dominant cause is the MPQ minimum-run floor: a low-volume compound whose
per-campaign requirement sits below the item-type's Minimum Run Qty is bumped up
to that floor, so most of the batch is over-production.

This module is pure analytics layered ON TOP of Phases 2-3 (demand + sizing) and
the Phase-7 dispatch reason codes. It NEVER mutates any schedule artefact and is
fully deterministic (sorted output, no RNG / wall-clock).

Two non-production component classes are reported with waste 0 (not -100%):
  * CARCASS items - made on op-195 and FOLDED into the green-tyre (op-200) build
    by the sizing B1 fold, so they produce no separate lots. produced := required.
  * FINISHED GOODS / curing op item - made by the DRUM 1:1 (op-210), never a
    dispatchable lot. produced := required.

The MM<->MTR unit reality is surfaced (not hidden): length items carry their
demand/lot qty in MTR (BOM MM / 1000) while the MPQ table is quoted in MTR; the
BOM *input* unit is MM, a 1000x mismatch, so the MPQ floor is effectively
non-binding on those items. `unit_note` flags this per row.
"""
from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd

from . import config as C
from .dag import SkuDag
from .ingest import SkuData
from .sizing import _mpq_for, _PER_BLOCK_TYPES  # reuse the exact sizing bounds/awareness


WASTE_COLUMNS: List[str] = [
    "sku", "component", "item_type", "uom",
    "required(used)", "produced(run)", "waste_qty", "waste_pct",
    "mpq_min", "mpq_max", "n_lots", "reason", "unit_note", "aging_scrap_qty",
]

# FIX-6: sentinel SKU label for COMPONENT/PLANT-level pooled-compound rows.
# Pooled bulk compounds (C.POOLED_CROSS_SKU_TYPES) are produced once across ALL
# consuming SKUs (cross-SKU pooling, FIX-5); their waste is therefore meaningful
# only at the plant level (required-and-produced summed across SKUs), never per
# (sku, component). Such rows carry sku == POOL_SKU so a per-SKU filter can never
# split a pooled batch into an inflated-positive anchor row + negative ghost rows.
POOL_SKU: str = "POOL"

# Item types whose demand/lot UOM is MTR but whose BOM *input* unit is MM, so the
# MPQ floor (quoted in MTR) is ~1000x larger than the metres actually run -> the
# floor is effectively non-binding. Length-sheet / strip / liner family.
_LENGTH_MM_ITEM_TYPES = (
    "cap strip", "cap", "ply", "inner liner", "innerliner",
    "sidewall", "side wall", "steel belt", "belt",
)

# Aging-scrap reason codes (Phase-7 dispatch): material made then expired.
_AGING_SCRAP_CODES = ("INFEASIBLE_AGING", "CUREBY_EXPIRED")


def _is_carcass_item(item: str, dag: SkuDag) -> bool:
    """True if `item` is a carcass (op-195) that the sizing B1 fold absorbs into a
    single green-tyre (op-200) build - mirrors sizing.make_lots' carcass detection
    so the waste matrix classifies exactly the items that produce no lots."""
    op = dag.ops.get(item)
    if op is None or op.op_seq != C.OP_CARCASS_BUILD:
        return False
    # exactly one MADE consumer, and that consumer is a green-tyre build (op-200)
    made_cons = [c for c in sorted(dag.graph.predecessors(item))
                 if item in dag.graph and dag.ops.get(c) is not None] \
        if item in dag.graph else []
    if len(made_cons) != 1:
        return False
    gt_op = dag.ops.get(made_cons[0])
    return gt_op is not None and gt_op.op_seq == C.OP_BUILD


def _is_curing_item(item: str, dag: SkuDag) -> bool:
    """True if `item` is the FG / curing-op item made 1:1 by the fixed drum."""
    if item == dag.fg_code:
        return True
    op = dag.ops.get(item)
    return op is not None and op.op_seq == C.OP_CURING


def _is_pooled_type(item_type: str) -> bool:
    """True if `item_type` is a CROSS-SKU pooled bulk compound (final / master
    compound, small chemical - C.POOLED_CROSS_SKU_TYPES). Such items are produced
    ONCE across all consuming SKUs (FIX-5), so their waste is computed at the
    plant/component level by `pooled_waste_matrix`, never per (sku, component).

    Honours CROSS_SKU_POOL_ENABLED: when pooling is OFF there are NO POOL- lots, so
    compounds are produced per-SKU and must be reported in the PER-SKU waste matrix
    like any other component (else they vanish from waste_matrix.csv entirely)."""
    return str(item_type).strip().lower() in C._pooled_cross_sku_types()


def _length_mm_unit_note(item_type: str, uom: str) -> str:
    """Return a data note when the MPQ floor is effectively non-binding because
    the BOM input unit (MM) differs 1000x from the MPQ/lot UOM (MTR)."""
    if str(uom).strip().upper() in ("MTR", "MTRS", "M") and \
            str(item_type).strip().lower() in _LENGTH_MM_ITEM_TYPES:
        return ("MPQ UOM is MTR but BOM input unit is MM (1000x mismatch) - "
                "MPQ floor effectively non-binding")
    return ""


def _classify(required: float, produced: float, waste_pct: float,
              min_block_req: float, mpq_min: float, mpq_max: float,
              n_lots: int, uom: str) -> str:
    """Deterministic reason classification (see module docstring / spec)."""
    if waste_pct <= 0.02:
        return "Batch rounding (negligible)"
    if mpq_min and mpq_min > 0 and min_block_req < mpq_min:
        u = str(uom).strip().upper() or "units"
        return f"MPQ minimum-run floor ({mpq_min:g} {u} min batch)"
    if n_lots > 1:
        return "MPQ-max split / batch rounding"
    return "Campaign/lot rounding"


def _aging_scrap_by_item(dispatch_df: Optional[pd.DataFrame],
                         lots_df: pd.DataFrame) -> Dict[str, float]:
    """Map item -> produced qty of lots that ended INFEASIBLE_AGING / CUREBY_EXPIRED
    (material made but expired). None-safe; returns {} if no dispatch frame."""
    if dispatch_df is None or dispatch_df.empty or lots_df.empty:
        return {}
    if "check_type" not in dispatch_df.columns or "lot_id" not in dispatch_df.columns:
        return {}
    scrap_lots = dispatch_df.loc[
        dispatch_df["check_type"].isin(_AGING_SCRAP_CODES), "lot_id"].unique()
    if len(scrap_lots) == 0:
        return {}
    sub = lots_df[lots_df["lot_id"].isin(set(scrap_lots))]
    if sub.empty:
        return {}
    return sub.groupby("item")["qty"].sum().to_dict()


def waste_matrix(data: SkuData, dag: SkuDag, demand_df: pd.DataFrame,
                 lots_df: pd.DataFrame, dispatch_df: Optional[pd.DataFrame] = None,
                 sku: Optional[str] = None) -> pd.DataFrame:
    """One row per made component: required(used) vs produced(run) + waste + reason.

    Args:
      data, dag    : the SKU's ingested data + DAG (for op_seq / fold / mpq).
      demand_df    : Phase-2 explode_demand frame (may be multi-sku).
      lots_df      : Phase-3 make_lots frame (may be multi-sku).
      dispatch_df  : OPTIONAL Phase-7 infeasibility/violation frame (lot_id,
                     check_type). When given, an `aging_scrap_qty` column is filled
                     with produced qty of lots that expired (separate from MPQ waste).
      sku          : OPTIONAL SKU filter; defaults to data.sku. Both frames are
                     filtered to this SKU so a concatenated multi-sku frame is safe.

    Returns a deterministic frame (sorted by waste_pct desc, then component) with
    WASTE_COLUMNS. UOMs are never summed across families by the caller summary.
    """
    target = (sku or data.sku)
    dem = demand_df
    lots = lots_df
    if not dem.empty and "sku" in dem.columns:
        dem = dem[dem["sku"] == target]
    if not lots.empty and "sku" in lots.columns:
        lots = lots[lots["sku"] == target]

    # required(used): sum of demand required_qty per item (what the tyres consume)
    if dem.empty:
        req_by_item: Dict[str, float] = {}
        uom_by_item: Dict[str, str] = {}
        type_by_item: Dict[str, str] = {}
        min_block_req: Dict[str, float] = {}
    else:
        req_by_item = dem.groupby("item")["required_qty"].sum().to_dict()
        uom_by_item = (dem.groupby("item")["uom"].first().to_dict())
        type_by_item = (dem.groupby("item")["item_type"].first().to_dict())
        # min per-consuming-block requirement (the tightest single-campaign base
        # run, which is what the MPQ floor would bump up if it is below mpq_min).
        per_block = (dem.groupby(["item", "source_curing_block"])["required_qty"]
                     .sum().reset_index())
        min_block_req = per_block.groupby("item")["required_qty"].min().to_dict()

    # produced(run): sum of make_lots qty per item (what the engine actually runs)
    if lots.empty:
        prod_by_item: Dict[str, float] = {}
        nlots_by_item: Dict[str, int] = {}
    else:
        prod_by_item = lots.groupby("item")["qty"].sum().to_dict()
        nlots_by_item = lots.groupby("item")["lot_id"].nunique().to_dict()

    aging_scrap = _aging_scrap_by_item(dispatch_df, lots)

    # universe of components = anything that appears as made demand or as a lot.
    # FIX-6: EXCLUDE cross-SKU pooled bulk compounds here - they are produced once
    # across all SKUs (FIX-5) and would otherwise show inflated-positive waste on
    # the anchor SKU and NEGATIVE waste on every other consuming SKU. They are
    # reported instead at the plant/component level by `pooled_waste_matrix`.
    def _type_of(it: str) -> str:
        return type_by_item.get(it) or data.item_type.get(it, "Unknown")

    components = sorted(
        it for it in (set(req_by_item) | set(prod_by_item))
        if not _is_pooled_type(_type_of(it)))

    rows: List[dict] = []
    for item in components:
        required = float(req_by_item.get(item, 0.0))
        produced = float(prod_by_item.get(item, 0.0))
        item_type = type_by_item.get(item) or data.item_type.get(item, "Unknown")
        uom = uom_by_item.get(item, "NOS")
        n_lots = int(nlots_by_item.get(item, 0))
        # MASTERS: report the master MPQ bounds when ON (item_code feeds the
        # compound/roll min; machine pool not threaded here -> compound min uses
        # the default batch_kg, acceptable for this read-only analytics frame).
        op = dag.ops.get(item)
        mpq_min, mpq_max = _mpq_for(
            item_type, data.mpq, item_code=item, line="PCR",
            machine_names=(op.machines if op is not None else None))
        unit_note = _length_mm_unit_note(item_type, uom)
        scrap = float(aging_scrap.get(item, 0.0))

        if _is_carcass_item(item, dag):
            # Folded into the green-tyre build by the B1 fold: produces no separate
            # lots. Set produced := required so it is not shown as -100% waste.
            produced = required
            waste_qty = 0.0
            waste_pct = 0.0
            reason = "Folded into Green-Tyre build (not separately produced)"
        elif _is_curing_item(item, dag):
            # FG / curing op: made 1:1 by the fixed drum, never a dispatchable lot.
            produced = required
            waste_qty = 0.0
            waste_pct = 0.0
            reason = "= Drum (curing 1:1)"
        else:
            waste_qty = produced - required
            waste_pct = (waste_qty / produced) if produced > 0 else 0.0
            mbr = float(min_block_req.get(item, required))
            reason = _classify(required, produced, waste_pct, mbr,
                               mpq_min, mpq_max, n_lots, uom)

        rows.append({
            "sku": target, "component": item, "item_type": item_type, "uom": uom,
            "required(used)": round(required, 4), "produced(run)": round(produced, 4),
            "waste_qty": round(waste_qty, 4), "waste_pct": round(waste_pct, 6),
            "mpq_min": mpq_min, "mpq_max": mpq_max, "n_lots": n_lots,
            "reason": reason, "unit_note": unit_note,
            "aging_scrap_qty": round(scrap, 4),
        })

    if not rows:
        return pd.DataFrame(columns=WASTE_COLUMNS)
    df = pd.DataFrame(rows, columns=WASTE_COLUMNS)
    # deterministic ordering: worst waste first, then component id tie-break
    df = df.sort_values(["waste_pct", "component"],
                        ascending=[False, True]).reset_index(drop=True)
    return df


def pooled_waste_matrix(demand_df: pd.DataFrame, lots_df: pd.DataFrame,
                        dispatch_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """COMPONENT / PLANT-level waste for CROSS-SKU pooled bulk compounds (FIX-6).

    Pooled bulk compounds (C.POOLED_CROSS_SKU_TYPES: final / master compound, small
    chemical) are produced ONCE across all consuming SKUs (FIX-5 pooling): a pooled
    lot is anchored to the earliest-deadline consumer SKU, with the other consuming
    (sku|block|consumer) edges carried in `parent_lot_ids`. Computing waste per
    (sku, component) is therefore meaningless - the anchor SKU sees the WHOLE batch
    vs only its own pull (inflated positive), every other SKU sees produced 0 vs its
    own pull (negative). This function instead aggregates BOTH sides across ALL SKUs
    by item code and emits ONE row per pooled component:

        required = sum(demand.required_qty) across ALL SKUs for the item
        produced = sum(pooled lot.qty) for the item, counted ONCE per physical lot
                   (taken straight from the lot frame, keyed by item - so a lot is
                   never double-counted via its multiple parent_lot_ids edges)
        waste    = produced - required   (>= 0 by construction at plant level)

    sku == POOL_SKU on every row. Deterministic (sorted). UOM-safe (per item).
    Returns a frame with WASTE_COLUMNS; empty frame if no pooled lots/demand exist.
    """
    dem = demand_df
    lots = lots_df
    pooled_dem = (dem[dem["item_type"].apply(_is_pooled_type)]
                  if dem is not None and not dem.empty else
                  pd.DataFrame(columns=getattr(dem, "columns", [])))
    pooled_lots = (lots[lots["item_type"].apply(_is_pooled_type)]
                   if lots is not None and not lots.empty else
                   pd.DataFrame(columns=getattr(lots, "columns", [])))
    if pooled_lots.empty and pooled_dem.empty:
        return pd.DataFrame(columns=WASTE_COLUMNS)

    # required(used): demand summed across ALL SKUs per pooled item.
    if pooled_dem.empty:
        req_by_item: Dict[str, float] = {}
        uom_by_item: Dict[str, str] = {}
        type_by_item: Dict[str, str] = {}
    else:
        req_by_item = pooled_dem.groupby("item")["required_qty"].sum().to_dict()
        uom_by_item = pooled_dem.groupby("item")["uom"].first().to_dict()
        type_by_item = pooled_dem.groupby("item")["item_type"].first().to_dict()

    # produced(run): pooled lot qty summed ONCE per item from the lot frame. Each
    # pooled lot appears exactly once here (keyed by item), so cross-SKU edges in
    # parent_lot_ids can NOT double-count. Deduplicate on lot_id defensively.
    if pooled_lots.empty:
        prod_by_item: Dict[str, float] = {}
        nlots_by_item: Dict[str, int] = {}
        mpq_by_item: Dict[str, tuple] = {}
        type_from_lots: Dict[str, str] = {}
        uom_from_lots: Dict[str, str] = {}
    else:
        uniq = pooled_lots.drop_duplicates(subset=["lot_id"])
        prod_by_item = uniq.groupby("item")["qty"].sum().to_dict()
        nlots_by_item = uniq.groupby("item")["lot_id"].nunique().to_dict()
        type_from_lots = uniq.groupby("item")["item_type"].first().to_dict()
        uom_from_lots = uniq.groupby("item")["uom"].first().to_dict()
        mpq_by_item = {}
        for it, g in uniq.groupby("item"):
            mn = float(g["mpq_min"].iloc[0]) if "mpq_min" in g else 0.0
            mx = float(g["mpq_max"].iloc[0]) if "mpq_max" in g else 0.0
            mpq_by_item[str(it)] = (mn, mx)

    aging_scrap = _aging_scrap_by_item(dispatch_df, pooled_lots)

    components = sorted(set(req_by_item) | set(prod_by_item))
    rows: List[dict] = []
    for item in components:
        required = float(req_by_item.get(item, 0.0))
        produced = float(prod_by_item.get(item, 0.0))
        item_type = type_by_item.get(item) or type_from_lots.get(item, "Unknown")
        uom = uom_by_item.get(item) or uom_from_lots.get(item, "KG")
        n_lots = int(nlots_by_item.get(item, 0))
        mpq_min, mpq_max = mpq_by_item.get(item, (0.0, 0.0))
        unit_note = _length_mm_unit_note(item_type, uom)
        scrap = float(aging_scrap.get(item, 0.0))

        waste_qty = produced - required
        waste_pct = (waste_qty / produced) if produced > 0 else 0.0
        if waste_pct <= 0.02:
            reason = "Pooled compound (plant-level, batch rounding)"
        elif n_lots > 1:
            reason = "Pooled compound (plant-level, MPQ-max split)"
        else:
            reason = "Pooled compound (plant-level, MPQ floor)"

        rows.append({
            "sku": POOL_SKU, "component": item, "item_type": item_type, "uom": uom,
            "required(used)": round(required, 4), "produced(run)": round(produced, 4),
            "waste_qty": round(waste_qty, 4), "waste_pct": round(waste_pct, 6),
            "mpq_min": mpq_min, "mpq_max": mpq_max, "n_lots": n_lots,
            "reason": reason, "unit_note": unit_note,
            "aging_scrap_qty": round(scrap, 4),
        })

    if not rows:
        return pd.DataFrame(columns=WASTE_COLUMNS)
    df = pd.DataFrame(rows, columns=WASTE_COLUMNS)
    df = df.sort_values(["waste_pct", "component"],
                        ascending=[False, True]).reset_index(drop=True)
    return df


def waste_summary(matrix_df: pd.DataFrame) -> dict:
    """Roll the waste matrix into headline numbers.

    Returns a dict with:
      * by_item_type : {item_type: {required, produced, waste_qty}}  (qty sums are
        NOT meaningful across UOMs, but item-type groups are usually single-UOM;
        each group also carries its dominant uom).
      * yield_by_uom : {uom: yield_pct} where yield_pct = sum(required)/sum(produced)
        per UOM family (KG separate from NOS / MTR - never summed across UOMs).
      * waste_by_uom : {uom: total_waste_qty}
      * overall      : convenience top-line (KG family, the dominant material axis):
        kg_yield_pct, kg_waste_qty, components, components_over_50pct,
        top_component (component with the largest waste_qty overall).
    """
    if matrix_df is None or matrix_df.empty:
        return {
            "by_item_type": {}, "yield_by_uom": {}, "waste_by_uom": {},
            "overall": {"kg_yield_pct": 100.0, "kg_waste_qty": 0.0,
                        "components": 0, "components_over_50pct": 0,
                        "top_component": None, "aging_scrap_qty": 0.0},
        }

    by_item_type: Dict[str, dict] = {}
    for it, g in matrix_df.groupby("item_type"):
        by_item_type[str(it)] = {
            "required": float(g["required(used)"].sum()),
            "produced": float(g["produced(run)"].sum()),
            "waste_qty": float(g["waste_qty"].sum()),
            "uom": str(g["uom"].mode().iloc[0]) if not g["uom"].mode().empty
            else str(g["uom"].iloc[0]),
        }

    yield_by_uom: Dict[str, float] = {}
    waste_by_uom: Dict[str, float] = {}
    for u, g in matrix_df.groupby("uom"):
        prod = float(g["produced(run)"].sum())
        req = float(g["required(used)"].sum())
        yield_by_uom[str(u)] = round(100.0 * req / prod, 2) if prod > 0 else 100.0
        waste_by_uom[str(u)] = round(float(g["waste_qty"].sum()), 4)

    kg_mask = matrix_df["uom"].astype(str).str.upper().isin(["KG", "KGS"])
    kg = matrix_df[kg_mask]
    kg_prod = float(kg["produced(run)"].sum())
    kg_req = float(kg["required(used)"].sum())
    kg_yield = round(100.0 * kg_req / kg_prod, 2) if kg_prod > 0 else 100.0

    top_row = matrix_df.sort_values(
        ["waste_qty", "component"], ascending=[False, True]).head(1)
    top_component = (str(top_row["component"].iloc[0]) if not top_row.empty
                     and float(top_row["waste_qty"].iloc[0]) > 0 else None)

    return {
        "by_item_type": by_item_type,
        "yield_by_uom": yield_by_uom,
        "waste_by_uom": waste_by_uom,
        "overall": {
            "kg_yield_pct": kg_yield,
            "kg_waste_qty": round(float(kg["waste_qty"].sum()), 4),
            "components": int(len(matrix_df)),
            "components_over_50pct": int((matrix_df["waste_pct"] > 0.50).sum()),
            "top_component": top_component,
            "aging_scrap_qty": round(float(matrix_df["aging_scrap_qty"].sum()), 4),
        },
    }
