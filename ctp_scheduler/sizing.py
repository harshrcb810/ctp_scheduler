"""Phase 3 - Lot sizing (MPQ).

Converts continuous demand into discrete machine-runnable lots respecting MPQ
(C5). We size per (item, curing_block) so each lot maps cleanly to a consuming
curing block - this keeps the build->cure AND-join and the aging window exact
per consumer and avoids ever minting an over-aged campaign.

Green Tyre / Carcass / FG: size 1:1 to the consuming curing block.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd

from . import config as C
from . import masters as M
from . import units
from .dag import SkuDag, OpInfo
from .ingest import SkuData, proc_to_minutes


def _sheet_spec_for(item: str, item_type: str) -> Tuple[float, float, bool]:
    """(Width_m, PerMeterWeight_kg_per_m2, estimated) for a calender product.

    Looks up a per-product override (by item code, then item-type), else falls
    back to the plant cap-ply default and flags ESTIMATED."""
    spec = C.CALENDER_SHEET_SPEC.get(item)
    if spec is None:
        spec = C.CALENDER_SHEET_SPEC.get(str(item_type).strip())
    if spec is not None:
        return float(spec[0]), float(spec[1]), False
    return (C.CALENDER_DEFAULT_WIDTH_M, C.CALENDER_DEFAULT_PERMETER_KG, True)


def _is_calender(op: OpInfo) -> bool:
    if any(m in C.CALENDER_MACHINES for m in op.machines):
        return True
    return str(op.stage).strip() in C.CALENDER_STAGES


def _is_cap_ply(item: str, item_type: str) -> bool:
    """True for the cap-ply item (the wide-sheet cap strip) whose BOM weight is
    the inflated one. Matched by item-type ("Cap Strip"/"Cap") or item-code
    prefix ("CAP"). Scoped narrowly so the MES override never touches the other
    calendered sheets (CPJ*/EG*/HTPOLY*/IL*), which carry real per-tyre weights."""
    if str(item_type).strip().lower() in C.CALENDER_CAP_ITEM_TYPES:
        return True
    code = str(item).strip().upper()
    return any(code.startswith(p) for p in C.CALENDER_CAP_ITEM_PREFIXES)


def _cap_mes_weight(item: str, sku: str) -> float:
    """MES cap-ply sheet weight kg/tyre for this cap item: a per-item or per-SKU
    override if present, else the plant MES reference (0.278 kg/tyre)."""
    tab = C.CALENDER_CAP_MES_KG
    if item in tab:
        return float(tab[item])
    if sku in tab:
        return float(tab[sku])
    return float(C.CALENDER_MES_CAP_KG_PER_TYRE)


def _is_extruder(op: OpInfo) -> bool:
    return any(m in C.EXTRUDER_MACHINES for m in op.machines)


def _is_calender_pool(item_type: str) -> bool:
    """FIX-1: True for the wide-sheet MOTHER-ROLL calender products (cap strip /
    calandared roll) that are pooled across blocks (one run feeds many tyres),
    NOT pinned 1:1 per curing block."""
    return str(item_type).strip().lower() in C.CALENDER_POOL_ITEM_TYPES


def _tyres_in_lot(item: str, lot_qty: float, uom: str, dag: SkuDag) -> float:
    """Convert a lot's qty (in its consumption UOM) into a TYRE COUNT using the
    item's per-tyre cumulative consumption captured in the DAG.

    qty_per_tyre[item] is in the BOM input unit (MM for length items, KG for
    weight items, NOS for pieces). The lot uom is MTR for length items (mm/1000),
    KG for weight, NOS for pieces. We normalise the per-tyre figure to the lot's
    unit before dividing so the ratio is a pure tyre count."""
    per = dag.qty_per_tyre.get(item)
    if not per or per <= 0:
        return max(lot_qty, 0.0)  # fallback: treat qty as tyre count
    u = str(uom).strip().upper()
    child_unit = str(dag.child_uom.get(item, "")).strip().upper()
    per_in_lot_unit = per
    # length: per is MM, lot is MTR -> convert per to metres
    if child_unit == "MM" and u in ("MTR", "MTRS", "M"):
        per_in_lot_unit = per / 1000.0
    return lot_qty / per_in_lot_unit if per_in_lot_unit > 0 else max(lot_qty, 0.0)


def calender_extruder_minutes(op: OpInfo, item: str, item_type: str,
                              lot_qty: float, uom: str, dag: SkuDag,
                              sku: str = ""):
    """Return (proc_min, estimated, basis_note) for a wide-sheet calender or an
    extruder using the plant MOTHER-ROLL / PROFILE method, or None if this op is
    not a calender/extruder (caller then uses proc_to_minutes unchanged).

    Calender:  run_m_per_tyre = weight_kg_per_tyre / (Width * PerMeterWeight)
               proc_min = tyres * run_m_per_tyre / line_speed
    Extruder:  proc_min = tyres * profile_m_per_tyre / line_speed
               (sidewall already carries a BOM profile length -> left to
                proc_to_minutes; only TREAD, which has no BOM profile, is
                re-based here with an ESTIMATED plant-typical profile length.)
    """
    if not C.CALENDER_REBASE_ENABLED:
        return None
    line_speed = op.proc_time if (op.proc_time and op.proc_time > 0) else 24.3
    tyres = _tyres_in_lot(item, lot_qty, uom, dag)

    if _is_calender(op):
        weight = dag.item_weight_kg_per_tyre.get(item)
        if not weight or weight <= 0:
            return None  # no sheet weight -> cannot re-base; fall back
        width, permeter, est_spec = _sheet_spec_for(item, item_type)
        denom = width * permeter
        if denom <= 0:
            return None
        # FIX-3: for the cap-ply item, replace the inflated BOM weight with the
        # MES cap-ply sheet weight (0.278 kg/tyre) so the charge is honest and
        # the WEIGHT_INFLATED flag clears. Scoped to cap-ply only.
        mes_override = False
        bom_weight = weight
        if (C.CALENDER_CAP_MES_OVERRIDE_ENABLED
                and _is_cap_ply(item, item_type)):
            weight = _cap_mes_weight(item, sku)
            mes_override = True
        run_m_per_tyre = weight / denom
        proc_min = tyres * run_m_per_tyre / line_speed
        # INFLATED-weight flag: only if we are STILL charging a weight that is
        # >ratio x the MES reference (i.e. no MES override applied).
        inflated = weight > C.CALENDER_MES_CAP_KG_PER_TYRE * C.CALENDER_BOM_INFLATION_FLAG_RATIO
        note = (f"CALENDER mother-roll run={run_m_per_tyre:.4f}m/tyre "
                f"w={weight:.4f}kg/tyre W={width}xPMW={permeter}"
                + (f" MES_CAP_OVERRIDE(BOM={bom_weight:.4f}kg)" if mes_override else "")
                + (" SPEC_ESTIMATED" if est_spec else "")
                + (" WEIGHT_INFLATED_vs_MES" if inflated else ""))
        # estimated whenever the sheet spec is a default OR the weight is the
        # inflated BOM fallback (data gap must stay auditable). An MES-overridden
        # cap charge is honest -> not flagged estimated on the weight axis.
        estimated = est_spec or inflated
        return (proc_min, estimated, note)

    if _is_extruder(op):
        # Only re-base the TREAD under-charge (no BOM profile length). Sidewall &
        # any extruded item that carries a real BOM MM profile is left to
        # proc_to_minutes (already correct - do not over-engineer).
        is_tread = str(item_type).strip().lower() in C.TREAD_ITEM_TYPES
        has_bom_profile = bool(dag.bom_len_mm.get(item))
        if is_tread and not has_bom_profile:
            profile = C.TREAD_PROFILE_M_PER_TYRE
            proc_min = tyres * profile / line_speed
            note = f"EXTRUDER tread profile={profile}m/tyre ESTIMATED (no BOM length)"
            return (proc_min, True, note)
        return None  # sidewall / profile-bearing extruded item: unchanged

    return None


# MPQ MASTER OVERRIDE (config.USE_MPQ_TRANSFER_MASTERS): when ON, the plant MPQ
# master (per line, per item-type) is the SOLE source of MPQ. The per-SKU MPQ
# sheet, C.MPQ_TYPE_DEFAULTS and the generic DEFAULT_MPQ [1,99999] are IGNORED.
# MAX is UNBOUNDED (0.0 -> no cap; C5's `if mx and mx > 0` never fires). MIN is
# resolved per item-type (numeric / compound 3xbatch / calandered roll length).
def _master_mpq(item_type: str, item_code: str, line: str,
                machine_names) -> "M.MpqResolution":
    """Master MPQ resolution for one item, or None when the master is off / the
    type is absent for this line (caller then keeps the legacy resolution)."""
    if not C.USE_MPQ_TRANSFER_MASTERS:
        return None
    return M.resolve_mpq(line or "PCR", item_type, item_code=item_code,
                         machine_names=list(machine_names or []))


def _resolve_transfer(item_type: str, line: str, fallback: float) -> float:
    """Transfer minutes for (line, item-type). When the Transfer master is on it
    is the SOLE source (PCR/TBR column by line); item-types absent from the
    master default to C.TRANSFER_MIN (10). Off -> the routing/flat fallback."""
    if not C.USE_MPQ_TRANSFER_MASTERS:
        return fallback
    res = M.resolve_transfer(line or "PCR", item_type)
    if res is None:
        return fallback
    return res[0]


def _mpq_for(item_type: str, mpq: Dict[str, Tuple[float, float]],
             item_code: str = "", line: str = "PCR",
             machine_names=None) -> Tuple[float, float]:
    """Resolve (mpq_min, mpq_max) for an item-type.

    When USE_MPQ_TRANSFER_MASTERS is True the plant MPQ master wins and the MAX
    is UNBOUNDED (0.0). Otherwise the legacy order:
      1. the SKU's own MPQ sheet (legitimate, measured) - never overridden;
      2. C.MPQ_TYPE_DEFAULTS - bounded ESTIMATED fallback for the 5 item-types
         that are produced but absent from every per-SKU MPQ sheet;
      3. the generic, non-binding DEFAULT_MPQ [1, 99999] (last resort).
    """
    res = _master_mpq(item_type, item_code, line, machine_names)
    if res is not None:
        return (res.mn, res.mx)
    key = str(item_type).strip().lower()
    if key in mpq:
        return mpq[key]
    if key in C.MPQ_TYPE_DEFAULTS:
        return C.MPQ_TYPE_DEFAULTS[key]
    return (C.DEFAULT_MPQ_MIN, C.DEFAULT_MPQ_MAX)


def _mpq_uom_for(item_type: str, mpq: Dict[str, Tuple[float, float]],
                mpq_uom: Dict[str, str], item_code: str = "",
                line: str = "PCR", machine_names=None) -> str:
    """The UOM the MPQ bounds for ``item_type`` are STATED in.

    The MPQ sheet's own UOM column wins (when the type is in the per-SKU sheet);
    otherwise the ESTIMATED/default fallbacks (C.MPQ_TYPE_DEFAULTS) are stated in
    the convention documented in config: KG for mass sub-batches (small chemical
    / apex), MTR for length stock (steel-belt edge / slitted / pre-cut roll). For
    the generic [1, 99999] default the bound is treated as NOS (count), so a
    count item is never wrongly scaled.

    When the MPQ master is on, the bound UOM comes from the master resolution
    (KG for compounds, MTR for calandered rolls, the row's min_uom otherwise)."""
    res = _master_mpq(item_type, item_code, line, machine_names)
    if res is not None:
        return res.uom
    key = str(item_type).strip().lower()
    if key in mpq and key in mpq_uom:
        return mpq_uom[key]
    if key in C.MPQ_TYPE_DEFAULTS:
        # fallbacks documented in config: KG for the mass types, MTR for length.
        if key in ("steel belt edge strip", "slitted material",
                   "pre cut roll material"):
            return "MTR"
        return "KG"
    return "NOS"


def mpq_in_lot_uom(item_type: str, lot_uom: str,
                   mpq: Dict[str, Tuple[float, float]],
                   mpq_uom: Dict[str, str], item_code: str = "",
                   line: str = "PCR", machine_names=None) -> Tuple[float, float]:
    """FIX B: return (mpq_min, mpq_max) RE-EXPRESSED IN THE LOT'S UOM.

    The raw MPQ bounds may be STATED in a different unit of the SAME dimension
    than the lot qty (length: MTR bound vs MM lot; mass: KG vs MT). Comparing the
    raw numbers disables the floor by up to 1000x. We reduce BOTH the bound and
    the lot unit to the canonical unit of their shared dimension, then re-scale
    the bound back into the LOT's unit so the existing `max(qty, mpq_min)` floor
    is unit-correct WITHOUT changing the stored/displayed lot UOM (outputs keep
    MM/MTR as-is). Cross-dimension or unknown units fall back to the raw bounds
    (and are surfaced by the C5 validator), never silently 1000x-wrong.
    """
    mn, mx = _mpq_for(item_type, mpq, item_code=item_code, line=line,
                      machine_names=machine_names)
    b_uom = _mpq_uom_for(item_type, mpq, mpq_uom, item_code=item_code,
                         line=line, machine_names=machine_names)
    try:
        # factor that turns ONE bound-unit into ONE lot-unit (same dimension).
        bound_canon, b_dim = units.to_canonical(1.0, b_uom)
        lot_canon, l_dim = units.to_canonical(1.0, lot_uom)
        if b_dim != l_dim or lot_canon == 0:
            return mn, mx                      # cross-dimension -> leave raw
        scale = bound_canon / lot_canon        # bound-unit -> lot-unit
    except units.UnknownUomError:
        return mn, mx                          # unknown unit -> leave raw (C5 flags)
    return mn * scale, mx * scale


def _mpq_source(item_type: str, mpq: Dict[str, Tuple[float, float]],
                item_code: str = "", line: str = "PCR",
                machine_names=None) -> str:
    """Provenance of the MPQ bounds for ``item_type`` (for the lot audit):
    ``"master"`` (plant MPQ master, when USE_MPQ_TRANSFER_MASTERS), else
    ``"sheet"`` (real per-SKU MPQ), ``"estimated"`` (C.MPQ_TYPE_DEFAULTS - needs
    plant sign-off), or ``"default"`` (generic [1,99999], still non-binding)."""
    res = _master_mpq(item_type, item_code, line, machine_names)
    if res is not None:
        return res.source
    key = str(item_type).strip().lower()
    if key in mpq:
        return "sheet"
    if key in C.MPQ_TYPE_DEFAULTS:
        return "estimated"
    return "default"


def _aging_for(item: str, item_type: str, data: SkuData) -> Tuple[float, float]:
    key = str(item_type).strip().lower()
    # PLANT AGING MASTER (data/aging_master.csv, from the plant screenshots) is
    # AUTHORITATIVE when enabled - it overrides the measured per-item row, the L3
    # table, the compound override and the 8h green-tyre cure-by for every listed
    # item-type. Unlisted types (raw materials) fall through to the old logic.
    if C.USE_PLANT_AGING and key in C.PLANT_AGING_BY_TYPE:
        return C.PLANT_AGING_BY_TYPE[key]
    # An item's OWN measured Aging-Master row wins, then green-tyre, then the L3
    # per-item-type table, then default.
    if item in data.aging:
        amin, amax = data.aging[item]
    elif key in ("green tyre", "green tyres"):
        return (C.GREEN_TYRE_CUREBY_MIN_H, C.GREEN_TYRE_CUREBY_MAX_H)
    elif key in C.SHELF_LIFE_BY_TYPE:
        amin, amax = C.SHELF_LIFE_BY_TYPE[key]
    else:
        amin, amax = (C.DEFAULT_AGING_MIN_H, C.DEFAULT_AGING_MAX_H)
    # PLANT COMPOUND-MAX OVERRIDE: all compounds genuinely hold 72h, so force the
    # MAX (scorch) band to the certified value even over a tighter measured (e.g.
    # 24h) row. MIN side untouched. None -> measured-row-wins (old behaviour).
    if (C.COMPOUND_AGING_MAX_OVERRIDE_H is not None
            and key in C.COMPOUND_AGING_TYPES):
        amax = C.COMPOUND_AGING_MAX_OVERRIDE_H
    return (amin, amax)


def _buffer_for(item_type: str, data: SkuData) -> float:
    key = str(item_type).strip().lower()
    if key in data.buffer:
        return data.buffer[key]
    return C.DEFAULT_BUFFER_H


# Item types kept STRICTLY 1:1 per curing block (one physical unit per cured
# tyre set) - never campaign-merged across blocks.
# Steel belt and tread are per-tyre block inputs (each green tyre needs its own
# belt/tread package built to that tyre's dimensions) -> pinned 1:1 per block.
# FIX-1: the cap strip (and the wide-sheet "CALANDARED ROLL") are NO LONGER here.
# They are made on the single 4-Roll Calender (901) as a wide MOTHER ROLL that
# feeds MANY tyres (one ~13-min run, ~111 rolls/day, 97% idle). Pinning them 1:1
# per block minted a tiny ALAP lot per press that aged out before its GT built
# (thousands UNREACHED/INFEASIBLE_AGING). Removing them lets Phase-3 campaign-
# merge (_campaigns) pool their demand across consecutive curing blocks INSIDE
# the aging window (span_max = (amax-amin)*60 - transfer), so one mother-roll-
# sized lot is made ahead and drawn as stock. C.CALENDER_POOL_ITEM_TYPES lists
# the pooled types. Green Tyre / Carcass / FG stay strictly 1:1 (never pooled).
_PER_BLOCK_TYPES = (
    "green tyre", "green tyres", "carcass", "finished goods",
    "steel belt", "tread",
)


def _campaigns(buckets: List[Tuple[str, float, float]], mpq_max: float,
               span_max_min: float) -> List[dict]:
    """Merge consecutive-deadline demand buckets into campaigns.

    buckets: sorted list of (block_id, deadline_min, qty). A campaign grows while
    running qty <= mpq_max AND (deadline_span <= span_max_min). Anchors on the
    EARLIEST (tightest) deadline/block it covers. Deterministic (input sorted).
    """
    camps: List[dict] = []
    cur = None
    for block, deadline, qty in buckets:
        if cur is None:
            cur = {"block": block, "deadline": deadline, "qty": qty,
                   "blocks": [block]}
            continue
        span_ok = (deadline - cur["deadline"]) <= span_max_min
        qty_ok = (mpq_max <= 0) or (cur["qty"] + qty <= mpq_max)
        if span_ok and qty_ok:
            cur["qty"] += qty
            cur["blocks"].append(block)
        else:
            camps.append(cur)
            cur = {"block": block, "deadline": deadline, "qty": qty,
                   "blocks": [block]}
    if cur is not None:
        camps.append(cur)
    return camps


# UOMs that are DISCRETE physical pieces and therefore must be whole-number lot
# quantities (you cannot make 34.5 bead-apex pieces). Everything else (KG, MTR,
# M, MM - mass / length) is CONTINUOUS and may carry a fractional lot qty.
# Lower-cased match. NOS/PCS/EA are the discrete piece units in CTP routing data.
DISCRETE_UOMS = ("nos", "pcs", "ea", "pc", "no", "nos.", "each")


def _is_discrete_uom(uom: str) -> bool:
    """True for a discrete piece UOM (NOS/PCS/EA...), whose lot quantities MUST be
    integers. False for continuous mass/length UOMs (KG/MTR/M/MM)."""
    return str(uom).strip().lower() in DISCRETE_UOMS


def _integer_partition(total_int: int, n: int) -> List[int]:
    """Split a whole-number ``total_int`` into ``n`` BALANCED positive integers
    whose sum is EXACTLY ``total_int``. The first ``r = total_int mod n`` parts
    get ceil(total/n), the rest get floor(total/n) - the unique balanced split.
    Deterministic (no RNG; remainder distributed by sorted/leading order).

    Pre: total_int >= 1, n >= 1, n <= total_int (caller guarantees a lot count
    that does not force a sub-1 piece). Returns a list of length ``n``."""
    n = max(1, int(n))
    total_int = int(total_int)
    base = total_int // n
    rem = total_int - base * n           # 0 <= rem < n
    # leading `rem` lots get one extra piece (deterministic, balanced)
    return [base + 1] * rem + [base] * (n - rem)


def _discrete_lot_qtys(qty: float, mpq_min: float, mpq_max: float,
                       n_hint: int) -> List[int]:
    """Integer lot quantities for a DISCRETE-piece campaign whose continuous size
    is ``qty`` (already MPQ-aware via ``n_hint``), so SUM(lots) >= ceil(qty)
    (never under-produce) and every lot is within [mpq_min, mpq_max] (C5 exact).

    BUG-1 FIX: the continuous sizer split a campaign into ``n_hint`` equal
    FRACTIONAL parts (e.g. 34.5 NOS). For a discrete item we instead produce the
    SAME demand as a balanced INTEGER partition. Algorithm (deterministic):
      total = ceil(qty)                         # cover demand, never under
      n     = max(n_hint, ceil(total/mpq_max))  # enough lots so none breaks MAX
              clamped so n <= total              # never force a sub-1 lot
      parts = balanced integer partition of total into n
      then: if the floor part < mpq_min, MERGE/REBALANCE is impossible without
      breaching MAX, so we re-floor by reducing n until min holds, and if the
      whole total is below mpq_min we floor the single lot to mpq_min ONCE
      (mirrors _pack_batches' sparse-window rule; the over-build is the
      irreducible MPQ-min residual). All integer, all in [min,max].
    """
    eps = 1e-9
    if qty <= eps:
        return []
    total = int(math.ceil(qty - eps))
    if total < 1:
        total = 1
    mn = int(math.ceil(mpq_min - eps)) if (mpq_min and mpq_min > 0) else 1
    mn = max(1, mn)
    mx = int(math.floor(mpq_max + eps)) if (mpq_max and mpq_max > 0) else 0
    # whole campaign below the runnable minimum: floor ONCE (C5 MIN, irreducible).
    if total < mn:
        return [mn]
    # Lot count: enough lots so every lot is <= MAX, but at least the continuous
    # hint. This MIRRORS the continuous sizer (n = ceil(qty/mx)), so discrete and
    # continuous produce the same number of lots for the same campaign.
    n = max(1, int(n_hint))
    if mx and mx > 0:
        n = max(n, int(math.ceil(total / mx - eps)))
    n = min(n, total)            # never force a sub-1-piece lot
    parts = _integer_partition(total, n)
    # C5 MIN: a balanced part may dip below MIN when (total/n) < mn (the hint
    # inflated n). The continuous sizer handles this by FLOORING each such lot up
    # to mn individually (max(base, mn)), accepting a small bounded over-build -
    # NEVER by over-FILLING a lot above MAX. We mirror that exactly: raise any
    # below-min part to mn. With mn <= mx this keeps every lot in [mn, mx]; the
    # sum still covers demand (it can only grow). Deterministic (pure integer).
    if mn > 1:
        parts = [p if p >= mn else mn for p in parts]
    return parts


def _pack_batches(qty: float, mpq_min: float, mpq_max: float) -> List[float]:
    """Size a SINGLE campaign's total ``qty`` into the FEWEST runnable batches,
    every batch within [mpq_min, mpq_max] (C5 EXACT - never below min, never above
    max), with total over-production bounded to AT MOST one partial batch.

    Compound waste fix (cross-SKU pooling): the per-campaign sizer must pack the
    POOLED total into the fewest 840-KG (mpq_max) batches, NOT mint one mpq_min
    (210-KG) batch per demand fragment. The minimal C5-safe packing is:

        n = max(1, ceil(qty / mpq_max))          # fewest batches that hold qty
        each batch = qty / n                      # even split -> all <= mpq_max

      * For qty >= mpq_min the even split keeps every batch in [mpq_min, mpq_max]
        and produced == qty EXACTLY (zero waste). This is provably the fewest
        batches: n-1 batches max out at (n-1)*mpq_max < qty, so n is required; and
        an even split is the unique way to keep the last (remainder) batch from
        falling below mpq_min while not exceeding mpq_max (since mpq_max >= 2*mpq_min
        for the compounds, qty/n > mpq_max/2 >= mpq_min). Absorbing a sub-min tail
        by OVER-filling a batch is rejected - that would breach C5 MPQ_ABOVE_MAX.

      * If the WHOLE campaign qty is below mpq_min (a sparse aging-window whose
        demand cannot be merged with neighbours without over-aging - C2), it
        floors to mpq_min ONCE. That single floored batch per starved span-window
        is the irreducible, aging-forced residual (NOT a fresh min-batch per
        fragment, which was the old waste).

    Returns the per-batch qty list. Deterministic: pure arithmetic.
    """
    eps = 1e-9
    if qty <= eps:
        return []
    if qty + eps < mpq_min:
        # sparse window below the runnable minimum: floor ONCE (C5). Irreducible
        # aging-forced over-production (cannot merge forward without scorch - C2).
        return [mpq_min]
    if mpq_max and mpq_max > 0:
        n = max(1, int(math.ceil(qty / mpq_max - eps)))
    else:
        n = 1  # unbounded campaign (span-only pooled run): one lot.
    base = qty / n
    # even split -> every batch in [mpq_min, mpq_max], produced == qty (no waste).
    return [base] * n


def make_lots(data: SkuData, dag: SkuDag, demand: pd.DataFrame) -> pd.DataFrame:
    """Return lot rows (one row per lot) - SCHEMA_LOT (partial; windows filled
    in Phase 4). Components/compounds are CAMPAIGN-MERGED across consecutive
    curing blocks inside the aging window (spec Phase 3); GT/Carcass/FG stay
    1:1 per block."""
    lots: List[dict] = []
    seq = 0
    # The FG's making op is op-210 == the DRUM, which is already pinned 1:1 from
    # Curing_Sch_PCR.csv (C6). The FG must therefore NEVER be dispatched as a
    # separate op-210 lot (that would synthesize curing on presses, parallel to
    # the real drum rows). The green tyre (op-200, the FG's sole child) is the
    # true build that feeds the drum, so we re-anchor its consumer to the drum
    # block directly (consumer_item = "" -> drum-fed). This makes the build->cure
    # anchor and the 6h GT cure-by band apply to the GT vs the real op-210 start.
    drum_items = {it for it, op in dag.ops.items()
                  if op is not None and op.op_seq == C.OP_CURING}

    # ERROR-1 FIX (TBM double-count): a CARCASS item is made on op-195 and is
    # consumed ONLY by an op-200 (green-tyre) build. Op-195 and op-200 share the
    # SAME PCR TBM pool (BUILD_OPS); emitting BOTH as dispatchable lots charges
    # the 11-TBM pool TWICE for one physical two-stage build. We therefore make
    # the carcass a NON-DISPATCHED node (like a raw-material leaf): it is skipped
    # in the lot loop (its carcass->GT BOM edge and the GT cure-by anchor are
    # untouched - the DAG/topo/qty-per-tyre sweep stay as-is), and its per-tyre
    # build minutes are FOLDED into the GT lot so the pool is charged ONCE for the
    # combined build. carcass_gt_child: carcass item -> its GT consumer.
    carcass_items: set = set()
    carcass_gt_child: Dict[str, str] = {}
    for it, op in dag.ops.items():
        if op is None or op.op_seq != C.OP_CARCASS_BUILD:
            continue
        made_cons = [c for c in _all_consumers_of(it, dag)
                     if c and (dag.ops.get(c) is not None)]
        if len(made_cons) != 1:
            continue
        gt = made_cons[0]
        gt_op = dag.ops.get(gt)
        if gt_op is not None and gt_op.op_seq == C.OP_BUILD:
            carcass_items.add(it)
            carcass_gt_child[gt] = it
    # GT item -> carcass making op, for folding the carcass cycle into the GT proc
    gt_carcass_op: Dict[str, OpInfo] = {
        gt: dag.ops[carc] for gt, carc in carcass_gt_child.items()
        if dag.ops.get(carc) is not None
    }
    # REGRESSION FIX (carcass-fold cascade): the carcass (op-195) is folded into
    # its GT (op-200) and is NEVER dispatched as a standalone lot. But the
    # carcass's OWN children (inner-liner / ply / sidewall / bead-apex etc.)
    # resolve their consumer to the carcass via the BOM graph. With the carcass
    # un-dispatched, those producers anchored to a consumer lot that is never
    # placed -> the dispatcher reports them UNREACHED and the whole sub-chain
    # cascades (16k UNREACHED, 0% OTIF). Since the carcass build is physically
    # absorbed into the GT build, its consumed children are in reality consumed
    # by the GT lot. Re-point any consumer edge that lands on a folded carcass to
    # that carcass's GT, so the children anchor to a lot that IS dispatched.
    folded_carcass_to_gt: Dict[str, str] = {
        carc: gt for gt, carc in carcass_gt_child.items()
    }

    def _made_consumers(it: str) -> List[str]:
        """BUG-04 FIX: an item may feed SEVERAL made consumers (e.g. a calendered
        roll CPJ* feeding B1-/B2- belt builds, or a final compound feeding two
        sub-assemblies). The old `_consumer_of` returned only sorted(preds)[0],
        anchoring the item to ONE consumer and orphaning the others - so the
        item under-produced and the un-anchored consumer chain went UNREACHED.
        We now return EVERY made consumer (deterministically sorted) and emit one
        lot-set PER consuming edge, each anchored to its own consumer. Drum/FG
        consumers collapse to "" (drum-fed)."""
        cs = _all_consumers_of(it, dag)
        out: List[str] = []
        for c in cs:
            # REGRESSION FIX: a folded carcass is never dispatched; its children
            # are physically consumed by the GT it folds into. Re-point the edge
            # to the GT so those producers anchor to a real (dispatched) lot
            # instead of going UNREACHED.
            c = folded_carcass_to_gt.get(c, c)
            cc = "" if c in drum_items else c
            # keep only consumers that are either drum-fed or themselves made
            # (RM/leaf parents are not scheduled, so an edge to them is inert and
            # would create a phantom consumer that is never placed).
            if cc == "" or cc in dag.ops:
                out.append(cc)
        # de-dup while preserving sorted order; if nothing usable, treat as drum
        seen = set()
        uniq = []
        for c in out:
            if c not in seen:
                seen.add(c)
                uniq.append(c)
        return uniq if uniq else [""]

    consumers_of = {it: _made_consumers(it) for it in demand["item"].unique()}

    for item, gi in demand.groupby("item", sort=True):
        op = dag.ops.get(item)
        if op is None:
            continue  # not a made item
        if op.op_seq == C.OP_CURING:
            # FG/curing op: NOT a dispatchable lot - the drum row is the anchor.
            continue
        if item in carcass_items:
            # ERROR-1 FIX: carcass (op-195) is folded into its GT (op-200) lot so
            # the shared TBM pool is charged ONCE for the two-stage build. Skip
            # emitting a separate dispatchable carcass lot (it becomes a
            # non-dispatched node; its carcass->GT BOM edge stays intact).
            continue
        item_type = gi["item_type"].iloc[0]
        # FIX-5: bulk compounds (final/master/small-chemical) are POOLED across ALL
        # SKUs by item code and emitted ONLY by sizing.merge_cross_sku_campaigns in
        # the pipeline. Skip them here so they are never double-emitted (and never
        # charged the MPQ-min floor once per SKU).
        # L2: the wide-sheet calendered MOTHER ROLLS (cap strip / CALANDARED ROLL)
        # are ALSO pooled cross-SKU now (one physical mother roll per shelf-window
        # serves every consuming SKU), so they too are emitted ONLY in the pool
        # pass and skipped here.
        _itl = str(item_type).strip().lower()
        # CROSS_SKU_POOL_ENABLED gate: when OFF, the bulk compounds are NOT pooled
        # cross-SKU - they fall through to the normal per-SKU campaign sizing below
        # (the ~63% full-drum state). When ON they are emitted ONLY by
        # merge_cross_sku_campaigns and skipped here. The L2 calender cross-SKU set
        # is gated independently by L2_CALENDER_CROSS_SKU_POOL.
        if (_itl in C._pooled_cross_sku_types()
                or (C.L2_CALENDER_CROSS_SKU_POOL
                    and _itl in C.POOLED_CROSS_SKU_CALENDER_TYPES)):
            continue
        uom = gi["uom"].iloc[0]
        line_cls = "PCR"   # all CTP SKUs are PCR today; TBR rows supported in master
        # FIX B: MPQ bounds RE-EXPRESSED in the lot's UOM (canonical compare), so
        # the floor binds even when the bound is stated MTR/KG and the lot carries
        # a different unit of the same dimension. Stored qty stays in lot UOM.
        # MASTERS: resolved from the plant MPQ master (per line, per item-type) -
        # item_code + the op's mixing machine pool feed the compound/roll rules.
        mn, mx = mpq_in_lot_uom(item_type, uom, data.mpq, data.mpq_uom,
                                item_code=item, line=line_cls,
                                machine_names=op.machines)
        mpq_src = _mpq_source(item_type, data.mpq, item_code=item,
                              line=line_cls, machine_names=op.machines)
        amin, amax = _aging_for(item, item_type, data)
        buf = _buffer_for(item_type, data)
        # TRANSFER: per (line, item-type) from the Transfer master when ON, else
        # the routing transfer_time_min / flat default (legacy).
        transfer = _resolve_transfer(item_type, line_cls,
                                     op.transfer_min or C.TRANSFER_MIN)
        is_per_block = str(item_type).strip().lower() in _PER_BLOCK_TYPES
        is_cal_pool = _is_calender_pool(item_type)
        # MASTERS: a calandered-roll / compound run sized to the master spool /
        # 3-batch MIN can legitimately exceed any per-tyre cap -> pooling_exempt.
        master_pool_exempt = False
        if C.USE_MPQ_TRANSFER_MASTERS:
            _res = M.resolve_mpq(line_cls, item_type, item_code=item,
                                 machine_names=list(op.machines or []))
            if _res is not None:
                master_pool_exempt = _res.pooling_exempt

        # demand buckets per (block, deadline), sorted by deadline then block
        agg = (gi.groupby(["source_curing_block", "curing_deadline"])
               ["required_qty"].sum().reset_index()
               .sort_values(["curing_deadline", "source_curing_block"]))
        buckets = [(r["source_curing_block"], float(r["curing_deadline"]),
                    float(r["required_qty"])) for _, r in agg.iterrows()]

        if is_per_block:
            camps = [{"block": b, "deadline": d, "qty": q, "blocks": [b]}
                     for (b, d, q) in buckets]
        else:
            # span budget: how far apart deadlines may be merged without over-aging
            span_max = max(0.0, (amax - amin) * 60.0 - transfer)
            # FIX-1: wide-sheet calender MOTHER-ROLL products pool by SPAN only.
            # The per-tyre MPQ max (e.g. cap strip 214 MTR) is a narrow-component
            # cap, NOT the mother-roll size; using it as the campaign qty cap
            # would re-split every window back into ~13 tiny ALAP lots per block
            # (the original failure). A pooled calender campaign uses
            # CALENDER_POOL_MAX_QTY (0 = unbounded within the span) so ONE
            # mother-roll-scale run covers every tyre in the aging window; its
            # re-based proc stays tiny. Other (non-calender) components keep the
            # real MPQ-bounded campaign.
            cap_qty = C.CALENDER_POOL_MAX_QTY if is_cal_pool else mx
            camps = _campaigns(buckets, cap_qty, span_max)

        # BUG-04 / FIX-3a: a multi-consumer item is a SHARED physical material
        # (one calendered roll / final-compound batch is slit/drawn for ALL
        # consumers of the same item code in the same window). The demand
        # `required_qty` already SUMS every consumer's per-tyre pull, so we must
        # size the campaign TOTAL ONCE and then ANCHOR the resulting physical lots
        # across the consuming edges - NOT emit a full MPQ-floored lot-set per
        # edge. The old per-edge split sized qty/n_edges and floored EACH half to
        # MPQ-min, so a 236 MTR roll feeding two belt builds became 2 x 200 MTR
        # (C5 floor charged twice) - pure over-production. Sizing the total once
        # and round-robin-anchoring keeps every producer lot reachable (each
        # anchors to a real consumer); a consumer beyond the lot count simply
        # shares a pooled lot (it still dispatches - it does not require its own
        # anchored producer). Determinism preserved (cons_edges sorted, seq order).
        cons_edges = consumers_of.get(item, [""])
        n_edges = len(cons_edges)
        # CONTINUOUS edge cursor across this item's campaigns/lots so a stream of
        # single-lot campaigns (e.g. a pooled calender mother-roll, n_lots==1 per
        # window) still distributes its anchors across EVERY consuming edge rather
        # than always landing on edge 0 (which would leave the other consumers'
        # input un-anchored). Deterministic (cons_edges sorted, cursor sequential).
        edge_cursor = 0

        for camp in camps:
            qty = camp["qty"]
            if qty <= 0:
                continue
            deadline = camp["deadline"]
            block = camp["block"]
            # size the TOTAL campaign qty once (C5 MPQ on the whole, not per edge).
            # FIX-1: a pooled wide-sheet calender campaign is ONE mother-roll run
            # feeding every tyre in the window, so it is NOT re-split to the
            # per-tyre MPQ max (that would un-do the pooling). Its re-based proc
            # (mother-roll method) stays tiny even for a window's worth of tyres.
            if is_cal_pool:
                n_lots = 1
            else:
                n_lots = (math.ceil(qty / mx)
                          if (mx and mx > 0 and qty > mx) else 1)
            base = qty / n_lots
            cont_lot_qty = max(base, mn) if base < mn else base
            # BUG-1 FIX: DISCRETE-piece items (NOS/PCS/EA) must carry WHOLE-number
            # lot quantities - you cannot make 34.5 bead-apex pieces. Replace the
            # n equal fractional parts with a balanced INTEGER partition of
            # ceil(qty) so SUM(lots) >= demand (never under) and every lot stays
            # in [mpq_min, mpq_max] (C5). Continuous items (KG/MTR) are UNCHANGED:
            # they keep the fractional even split. Pooled calender mother rolls are
            # MTR (continuous) and never discrete, so this never perturbs them.
            if (not is_cal_pool) and _is_discrete_uom(uom):
                lot_qtys = [float(q)
                            for q in _discrete_lot_qtys(qty, mn, mx, n_lots)]
            else:
                lot_qtys = [cont_lot_qty] * n_lots
            for lot_qty in lot_qtys:
                    # anchor this physical lot to one consumer edge (round-robin
                    # via a CONTINUOUS cursor, deterministic). When the lot count
                    # is < n_edges across all campaigns the remaining consumers
                    # share the pooled lots (still dispatchable).
                    consumer_item = cons_edges[edge_cursor % n_edges]
                    edge_cursor += 1
                    seq += 1
                    lot_id = f"{data.sku}-{_short(item)}-{int(deadline)}-{seq:05d}"
                    rebase = calender_extruder_minutes(
                        op, item, item_type, lot_qty, uom, dag, sku=data.sku)
                    if rebase is not None:
                        proc_min, estimated, _basis = rebase
                    else:
                        proc_min, estimated = proc_to_minutes(
                            op.proc_time, op.proc_uom, op.batch_size, lot_qty,
                            bom_len_mm=dag.bom_len_mm.get(item), qty_uom=uom,
                        )
                    # ERROR-1 FIX: fold the carcass (op-195) per-tyre build cycle
                    # into THIS GT (op-200) lot's proc, so the shared TBM pool is
                    # charged ONCE for the combined two-stage build. Added BEFORE
                    # the whole-minute ceil below so the combined run rounds once.
                    # GTCT EVIDENCE: build_cycle_sec is the COMPLETE per-tyre
                    # build rate (one GT off the TBM per cycle, carcass plies
                    # built INSIDE it). ingest stamps that full cycle onto BOTH
                    # op-195 and op-200, so adding carc_min here charges the TBM
                    # pool the FULL cycle TWICE -> ~2x undercount of building
                    # capacity. CARCASS_FOLD_CHARGES_TBM defaults False: the
                    # carcass adds 0 extra TBM minutes (already inside the rate).
                    # The carcass is still folded for mass-balance/pegging and
                    # stays a non-dispatched node; only the double charge is gone.
                    carc_op = gt_carcass_op.get(item)
                    if carc_op is not None:
                        carc_min, carc_est = proc_to_minutes(
                            carc_op.proc_time, carc_op.proc_uom,
                            carc_op.batch_size, lot_qty,
                            bom_len_mm=dag.bom_len_mm.get(carc_op.item),
                            qty_uom=uom,
                        )
                        if C.CARCASS_FOLD_CHARGES_TBM:
                            proc_min += carc_min
                        estimated = estimated or carc_est
                    proc_eff = proc_min / (op.efficiency or C.EFFICIENCY)
                    # Round effective run-time UP to whole minutes so fractional
                    # M/MIN/SEC-BATCH proc math cannot accumulate sub-minute drift
                    # that lands an ALAP lot microscopically over a hard aging band
                    # (Round-2 MARGINAL C2 fix). Conservative: never under-runs.
                    if C.ROUND_PROC_TO_WHOLE_MIN:
                        proc_eff = float(math.ceil(proc_eff - 1e-9))
                    lots.append({
                        "lot_id": lot_id, "sku": data.sku, "item": item,
                        "item_type": item_type, "qty": lot_qty, "uom": uom,
                        "op_seq": op.op_seq, "stage": op.stage,
                        "machines": ",".join(op.machines),
                        "proc_min": proc_min, "proc_eff_min": proc_eff,
                        "aging_min_h": amin, "aging_max_h": amax, "buffer_h": buf,
                        "transfer_min": transfer,
                        "curing_deadline": deadline,
                        "consumer_item": consumer_item,
                        "is_bottleneck": op.stage == C.STAGE_FINAL_MIX,
                        "est": float("nan"), "lst": float("nan"),
                        "slack_min": float("nan"), "infeasible_flag": False,
                        "estimated_proc": estimated, "line_class": "PCR",
                        "parent_lot_ids": "", "source_curing_block": block,
                        # TASK B: record applied MPQ bounds + provenance so the
                        # data gap stays VISIBLE (estimated fallbacks flagged).
                        "mpq_min": mn, "mpq_max": mx,
                        "mpq_source": mpq_src,
                        # BUG-2 (traceability): the carcass (op-195) this GT folds
                        # in is charged ONCE inside the GT proc above, but is never
                        # a dispatchable item -> mass-balance/pegging saw it as 0%
                        # produced. Carry its item code (and UOM) on the GT lot so
                        # dispatch can emit a 0-DURATION, MACHINE-BLANK companion
                        # "produced" row at the GT build end - counted in produced
                        # sums but EXCLUDED from machine overlap (C4) and TBM
                        # capacity (no extra minutes). Empty when no folded carcass.
                        "carcass_item": (carc_op.item if carc_op is not None
                                         else ""),
                        "carcass_uom": uom,
                        # FIX-5: a pooled wide-sheet calender mother-roll lot is ONE
                        # physical roll feeding many tyres, so its qty INTENTIONALLY
                        # exceeds the per-tyre MPQ_max (that cap is a narrow-strip
                        # lot cap, not the mother-roll size). Flag it so a validator
                        # / our own C5 check distinguishes intentional pooling from a
                        # real MPQ breach. Per-tyre / per-block items (Green Tyre,
                        # Carcass, Bead Apex NOS pieces, ...) are NOT exempt.
                        # MASTERS: a compound/calandered-roll lot sized to the
                        # plant 3-batch / spool MIN may exceed any per-tyre cap.
                        "pooling_exempt": bool(is_cal_pool or master_pool_exempt),
                    })
    if not lots:
        cols = C.SCHEMA_LOT + ["source_curing_block"]
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(lots)


def _drum_items_of(dag: SkuDag) -> set:
    return {it for it, op in dag.ops.items()
            if op is not None and op.op_seq == C.OP_CURING}


def _folded_carcass_to_gt(dag: SkuDag) -> Dict[str, str]:
    """Mirror make_lots' carcass-fold map for ONE dag: folded-carcass item -> GT.

    A carcass (op-195) with exactly one made consumer that is a GT (op-200) is
    folded into the GT build and never dispatched; its children re-point to the
    GT. We need the same re-point in the pool pass so a compound feeding a folded
    carcass anchors to that carcass's GT (a real dispatched lot)."""
    out: Dict[str, str] = {}
    for it, op in dag.ops.items():
        if op is None or op.op_seq != C.OP_CARCASS_BUILD:
            continue
        made_cons = [c for c in _all_consumers_of(it, dag)
                     if c and (dag.ops.get(c) is not None)]
        if len(made_cons) != 1:
            continue
        gt = made_cons[0]
        gt_op = dag.ops.get(gt)
        if gt_op is not None and gt_op.op_seq == C.OP_BUILD:
            out[it] = gt
    return out


def _resolve_consumers_in(item: str, dag: SkuDag, drum_items: set,
                          folded: Dict[str, str]) -> List[str]:
    """Resolve the consumer-item(s) of `item` inside ONE sku's dag, applying the
    same drum-fed collapse ("") and folded-carcass re-point that make_lots uses,
    so a pooled producer anchors to the SAME consumer edge a per-sku lot would."""
    out: List[str] = []
    for c in _all_consumers_of(item, dag):
        c = folded.get(c, c)
        cc = "" if c in drum_items else c
        if cc == "" or cc in dag.ops:
            out.append(cc)
    seen = set()
    uniq = []
    for c in out:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq if uniq else [""]


def merge_cross_sku_campaigns(
        demand_df: pd.DataFrame,
        sku_dags: Dict[str, SkuDag],
        sku_datas: Dict[str, SkuData]) -> pd.DataFrame:
    """FIX-5: pool the BULK compound demand (final/master/small-chemical) across
    ALL SKUs by ITEM CODE and emit one campaign stream per (item, shelf-window).

    The concatenated `demand_df` carries every SKU's per-block pull of each
    compound. Per-SKU make_lots SKIPS these item-types (guarded on
    C.POOLED_CROSS_SKU_TYPES), so they are produced ONLY here.

    Algorithm (deterministic):
      * group demand by item code (across SKUs);
      * build (deadline, qty, sku, block, consumer_item) buckets sorted by
        (deadline, sku, block) - C8 deterministic ordering;
      * MPQ/aging are taken from the SHARED item. We ASSERT they agree across
        consuming SKUs; on disagreement we take the SAFE bound (tightest aging:
        max(amin), min(amax); MPQ from the first sku, verified identical) so no
        pooled lot is ever consumed outside ANY puller's band (C2 exact);
      * run the existing _campaigns(buckets, mpq_max, span_max) on the pooled
        deadline timeline (span_max = (amax-amin)*60 - transfer);
      * size each campaign to MPQ (C5 on the POOLED total) and emit one lot per
        MPQ slice. The lot ANCHORS to the EARLIEST-deadline consumer in the
        campaign (tightest puller binds AND-join + aging-max); the other
        (sku, consumer, block) edges are carried in parent_lot_ids for handoff
        and consumed cross-SKU by dispatch.

    Returns a partial-SCHEMA_LOT frame (+source_curing_block) with the SAME
    columns make_lots emits, so the pipeline can concat them directly.
    """
    cols = C.SCHEMA_LOT + ["source_curing_block"]
    if demand_df is None or demand_df.empty:
        return pd.DataFrame(columns=cols)

    # L2: pool BOTH the bulk compounds AND the wide-sheet calendered mother rolls
    # across SKUs. The two families differ ONLY in sizing (compounds pack to MPQ;
    # calender rolls are span-only mother-roll runs) - resolved per item below.
    pool_types = set(C._pooled_cross_sku_types())
    if C.L2_CALENDER_CROSS_SKU_POOL:
        pool_types |= set(C.POOLED_CROSS_SKU_CALENDER_TYPES)
    pooled = demand_df[
        demand_df["item_type"].astype(str).str.strip().str.lower().isin(pool_types)
    ]
    if pooled.empty:
        return pd.DataFrame(columns=cols)

    # per-sku consumer-resolution scaffolding (drum items + carcass fold), cached.
    drum_items: Dict[str, set] = {}
    folded: Dict[str, Dict[str, str]] = {}
    for s, dag in sku_dags.items():
        drum_items[s] = _drum_items_of(dag)
        folded[s] = _folded_carcass_to_gt(dag)

    lots: List[dict] = []
    seq = 0
    for item, gi in pooled.groupby("item", sort=True):
        item_type = gi["item_type"].iloc[0]
        uom = gi["uom"].iloc[0]
        skus_here = sorted(gi["sku"].astype(str).unique())
        # L2: calender mother-roll family uses span-only, mother-roll sizing (one
        # tiny re-based lot per shelf-window), NOT the bulk-compound MPQ packing.
        is_cal_pool = str(item_type).strip().lower() in \
            C.POOLED_CROSS_SKU_CALENDER_TYPES

        # ---- SAFE cross-SKU MPQ + aging (assert agreement, else tightest) ----
        mpq_set = set()
        amins: List[float] = []
        amaxs: List[float] = []
        transfers: List[float] = []
        anchor_dag = None
        anchor_op = None
        for s in skus_here:
            data = sku_datas.get(s)
            dag = sku_dags.get(s)
            if data is None or dag is None:
                continue
            # FIX B / MASTERS: bounds re-expressed in the pooled item's lot UOM
            # (canonical), resolved from the plant MPQ master (per line/item-type)
            # when ON - item_code + the mixing machine pool feed compound/roll min.
            op_s = dag.ops.get(item)
            mach = (op_s.machines if op_s is not None else [])
            mn, mx = mpq_in_lot_uom(item_type, uom, data.mpq, data.mpq_uom,
                                    item_code=item, line="PCR",
                                    machine_names=mach)
            mpq_set.add((mn, mx))
            amin_s, amax_s = _aging_for(item, item_type, data)
            amins.append(amin_s)
            amaxs.append(amax_s)
            if op_s is not None:
                transfers.append(_resolve_transfer(
                    item_type, "PCR", op_s.transfer_min or C.TRANSFER_MIN))
                if anchor_op is None:
                    anchor_op = op_s
                    anchor_dag = dag
        if anchor_op is None:
            continue  # no sku actually makes this item -> nothing to pool
        # C5: MPQ on the pooled total. Item-type-keyed MPQ is identical across
        # SKUs in CTP data; if it ever diverges, take the TIGHTEST band (largest
        # min floor, smallest max cap) so the pooled lot satisfies every puller.
        if len(mpq_set) == 1:
            mn, mx = next(iter(mpq_set))
        else:
            mn = max(b[0] for b in mpq_set)
            mx = min(b[1] for b in mpq_set if b[1] > 0) if any(
                b[1] > 0 for b in mpq_set) else 0.0
        # C2: tightest aging band binds (shortest shelf life, latest availability).
        amin = max(amins) if amins else C.DEFAULT_AGING_MIN_H
        amax = min(amaxs) if amaxs else C.DEFAULT_AGING_MAX_H
        buf = _buffer_for(item_type, sku_datas[skus_here[0]])
        transfer = max(transfers) if transfers else C.TRANSFER_MIN

        # ---- pooled demand buckets (deadline, qty, sku, block, consumer) ----
        # consumer-item per (sku, block) resolved in that sku's own dag.
        agg = (gi.groupby(["sku", "source_curing_block", "curing_deadline"])
               ["required_qty"].sum().reset_index())
        # C8: deterministic order across SKUs -> (deadline, sku, block)
        agg = agg.sort_values(["curing_deadline", "sku", "source_curing_block"])
        raw_buckets = []  # (block, deadline, qty, sku, consumer_item)
        for _, rr in agg.iterrows():
            s = str(rr["sku"])
            dag = sku_dags.get(s)
            if dag is None:
                continue
            cons_list = _resolve_consumers_in(
                item, dag, drum_items.get(s, set()), folded.get(s, {}))
            # one bucket per (consuming edge) so every consumer is anchorable;
            # the pooled total is preserved (qty split evenly across this block's
            # edges, mirroring make_lots' multi-consumer total-then-anchor).
            n_edges = len(cons_list)
            per = float(rr["required_qty"]) / n_edges if n_edges else float(rr["required_qty"])
            for ci in cons_list:
                raw_buckets.append((str(rr["source_curing_block"]),
                                    float(rr["curing_deadline"]), per, s, ci))
        if not raw_buckets:
            continue
        # deterministic: deadline, sku, block, consumer
        raw_buckets.sort(key=lambda b: (b[1], b[3], b[0], b[4]))

        # span budget for campaign-merge (over-aging guard, both-sided in dispatch)
        span_max = max(0.0, (amax - amin) * 60.0 - transfer)
        # L2: a pooled calender mother-roll merges by SPAN ONLY (one wide roll
        # feeds every tyre in the window); its per-tyre MPQ_max is a narrow-strip
        # cap, not the roll size. Use CALENDER_POOL_MAX_QTY (0 = unbounded within
        # the span). Bulk compounds keep the real MPQ_max as the merge cap.
        merge_cap = C.CALENDER_POOL_MAX_QTY if is_cal_pool else mx

        # _campaigns operates on (block, deadline, qty); we carry the richer edge
        # list per campaign by re-running the same greedy merge inline so we keep
        # the (sku, block, consumer) members for anchoring + parent_lot_ids.
        camps: List[dict] = []
        cur = None
        for (block, deadline, qty, s, ci) in raw_buckets:
            if cur is None:
                cur = {"deadline": deadline, "qty": qty,
                       "edges": [(deadline, s, block, ci, qty)]}
                continue
            span_ok = (deadline - cur["deadline"]) <= span_max
            qty_ok = (merge_cap <= 0) or (cur["qty"] + qty <= merge_cap)
            if span_ok and qty_ok:
                cur["qty"] += qty
                cur["edges"].append((deadline, s, block, ci, qty))
            else:
                camps.append(cur)
                cur = {"deadline": deadline, "qty": qty,
                       "edges": [(deadline, s, block, ci, qty)]}
        if cur is not None:
            camps.append(cur)

        for camp in camps:
            qty = camp["qty"]
            if qty <= 0:
                continue
            edges = camp["edges"]
            # ANCHOR = earliest-deadline edge (tightest puller). Deterministic:
            # edges already sorted by (deadline, sku, block, consumer).
            anchor = min(edges, key=lambda e: (e[0], e[1], e[2], e[3]))
            a_deadline, a_sku, a_block, a_consumer, _aq = anchor
            # L2: a pooled calender mother roll is ONE physical wide roll feeding
            # the whole shelf-window across every consuming SKU -> ONE lot at the
            # pooled span total (not re-split to the per-tyre MPQ_max, which would
            # un-do the pooling). Bulk compounds keep MPQ packing (C5 on the total).
            if is_cal_pool:
                # one mother roll for the window; floor to MPQ_min so C5 MIN holds
                # (mirrors the within-SKU calender pool, which floors base->mn).
                batch_qtys = [max(qty, mn) if mn and mn > 0 else qty]
            else:
                # size the POOLED span-window total into the FEWEST 840-KG
                # (mpq_max) batches (C5 EXACT), NOT one mpq_min batch per fragment.
                # Over-charge bounded to AT MOST one partial batch per window; a
                # window below mpq_min floors ONCE. See _pack_batches.
                # BUG-1 FIX: a DISCRETE-piece pooled item (NOS/PCS/EA) must carry
                # whole-number batches too. Bulk compounds are KG (continuous), so
                # this is defensive - it only triggers if a discrete item is ever
                # cross-SKU pooled. _pack_batches stays exact for KG/continuous.
                if _is_discrete_uom(uom):
                    n_hint = (math.ceil(qty / mx)
                              if (mx and mx > 0 and qty > mx) else 1)
                    batch_qtys = [float(q)
                                  for q in _discrete_lot_qtys(qty, mn, mx, n_hint)]
                else:
                    batch_qtys = _pack_batches(qty, mn, mx)
            # other (sku, consumer, block) edges carried for handoff + cross-sku
            # dispatch matching. Deterministic, de-duplicated, sorted.
            edge_tags = sorted({
                f"{s}|{block}|{ci}" for (_d, s, block, ci, _q) in edges
            })
            parent_tags = ";".join(edge_tags)
            for lot_qty in batch_qtys:
                seq += 1
                lot_id = f"POOL-{_short(item)}-{int(a_deadline)}-{seq:05d}"
                # L2: re-base a calender mother-roll lot by the plant MOTHER-ROLL
                # method (sizing.calender_extruder_minutes) so its run stays tiny
                # (~minutes) for a whole window of tyres, exactly as the within-SKU
                # calender pool did. Falls back to proc_to_minutes if not re-basable.
                rebase = (calender_extruder_minutes(
                              anchor_op, item, item_type, lot_qty, uom,
                              anchor_dag, sku=a_sku)
                          if is_cal_pool else None)
                if rebase is not None:
                    proc_min, estimated, _basis = rebase
                else:
                    proc_min, estimated = proc_to_minutes(
                        anchor_op.proc_time, anchor_op.proc_uom,
                        anchor_op.batch_size, lot_qty,
                        bom_len_mm=anchor_dag.bom_len_mm.get(item), qty_uom=uom,
                    )
                proc_eff = proc_min / (anchor_op.efficiency or C.EFFICIENCY)
                if C.ROUND_PROC_TO_WHOLE_MIN:
                    proc_eff = float(math.ceil(proc_eff - 1e-9))
                lots.append({
                    "lot_id": lot_id, "sku": a_sku, "item": item,
                    "item_type": item_type, "qty": lot_qty, "uom": uom,
                    "op_seq": anchor_op.op_seq, "stage": anchor_op.stage,
                    "machines": ",".join(anchor_op.machines),
                    "proc_min": proc_min, "proc_eff_min": proc_eff,
                    "aging_min_h": amin, "aging_max_h": amax, "buffer_h": buf,
                    "transfer_min": transfer,
                    "curing_deadline": a_deadline,
                    "consumer_item": a_consumer,
                    "is_bottleneck": anchor_op.stage == C.STAGE_FINAL_MIX,
                    "est": float("nan"), "lst": float("nan"),
                    "slack_min": float("nan"), "infeasible_flag": False,
                    "estimated_proc": estimated, "line_class": "PCR",
                    "parent_lot_ids": parent_tags, "source_curing_block": a_block,
                    # TASK B: pooled MPQ provenance from the anchor SKU's masters.
                    "mpq_min": mn, "mpq_max": mx,
                    "mpq_source": _mpq_source(
                        item_type, sku_datas[a_sku].mpq, item_code=item,
                        line="PCR", machine_names=anchor_op.machines),
                    # FIX-5: a cross-SKU pooled calender mother roll is ONE physical
                    # wide roll for the whole shelf-window across every consuming SKU,
                    # so it legitimately exceeds per-tyre MPQ_max. Flag it exempt.
                    # MASTERS: a compound sized to the 3-batch MIN is also exempt
                    # (the unbounded-max plant rule means no pooled lot is capped).
                    "pooling_exempt": bool(
                        is_cal_pool
                        or (C.USE_MPQ_TRANSFER_MASTERS
                            and (M.resolve_mpq(
                                "PCR", item_type, item_code=item,
                                machine_names=list(anchor_op.machines or [])
                                ) or M.MpqResolution(0, 0, "", "", "")).pooling_exempt)),
                })
    if not lots:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(lots)


def _consumer_of(item: str, dag: SkuDag) -> str:
    """Immediate parent (consumer) in the BOM graph; for FG -> curing.

    Retained for compatibility / single-consumer callers; returns the first
    (deterministic) parent. BUG-04 callers use _all_consumers_of instead."""
    if item not in dag.graph:
        return ""
    preds = list(dag.graph.predecessors(item))
    if not preds:
        return ""  # FG -> consumed by curing block
    # deterministic pick (sorted)
    return sorted(preds)[0]


def _all_consumers_of(item: str, dag: SkuDag) -> List[str]:
    """BUG-04: ALL immediate parents (consumers) in the BOM graph, sorted for
    determinism. An item feeding several consumers is anchored to EACH, so we
    never orphan a consumer chain by collapsing to a single sorted(preds)[0]."""
    if item not in dag.graph:
        return [""]  # not a graph node (e.g. carcass folded into GT) -> drum-anchored
    preds = sorted(dag.graph.predecessors(item))
    if not preds:
        return [""]  # FG -> consumed by curing block (drum)
    return preds


def _short(item: str) -> str:
    return "".join(c for c in str(item) if c.isalnum())[:14]
