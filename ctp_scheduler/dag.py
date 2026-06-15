"""Phase 1 - DAG construction & precedence model.

Builds the parent->child BOM graph for one SKU, attaches the curing drum
anchors, proves acyclicity (Kahn), and resolves every routed item's making
operation + machine pool. Each routed BOM item produces a node; the BOM edges
encode precedence (parent consumes child).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import networkx as nx
import pandas as pd

from . import config as C
from .ingest import SkuData, parse_machine_cell, _isnan


# ---------------------------------------------------------------------------
@dataclass
class OpInfo:
    item: str
    op_seq: int
    op_name: str
    department: str
    stage: str
    machines: List[str]
    proc_time: float
    proc_uom: str
    batch_size: float
    transfer_min: float
    efficiency: float


def stage_of(op_seq: int, op_name: str, machines: List[str],
             department: str = "") -> str:
    if op_seq == C.OP_MASTER_MIX:
        return C.STAGE_MASTER_MIX
    if op_seq == C.OP_FINAL_MIX:
        return C.STAGE_FINAL_MIX
    if op_seq in C.BUILD_OPS:
        return C.STAGE_BUILDING
    if op_seq == C.OP_CURING:
        return C.STAGE_CURING
    nm = str(op_name).strip()
    # Round-2 COSMETIC fix: some Bead-Apex op-70 rows carry operation_name
    # "Unknown" (a placeholder) although they are valid Bead Apexing units on
    # machines 0301/201/202. Derive the real stage from the department so the
    # schedule has no "Unknown" stage rows (no constraint impact).
    if not nm or nm.lower() in ("nan", "unknown"):
        dept = str(department).strip()
        if dept and dept.lower() not in ("nan", "", "unknown"):
            return C.STAGE_BY_DEPARTMENT.get(dept.lower(), dept)
        return f"op{op_seq}"
    return nm


def _primary_op_per_item(routing: pd.DataFrame) -> Dict[str, OpInfo]:
    """For each routed_product, pick its making operation. An item may have a
    single op; if several rows share the routed_product+op, pick the primary /
    longest proc. Returns item -> OpInfo (the terminal stage that makes it)."""
    ops: Dict[str, OpInfo] = {}
    rt = routing.copy()
    rt["operation_seq"] = pd.to_numeric(rt["operation_seq"], errors="coerce")
    for item, g in rt.groupby("routed_product"):
        item = str(item).strip()
        # choose the op_seq that "produces" the item: highest op_seq in its
        # chain (closest to consumption) - for compounds that's 10/20, etc.
        # In CTP each routed item has one op_seq; take max for safety.
        best_seq = int(g["operation_seq"].max())
        sub = g[g["operation_seq"] == best_seq]
        # primary row, else longest proc
        prim = sub[sub.get("is_primary", 0) == 1]
        row = (prim.iloc[0] if len(prim) else sub.iloc[0])
        machines = parse_machine_cell(row.get("machines"))
        # Final/Master mixers may come as the canonical comma cell.
        op_name = str(row.get("operation_name", "")).strip()
        department = str(row.get("department", "")).strip()
        try:
            proc = float(row.get("proc_time"))
        except (TypeError, ValueError):
            proc = 30.0
        # DB export carries blank proc_time on some rows (parses to NaN, which the
        # try/except above does NOT catch since float(nan) is valid). A NaN proc
        # propagates to sizing's proc_eff ceil() and aborts. Floor to the same
        # 30-min ESTIMATED default; proc_to_minutes will re-flag estimated.
        if math.isnan(proc):
            proc = 30.0
        bs = row.get("batch_size")
        bs = float(bs) if bs is not None and not _isnan(bs) else float("nan")
        try:
            eff = float(row.get("efficiency"))
        except (TypeError, ValueError):
            eff = C.EFFICIENCY
        if not eff or math.isnan(eff) or eff <= 0:
            eff = C.EFFICIENCY
        tr = row.get("transfer_time_min")
        try:
            tr = float(tr)
        except (TypeError, ValueError):
            tr = C.TRANSFER_MIN
        if math.isnan(tr):
            tr = C.TRANSFER_MIN
        ops[item] = OpInfo(
            item=item, op_seq=best_seq, op_name=op_name,
            department=department,
            stage=stage_of(best_seq, op_name, machines, department),
            machines=machines, proc_time=proc,
            proc_uom=str(row.get("proc_time_UOM", "")).strip(),
            batch_size=bs, transfer_min=tr, efficiency=eff,
        )
    return ops


@dataclass
class SkuDag:
    sku: str
    graph: nx.DiGraph                 # parent -> child (consumes)
    topo_producers_first: List[str]   # producers (leaves) first
    ops: Dict[str, OpInfo]            # routed item -> making op
    qty_per_parent: Dict[Tuple[str, str], float]
    bom_len_mm: Dict[str, float]      # child -> qty in mm (for M/MIN proc)
    child_uom: Dict[str, str]         # child -> BOM input unit (MM / KG / NOS)
    fg_code: str
    # Calender re-base support (mother-roll method):
    item_weight_kg_per_tyre: Dict[str, float] = field(default_factory=dict)
    # ^ for each MADE item: sheet weight per tyre = sum of its own BOM KG inputs
    #   (fabric/cord + final compound) scaled to the per-tyre consumption basis.
    qty_per_tyre: Dict[str, float] = field(default_factory=dict)
    # ^ cumulative per-FG-tyre consumption of each item in its BOM input unit
    #   (MM for length items, KG for weight items, NOS for pieces).
    issues: List[str] = field(default_factory=list)


def build_dag(data: SkuData) -> SkuDag:
    bom = data.bom
    g = nx.DiGraph()
    qpp: Dict[Tuple[str, str], float] = {}
    bom_len_mm: Dict[str, float] = {}
    issues: List[str] = []

    child_uom: Dict[str, str] = {}
    # Per-item sheet weight per ONE unit of that item's output basis: sum of the
    # item's own BOM input rows whose consumption unit is KG (fabric/cord +
    # final compound = the calendered sheet weight). For a calendered roll
    # (HTPOLY350/CPJ*) the output qty IS the per-tyre KG, so this sum already
    # equals the per-tyre sheet weight. For a cap strip (CAP*) the inputs are the
    # genuine per-tyre cord+compound KG while the output qty is the strip MM
    # length, so this sum is the per-tyre sheet weight directly.
    item_kg_per_output: Dict[str, float] = {}
    item_output_qty: Dict[str, float] = {}

    # aggregate qty_per_parent = sum(qty)/sum(output qty) per (parent, child)
    agg: Dict[Tuple[str, str], Tuple[float, float]] = {}
    for _, r in bom.iterrows():
        parent = str(r["Output"]).strip()
        child = str(r["input code"]).strip()
        try:
            q = float(r["qty"])
            oq = float(r["output qty"])
        except (TypeError, ValueError):
            continue
        if not parent or not child or parent == "nan" or child == "nan":
            continue
        if q == 0 or math.isnan(q):       # drop zero-qty / WA lines
            continue
        # NOTE (BUG-3): 25 rows in this dataset carry output_qty <= 0 (e.g. the
        # SELS0 compound chain). The existing `oq if oq else 1.0` keeps the edge
        # with a guessed ratio; SKIPPING such rows would DROP the producer edge
        # entirely and could starve a needed compound. Neither is clearly right on
        # malformed data - keep the existing behaviour until the plant supplies
        # valid output_qty (a DATA fix, not a code fix). Do not silently drop.
        key = (parent, child)
        sq, so = agg.get(key, (0.0, 0.0))
        agg[key] = (sq + q, so + (oq if oq else 1.0))
        # capture mm length for M/MIN proc and the child's demand UOM
        unit = str(r.get("unit.1", "")).strip().upper()
        if unit == "MM":
            bom_len_mm[child] = q
        if unit:
            child_uom[child] = unit
        # sheet-weight accumulation: KG inputs of `parent`'s own recipe
        if unit in ("KG", "KGS"):
            item_kg_per_output[parent] = item_kg_per_output.get(parent, 0.0) + q
        item_output_qty.setdefault(parent, oq if oq else 1.0)

    for (parent, child), (sq, so) in agg.items():
        # NOTE (BUG-2, investigated 2026-06): a duplicate (parent,child) BOM pair
        # makes so = sum(output_qty) over its rows; if output_qty were a per-PARENT
        # constant this would under-count the child ratio. BUT this dataset has 24
        # duplicate pairs AND 9 parents whose rows carry DIFFERENT output_qty
        # (per-row, not per-parent), so neither sq/so nor sq/item_output_qty[parent]
        # is unambiguously correct. The right denominator depends on BOM semantics
        # the plant must clarify (are duplicate rows additive consumption lines that
        # share one parent output, or alternative recipes?). Left as sq/so pending
        # that clarification - do NOT change without confirming the BOM convention.
        ratio = sq / so if so else sq
        g.add_edge(parent, child, qty_per_parent=ratio)
        qpp[(parent, child)] = ratio

    fg = data.fg_code
    if fg not in g:
        # FG may be the BOM top output; ensure node exists
        g.add_node(fg)

    # keep only the subtree reachable from FG (descendants) + FG
    if fg in g:
        keep = set(nx.descendants(g, fg)) | {fg}
        g = g.subgraph(keep).copy()

    if not nx.is_directed_acyclic_graph(g):
        try:
            cyc = nx.find_cycle(g)
            issues.append(f"CYCLE: {cyc}")
        except nx.NetworkXNoCycle:
            pass

    ops = _primary_op_per_item(data.routing)

    # producers-first topological order (children before parents)
    try:
        topo = list(nx.topological_sort(g))   # parents before children
        topo_pf = list(reversed(topo))        # producers (leaves) first
    except nx.NetworkXUnfeasible:
        topo_pf = sorted(g.nodes())
        issues.append("TOPO_FAILED")

    # ---- per-tyre cumulative consumption (FG = 1 tyre) --------------------
    # Sweep parents-first accumulating req[child] += req[parent] * ratio. This
    # gives, for every made item, the quantity consumed per ONE FG tyre in the
    # item's BOM input unit (MM / KG / NOS). Used to convert a calender/extruder
    # lot's qty back into a TYRE COUNT for the mother-roll run-length charge.
    qty_per_tyre: Dict[str, float] = {}
    try:
        sweep_order = list(nx.topological_sort(g))  # parents before children
    except nx.NetworkXUnfeasible:
        sweep_order = list(g.nodes())
    req1: Dict[str, float] = {n: 0.0 for n in g.nodes()}
    if fg in req1:
        req1[fg] = 1.0
    for node in sweep_order:
        base = req1.get(node, 0.0)
        if base <= 0:
            continue
        for child in g.successors(node):
            ratio = g[node][child].get("qty_per_parent", 1.0)
            req1[child] = req1.get(child, 0.0) + base * ratio
    for it, v in req1.items():
        if v > 0:
            qty_per_tyre[it] = v

    # ---- sheet weight per tyre (KG) ---------------------------------------
    # For a calendered ROLL the output qty already equals the per-tyre KG, so the
    # KG-input sum IS the per-tyre weight. For a cap STRIP the output qty is the
    # strip MM length, but the KG-input sum is still the genuine per-tyre cord +
    # compound weight (the inputs are quoted per tyre, not per metre). In both
    # cases the raw KG-input sum is the per-tyre sheet weight, so we use it
    # directly (per-tyre basis), independent of the output qty's unit.
    item_weight_kg_per_tyre: Dict[str, float] = {}
    for it, kg in item_kg_per_output.items():
        if kg > 0:
            item_weight_kg_per_tyre[it] = kg

    return SkuDag(
        sku=data.sku, graph=g, topo_producers_first=topo_pf, ops=ops,
        qty_per_parent=qpp, bom_len_mm=bom_len_mm, child_uom=child_uom,
        fg_code=fg, item_weight_kg_per_tyre=item_weight_kg_per_tyre,
        qty_per_tyre=qty_per_tyre, issues=issues,
    )


def expand_machine_pool(machines: List[str]) -> List[str]:
    """Already individual ids from parse_machine_cell; just unique-sort."""
    return sorted(set(machines))
