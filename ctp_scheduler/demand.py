"""Phase 2 - Backward demand explosion (gross).

For each curing block of an SKU, explode required quantities down the BOM
(parent -> child accumulating required[child] += required[parent]*qty_per_parent).
Only items that have a routing op (are 'made') become schedulable demand; pure
RM leaves (small chemicals, no routing) are always-available and not scheduled.

NO inventory netting (decision #8: assume no on-hand) -> gross == net.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import networkx as nx
import pandas as pd

from . import config as C
from .dag import SkuDag
from .ingest import SkuData, to_minutes_epoch


@dataclass
class CuringBlock:
    block_id: str
    sku: str
    qty: float
    start_min: float       # epoch minutes (fixed - the drum)
    end_min: float
    machine: str           # press id


def curing_blocks_for_sku(drum: pd.DataFrame, sku: str) -> List[CuringBlock]:
    sub = drum[(drum["SKUCode"] == sku) &
               (~drum["SKUCode"].isin(C.NON_PRODUCTION_SKUS))].copy()
    sub = sub.sort_values(["StartTime", "Machine"]).reset_index(drop=True)
    blocks: List[CuringBlock] = []
    for i, r in sub.iterrows():
        if r["Qty"] <= 0:
            continue
        s = to_minutes_epoch(r["StartTime"])
        e = to_minutes_epoch(r["EndTime"])
        # skip drum rows with unparseable times (cannot anchor the pull) or a
        # non-positive duration; honoured as data gaps, not silently scheduled.
        if pd.isna(s) or pd.isna(e) or e <= s:
            continue
        blocks.append(CuringBlock(
            block_id=f"{sku}-CB-{i:04d}",
            sku=sku, qty=float(r["Qty"]),
            start_min=s, end_min=e,
            machine=str(r["Machine"]).strip(),
        ))
    return blocks


def explode_demand(data: SkuData, dag: SkuDag,
                   blocks: List[CuringBlock]) -> pd.DataFrame:
    """Return demand rows keyed per (curing block) for traceability."""
    g = dag.graph
    fg = dag.fg_code
    item_type = data.item_type
    # parents-first order for the sweep (FG first, then down)
    try:
        order = list(nx.topological_sort(g))
    except nx.NetworkXUnfeasible:
        order = list(g.nodes())

    rows: List[dict] = []
    for cb in blocks:
        req: Dict[str, float] = {n: 0.0 for n in g.nodes()}
        req[fg] = cb.qty
        for node in order:
            base = req.get(node, 0.0)
            if base <= 0:
                continue
            for child in g.successors(node):
                ratio = g[node][child].get("qty_per_parent", 1.0)
                req[child] = req.get(child, 0.0) + base * ratio
        for item, q in req.items():
            if q <= 0:
                continue
            # only scheduled if it has a making op (routing) -- else RM leaf
            if item not in dag.ops and item != fg:
                continue
            # Demand UOM follows the BOM input unit so MPQ matching is unit-correct.
            # MM length -> MTR (divide by 1000); KG/NOS pass through; FG -> NOS.
            uom = dag.child_uom.get(item, "NOS").upper()
            req_qty = q
            if uom == "MM":
                req_qty = q / 1000.0
                uom = "MTR"
            elif uom in ("M", "MTR", "MTRS"):
                uom = "MTR"
            elif uom in ("KG", "KGS"):
                uom = "KG"
            else:
                uom = "NOS"
            rows.append({
                "item": item,
                "item_type": item_type.get(item, "Unknown"),
                "required_qty": req_qty,
                "uom": uom,
                "source_curing_block": cb.block_id,
                "curing_deadline": cb.start_min,   # cure must start by here
                "line_class": "PCR",
                "sku": data.sku,
            })
    if not rows:
        return pd.DataFrame(columns=C.SCHEMA_DEMAND)
    df = pd.DataFrame(rows)
    df = df.sort_values(["curing_deadline", "source_curing_block", "item"]).reset_index(drop=True)
    return df
