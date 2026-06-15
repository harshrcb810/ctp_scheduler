"""Phase 4 - Backward time windows (EST / LST + buffer).

Computes each lot's [EST, LST] backward from the drum (curing block start),
propagating consumer-start up the BOM chain. Because lots are sized per curing
block, the consumer chain is unambiguous: FG -> curing block start; every other
item -> the (single) consumer lot in the same block.

Window equations (design doc Phase 4):
  LST = consumer_start - transfer - proc_eff - aging_min - buffer
  EST = consumer_start - transfer - proc_eff - aging_max
  slack = LST - EST ;  infeasible = EST > LST
"""
from __future__ import annotations

from typing import Dict

import networkx as nx
import pandas as pd

from . import config as C
from .dag import SkuDag


def compute_windows(lots: pd.DataFrame, dag: SkuDag,
                    block_starts: Dict[str, float]) -> pd.DataFrame:
    """block_starts: source_curing_block -> drum start (epoch minutes)."""
    if lots.empty:
        return lots
    g = dag.graph
    # parents-first order (FG first) so consumer_start is known before child
    try:
        topo = list(nx.topological_sort(g))
    except nx.NetworkXUnfeasible:
        topo = sorted(g.nodes())
    rank = {n: i for i, n in enumerate(topo)}

    lots = lots.copy()
    lots["_rank"] = lots["item"].map(lambda x: rank.get(x, 10**9))
    # consumer_planned_start[(block, item)] -> the target start (LST) of that item
    planned: Dict[tuple, float] = {}
    # L15: per-item LST index across blocks, so a producer whose consumer was
    # campaign-merged onto a DIFFERENT block resolves the merged consumer's LST
    # (the same way dispatch._merged_consumer resolves the consumer lot), instead
    # of falling back to its OWN drum-block start. (item) -> sorted [(deadline,
    # lst)] ascending; we pick the latest consumer whose deadline <= this lot's.
    planned_by_item: Dict[str, list] = {}

    # process consumers before producers (ascending rank)
    out_rows = []
    for _, lot in lots.sort_values(["source_curing_block", "_rank", "lot_id"]).iterrows():
        block = lot["source_curing_block"]
        item = lot["item"]
        consumer = lot["consumer_item"]
        deadline = float(lot["curing_deadline"])
        if not consumer:           # FG consumed by the curing block (the drum)
            cs = block_starts.get(block, lot["curing_deadline"])
        else:
            cs = planned.get((block, consumer))
            if cs is None:
                # L15: consumer campaign-merged/pooled onto another block - resolve
                # it the way dispatch does (latest consumer LST whose deadline <=
                # this producer's) rather than falling back to the drum start.
                cands = planned_by_item.get(consumer)
                if cands:
                    cands = sorted(cands)         # ascending (deadline, lst)
                    chosen = None
                    for d, c_lst in cands:        # ascending deadline
                        if d <= deadline + 1e-6:
                            chosen = c_lst
                        else:
                            break
                    cs = chosen if chosen is not None else cands[0][1]
            if cs is None:
                cs = block_starts.get(block, lot["curing_deadline"])
        transfer = lot["transfer_min"]
        pe = lot["proc_eff_min"]
        amin = lot["aging_min_h"] * 60.0
        amax = lot["aging_max_h"] * 60.0
        buf = lot["buffer_h"] * 60.0
        lst = cs - transfer - pe - amin - buf
        # L15: the EST/wall-clock-MAX side drops `transfer` to match the ENFORCED
        # aging-max gap (gap_aging = cs - end, transfer NOT removed when
        # AGING_SUBTRACTS_TRANSFER is False - see dispatch/validate). The MIN/LST
        # side keeps transfer (availability). Gated on WINDOWS_EST_DROP_TRANSFER:
        # the EST/slack feed the criticality PQ order only (the commit-test is
        # authoritative), and dropping transfer here perturbed the dispatch order
        # enough to STARVE some final-compound chains (more INFEASIBLE_AGING). The
        # merged/pooled-consumer LST resolution above is the real correctness fix
        # and is kept unconditionally; the transfer-drop is OFF by default until
        # the plant certifies it improves the plan.
        if C.WINDOWS_EST_DROP_TRANSFER and not C.AGING_SUBTRACTS_TRANSFER:
            est = cs - pe - amax
        else:
            est = cs - transfer - pe - amax
        slack = lst - est
        infeasible = est > lst
        # this lot's planned (ALAP) start is its LST; producers anchor on it
        planned[(block, item)] = lst
        planned_by_item.setdefault(item, []).append((deadline, lst))
        r = lot.to_dict()
        r["est"] = est
        r["lst"] = lst
        r["slack_min"] = slack
        r["infeasible_flag"] = bool(infeasible)
        r["consumer_start"] = cs
        out_rows.append(r)

    res = pd.DataFrame(out_rows)
    if "_rank" in res.columns:
        res = res.drop(columns=["_rank"])
    return res
