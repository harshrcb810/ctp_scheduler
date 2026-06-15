"""Phase 6 - Drum pinning (curing anchors + shared timeline pre-load).

The curing blocks ARE the drum (C6) - fixed start/end on their press. We record
them as pinned curing rows on the shared timeline (one timeline per press) so
the build->cure precedence and the press non-overlap are honoured by the same
machinery as upstream stages. Building (195/200) is dispatched in Phase 7 on the
shared TBM timelines; we pre-create those timelines here.
"""
from __future__ import annotations

from typing import Dict, List

import pandas as pd

from . import config as C
from .demand import CuringBlock, curing_blocks_for_sku
from .ingest import to_minutes_epoch
from .timeline import MachineTimeline


def build_shared_timelines(changeover_min: float = C.CHANGEOVER_MIN) -> Dict[str, MachineTimeline]:
    """Return an empty timeline registry (lazily extended per machine)."""
    return {}


def get_timeline(timelines: Dict[str, MachineTimeline], machine: str,
                 changeover_min: float = C.CHANGEOVER_MIN) -> MachineTimeline:
    if machine not in timelines:
        timelines[machine] = MachineTimeline(machine_id=machine,
                                             changeover_min=changeover_min)
    return timelines[machine]


def pin_curing(timelines: Dict[str, MachineTimeline],
               blocks: List[CuringBlock]) -> List[dict]:
    """Pin every curing block onto its press timeline (changeover=0: drum carries
    its own C/O blocks). Returns curing schedule rows.

    D1 FIX (curing 1:1 with drum): there is exactly ONE curing row per drum
    block. The row's identity is the drum block itself (`lot_id == block_id`,
    `item == block_id`) - NOT a synthesised ``CURE::<sku>`` token. Curing is the
    immutable op-210 anchor consumed by the rope; it is never fabricated as a
    lot. start/end are the verbatim drum times (the press slot)."""
    rows: List[dict] = []
    for cb in sorted(blocks, key=lambda b: (b.start_min, b.machine, b.block_id)):
        tl = get_timeline(timelines, cb.machine, changeover_min=0.0)
        proc = cb.end_min - cb.start_min
        tl.commit(cb.start_min, proc, cb.block_id, cb.sku)
        rows.append({
            "lot_id": cb.block_id, "sku": cb.sku,
            # item == the drum block identity (1:1 anchor), not CURE::<sku>
            "item": cb.block_id,
            "item_type": "Curing", "stage": C.STAGE_CURING,
            "op_seq": C.OP_CURING, "machine": cb.machine, "qty": cb.qty,
            "uom": "NOS", "start": cb.start_min, "end": cb.end_min,
            "duration_min": proc, "changeover_min": 0.0, "is_curing": True,
            # the curing row carries its own block id as the consumer key so the
            # green-tyre build that feeds it can be matched 1:1 in Phase 8.
            "consumer_item": "", "aging_min_h": 0.0, "aging_max_h": 0.0,
            "gap_to_consumer_min": 0.0, "status": "PINNED",
            "source_curing_block": cb.block_id, "consumer_lot_id": "",
        })
    return rows


def pin_press_occupancy(timelines: Dict[str, MachineTimeline],
                        drum: pd.DataFrame,
                        scheduled_skus: List[str]) -> List[dict]:
    """BUG-10 FIX: reserve press time for EVERY real drum production row whose
    SKU is NOT being built this run (quarantined stubs like SKU-D, or SKUs with
    no recipe). The drum (Curing_Sch_PCR.csv) is a real plant schedule: those
    presses are genuinely occupied even though we do not build those tyres. If we
    pin only the scheduled SKUs' blocks, the un-scheduled SKUs' 441 press slots
    (SKU-D) look falsely FREE, and the bottleneck/press-occupancy KPIs lie.

    These rows are OCCUPANCY-ONLY anchors: status RESERVED, no consumer, no
    build chain. They are never dispatched and never feed a green tyre - they
    only block the press interval so non-overlap and utilisation reflect reality.
    Deterministic (drum rows sorted by start/machine/sku)."""
    rows: List[dict] = []
    prod = drum[(~drum["SKUCode"].isin(C.NON_PRODUCTION_SKUS)) &
                (~drum["SKUCode"].isin(set(scheduled_skus)))].copy()
    prod = prod.sort_values(["StartTime", "Machine", "SKUCode"]).reset_index(drop=True)
    for i, r in prod.iterrows():
        if r["Qty"] <= 0:
            continue
        s = to_minutes_epoch(r["StartTime"])
        e = to_minutes_epoch(r["EndTime"])
        if pd.isna(s) or pd.isna(e) or e <= s:
            continue
        sku = str(r["SKUCode"]).strip()
        machine = str(r["Machine"]).strip()
        block_id = f"{sku}-RESV-{i:05d}"
        tl = get_timeline(timelines, machine, changeover_min=0.0)
        proc = e - s
        tl.commit(s, proc, block_id, sku)
        rows.append({
            "lot_id": block_id, "sku": sku, "item": block_id,
            "item_type": "Curing", "stage": C.STAGE_CURING,
            "op_seq": C.OP_CURING, "machine": machine, "qty": float(r["Qty"]),
            "uom": "NOS", "start": s, "end": e,
            "duration_min": proc, "changeover_min": 0.0, "is_curing": True,
            "consumer_item": "", "aging_min_h": 0.0, "aging_max_h": 0.0,
            "gap_to_consumer_min": 0.0, "status": "RESERVED",
            "source_curing_block": block_id, "consumer_lot_id": "",
        })
    return rows


def pin_nonproduction_occupancy(timelines: Dict[str, MachineTimeline],
                                drum: pd.DataFrame) -> List[dict]:
    """L6: reserve press time for CHANGEOVER (360 min) and MOULD_CLEAN drum rows.

    These NON_PRODUCTION_SKUS rows are filtered out of demand and the GT
    fulfilment denominator everywhere (they are not tyres) - but they are REAL
    press occupancy: while a press is mid-mould-change / changeover it is NOT
    free, so no green-tyre build may be validated as feeding that press during
    the block. We pin them as RESERVED occupancy intervals on their own press
    timeline (the same mechanism as pin_press_occupancy), so dispatch's
    non-overlap on the shared press timeline blocks any build from claiming the
    press mid-changeover. They are NEVER dispatched, never feed a green tyre, and
    never enter any demand / fulfilment count. C6 (drum 1:1) is untouched -
    these are occupancy-only anchors keyed by their own RESV id, not curing
    blocks, and C6's drum-match only scores PINNED/RESERVED real-production rows.
    Deterministic (drum rows sorted by start/machine/sku)."""
    rows: List[dict] = []
    np_rows = drum[drum["SKUCode"].isin(C.NON_PRODUCTION_SKUS)].copy()
    np_rows = np_rows.sort_values(
        ["StartTime", "Machine", "SKUCode"]).reset_index(drop=True)
    for i, r in np_rows.iterrows():
        s = to_minutes_epoch(r["StartTime"])
        e = to_minutes_epoch(r["EndTime"])
        if pd.isna(s) or pd.isna(e) or e <= s:
            continue
        sku = str(r["SKUCode"]).strip()
        machine = str(r["Machine"]).strip()
        block_id = f"{sku}-RESV-{i:05d}"
        tl = get_timeline(timelines, machine, changeover_min=0.0)
        proc = e - s
        tl.commit(s, proc, block_id, sku)
        rows.append({
            "lot_id": block_id, "sku": sku, "item": block_id,
            "item_type": sku, "stage": C.STAGE_CURING,
            "op_seq": C.OP_CURING, "machine": machine, "qty": 0.0,
            "uom": "NOS", "start": s, "end": e,
            "duration_min": proc, "changeover_min": 0.0, "is_curing": True,
            "consumer_item": "", "aging_min_h": 0.0, "aging_max_h": 0.0,
            "gap_to_consumer_min": 0.0, "status": "RESERVED",
            "source_curing_block": block_id, "consumer_lot_id": "",
        })
    return rows
