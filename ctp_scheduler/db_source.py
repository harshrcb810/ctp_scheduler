"""DB-backed input source for the CTP scheduler (schema: jkplanning_CTP).

Reads the per-line master tables straight from the plant MySQL and reshapes them
into the EXACT DataFrame column contract the existing engine expects (the same
columns `ingest.load_sku` / `dag.build_dag` read from the per-SKU workbooks), so
the proven ingest/dag/sizing path runs UNCHANGED on DB-sourced data.

Tables consumed (per line, suffix _pcr / _tbr):
    jkt_bom_<line>          -> BOM sheet      (Output/input code/qty/output qty/unit)
    jkt_routing_<line>      -> Routing sheet  (operation_seq/proc_time/machines/...)
    jkt_aging_master_<line> -> Aging Master   (ItemCode/Min/MaxAging/...)
    jkt_mpq_<line>          -> MPQ            (Item Type/Min/Max Run Qty/UOM)
    jkt_buffer_master_<line>-> Buffer Master  (Item type/Buffer Level (Hrs))
    jkt_itemType_master_<line>-> ItemType Master (ItemCode/ItemType)
    CuringSchedule_<line>   -> the DRUM       (Date/Shift/Machine/SKUCode/.../Qty)

Connection is READ-ONLY. Credentials come from DB_* env vars (falling back to the
values the archived backend shipped). Loaded tables are cached per (schema, line)
so a full run hits the DB once. Deterministic: every frame is returned sorted.
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Dict, Optional

import pandas as pd

# --- connection ------------------------------------------------------------
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",     "35.208.174.2"),
    "database": os.environ.get("DB_NAME",     "jkplanning_CTP"),
    "user":     os.environ.get("DB_USER",     "root"),
    "password": os.environ.get("DB_PASSWORD", "Dev112233"),
}


def _connect(cfg: Optional[Dict] = None):
    import pymysql
    c = {**DB_CONFIG, **(cfg or {})}
    return pymysql.connect(
        host=c["host"], user=c["user"], password=c["password"],
        database=c["database"], connect_timeout=15, read_timeout=120)


@lru_cache(maxsize=None)
def _table(name: str) -> pd.DataFrame:
    """Read a whole table once, cached (deterministic per process)."""
    conn = _connect()
    try:
        return pd.read_sql(f"SELECT * FROM `{name}`", conn)
    finally:
        conn.close()


def _line_suffix(line: str) -> str:
    l = str(line).strip().lower()
    if l not in ("pcr", "tbr"):
        raise ValueError(f"line must be 'pcr' or 'tbr', got {line!r}")
    return l


# --- column maps: DB column -> engine-expected column ----------------------
_BOM_MAP = {
    "Parent": "Output",
    "child": "input code",
    "child_quantity": "qty",
    "Parent_qty": "output qty",
    "child_Unit": "unit.1",
    "Parent_unit": "unit",
    "child_description": "description",
}
_MPQ_MAP = {
    "Item_Type": "Item Type",
    "Minimum_Run_Qty": "Minimum Run Qty",
    "Maximum_Run_Qty": "Maximum Run Qty",
    # UOM unchanged
}


def sku_input_frames(sku: str, line: str = "pcr",
                     bom_equipment: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """Return the 6 engine-format DataFrames for one SKU from the DB:
    {bom, routing, aging, mpq, buffer, itemtype}. Columns match what
    ingest.load_sku reads from the per-SKU workbook sheets.

    bom_equipment: if the SKU's BOM spans multiple building-machine groups
    (DB `Equipment`: BJ GROUP / TWO STAGE TBM / UNISTAGE GROUP / VMIMAXX GROUP),
    restrict to this group. None => keep all rows (engine sums) and the multi-
    equipment fan-out is reported by the caller. See OPEN DECISION in the module
    docstring / Section H of the analysis.
    """
    s = _line_suffix(line)
    skn = str(sku).strip()

    # BOM ----------------------------------------------------------------
    bom = _table(f"jkt_bom_{s}")
    bom = bom[bom["Super_parent"].astype(str).str.strip() == skn].copy()
    if bom_equipment is not None and "Equipment" in bom.columns:
        bom = bom[bom["Equipment"].astype(str).str.strip() == str(bom_equipment).strip()]
    bom = bom.rename(columns=_BOM_MAP)
    bom = bom.sort_values([c for c in ("Output", "input code") if c in bom.columns]) \
             .reset_index(drop=True)

    # Routing ------------------------------------------------------------
    rt = _table(f"jkt_routing_{s}")
    rt = rt[rt["finished_product"].astype(str).str.strip() == skn].copy()
    if "operation_seq" in rt.columns:
        rt = rt.sort_values(["routed_product", "operation_seq"]
                            if "routed_product" in rt.columns else ["operation_seq"])
    rt = rt.reset_index(drop=True)

    # Aging Master (whole table; ingest filters by item) -----------------
    aging = _table(f"jkt_aging_master_{s}").copy()

    # MPQ ----------------------------------------------------------------
    mpq = _table(f"jkt_mpq_{s}").rename(columns=_MPQ_MAP).copy()

    # Buffer Master ------------------------------------------------------
    buf = _table(f"jkt_buffer_master_{s}").copy()

    # ItemType Master (dedupe exact duplicate ItemCode rows) -------------
    it = _table(f"jkt_itemType_master_{s}")
    if "ItemCode" in it.columns:
        it = it.drop_duplicates(subset=["ItemCode"]).reset_index(drop=True)

    return {"bom": bom, "routing": rt, "aging": aging,
            "mpq": mpq, "buffer": buf, "itemtype": it}


def available_skus_db(line: str = "pcr") -> list:
    """SKUs that have BOTH a BOM and a routing in the DB (schedulable)."""
    s = _line_suffix(line)
    bom = set(_table(f"jkt_bom_{s}")["Super_parent"].astype(str).str.strip())
    rt = set(_table(f"jkt_routing_{s}")["finished_product"].astype(str).str.strip())
    return sorted(bom & rt)


def bom_equipment_groups(sku: str, line: str = "pcr") -> list:
    """The distinct building-machine Equipment groups in this SKU's BOM."""
    s = _line_suffix(line)
    bom = _table(f"jkt_bom_{s}")
    sub = bom[bom["Super_parent"].astype(str).str.strip() == str(sku).strip()]
    return sorted(sub["Equipment"].dropna().astype(str).str.strip().unique())


if __name__ == "__main__":  # quick self-test against the live DB
    import sys
    line = sys.argv[1] if len(sys.argv) > 1 else "pcr"
    skus = available_skus_db(line)
    print(f"[db] line={line}: {len(skus)} SKUs with BOM+routing")
    sku = skus[0]
    eqs = bom_equipment_groups(sku, line)
    print(f"[db] sample SKU {sku}: BOM equipment groups = {eqs}")
    f = sku_input_frames(sku, line)
    for name, df in f.items():
        print(f"[db]   {name:9s} rows={len(df):5d} cols={list(df.columns)[:9]}")
