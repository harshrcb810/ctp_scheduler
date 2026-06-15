"""Plant MPQ + Transfer MASTERS (authoritative, SOLE source of MPQ + transfer).

Two plant-validated master files become the single source of truth for lot MPQ
(per line, per item-type) and inter-stage transfer time (per line, per item-type)
when config.USE_MPQ_TRANSFER_MASTERS is True:

  * data/MPQ_master_corrected.csv (+ .xlsx sheets MPQ / Mixer_Batches /
    Calender_Roll_Lengths)
  * data/Transfer_master_corrected.csv

The masters are CONSUMED READ-ONLY - this module never writes back to them and
never edits a single value. It only resolves them into engine numbers.

MPQ MAX = UNBOUNDED for every item-type/line (plant directive: no maximum,
produce to demand). The resolver returns mpq_max = 0.0, which the engine already
treats as "no cap" (C5's `if mx and mx > 0` guard never fires), so no lot is ever
capped above by C5 when masters are on.

MPQ MIN resolution (the plant values):
  * NUMERIC min ("230 MTR", "1000 Nos") -> used directly in min_uom; the caller
    (sizing.mpq_in_lot_uom) canonicalises via units.py to the lot's dimension.
  * COMPOUND min ("3 batches" / "1 batch"): min_kg = min_batches * batch_kg of
    THAT item's mixing machine. The item's op-10/op-20 routing machine name is
    mapped to Mixer_Batches.batch_kg (e.g. on 430-2 -> 3*350 = 1050 KG). When the
    item is eligible on SEVERAL mixers we take the SMALLEST batch_kg in the pool
    (the smallest physically-runnable batch -> smallest min, deterministic). If
    the machine is unknown, default min_batches * 230 KG.
  * CALANDARED ROLL ("spool/dipped roll length"): min = the item_code's length_m
    from Calender_Roll_Lengths if present; else a roll-type default
    (steel ~6000 / fabric ~2000 MTR).

Determinism (C8): a fixed registry keyed on (line, item_type); machine batch_kg
resolution sorts deterministically; no clock, no RNG, no I/O beyond the one-time
cached load of the master file bytes.
"""
from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

from . import config as C


# ---------------------------------------------------------------------------
# Cached master payload (pure function of the master file bytes -> C8 safe).
# ---------------------------------------------------------------------------
@dataclass
class MasterTables:
    # (line, item_type_lower) -> raw MPQ master row dict
    mpq: Dict[Tuple[str, str], dict] = field(default_factory=dict)
    # mixer batch table: normalised machine name -> batch_kg
    mixer_batch_kg: Dict[str, float] = field(default_factory=dict)
    # calender roll lengths: item_code -> length_m (None entries dropped)
    roll_length_m: Dict[str, float] = field(default_factory=dict)
    # (line, item_type_lower) -> (transfer_min_PCR, transfer_min_TBR)
    transfer: Dict[Tuple[str, str], Tuple[float, float]] = field(default_factory=dict)
    loaded: bool = False


_MASTERS: Optional[MasterTables] = None


def _norm_machine_name(name: str) -> str:
    """Normalise a routing mixer machine name to the Mixer_Batches key.

    Routing carries names like 'F270 M', 'K430-2 M', 'F270 F', 'K310 F'. The
    Mixer_Batches sheet keys are '270M', '440M', '430-1', '430-2', '270F', '310F'.
    We strip the leading vendor letter (F/K) and whitespace, fold a trailing
    role token (' M' master / ' F' final) onto the numeric/dash core: '270 M' ->
    '270M', '430-2 M' -> '430-2', '310 F' -> '310F'. Deterministic, pure string.
    """
    s = str(name).strip()
    if not s:
        return ""
    # drop a single leading vendor letter (F.. / K..) before the digits
    m = re.match(r"^[A-Za-z]?\s*([0-9][0-9A-Za-z\-]*)\s*([MF])?\s*$", s)
    if not m:
        return s.replace(" ", "").upper()
    core, role = m.group(1), (m.group(2) or "")
    return f"{core}{role}".upper()


def load_masters(mpq_xlsx: str = None, mpq_csv: str = None,
                 transfer_csv: str = None) -> MasterTables:
    """Load and cache the MPQ + Transfer masters (read-only). Pure function of
    the file bytes; cached for determinism (C8). Missing files -> empty tables
    (resolvers then fall back to engine defaults, logged by the caller)."""
    global _MASTERS
    if _MASTERS is not None and mpq_xlsx is None and mpq_csv is None \
            and transfer_csv is None:
        return _MASTERS

    mt = MasterTables()
    xlsx = mpq_xlsx or getattr(C, "MPQ_MASTER_XLSX", None)
    csv = mpq_csv or getattr(C, "MPQ_MASTER_CSV", None)
    tcsv = transfer_csv or getattr(C, "TRANSFER_MASTER_CSV", None)

    # ---- MPQ master rows (per line, item_type) ----
    mpq_df = None
    if xlsx and os.path.exists(xlsx):
        try:
            mpq_df = pd.read_excel(xlsx, sheet_name="MPQ")
        except Exception:
            mpq_df = None
    if mpq_df is None and csv and os.path.exists(csv):
        mpq_df = pd.read_csv(csv)
    if mpq_df is not None:
        mpq_df.columns = [str(c).strip() for c in mpq_df.columns]
        for _, r in mpq_df.iterrows():
            line = str(r.get("line", "")).strip().upper()
            it = str(r.get("item_type", "")).strip().lower()
            if not line or not it:
                continue
            mt.mpq[(line, it)] = {
                "min_run_qty": r.get("min_run_qty"),
                "min_uom": r.get("min_uom"),
                "max_run_qty": r.get("max_run_qty"),
                "min_batches": r.get("min_batches"),
                "note": r.get("note"),
            }

    # ---- Mixer_Batches + Calender_Roll_Lengths (xlsx only) ----
    if xlsx and os.path.exists(xlsx):
        try:
            mb = pd.read_excel(xlsx, sheet_name="Mixer_Batches")
            mb.columns = [str(c).strip() for c in mb.columns]
            for _, r in mb.iterrows():
                nm = _norm_machine_name(r.get("machine_name", ""))
                try:
                    kg = float(r.get("batch_kg"))
                except (TypeError, ValueError):
                    continue
                if nm and kg > 0:
                    mt.mixer_batch_kg[nm] = kg
        except Exception:
            pass
        try:
            cr = pd.read_excel(xlsx, sheet_name="Calender_Roll_Lengths")
            cr.columns = [str(c).strip() for c in cr.columns]
            for _, r in cr.iterrows():
                code = str(r.get("item_code", "")).strip()
                lm = r.get("length_m")
                if code and pd.notna(lm):
                    try:
                        mt.roll_length_m[code] = float(lm)
                    except (TypeError, ValueError):
                        continue
        except Exception:
            pass

    # ---- Transfer master (per line, item_type) ----
    if tcsv and os.path.exists(tcsv):
        tdf = pd.read_csv(tcsv)
        tdf.columns = [str(c).strip() for c in tdf.columns]
        for _, r in tdf.iterrows():
            it = str(r.get("item_type", "")).strip().lower()
            if not it:
                continue
            # NaN-guard (BUG): float(NaN) does NOT raise, so a blank transfer
            # cell would leak NaN into the window math (LST = cs - transfer - ...
            # becomes NaN -> lot never schedulable / NaN sort). pd.notna catches
            # the blank; the try still catches a non-numeric string. Both default.
            _vp = r.get("transfer_min_PCR")
            try:
                tp = float(_vp) if pd.notna(_vp) else C.TRANSFER_MIN
            except (TypeError, ValueError):
                tp = C.TRANSFER_MIN
            _vt = r.get("transfer_min_TBR")
            try:
                tt = float(_vt) if pd.notna(_vt) else C.TRANSFER_MIN
            except (TypeError, ValueError):
                tt = C.TRANSFER_MIN
            mt.transfer[("ANY", it)] = (tp, tt)

    mt.loaded = bool(mt.mpq or mt.transfer)
    if mpq_xlsx is None and mpq_csv is None and transfer_csv is None:
        _MASTERS = mt
    return mt


# ---------------------------------------------------------------------------
# MPQ resolution from the master (per line, per item-type).
# ---------------------------------------------------------------------------
@dataclass
class MpqResolution:
    mn: float            # resolved MPQ minimum (in `uom`)
    mx: float            # MPQ maximum: 0.0 = UNBOUNDED (no cap) by plant directive
    uom: str             # the unit the bounds are stated in (for canonicalisation)
    source: str          # "master" / "master-default"
    basis: str           # human-readable resolution note (audit)
    pooling_exempt: bool = False   # calander/compound pooled run exceeds per-tyre cap


_COMPOUND_TYPES = {
    "master compound", "final compound", "small chemical", "apex",
}
_CALENDER_ROLL_TYPES = {
    "calandared roll", "calendared roll", "calandered roll",
    "pre cut roll material",
}

# default mixer batch_kg when the item's mixing machine cannot be resolved.
DEFAULT_BATCH_KG = 230.0
# default calandered-roll spool/dipped-roll lengths (MTR) when item_code absent.
DEFAULT_STEEL_SPOOL_M = 6000.0
DEFAULT_FABRIC_ROLL_M = 2000.0


def _min_batch_kg_for_item(machine_names: List[str],
                           masters: MasterTables) -> Tuple[float, str]:
    """Smallest physically-runnable batch_kg across the item's eligible mixer
    pool (deterministic), with the resolved machine names for the audit. The
    smallest batch gives the smallest 3-batch min floor (conservative). Falls
    back to DEFAULT_BATCH_KG when no name maps to Mixer_Batches."""
    found: List[Tuple[str, float]] = []
    for nm in machine_names:
        key = _norm_machine_name(nm)
        kg = masters.mixer_batch_kg.get(key)
        # dash-cored master mixers (430-1/430-2) carry NO role suffix in the
        # Mixer_Batches sheet, while 270M/440M/270F/310F DO. The routing name
        # '430-2 M' normalises to '430-2M'; retry with the trailing role dropped
        # so it matches the sheet key '430-2'. Deterministic.
        if (kg is None or kg <= 0) and key and key[-1] in ("M", "F"):
            kg = masters.mixer_batch_kg.get(key[:-1])
            if kg and kg > 0:
                key = key[:-1]
        if kg and kg > 0:
            found.append((key, kg))
    if not found:
        return DEFAULT_BATCH_KG, "default(230)"
    # deterministic: smallest batch_kg, tie-break by machine key
    found.sort(key=lambda t: (t[1], t[0]))
    key, kg = found[0]
    return kg, key


def _roll_length_for_item(item_code: str, note: str,
                          masters: MasterTables) -> Tuple[float, str]:
    """Calandered-roll min length (MTR): item_code's length_m from the master if
    present, else a roll-type default (steel ~6000 / fabric ~2000) inferred from
    the note text. Deterministic."""
    lm = masters.roll_length_m.get(str(item_code).strip())
    if lm and lm > 0:
        return float(lm), f"roll_length[{item_code}]"
    nlow = str(note).lower()
    if "fabric" in nlow or "dipped" in nlow:
        return DEFAULT_FABRIC_ROLL_M, "default_fabric_roll(2000)"
    return DEFAULT_STEEL_SPOOL_M, "default_steel_spool(6000)"


def resolve_mpq(line: str, item_type: str, item_code: str = "",
                machine_names: Optional[List[str]] = None,
                masters: Optional[MasterTables] = None) -> Optional[MpqResolution]:
    """Resolve (mn, mx=UNBOUNDED, uom, source) for (line, item_type) from the
    plant MPQ master. Returns None when masters are not loaded / the type is
    absent for this line (caller then keeps the legacy resolution).

    The MAX is always 0.0 (unbounded) by plant directive. The MIN is resolved per
    the rules in the module docstring (numeric / compound 3xbatch / roll length).
    """
    masters = masters or load_masters()
    if not masters.loaded:
        return None
    line = str(line).strip().upper() or "PCR"
    it = str(item_type).strip().lower()
    row = masters.mpq.get((line, it))
    if row is None:
        return None

    note = str(row.get("note", "") or "")
    raw_min = row.get("min_run_qty")
    raw_uom = str(row.get("min_uom", "") or "").strip() or "NOS"
    min_batches = row.get("min_batches")
    try:
        nb = int(float(min_batches)) if pd.notna(min_batches) else 0
    except (TypeError, ValueError):
        nb = 0

    raw_s = str(raw_min).strip().lower() if raw_min is not None else ""

    # 1) COMPOUND ("N batches"): min_kg = N * batch_kg of the item's mixer.
    if it in _COMPOUND_TYPES and ("batch" in raw_s):
        n = nb if nb > 0 else (3 if "3" in raw_s else 1)
        batch_kg, mach = _min_batch_kg_for_item(machine_names or [], masters)
        mn = n * batch_kg
        return MpqResolution(
            mn=mn, mx=0.0, uom="KG", source="master",
            basis=f"COMPOUND {n}x batch_kg({mach}={batch_kg:g})={mn:g}KG",
            pooling_exempt=False)

    # 2) CALANDARED ROLL ("spool/dipped roll length"): item_code -> length_m.
    if it in _CALENDER_ROLL_TYPES and ("spool" in raw_s or "roll" in raw_s
                                       or "length" in raw_s):
        lm, src = _roll_length_for_item(item_code, note, masters)
        return MpqResolution(
            mn=lm, mx=0.0, uom="MTR", source="master",
            basis=f"CALANDARED_ROLL min={lm:g}MTR via {src}",
            pooling_exempt=True)

    # 3) NUMERIC min in min_uom (used directly; caller canonicalises to lot UOM).
    try:
        mn = float(raw_min)
    except (TypeError, ValueError):
        # non-numeric, non-compound, non-roll -> fall back to a soft floor of 1.
        return MpqResolution(
            mn=1.0, mx=0.0, uom=raw_uom, source="master-default",
            basis=f"non-numeric min '{raw_s}' -> floor 1 {raw_uom}",
            pooling_exempt=False)
    return MpqResolution(
        mn=mn, mx=0.0, uom=raw_uom, source="master",
        basis=f"numeric min {mn:g} {raw_uom}", pooling_exempt=False)


# ---------------------------------------------------------------------------
# Transfer resolution from the master (per line, per item-type).
# ---------------------------------------------------------------------------
def resolve_transfer(line: str, item_type: str,
                     masters: Optional[MasterTables] = None
                     ) -> Optional[Tuple[float, str]]:
    """Resolve (transfer_min, source) for (line, item_type) from the Transfer
    master: the PCR column for a PCR line, the TBR column for a TBR line. Returns
    None when masters are not loaded; (default, "master-default") when the
    item-type is absent (caller logs the default-10 fallback)."""
    masters = masters or load_masters()
    if not masters.loaded:
        return None
    line = str(line).strip().upper() or "PCR"
    it = str(item_type).strip().lower()
    row = masters.transfer.get(("ANY", it))
    if row is None:
        return (C.TRANSFER_MIN, "master-default")
    tp, tt = row
    val = tt if line == "TBR" else tp
    return (float(val), "master")
