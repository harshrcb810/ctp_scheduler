"""Canonical UOM conversion layer (FIX B).

The plant master data mixes units WITHIN a single physical dimension:
  * length is carried as MM in the BOM but MTR/M in the MPQ sheet (and in the
    lot UOM after demand explosion);
  * mass is carried as KG almost everywhere but a master could specify MT;
  * count is NOS/PCS/EA.

Comparing a length lot qty against a length MPQ bound, or a mass lot against a
mass MPQ bound, is only correct if BOTH operands are first reduced to the SAME
canonical unit of their shared DIMENSION. A raw `max(qty, mpq_min)` that puts
50 MTR next to 73,018 MM is off by 1000x and silently DISABLES the MPQ floor.

This module is the single source of truth for that reduction:

    to_canonical(qty, uom) -> (qty_canonical, dim)

  * length  -> canonical METRES   (MM / 1000 ; M / MTR / MTRS = 1)
  * mass    -> canonical KG        (MT / TONNE * 1000 ; KG / KGS = 1)
  * count   -> canonical NOS       (NOS / PCS / EA / ... = 1)

An UNKNOWN unit RAISES (UnknownUomError) rather than silently passing a value
through at a wrong scale - a 1000x error must never be swallowed. Callers that
must stay non-fatal can catch it and flag the lot.

Pure / deterministic: a fixed registry, no clock, no RNG, no I/O.
"""
from __future__ import annotations

from typing import Tuple

# Dimension tags.
DIM_LENGTH = "length"
DIM_MASS = "mass"
DIM_COUNT = "count"

# Registry: normalised (upper-cased, stripped) UOM token ->
#   (dimension, factor_to_canonical).
# canonical length = METRE ; canonical mass = KG ; canonical count = NOS.
_REGISTRY = {
    # ---- length (canonical METRE) ----
    "MM": (DIM_LENGTH, 1.0 / 1000.0),
    "CM": (DIM_LENGTH, 1.0 / 100.0),
    "M": (DIM_LENGTH, 1.0),
    "MTR": (DIM_LENGTH, 1.0),
    "MTRS": (DIM_LENGTH, 1.0),
    "MTR.": (DIM_LENGTH, 1.0),
    "METER": (DIM_LENGTH, 1.0),
    "METERS": (DIM_LENGTH, 1.0),
    "METRE": (DIM_LENGTH, 1.0),
    "METRES": (DIM_LENGTH, 1.0),
    # ---- mass (canonical KG) ----
    "KG": (DIM_MASS, 1.0),
    "KGS": (DIM_MASS, 1.0),
    "KGM": (DIM_MASS, 1.0),
    "GM": (DIM_MASS, 1.0 / 1000.0),
    "G": (DIM_MASS, 1.0 / 1000.0),
    "MT": (DIM_MASS, 1000.0),
    "TON": (DIM_MASS, 1000.0),
    "TONNE": (DIM_MASS, 1000.0),
    "TONNES": (DIM_MASS, 1000.0),
    # ---- count (canonical NOS) ----
    "NOS": (DIM_COUNT, 1.0),
    "NOS.": (DIM_COUNT, 1.0),
    "NO": (DIM_COUNT, 1.0),
    "NO.": (DIM_COUNT, 1.0),
    "PCS": (DIM_COUNT, 1.0),
    "PC": (DIM_COUNT, 1.0),
    "EA": (DIM_COUNT, 1.0),
    "EACH": (DIM_COUNT, 1.0),
    "UNIT": (DIM_COUNT, 1.0),
    "UNITS": (DIM_COUNT, 1.0),
}


class UnknownUomError(ValueError):
    """Raised when a UOM token is not in the conversion registry.

    Swallowing an unknown unit would risk a silent 1000x scale error in an MPQ
    compare, so the resolver is fail-loud by default. Callers that need to stay
    non-fatal must catch this explicitly and flag the lot."""


def _norm(uom: str) -> str:
    return str(uom).strip().upper() if uom is not None else ""


def dimension_of(uom: str) -> str:
    """Return the canonical DIMENSION tag for a UOM token, or raise."""
    key = _norm(uom)
    spec = _REGISTRY.get(key)
    if spec is None:
        raise UnknownUomError(f"unknown UOM {uom!r} (not in conversion registry)")
    return spec[0]


def to_canonical(qty: float, uom: str) -> Tuple[float, str]:
    """Reduce ``qty`` (in unit ``uom``) to its DIMENSION's canonical unit.

    Returns (qty_canonical, dim) where dim is one of DIM_LENGTH (metres),
    DIM_MASS (kg) or DIM_COUNT (nos). Raises UnknownUomError on an unregistered
    unit (never silently returns the raw value at a wrong scale).
    """
    key = _norm(uom)
    spec = _REGISTRY.get(key)
    if spec is None:
        raise UnknownUomError(f"unknown UOM {uom!r} (not in conversion registry)")
    dim, factor = spec
    return float(qty) * factor, dim


def same_dimension(uom_a: str, uom_b: str) -> bool:
    """True iff both UOMs reduce to the same canonical dimension. Raises on an
    unknown unit (so a typo'd bound unit is caught, not assumed compatible)."""
    return dimension_of(uom_a) == dimension_of(uom_b)
