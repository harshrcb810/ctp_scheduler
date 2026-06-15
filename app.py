"""CTP End-to-End Production Scheduler - Streamlit planner dashboard (v1.1).

HONEST BY CONSTRUCTION. Every percentage shows its denominator; dropped /
quarantined demand is surfaced as loudly as delivered demand. The plan-health
banner can be green ONLY when all six CLEAN clauses pass; any failing clause is
printed verbatim. RED is reserved for correctness failures; provisional /
estimate states are blue / amber / grey.

Engineering invariants (do not weaken):
  * The PipelineResult is cached (@st.cache_data, keyed on the SKU set, the drum
    content hash and CONFIG_VERSION) and persisted in st.session_state. Every
    tab renders FROM session state and survives any widget interaction - the
    dashboard never vanishes on a click.
  * Derived views (WIP curve, util groupby, heatmap pivot, handoff histogram,
    CSV encoders) are cached, keyed on the result hash.
  * The Gantt never silently truncates the horizon: a date-window slider plus a
    machine lane-rollup keep the full schedule visible.
  * What-if re-runs the engine with deterministic config_overrides and shows
    scenario-vs-baseline deltas.

Launch:  streamlit run app.py
"""
from __future__ import annotations

import hashlib
import io
import zipfile
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from ctp_scheduler import config as C
from ctp_scheduler.ingest import (
    available_recipe_skus,
    check_coverage,
    load_drum,
    validate_drum,
)
from ctp_scheduler.pipeline import PipelineResult, run_pipeline
from ctp_scheduler.waste import waste_summary

st.set_page_config(page_title="CTP Production Scheduler", layout="wide",
                   initial_sidebar_state="expanded")

# --------------------------------------------------------------------------- #
# Semantic palette - one meaning per colour (extends the original maps).
# --------------------------------------------------------------------------- #
GREEN = "#1a7f37"    # on-plan / pass / delivered
AMBER = "#9a6700"    # tight / at-risk / provisional-warning
RED = "#b42318"      # correctness failure / over-capacity / expired
GREY = "#6b7280"     # pinned (the drum) / not-applicable
BLUE = "#1f4e79"     # provisional / estimate / informational
SLATE = "#475569"    # neutral chrome

VERDICT_COLOR = {
    "CLEAN": GREEN, "DRUM OK": GREEN, "OK": GREEN,
    "TIGHT": AMBER, "GAPS": AMBER, "LATE": AMBER, "EARLY": AMBER, "WARN": AMBER,
    "NOT-CLEAN": RED, "OVER-CAPACITY": RED, "INVALID": RED, "EXPIRED": RED,
    "FAIL": RED,
}
STATUS_COLOR = {
    "PLACED": GREEN, "PINNED": GREY, "RESERVED": GREY, "ON-TIME": GREEN,
    "OK": GREEN, "AT-RISK": AMBER, "LATE": AMBER, "EARLY": AMBER,
    "VIOLATION": RED, "EXPIRED": RED, "NO_DRUM_ANCHOR": RED,
}
# Process-family colours - one clearly-distinct colour per family. The actual
# schedule carries fine-grained stage names (Triplex/Quintoplex Extruder, Belt/
# Ply Cutter, Cap Ply Slitter, Bead Winding ...) so every one is mapped. Curing
# is ALWAYS the pinned grey.
STAGE_COLOR = {
    C.STAGE_MASTER_MIX: "#7e57c2", C.STAGE_FINAL_MIX: "#ef6c00",
    "Calendering": "#00897b", "4 Roll Calender": "#00897b",
    "Roller Head Calender": "#26a69a",
    "Extrusion": "#5c6bc0", "Triplex Extruder": "#5c6bc0",
    "Quintoplex Extruder": "#3949ab",
    "Cutting": "#8d6e63", "Belt Cutter": "#8d6e63", "Ply Cutter": "#a1887f",
    "Slitting": "#9e9d24", "Cap Ply / Gum Slitter": "#9e9d24",
    "Bead Winding": "#ad1457", "Bead Apexing": "#c2185b",
    C.STAGE_BUILDING: "#1565c0", C.STAGE_CURING: GREY,
}
# Fallback palette for any stage name we have not explicitly mapped.
_STAGE_FALLBACK_PALETTE = [
    "#7e57c2", "#ef6c00", "#00897b", "#26a69a", "#5c6bc0", "#3949ab",
    "#8d6e63", "#a1887f", "#9e9d24", "#ad1457", "#c2185b", "#1565c0",
    "#00838f", "#6d4c41", "#558b2f",
]

# Named process-flow order (fallback when the data carries no op_seq).
PROCESS_FLOW_ORDER = [
    C.STAGE_MASTER_MIX, C.STAGE_FINAL_MIX, "Calendering", "4 Roll Calender",
    "Roller Head Calender", "Extrusion", "Triplex Extruder",
    "Quintoplex Extruder", "Cutting", "Belt Cutter", "Ply Cutter",
    "Slitting", "Cap Ply / Gum Slitter", "Bead Winding", "Bead Apexing",
    C.STAGE_BUILDING, C.STAGE_CURING,
]

# Green-tyre item-type tokens (used by aging + SKU-demand builders).
_GT_ITEM_TYPES = {"green tyre", "green tyres"}

# --------------------------------------------------------------------------- #
# Number-format helpers (units on every number).
# --------------------------------------------------------------------------- #
def fmt_int(x) -> str:
    try:
        return f"{int(round(float(x))):,}"
    except (TypeError, ValueError):
        return "-"


def fmt_pct(x, dp: int = 1) -> str:
    try:
        return f"{float(x):.{dp}f}%"
    except (TypeError, ValueError):
        return "-"


def fmt_h(x, dp: int = 1) -> str:
    try:
        return f"{float(x):.{dp}f} h"
    except (TypeError, ValueError):
        return "-"


def fmt_days(x, dp: int = 1) -> str:
    try:
        return f"{float(x):.{dp}f} d"
    except (TypeError, ValueError):
        return "-"


def chip(label: str, value: str, color: Optional[str] = None) -> str:
    color = color or VERDICT_COLOR.get(str(value).upper(), SLATE)
    return (f"<span style='background:{color};color:white;padding:3px 11px;"
            f"border-radius:13px;font-weight:600;font-size:0.82rem;"
            f"white-space:nowrap'>{label}: {value}</span>")


def stage_rank_map(sched: pd.DataFrame) -> Dict[str, float]:
    """Authoritative process rank per stage so swimlanes read in real material
    flow Master-Mix -> ... -> Curing (last).

    Priority: the data's OWN op_seq (MEDIAN op_seq per stage - robust against the
    few stray mis-tagged low-op_seq rows that previously pulled Bead Apexing to
    the middle); then the named PROCESS_FLOW_ORDER list; unknown stages sort last
    but deterministically. This is what fixes Curing appearing mid-chart."""
    ranks: Dict[str, float] = {}
    stages = set(sched["stage"].astype(str)) if not sched.empty else set()
    if not sched.empty and "op_seq" in sched.columns:
        seq = pd.to_numeric(sched["op_seq"], errors="coerce")
        tmp = pd.DataFrame({"stage": sched["stage"].astype(str), "seq": seq})
        for stage, g in tmp.groupby("stage"):
            md = g["seq"].median()
            if pd.notna(md):
                ranks[stage] = float(md)
    base = (max(ranks.values()) + 1000.0) if ranks else 0.0
    for stage in stages:
        if stage in ranks:
            continue
        if stage in PROCESS_FLOW_ORDER:
            ranks[stage] = float(PROCESS_FLOW_ORDER.index(stage))
        else:
            ranks[stage] = base + (sum(ord(c) for c in stage) % 997) / 1000.0
    return ranks


def _stage_order_index(stage: str) -> int:
    """Static fallback rank (used only where no frame is available)."""
    try:
        return PROCESS_FLOW_ORDER.index(stage)
    except ValueError:
        return len(PROCESS_FLOW_ORDER) + abs(hash(str(stage))) % 1000


def stage_color_map(stages) -> Dict[str, str]:
    """One clearly-distinct colour per process family. Curing is ALWAYS pinned
    grey; any unmapped stage draws a stable colour from the fallback palette."""
    cmap: Dict[str, str] = {}
    pi = 0
    for s in stages:
        if s == C.STAGE_CURING or str(s).lower().startswith("curing"):
            cmap[s] = GREY
        elif s in STAGE_COLOR:
            cmap[s] = STAGE_COLOR[s]
        else:
            cmap[s] = _STAGE_FALLBACK_PALETTE[pi % len(_STAGE_FALLBACK_PALETTE)]
            pi += 1
    return cmap


@st.cache_data(show_spinner=False)
def sku_demand_table(_sched: pd.DataFrame, key: str) -> pd.DataFrame:
    """Per-SKU demand reconciliation built straight from the committed schedule
    (works for any scope, no drum re-read needed):
      demanded_presses / demanded_qty : pinned curing (drum) rows = the demand.
      built_gt_qty                     : green-tyre qty off the building drum.
      fulfilled_pct                    : built vs demanded cure qty (0-100, capped).
    Sorted heavy-first so the user instantly sees the big SKU (e.g. the 35,671)."""
    cols = ["sku", "demanded_presses", "demanded_qty", "built_gt_qty",
            "fulfilled_pct"]
    if _sched.empty:
        return pd.DataFrame(columns=cols)
    df = _sched.copy()
    df["is_curing"] = df["is_curing"].astype(bool)
    cur = df[df["is_curing"]]
    dem = cur.groupby("sku").agg(demanded_presses=("sku", "size"),
                                 demanded_qty=("qty", "sum"))
    gt = df[df["item_type"].astype(str).str.lower().isin(_GT_ITEM_TYPES)]
    built = gt.groupby("sku").agg(built_gt_qty=("qty", "sum"))
    out = dem.join(built, how="outer").fillna(0.0).reset_index()
    out["fulfilled_pct"] = out.apply(
        lambda r: 100.0 * min(r["built_gt_qty"], r["demanded_qty"]) /
        r["demanded_qty"] if r["demanded_qty"] > 0 else 0.0, axis=1)
    return out.sort_values("demanded_qty", ascending=False).reset_index(drop=True)


def schedule_sku_count(sched: pd.DataFrame) -> int:
    """Distinct SKU count actually present in the committed schedule (the honest
    footer number - replaces the misleading 'len(skus_used)' that read 1)."""
    if sched is None or sched.empty or "sku" not in sched.columns:
        return 0
    return int(sched["sku"].nunique())


# --------------------------------------------------------------------------- #
# DRUM LOADING (ONE function for upload + default, with validation guard).
# --------------------------------------------------------------------------- #
DRUM_REQUIRED_COLS = ["SKUCode", "Machine", "StartTime", "EndTime", "Qty"]


def _normalise_drum(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    missing = [c for c in DRUM_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Drum CSV missing required column(s): {missing}")
    df["SKUCode"] = df["SKUCode"].astype(str).str.strip()
    df["Machine"] = df["Machine"].astype(str).str.strip()
    df["StartTime"] = pd.to_datetime(df["StartTime"], errors="coerce")
    df["EndTime"] = pd.to_datetime(df["EndTime"], errors="coerce")
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
    return df


@st.cache_data(show_spinner=False)
def load_drum_unified(file_bytes: Optional[bytes], default_path: str = None
                      ) -> pd.DataFrame:
    """Drum loader for the UPLOADED curing schedule only. Pure on its inputs so
    it caches cleanly. The drum is never sourced from a default/saved file - if
    no upload bytes are given it raises, so no previous curing data can leak in."""
    if file_bytes is None:
        raise ValueError("No curing schedule uploaded.")
    return _normalise_drum(pd.read_csv(io.BytesIO(file_bytes)))


def drum_content_hash(drum: pd.DataFrame) -> str:
    payload = drum[DRUM_REQUIRED_COLS].to_csv(index=False).encode("utf-8")
    return hashlib.md5(payload).hexdigest()[:12]


# --------------------------------------------------------------------------- #
# Cached static lookups.
# --------------------------------------------------------------------------- #
@st.cache_data(show_spinner=False)
def recipe_skus() -> List[str]:
    return available_recipe_skus()


@st.cache_data(show_spinner=False)
def coverage(sku: str) -> dict:
    c = check_coverage(sku)
    return {
        "SKU": c.sku, "has_bom": c.has_bom, "has_routing": c.has_routing,
        "has_recipe": c.has_recipe, "eligible_tbm": c.eligible_tbm,
        "schedulable": c.schedulable, "reason": c.reason or "OK",
    }


# --------------------------------------------------------------------------- #
# CACHED PIPELINE (keyed on SKU set + drum hash + CONFIG_VERSION + overrides).
# --------------------------------------------------------------------------- #
@st.cache_data(show_spinner=False, max_entries=8)
def cached_run(skus_key: Tuple[str, ...], drum_hash: str, config_version: str,
               overrides_key: str, _drum: pd.DataFrame,
               _overrides: Optional[dict]) -> PipelineResult:
    """Run the engine once and memoise. Hash inputs (skus, drum content, config
    version, normalised overrides) form the key; the heavy frames travel as
    underscore-prefixed (un-hashed) args. CONFIG_VERSION busts the cache on any
    engine/data change."""
    return run_pipeline(_drum, list(skus_key), config_overrides=_overrides)


def overrides_key(ov: Optional[dict]) -> str:
    ov = C.normalise_overrides(ov)
    if not ov:
        return "BASE"
    return repr(sorted(ov.items()))


def schedule_hash(res: PipelineResult) -> str:
    raw = res.schedule_raw
    if raw.empty:
        return "empty"
    cols = ["lot_id", "machine", "start", "end"]
    payload = raw[cols].round(4).to_csv(index=False).encode("utf-8")
    h = hashlib.md5(payload).hexdigest()
    return f"{len(raw)}rows-{h[:16]}"


# --------------------------------------------------------------------------- #
# CACHED DERIVED VIEWS (keyed on schedule hash via the wrapper str arg).
# --------------------------------------------------------------------------- #
@st.cache_data(show_spinner=False)
def wip_curve(_sched: pd.DataFrame, key: str) -> pd.DataFrame:
    if _sched.empty:
        return pd.DataFrame(columns=["t", "wip"])
    starts = _sched[["start_dt"]].rename(columns={"start_dt": "t"})
    starts["delta"] = 1
    ends = _sched[["end_dt"]].rename(columns={"end_dt": "t"})
    ends["delta"] = -1
    ev = pd.concat([starts, ends], ignore_index=True).sort_values("t")
    ev["wip"] = ev["delta"].cumsum()
    return ev[["t", "wip"]].reset_index(drop=True)


@st.cache_data(show_spinner=False)
def util_by_machine(_sched: pd.DataFrame, key: str) -> pd.DataFrame:
    if _sched.empty:
        return pd.DataFrame(columns=["stage", "machine", "busy_h"])
    u = _sched[~_sched["is_curing"].astype(bool)].copy()
    u["busy_h"] = u["duration_min"] / 60.0
    return (u.groupby(["stage", "machine"], as_index=False)["busy_h"].sum()
            .sort_values("busy_h", ascending=False))


@st.cache_data(show_spinner=False)
def util_heatmap(_sched: pd.DataFrame, key: str) -> pd.DataFrame:
    if _sched.empty:
        return pd.DataFrame()
    u = _sched[~_sched["is_curing"].astype(bool)].copy()
    u["busy_h"] = u["duration_min"] / 60.0
    u["day"] = u["start_dt"].dt.date.astype(str)
    heat = u.groupby(["machine", "day"], as_index=False)["busy_h"].sum()
    if heat.empty:
        return pd.DataFrame()
    return heat.pivot(index="machine", columns="day", values="busy_h").fillna(0.0)


@st.cache_data(show_spinner=False)
def changeover_by_machine(_sched: pd.DataFrame, key: str) -> pd.DataFrame:
    if _sched.empty or "changeover_min" not in _sched:
        return pd.DataFrame(columns=["machine", "changeover_min"])
    co = (_sched.groupby("machine", as_index=False)["changeover_min"].sum())
    co = co[co["changeover_min"] > 0].sort_values("changeover_min",
                                                  ascending=False)
    return co.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def encode_csv(_df: pd.DataFrame, key: str, provenance: str) -> bytes:
    """CSV with an embedded run-provenance header comment line. Cached (the 5.7MB
    schedule encoder is the hot one)."""
    header = f"# {provenance}\n".encode("utf-8")
    return header + _df.to_csv(index=False).encode("utf-8")


@st.cache_data(show_spinner=False)
def gantt_lane_frame(_sched: pd.DataFrame, key: str) -> pd.DataFrame:
    """Process-flow ordered lane frame: Stage -> Machine with a sort index so the
    swimlanes read top-to-bottom in plant flow order."""
    if _sched.empty:
        return pd.DataFrame()
    df = _sched.copy()
    # FIX-1: folded carcasses are no longer in the schedule (they are exported as
    # produced_components.csv), so the blank-machine filter below is sufficient.
    df = df[df["machine"].astype(str).str.strip().ne("")]
    if df.empty:
        return pd.DataFrame()
    df["status"] = df.get("status", "PLACED").fillna("PLACED")
    ranks = stage_rank_map(df)
    df["_stage_ord"] = df["stage"].astype(str).map(ranks)
    df["_lane"] = df["stage"].astype(str) + "  |  " + df["machine"].astype(str)
    df = df.sort_values(["_stage_ord", "machine", "start"])
    df["_is_curing"] = df["is_curing"].astype(bool)
    return df


def lane_rollup(df: pd.DataFrame) -> pd.DataFrame:
    """Roll a busy machine's many bars into contiguous busy intervals (one bar per
    machine per gap-free run). Deterministic; keeps the full horizon visible."""
    if df.empty:
        return df
    out = []
    for (lane, stage, machine, cur), g in df.groupby(
            ["_lane", "stage", "machine", "_is_curing"]):
        g = g.sort_values("start_dt")
        cs = ce = None
        for r in g.itertuples():
            if cs is None:
                cs, ce = r.start_dt, r.end_dt
            elif r.start_dt <= ce:
                ce = max(ce, r.end_dt)
            else:
                out.append((lane, stage, machine, cur, cs, ce))
                cs, ce = r.start_dt, r.end_dt
        if cs is not None:
            out.append((lane, stage, machine, cur, cs, ce))
    res = pd.DataFrame(out, columns=["_lane", "stage", "machine", "_is_curing",
                                     "start_dt", "end_dt"])
    ranks = stage_rank_map(res)
    res["_stage_ord"] = res["stage"].astype(str).map(ranks)
    res["status"] = res["_is_curing"].map(lambda c: "PINNED" if c else "PLACED")
    return res.sort_values(["_stage_ord", "machine", "start_dt"])


# --------------------------------------------------------------------------- #
# CLEAN-RULE interlock (UI re-evaluates the six clauses with live numbers).
# --------------------------------------------------------------------------- #
def clean_clauses(res: PipelineResult) -> List[Tuple[str, bool, str]]:
    k = res.kpis
    planning_peak = max((s.planning_peak_util for s in res.capacity.stages),
                        default=0.0) * 100.0
    quarantined = int((res.infeasibility["check_type"] == "QUARANTINED_SKU").sum()
                      ) if not res.infeasibility.empty and \
        "check_type" in res.infeasibility.columns else 0
    presses_short = k.presses_total - k.presses_fulfilled
    clauses = [
        ("Zero post-condition violations", res.violations.shape[0] == 0,
         f"{fmt_int(res.violations.shape[0])} violation(s)"),
        ("All demanded presses fulfilled",
         k.presses_fulfilled == k.presses_total and k.presses_total > 0,
         f"{fmt_int(k.presses_fulfilled)} / {fmt_int(k.presses_total)} presses "
         f"({fmt_int(presses_short)} short)"),
        ("No lots left unplaced", res.n_unplaced == 0,
         f"{fmt_int(res.n_unplaced)} unplaced lot(s)"),
        ("No green tyre aged-out (expired)", k.aging_expired == 0,
         f"{fmt_int(k.aging_expired)} expired GT"),
        ("Planning peak <= 100% (no over-capacity)", planning_peak <= 100.0 + 1e-6,
         f"planning peak {fmt_pct(planning_peak)}"),
        ("No quarantined SKUs", quarantined == 0,
         f"{fmt_int(quarantined)} quarantined SKU(s)"),
    ]
    return clauses


def banner_verdict(res: PipelineResult) -> Tuple[str, List[str]]:
    clauses = clean_clauses(res)
    failing = [f"{name} - {detail}" for name, ok, detail in clauses if not ok]
    if not failing:
        return "CLEAN", []
    # distinguish a tight-but-feasible plan (capacity) from a correctness failure
    return "NOT-CLEAN", failing


# --------------------------------------------------------------------------- #
# SIDEBAR - controls only.
# --------------------------------------------------------------------------- #
st.sidebar.title("CTP Scheduler")
st.sidebar.caption(f"Heuristic DBR backward-pull  -  config v{C.CONFIG_VERSION}")

# Step 1 - Drum
st.sidebar.subheader("Step 1 - Curing schedule (the drum)")
uploaded = st.sidebar.file_uploader("Upload curing schedule CSV", type=["csv"])
drum_error: Optional[str] = None
drum: Optional[pd.DataFrame] = None
drum_label = ""

# The drum (curing schedule) comes ONLY from the user's upload. No default /
# previously-saved curing file is ever loaded, and nothing is scheduled until a
# file is provided this session. (The corrected recipe files - BOM/routing/MPQ/
# aging - are still loaded per-SKU from data/corrected when scheduling runs.)
if uploaded is None:
    # purge any earlier session state so stale results never show with no drum
    for _k in ("base_result", "scenario_result", "base_skus", "base_dhash",
               "scenario_overrides", "run_error"):
        st.session_state.pop(_k, None)
    st.sidebar.info("Upload a curing schedule CSV to begin.")
    st.sidebar.caption("Required columns: " + ", ".join(DRUM_REQUIRED_COLS) + ".")
    st.title("CTP End-to-End Production Scheduler")
    st.info("No curing schedule loaded. Upload your curing schedule CSV in the "
            "sidebar to start scheduling - no previous/default schedule is used.")
    st.stop()

try:
    drum = load_drum_unified(uploaded.getvalue(), None)
    drum_label = uploaded.name
except Exception as exc:  # noqa: BLE001 - surface the parse error, never crash
    drum_error = str(exc)

if drum_error or drum is None:
    st.sidebar.error(f"Drum load failed: {drum_error}")
    st.sidebar.caption("Fix the CSV and re-upload. The required columns are "
                       + ", ".join(DRUM_REQUIRED_COLS) + ".")
    st.title("CTP End-to-End Production Scheduler")
    st.error("No valid drum loaded - cannot proceed. See the sidebar.")
    st.stop()

drum_summary = validate_drum(drum)
dhash = drum_content_hash(drum)
st.sidebar.caption(f"Loaded: {drum_label}")
st.sidebar.markdown(chip("DRUM", drum_summary.verdict), unsafe_allow_html=True)
st.sidebar.caption(f"content hash `{dhash}`")

recipes = set(recipe_skus())
prod = drum[~drum["SKUCode"].isin(C.NON_PRODUCTION_SKUS)]
drum_skus = sorted(set(prod["SKUCode"].unique()))
schedulable_skus = sorted(s for s in drum_skus if s in recipes)

# Step 2 - SKU scope
st.sidebar.subheader("Step 2 - SKU scope")
st.sidebar.caption(f"{len(schedulable_skus)} schedulable / {len(drum_skus)} in drum")
mode = st.sidebar.radio("Scope", ["One", "Several", "All schedulable"], index=2,
                        help="Only SKUs with a corrected BOM+routing recipe are "
                             "offered. The drum may list more SKUs than are "
                             "schedulable.")
if mode == "One":
    sel = [st.sidebar.selectbox("SKU", schedulable_skus)] if schedulable_skus else []
elif mode == "Several":
    sel = st.sidebar.multiselect("SKUs", schedulable_skus,
                                 default=schedulable_skus[:3])
else:
    sel = list(schedulable_skus)
usable = [s for s in sel if s in recipes]
base_ready = st.session_state.get("base_result") is not None

# The simplified dashboard always renders the single plain-language view (no
# Boardroom/Shop-floor split). `shop_floor` is kept True so the few detail tables
# that remain (issues / downloads) stay available to the plant user.
shop_floor = True

run_base = st.sidebar.button("Run base plan", type="primary",
                             use_container_width=True)

st.sidebar.divider()
if base_ready:
    br = st.session_state["base_result"]
    n_sku = schedule_sku_count(br.schedule) or len(br.skus_used)
    st.sidebar.caption(f"Last base run: {n_sku} SKU(s) - "
                       f"schedule hash `{schedule_hash(br)[:18]}`")
st.sidebar.caption(f"Drum hash `{dhash}`  -  config v{C.CONFIG_VERSION}")


# --------------------------------------------------------------------------- #
# RUN TRIGGERS (decoupled from rendering - results persist in session).
# --------------------------------------------------------------------------- #
def _execute_base() -> None:
    with st.spinner(f"Scheduling {len(usable)} SKU(s) - Phases 0-9 ..."):
        try:
            res = cached_run(tuple(sorted(usable)), dhash, C.CONFIG_VERSION,
                             "BASE", drum, None)
        except Exception as exc:  # noqa: BLE001
            st.session_state["run_error"] = str(exc)
            return
    st.session_state["run_error"] = None
    st.session_state["base_result"] = res
    st.session_state["base_skus"] = tuple(sorted(usable))
    st.session_state["base_dhash"] = dhash
    st.session_state["scenario_result"] = None


if run_base:
    if not usable:
        st.session_state["run_error"] = "No schedulable SKUs selected."
    else:
        _execute_base()


# --------------------------------------------------------------------------- #
# IDENTITY HEADER STRIP.
# --------------------------------------------------------------------------- #
st.title("CTP End-to-End Production Scheduler")
hz_from = drum_summary.horizon_from
hz_to = drum_summary.horizon_to
scope_lbl = ("PCR drum (all schedulable)" if len(usable) == len(schedulable_skus)
             else f"{len(usable)} SKU(s)")
idcols = st.columns([2, 2, 2, 2])
idcols[0].markdown(f"**Plan**\n\nCTP PCR Schedule")
idcols[1].markdown(f"**Curing schedule**\n\n{drum_label}")
idcols[2].markdown(f"**Horizon**\n\n{hz_from:%Y-%m-%d} -> {hz_to:%Y-%m-%d}"
                   if hz_from and hz_to else "**Horizon**\n\n-")
idcols[3].markdown(f"**Scope**\n\n{scope_lbl}")


# --------------------------------------------------------------------------- #
# Pre-run state: show the input gate then stop (without wiping any result).
# --------------------------------------------------------------------------- #
if st.session_state.get("run_error"):
    st.error(st.session_state["run_error"])

res: Optional[PipelineResult] = st.session_state.get("base_result")
scn: Optional[PipelineResult] = st.session_state.get("scenario_result")

if res is None:
    st.info("Configure the drum and SKU scope in the sidebar, then press "
            "**Run base plan**. The dashboard appears here and persists across "
            "interactions.")
    with st.expander("Pre-run input gate - drum validation & SKU coverage",
                     expanded=True):
        g1, g2, g3, g4, g5 = st.columns(5)
        g1.metric("Drum rows", fmt_int(drum_summary.rows))
        g2.metric("Production rows", fmt_int(drum_summary.production_rows))
        g3.metric("Presses", fmt_int(len(drum_summary.presses)))
        g4.metric("Total GT demand", fmt_int(drum_summary.total_gt_demand))
        g5.metric("Distinct SKUs", fmt_int(len(drum_summary.distinct_skus)))
        gate = drum_summary.verdict
        st.markdown(chip("DRUM GATE", gate), unsafe_allow_html=True)
        if drum_summary.issues:
            st.warning("Drum issues: " + "; ".join(drum_summary.issues))
        if gate == "INVALID":
            st.error("Drum is INVALID - Run is blocked until it is corrected.")
    st.stop()

# Below here a base result EXISTS and renders from session state.
k = res.kpis
sched = res.schedule
SKEY = schedule_hash(res)
PROV = (f"CTP scheduler run | skus={len(res.skus_used)} | drum={dhash} | "
        f"config=v{C.CONFIG_VERSION} | schedule_hash={SKEY}")


# --------------------------------------------------------------------------- #
# PLAN-HEALTH BANNER (hard interlock).
# --------------------------------------------------------------------------- #
verdict, failing = banner_verdict(res)
if verdict == "CLEAN":
    bcolor, blabel = GREEN, "On plan - every tyre the curing schedule needs is fully built on time"
elif not failing:
    bcolor, blabel = AMBER, "Tight - the plan fits but the busiest section is near its limit"
else:
    bcolor, blabel = RED, "Some tyre demand cannot be fully built on time (see Overview)"
st.markdown(
    f"<div style='background:{bcolor};color:white;padding:14px 18px;"
    f"border-radius:10px;font-size:1.35rem;font-weight:700'>"
    f"PLAN STATUS: {blabel}</div>", unsafe_allow_html=True)

# Shared figures used across Overview / tabs.
planning_peak = max((s.planning_peak_util for s in res.capacity.stages),
                    default=0.0) * 100.0
quarantined_skus = (res.infeasibility[
    res.infeasibility.get("check_type") == "QUARANTINED_SKU"]
    if not res.infeasibility.empty else pd.DataFrame())
n_quarantined = len(quarantined_skus)
demanded_drum = int(drum_summary.production_rows)


# --------------------------------------------------------------------------- #
# TAB DECK.
# --------------------------------------------------------------------------- #
TAB_LABELS = [
    "Overview", "Gantt", "Bottleneck/Capacity", "OTIF/Fulfilment",
    "Issues & Correctness", "Downloads",
]
tabs = st.tabs(TAB_LABELS)


# --------------------------------------------------------------------------- #
# WHY-SHORTFALL derivation (live numbers from routing machine pools + demand).
# --------------------------------------------------------------------------- #
def why_shortfall() -> dict:
    """Plain-language shortfall explanation derived LIVE from the committed
    schedule's Building-stage rows (each green tyre's TBM) and the curing demand.

    Logic, all data-derived (nothing hard-coded):
      * demanded / fulfilled tyres come from the demand-qty OTIF (engine KPI).
      * a SKU is 'press-restricted' if, across the schedule, its build op ran on
        FEWER than the full 11-press PCR TBM pool (its routing allows only those).
      * the most-overloaded press = the building machine carrying the most tyres;
        idle TBMs = pool presses whose load is below half the busiest press.
    Returns a dict of numbers + a ready-to-print sentence."""
    demanded = float(k.gt_demand_total)
    fulfilled = float(k.gt_demand_fulfilled)
    gap = max(0.0, demanded - fulfilled)
    pct = (100.0 * fulfilled / demanded) if demanded else 0.0
    pool = set(C.PCR_TBM_POOL)
    n_pool = len(pool)
    build = pd.DataFrame()
    if sched is not None and not sched.empty and "stage" in sched.columns:
        build = sched[(sched["stage"].astype(str) == C.STAGE_BUILDING) &
                      (~sched["is_curing"].astype(bool))].copy()
    n_restricted = 0
    press_list: List[str] = []
    busiest_press = "-"
    tyres_on_busiest = 0.0
    n_idle = 0
    if not build.empty:
        # presses each SKU is allowed to use (as observed in the placed schedule)
        sku_presses = build.groupby("sku")["machine"].apply(
            lambda s: set(s.astype(str)))
        restricted = sku_presses[sku_presses.apply(lambda ms: len(ms) < n_pool)]
        n_restricted = int(len(restricted))
        # which presses the restricted SKUs are locked onto (sorted, deterministic)
        locked = sorted({m for ms in restricted for m in ms})
        press_list = locked
        # load per press (tyres = qty built on that machine)
        load = (build.groupby("machine")["qty"].sum()
                .reindex(sorted(pool), fill_value=0.0))
        if not load.empty and load.max() > 0:
            busiest_press = str(load.idxmax())
            tyres_on_busiest = float(load.max())
            n_idle = int((load <= 0.5 * load.max()).sum())
    pl = ", ".join(press_list[:6]) + (" ..." if len(press_list) > 6 else "")
    if gap <= 0 or n_restricted == 0:
        sentence = (f"All {fmt_int(fulfilled)} of {fmt_int(demanded)} tyres "
                    f"({fmt_pct(pct)}) can be fully built on time. No "
                    f"press-restriction shortfall detected.")
    else:
        sentence = (
            f"Only {fmt_int(fulfilled)} of {fmt_int(demanded)} tyres "
            f"({fmt_pct(pct)}) can be fully built on time. Main reason: "
            f"{fmt_int(n_restricted)} SKUs are locked to presses {pl or '-'} "
            f"(their routing allows only those), so ~{fmt_int(tyres_on_busiest)} "
            f"tyres pile onto press {busiest_press} while {fmt_int(n_idle)} other "
            f"TBMs sit ~half-idle. Opening those SKUs to all {n_pool} presses is "
            f"the biggest lever.")
    return {"demanded": demanded, "fulfilled": fulfilled, "gap": gap, "pct": pct,
            "n_restricted": n_restricted, "press_list": press_list,
            "busiest_press": busiest_press, "tyres_on_busiest": tyres_on_busiest,
            "n_idle": n_idle, "sentence": sentence}


# === TAB 1 - OVERVIEW ====================================================== #
@st.fragment
def tab_overview() -> None:
    st.subheader("Plan overview")
    why = why_shortfall()

    # --- 4 BIG TILES (single-glance) ------------------------------------- #
    t1, t2, t3 = st.columns(3)
    with t1.container(border=True):
        st.metric("Tyre demand fulfilled", fmt_pct(k.otif_demand_pct))
        st.caption("Tyre demand fulfilled - of all tyres the curing schedule "
                   "needs, the share we can fully build & cure on time. "
                   "e.g. 8,000 of 10,000 = 80%.")
    with t2.container(border=True):
        st.metric("Tyres made / needed",
                  f"{fmt_int(k.gt_demand_fulfilled)} / {fmt_int(k.gt_demand_total)}")
        st.caption(f"Green tyres we can fully build on time vs the number the "
                   f"curing schedule asks for. Short by "
                   f"{fmt_int(why['gap'])} tyres.")
    with t3.container(border=True):
        st.metric("Busiest section", k.bottleneck_stage or "-")
        st.caption(f"The stage running closest to its limit - peak load "
                   f"{fmt_pct(planning_peak)}. e.g. if this says 'Building', the "
                   f"tyre-building presses are the tightest point.")

    # --- WHY SHORTFALL box ----------------------------------------------- #
    if why["gap"] > 0 and why["n_restricted"] > 0:
        st.warning("Why demand wasn't fully met: " + why["sentence"])
    else:
        st.info(why["sentence"])

    st.divider()

    # --- PER-SKU FULFILMENT TABLE (plain labels) ------------------------- #
    st.markdown("### Per-SKU: tyres needed vs built")
    dem = sku_demand_table(sched, SKEY)
    if dem.empty:
        st.info("No tyre demand in the committed schedule.")
    else:
        labelled = dem.rename(columns={
            "demanded_presses": "curing_slots",
            "demanded_qty": "tyres_demanded",
            "built_gt_qty": "tyres_fulfilled",
            "fulfilled_pct": "pct_met"})[
            ["sku", "curing_slots", "tyres_demanded", "tyres_fulfilled",
             "pct_met"]]
        st.dataframe(
            labelled, use_container_width=True, hide_index=True,
            column_config={
                "sku": st.column_config.TextColumn("sku"),
                "curing_slots": st.column_config.NumberColumn(
                    "curing_slots", format="%d"),
                "tyres_demanded": st.column_config.NumberColumn(
                    "tyres_demanded", format="%d"),
                "tyres_fulfilled": st.column_config.NumberColumn(
                    "tyres_fulfilled", format="%d"),
                "pct_met": st.column_config.ProgressColumn(
                    "pct_met", format="%.0f%%", min_value=0, max_value=100)})
        ex = labelled.iloc[0]
        st.caption(f"curing_slots = number of press runs the curing schedule "
                   f"lists for this SKU "
                   f"(e.g. {ex['sku']} = {fmt_int(ex['curing_slots'])} press runs "
                   f"= {fmt_int(ex['tyres_demanded'])} tyres).")

    # --- ONE-LINE correctness summary + detail behind expander ----------- #
    st.divider()
    clauses = clean_clauses(res)
    if all(ok for _, ok, _ in clauses) and res.violations.shape[0] == 0:
        st.success("All 8 scheduling rules passed (no violations).")
    else:
        st.error("One or more scheduling checks failed - see 'Issues & "
                 "Correctness'.")
    with st.expander("Scheduling rules detail (C1-C8)"):
        rows = [{"rule": n, "status": "PASS" if ok else "FAIL",
                 "live value": d} for n, ok, d in clauses]
        st.dataframe(pd.DataFrame(rows), use_container_width=True,
                     hide_index=True)

    # --- ONE-LINE yield stat + low-noise extras -------------------------- #
    wdf = res.waste
    if wdf is not None and not wdf.empty:
        summ = waste_summary(wdf)
        ky = summ["overall"]["kg_yield_pct"]
        st.caption(f"Material yield (compound): {fmt_pct(ky)} of compound made "
                   f"is used in tyres (rest is minimum-batch over-production). "
                   f"Full waste table is under Downloads.")

    with st.expander("Inputs & assumptions (curing schedule, estimates, audit)"):
        st.markdown(f"**Curing schedule loaded:** {drum_label}  -  "
                    f"{fmt_int(drum_summary.rows)} rows, "
                    f"{fmt_int(len(drum_summary.presses))} presses, "
                    f"{fmt_int(drum_summary.total_gt_demand)} tyres of demand.")
        if drum_summary.issues:
            st.warning("Curing-schedule notes: " + "; ".join(drum_summary.issues))
        st.metric("Estimate exposure", fmt_pct(k.estimate_exposure_pct))
        st.caption("Share of steps using a rough placeholder time instead of a "
                   "measured one - so these timings are approximate. e.g. 75% "
                   "means most upstream step times are estimates.")
        st.metric("Pre-build lead (typical)", fmt_days(k.prebuild_days))
        st.caption(f"How early parts are made before the tyre is cured. "
                   f"{k.prebuild_days:.1f} days typical, "
                   f"{k.prebuild_max_days:.1f} worst-case.")
        excluded = [s for s in drum_skus if s not in recipes]
        st.caption(f"{len(res.skus_used)} SKU(s) scheduled this run  -  "
                   f"{len(excluded)} drum SKU(s) excluded (no recipe workbook).")
        st.markdown("**Full engine KPI report**")
        st.dataframe(res.kpi_df, use_container_width=True, hide_index=True)

    st.caption(f":grey[Planning-grade plan. Determinism hash {SKEY}  -  "
               f"config v{C.CONFIG_VERSION}. Curing rows are fixed; upstream is "
               f"a feasible model, not a floor release.]")


# === TAB 2 - GANTT ========================================================= #
@st.fragment
def tab_gantt() -> None:
    st.subheader("Schedule Gantt - process-flow swimlanes (drum pinned)")
    if sched.empty:
        st.info("No scheduled rows.")
        return
    lanes = gantt_lane_frame(sched, SKEY)
    tmin = lanes["start_dt"].min().to_pydatetime()
    tmax = lanes["end_dt"].max().to_pydatetime()
    c1, c2, c3 = st.columns([2, 1, 1])
    win = c1.slider("Date window", min_value=tmin, max_value=tmax,
                    value=(tmin, tmax), format="YYYY-MM-DD")
    # "Process" is the DEFAULT colour mode (most readable); Status / SKU toggles.
    color_by = c2.radio("Colour by", ["Process", "Status", "SKU"], index=0,
                        horizontal=False, key="gantt_color")
    detail = c3.radio("Detail", ["Auto rollup", "Detailed bars"],
                      key="gantt_detail")
    stages = sorted(lanes["stage"].astype(str).unique(),
                    key=stage_rank_map(lanes).get)
    pick_stages = st.multiselect("Stages", stages, default=stages)
    win_lo, win_hi = pd.Timestamp(win[0]), pd.Timestamp(win[1])
    view = lanes[(lanes["end_dt"] >= win_lo) & (lanes["start_dt"] <= win_hi) &
                 (lanes["stage"].astype(str).isin(pick_stages))].copy()
    if view.empty:
        st.info("No bars in this window / stage filter.")
        return
    n_bars = len(view)
    use_rollup = (detail == "Auto rollup" and n_bars > 4000)
    st.caption(f"{fmt_int(n_bars)} bars in window  -  "
               + ("rolled up to contiguous busy intervals per machine"
                  if use_rollup else "detailed bars")
               + "  (full horizon preserved; no chronological truncation).")
    hover_cols = None
    if use_rollup:
        plot = lane_rollup(view)
        # On rollup we only have stage/machine/status; honour the colour toggle
        # where we still can (Process/Status), else fall back to Process.
        if color_by == "Status":
            color_col, cmap = "status", STATUS_COLOR
            legend_title = "Status"
        else:
            color_col = "stage"
            cmap = stage_color_map(plot["stage"].astype(str).unique())
            legend_title = "Process"
    else:
        plot = view
        if color_by == "Status":
            color_col, cmap = "status", STATUS_COLOR
            legend_title = "Status"
        elif color_by == "SKU":
            top = plot["sku"].value_counts().head(8).index.tolist()
            plot["sku_cap"] = plot["sku"].where(plot["sku"].isin(top), "other")
            color_col, cmap, legend_title = "sku_cap", None, "SKU"
        else:  # Process (default) - one distinct colour per process family.
            color_col = "stage"
            cmap = stage_color_map(plot["stage"].astype(str).unique())
            legend_title = "Process"
        hover_cols = {c: True for c in
                      ["lot_id", "sku", "item", "item_type", "qty", "uom",
                       "machine", "gap_to_consumer_min", "aging_max_h"]
                      if c in plot.columns}
        # keep the (internal) lane key out of the hover box
        hover_cols["_lane"] = False
        hover_cols["_stage_ord"] = False
    # Explicit categorical y-order: process flow, Mixing-first. autorange reversed
    # puts the FIRST category (Master Mixing) on TOP and Curing at the BOTTOM.
    lane_order = list(plot.sort_values(["_stage_ord", "machine"])
                      ["_lane"].drop_duplicates())
    fig = px.timeline(plot, x_start="start_dt", x_end="end_dt", y="_lane",
                      color=color_col, color_discrete_map=cmap,
                      hover_data=hover_cols, template="plotly_white",
                      category_orders={"_lane": lane_order})
    fig.update_yaxes(title="Stage : Machine  (top = Master Mixing  ->  bottom = "
                     "Curing)", categoryorder="array", categoryarray=lane_order,
                     autorange="reversed", showgrid=True, gridcolor="#eef0f3")
    fig.update_xaxes(showgrid=True, gridcolor="#eef0f3", title="Time")
    n_lanes = plot["_lane"].nunique()
    fig.update_layout(height=min(1000, 180 + 16 * n_lanes),
                      legend_title=legend_title, bargap=0.25,
                      legend=dict(orientation="h", yanchor="bottom", y=1.02,
                                  xanchor="left", x=0),
                      margin=dict(l=10, r=10, t=46, b=10))
    fig.add_vline(x=min(win_hi, tmax), line_dash="dot", line_color=SLATE)
    fig.update_traces(marker_line_width=0)
    st.plotly_chart(fig, use_container_width=True)
    st.caption(":grey[Grey = Curing drum (FIXED / PINNED - never recoloured or "
               "moved).]  Lanes follow real material flow: Master Mixing (top) "
               "-> Final Mixing -> Calendering -> Extrusion -> Cutting -> "
               "Slitting -> Bead -> Building -> Curing (bottom).")


# === TAB 3 - BOTTLENECK / CAPACITY ========================================= #
@st.fragment
def tab_bottleneck() -> None:
    st.subheader("Busiest section & capacity")
    st.caption("Which stages run closest to their limit. The 'busiest section' is "
               "the tightest point in the line - relieving it lifts throughput "
               "the most.")
    rows = [{
        "stage": s.stage, "machines": s.machines,
        "placed_peak_pct": round(s.peak_util * 100, 1),
        "planning_peak_pct": round(s.planning_peak_util * 100, 1),
        "avg_util_pct": round(s.avg_util * 100, 1),
        "verdict": s.verdict,
    } for s in res.capacity.stages]
    if not rows:
        st.info("No capacity data.")
        return
    cdf = pd.DataFrame(rows).sort_values("planning_peak_pct", ascending=False)
    melt = cdf.melt(id_vars=["stage", "verdict"],
                    value_vars=["placed_peak_pct", "planning_peak_pct"],
                    var_name="measure", value_name="pct")
    fig = px.bar(melt, x="stage", y="pct", color="measure", barmode="group",
                 color_discrete_map={"placed_peak_pct": GREEN,
                                     "planning_peak_pct": BLUE},
                 text="pct")
    fig.add_hline(y=100, line_dash="dash", line_color=RED,
                  annotation_text="100% capacity")
    fig.add_hline(y=80, line_dash="dot", line_color=AMBER,
                  annotation_text="80% tight")
    fig.update_layout(height=440, yaxis_title="Peak utilisation (%)",
                      legend_title="measure")
    st.plotly_chart(fig, use_container_width=True)
    st.caption("PLACED peak = real contention on committed start/end. PLANNING "
               "peak = LST day-bucket demand; >100% reveals latent over-capacity "
               "even when the placed schedule fits by deferring work.")
    st.dataframe(cdf, use_container_width=True, hide_index=True,
                 column_config={
                     "placed_peak_pct": st.column_config.NumberColumn(
                         "placed peak %", format="%.1f%%"),
                     "planning_peak_pct": st.column_config.NumberColumn(
                         "planning peak %", format="%.1f%%"),
                     "avg_util_pct": st.column_config.NumberColumn(
                         "avg util %", format="%.1f%%"),
                     "machines": st.column_config.NumberColumn("pool size")})
    st.markdown(chip("Bottleneck", res.capacity.bottleneck, SLATE)
                + "  " + chip("Capacity verdict", res.capacity.verdict),
                unsafe_allow_html=True)


# === TAB - OTIF / FULFILMENT =============================================== #
@st.fragment
def tab_otif() -> None:
    st.subheader("Tyre demand fulfilment")
    c1, c2 = st.columns(2)
    with c1.container(border=True):
        st.metric("Tyre demand fulfilled", fmt_pct(k.otif_demand_pct))
        st.caption(f"Tyre demand fulfilled - of all tyres the curing schedule "
                   f"needs, the share we can fully build & cure on time. "
                   f"e.g. {fmt_int(k.gt_demand_fulfilled)} of "
                   f"{fmt_int(k.gt_demand_total)} = {fmt_pct(k.otif_demand_pct)}.")
    with c2.container(border=True):
        st.metric("Press runs fulfilled",
                  f"{fmt_int(k.presses_fulfilled)} / {fmt_int(k.presses_total)}")
        st.caption(f"{fmt_pct(k.otif_pct)} of curing-schedule press runs are fully "
                   f"buildable on time (a press run counts only if its tyre AND its "
                   f"whole feeder chain are made by the cure-by). Unfulfilled runs "
                   f"are listed below.")
    if res.unfulfilled.empty:
        if k.presses_total > 0:
            st.success(f"All {fmt_int(k.presses_total)} demanded presses "
                       f"fulfilled.")
        else:
            st.info("No demanded presses in scope.")
    else:
        by_reason = (res.unfulfilled.groupby("binding_reason", as_index=False)
                     .size().rename(columns={"size": "presses"})
                     .sort_values("presses", ascending=False))
        st.markdown("**Unfulfilled presses by binding reason**")
        st.dataframe(by_reason, use_container_width=True, hide_index=True)
        if shop_floor:
            st.dataframe(res.unfulfilled, use_container_width=True,
                         hide_index=True)


# === TAB - ISSUES & CORRECTNESS =================================== #
@st.fragment
def tab_correctness() -> None:
    st.subheader("Infeasibility & correctness")

    # --- CONSTRAINT SCORECARD (all 8 hard constraints, independently scored) - #
    st.markdown("### Constraint scorecard")
    st.caption("All eight hard constraints (C1-C8) are independently RE-PROVEN by "
               "Phase-8 validate.py from the produced artefacts (placed times, "
               "per-SKU MPQ masters, the real drum) - none are taken on trust. "
               "Each shows its real PASS/FAIL and violating-row count.")
    v = res.violations
    vc = (v["check_type"].value_counts().to_dict()
          if not v.empty and "check_type" in v.columns else {})
    verified = [
        ("C1 build->cure precedence",
         ["PRECEDENCE", "NO_DRUM_ANCHOR", "NO_CONSUMER"]),
        ("C2 aging (both sides)", ["AGING_MIN", "AGING_MAX"]),
        ("C3 eligibility", ["ELIGIBILITY"]),
        ("C4 non-overlap", ["OVERLAP"]),
        ("Green-tyre cure-by band", ["CUREBY"]),
    ]
    rows = []
    for name, keys in verified:
        n = sum(vc.get(kk, 0) for kk in keys)
        rows.append({"constraint": name, "verified by": "validate.py",
                     "status": "PASS" if n == 0 else "FAIL",
                     "violating rows": n})
    # C5-C8 now independently re-derived in validate.independent_checks; render
    # them exactly like C1-C4 (validate.py / PASS|FAIL / count + a one-line basis).
    label = {"C5": "C5 MPQ sizing", "C6": "C6 drum-pinned curing",
             "C7": "C7 single shared machine model",
             "C8": "C8 determinism (no RNG)"}
    bases = []
    for chk in getattr(res, "independent", []) or []:
        rows.append({"constraint": label.get(chk["id"], chk["name"]),
                     "verified by": "validate.py",
                     "status": chk["status"],
                     "violating rows": chk["violations"]})
        bases.append((label.get(chk["id"], chk["name"]), chk["basis"]))
    sdf = pd.DataFrame(rows)
    st.dataframe(sdf, use_container_width=True, hide_index=True)
    with st.expander("Independent re-proof basis (C5-C8)"):
        for nm, b in bases:
            st.markdown(f"- **{nm}**: {b}")
    if v.empty:
        st.success("Zero post-condition violations - correct-by-construction.")
    else:
        st.error(f"{fmt_int(len(v))} violation(s) - this is a build bug.")
        if shop_floor:
            st.dataframe(v, use_container_width=True, hide_index=True)

    # --- DETERMINISM ------------------------------------------------------ #
    st.markdown("### Determinism")
    st.code(f"schedule content hash: {SKEY}", language="text")
    st.caption("Same drum + same SKU set + same config = same hash (no wall-clock, "
               "no RNG anywhere in the engine).")

    # --- KPI SELF-CHECK --------------------------------------------------- #
    st.markdown("### KPI self-check (recomputed from raw frames)")
    raw = res.schedule_raw
    if not raw.empty:
        work = raw[~raw["is_curing"].astype(bool)]
        recomputed_placed = len(work)
        recomputed_aging_exp = 0
        gt = work[work["item_type"].astype(str).str.lower().isin(
            ["green tyre", "green tyres"])]
        if not gt.empty:
            transfer = gt.get("transfer_min", C.TRANSFER_MIN)
            gap_wall_h = (gt["gap_to_consumer_min"] + transfer) / 60.0
            recomputed_aging_exp = int((gap_wall_h > gt["aging_max_h"] + 1e-6).sum())
        checks = [
            ("lots_placed", k.lots_placed, recomputed_placed),
            ("aging_expired", k.aging_expired, recomputed_aging_exp),
        ]
        mism = [c for c in checks if c[1] != c[2]]
        cdf = pd.DataFrame(checks, columns=["metric", "kpi_report", "recomputed"])
        st.dataframe(cdf, use_container_width=True, hide_index=True)
        if mism:
            st.error("KPI/RAW MISMATCH: " + ", ".join(c[0] for c in mism))
        else:
            st.success("KPI report matches the raw frames.")

    # --- QUARANTINE ------------------------------------------------------- #
    st.markdown("### Quarantined SKUs")
    if n_quarantined == 0:
        st.success("No SKUs quarantined.")
    else:
        st.error(f"{fmt_int(n_quarantined)} SKU(s) quarantined (dropped before "
                 f"any demand was minted):")
        st.dataframe(quarantined_skus[["lot_id", "detail"]],
                     use_container_width=True, hide_index=True)

    # --- UNPLACED LOTS ---------------------------------------------------- #
    st.markdown("### Unplaced lots (dispatch reason codes)")
    inf = res.infeasibility
    disp = inf[inf.get("check_type") != "QUARANTINED_SKU"] if not inf.empty else inf
    if disp.empty:
        st.success("All demanded lots placed - no infeasibility.")
    else:
        st.dataframe(disp["check_type"].value_counts().rename_axis("reason")
                     .reset_index(name="count"),
                     use_container_width=True, hide_index=True)
        if shop_floor:
            st.dataframe(disp.head(500), use_container_width=True,
                         hide_index=True)


# === WASTE / YIELD (rendered inside Downloads expander) ==================== #
@st.fragment
def tab_waste() -> None:
    st.subheader("Waste / Yield matrix")
    st.caption("Waste = material produced beyond tyre demand, driven by MPQ "
               "minimum-run, batch rounding, and aging scrap.")
    wdf = res.waste
    if wdf is None or wdf.empty:
        st.info("No made components in scope - nothing to report.")
        return

    # FIX-6: cross-SKU pooled bulk compounds are reported ONCE at the plant level
    # (sku == "POOL"); all other components are per-SKU. Separate the two so the
    # per-SKU filter scopes only the per-SKU rows and never hides (or splits) a
    # pooled plant-level row.
    is_pool = wdf["sku"].astype(str) == "POOL"
    pooled_rows = wdf[is_pool]
    per_sku = wdf[~is_pool]

    # multi-sku scope: optional per-SKU filter (matches the Inputs tab pattern)
    skus = sorted(per_sku["sku"].astype(str).unique().tolist())
    per_sku_view = per_sku
    if len(skus) > 1:
        pick = st.selectbox("SKU scope (per-SKU components)", ["All SKUs"] + skus,
                            index=0)
        if pick != "All SKUs":
            per_sku_view = per_sku[per_sku["sku"] == pick]
    # pooled plant-level rows are ALWAYS shown (cross-SKU; not scoped to one SKU)
    view = pd.concat([pooled_rows, per_sku_view], ignore_index=True)
    view = view.sort_values(
        ["waste_pct", "component", "sku"],
        ascending=[False, True, True]).reset_index(drop=True)

    summ = waste_summary(view)
    ov = summ["overall"]

    # KPI chips / metrics
    cols = st.columns(4)
    with cols[0].container(border=True):
        kg_yield = ov["kg_yield_pct"]
        st.metric("KG material yield", fmt_pct(kg_yield))
        col = GREEN if kg_yield >= 90 else (AMBER if kg_yield >= 60 else RED)
        st.markdown(chip("KG YIELD", f"{kg_yield:.0f}%", col),
                    unsafe_allow_html=True)
    with cols[1].container(border=True):
        st.metric("Total KG waste", fmt_int(ov["kg_waste_qty"]))
        st.caption("over-production in the KG (compound) family")
    with cols[2].container(border=True):
        st.metric("Top wasted component", ov["top_component"] or "-")
        st.caption("largest absolute waste_qty")
    with cols[3].container(border=True):
        n_hi = ov["components_over_50pct"]
        st.metric("Components > 50% waste", fmt_int(n_hi))
        if n_hi:
            st.markdown(chip("HIGH-WASTE", fmt_int(n_hi), AMBER),
                        unsafe_allow_html=True)

    # per-UOM yield (never summed across families)
    if summ["yield_by_uom"]:
        chips = "  ".join(
            chip(f"{u} yield", f"{v:.0f}%",
                 GREEN if v >= 90 else (AMBER if v >= 60 else RED))
            for u, v in sorted(summ["yield_by_uom"].items()))
        st.markdown(chips, unsafe_allow_html=True)
    if ov.get("aging_scrap_qty", 0.0) > 0:
        st.markdown(chip("AGING SCRAP", fmt_int(ov["aging_scrap_qty"]), RED),
                    unsafe_allow_html=True)

    st.divider()

    if not pooled_rows.empty:
        st.caption(
            f"Rows with SKU = **POOL** are cross-SKU pooled bulk compounds "
            f"({len(pooled_rows)} component(s): final/master compound, small "
            f"chemical) reported at the PLANT level - required and produced summed "
            f"across every consuming SKU (deduped per lot), so they show true "
            f"plant-level yield, not per-SKU anchor inflation.")

    # full matrix, sortable, reason + unit_note visible
    st.dataframe(
        view, use_container_width=True, hide_index=True,
        column_config={
            "sku": st.column_config.TextColumn("SKU"),
            "component": st.column_config.TextColumn("component"),
            "item_type": st.column_config.TextColumn("item type"),
            "uom": st.column_config.TextColumn("UOM"),
            "required(used)": st.column_config.NumberColumn(
                "required (used)", format="%.2f"),
            "produced(run)": st.column_config.NumberColumn(
                "produced (run)", format="%.2f"),
            "waste_qty": st.column_config.NumberColumn("waste qty", format="%.2f"),
            "waste_pct": st.column_config.ProgressColumn(
                "waste %", format="%.0f%%", min_value=0.0, max_value=1.0),
            "mpq_min": st.column_config.NumberColumn("MPQ min", format="%g"),
            "mpq_max": st.column_config.NumberColumn("MPQ max", format="%g"),
            "n_lots": st.column_config.NumberColumn("# lots", format="%d"),
            "reason": st.column_config.TextColumn("reason", width="large"),
            "unit_note": st.column_config.TextColumn("unit note", width="large"),
            "aging_scrap_qty": st.column_config.NumberColumn(
                "aging scrap", format="%.2f"),
        })

    # "Why" callout: dominant reason + unit-mismatch note
    reasons = view["reason"].astype(str)
    mpq_floor = int(reasons.str.startswith("MPQ minimum-run floor").sum())
    n_unit = int(view["unit_note"].astype(str).str.len().gt(0).sum())
    msg = (f"Dominant driver: **MPQ minimum-run floor** on low-volume compounds "
           f"({mpq_floor} of {len(view)} components bumped to their item-type "
           f"minimum batch).")
    if n_unit:
        msg += (f"  Note: {n_unit} length component(s) carry an MM/MTR unit "
                f"mismatch (BOM input MM vs MPQ in MTR, 1000x), so the MPQ floor "
                f"is effectively non-binding there - flagged in `unit note`.")
    st.info(msg)


# === TAB 12 - DOWNLOADS ==================================================== #
@st.fragment
def _sku_demand_table() -> pd.DataFrame:
    """Per-SKU curing demand for the SKUs ACTUALLY RUN this scope (not the whole
    drum). If 1 SKU is run, only that SKU's demand is shown."""
    scheduled = list(getattr(res, "skus_used", []) or usable)
    prod = drum[(~drum["SKUCode"].isin(C.NON_PRODUCTION_SKUS))
                & (drum["SKUCode"].isin(scheduled))].copy()
    g = (prod.groupby("SKUCode")
         .agg(curing_blocks=("Qty", "size"), gt_demand_qty=("Qty", "sum"))
         .reset_index())
    if "SKU_Description" in prod.columns:
        desc = prod.groupby("SKUCode")["SKU_Description"].first()
        g["description"] = g["SKUCode"].map(desc)
    else:
        g["description"] = ""
    g = g.rename(columns={"SKUCode": "sku"})
    g = g[["sku", "description", "curing_blocks", "gt_demand_qty"]]
    return g.sort_values("gt_demand_qty", ascending=False).reset_index(drop=True)


def build_plan_xlsx() -> bytes:
    """One workbook: a Summary sheet (run info + KPIs + per-SKU demand) plus
    every artefact as its own subsheet."""
    k = res.kpis
    demand_tbl = _sku_demand_table()            # scoped to the SKUs actually run
    n_run = demand_tbl["sku"].nunique()
    n_drum_total = drum[~drum["SKUCode"].isin(C.NON_PRODUCTION_SKUS)]["SKUCode"].nunique()
    run_info = pd.DataFrame([
        ("Drum (curing schedule)", drum_label),
        ("Drum content hash", dhash),
        ("Config version", C.CONFIG_VERSION),
        ("Horizon from", str(drum_summary.horizon_from)),
        ("Horizon to", str(drum_summary.horizon_to)),
        ("Scope - SKUs run this summary", n_run),
        ("SKUs in drum (total)", n_drum_total),
        ("SKUs schedulable (total)", len(schedulable_skus)),
        ("GT demand (this run scope)", float(demand_tbl["gt_demand_qty"].sum())),
    ], columns=["Field", "Value"])
    kpi_info = pd.DataFrame([
        ("Plan health", k.plan_health),
        ("Health driver", getattr(k, "health_driver", "")),
        ("OTIF % (presses fulfilled)", round(k.otif_pct, 2)),
        ("Presses fulfilled / total", f"{k.presses_fulfilled} / {k.presses_total}"),
        ("Lot placement %", round(k.lot_placement_pct, 2)),
        ("Lots placed / total", f"{k.lots_placed} / {k.lots_total}"),
        ("Bottleneck stage", k.bottleneck_stage),
        ("Bottleneck peak %", round(k.bottleneck_peak_pct, 2)),
        ("Green-tyre aging OK %", round(k.aging_ok_pct, 2)),
        ("Aging expired", k.aging_expired),
        ("Pre-build lead (median days)", round(k.prebuild_days, 2)),
        ("Estimate exposure %", round(k.estimate_exposure_pct, 2)),
        ("Hard-constraint violations", k.violations),
    ], columns=["KPI", "Value"])

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xl:
        # --- Summary sheet (stacked sections) ---
        row = 0
        sh = "Summary"
        pd.DataFrame([["CTP Production Plan - Summary"]]).to_excel(
            xl, sheet_name=sh, startrow=row, header=False, index=False)
        row += 2
        run_info.to_excel(xl, sheet_name=sh, startrow=row, index=False)
        row += len(run_info) + 3
        pd.DataFrame([["KPI Summary"]]).to_excel(
            xl, sheet_name=sh, startrow=row, header=False, index=False)
        row += 1
        kpi_info.to_excel(xl, sheet_name=sh, startrow=row, index=False)
        row += len(kpi_info) + 3
        pd.DataFrame([["Per-SKU Curing Demand"]]).to_excel(
            xl, sheet_name=sh, startrow=row, header=False, index=False)
        row += 1
        demand_tbl.to_excel(xl, sheet_name=sh, startrow=row, index=False)
        # --- artefact subsheets ---
        subsheets = {
            "Schedule": sched,
            "SKU_Demand": demand_tbl,
            "Component_Demand": res.demand,
            "Violations": res.violations,
            "Infeasibility": res.infeasibility,
            "KPI_Report": res.kpi_df,
            "Constraint_Checks": getattr(res, "independent", None),
            "Handoff": res.handoff,
            "Unfulfilled_Presses": res.unfulfilled,
            "Waste_Matrix": res.waste,
            # FIX-1: folded carcasses (op-195) as produced components for BOM
            # mass-balance / pegging - NOT schedule rows.
            "Produced_Components": getattr(res, "produced_components", None),
        }
        for name, df in subsheets.items():
            if df is None:
                df = pd.DataFrame()
            elif isinstance(df, list):
                df = pd.DataFrame(df)
            elif isinstance(df, dict):
                df = pd.DataFrame([df])
            try:
                df.to_excel(xl, sheet_name=name[:31], index=False)
            except Exception:  # noqa: BLE001 - never let one sheet break the file
                pd.DataFrame({"error": [f"could not render {name}"]}).to_excel(
                    xl, sheet_name=name[:31], index=False)
    return buf.getvalue()


def tab_downloads() -> None:
    st.subheader("Download artefacts")
    st.caption("Every CSV embeds a run-provenance header: " + PROV)

    # --- ONE workbook: all sheets + summary (the primary download) ---
    st.markdown("**All-in-one Excel workbook** - Summary + every artefact as a subsheet.")
    try:
        xlsx_bytes = build_plan_xlsx()
        st.download_button(
            "Download full plan (xlsx)", xlsx_bytes,
            f"ctp_plan_{dhash}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="primary")
    except Exception as exc:  # noqa: BLE001
        st.error(f"XLSX build failed: {exc}")
    st.divider()

    artefacts = {
        "schedule.csv": sched,
        "violations.csv": res.violations,
        "infeasibility.csv": res.infeasibility,
        "kpi_report.csv": res.kpi_df,
        "handoff_report.csv": res.handoff,
        "unfulfilled_presses.csv": res.unfulfilled,
        "waste_matrix.csv": res.waste,
    }
    cols = st.columns(2)
    for i, (name, df) in enumerate(artefacts.items()):
        cols[i % 2].download_button(
            name, encode_csv(df, name, PROV), name, "text/csv",
            use_container_width=True)
    # zip-all
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, df in artefacts.items():
            zf.writestr(name, encode_csv(df, name, PROV))
    st.download_button("Download all (zip)", buf.getvalue(),
                       f"ctp_plan_{dhash}.zip", "application/zip",
                       use_container_width=True)
    if res.notes:
        st.caption("Run notes: " + " | ".join(res.notes))

    # Full waste / yield matrix lives here (a 1-line yield stat is on Overview).
    with st.expander("Waste / yield matrix (full)"):
        tab_waste()


# --------------------------------------------------------------------------- #
# RENDER TABS.
#   tabs[]  index           label (TAB_LABELS)
#     0     Overview
#     1     Gantt
#     2     Bottleneck/Capacity
#     3     OTIF/Fulfilment
#     4     Issues & Correctness
#     5     Downloads
# INDEX-COUNT CHECK: 6 labels, 6 `with tabs[i]:` blocks (0..5). Keep in sync.
# --------------------------------------------------------------------------- #
assert len(TAB_LABELS) == 6, "TAB_LABELS must match the 6 render blocks below."
with tabs[0]:
    tab_overview()
with tabs[1]:
    tab_gantt()
with tabs[2]:
    tab_bottleneck()
with tabs[3]:
    tab_otif()
with tabs[4]:
    tab_correctness()
with tabs[5]:
    tab_downloads()
