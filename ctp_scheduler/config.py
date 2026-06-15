"""CTP Scheduler - config constants, schemas and documented assumptions.

All values here are sourced from:
  - data/corrected/_scheduler_config.md
  - DECISIONS_AND_ASSUMPTIONS.md
  - CTP_Best_Scheduler_Design.docx (v1.0 spec)

No wall-clock, no RNG anywhere in the pipeline (C8 determinism).
"""
from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# CONFIG VERSION (cache-busting key for the Streamlit dashboard)
# ---------------------------------------------------------------------------
# Bump whenever a constant here, or any engine math, changes in a way that would
# invalidate a cached PipelineResult. app.py keys @st.cache_data on this value so
# a stale result can never be served after a code/data change.
CONFIG_VERSION = "1.17.0"  # DB-SOURCE MIGRATION: ingest now reads the consolidated database CSVs in data/from_db/ (jkt_routing/jkt_bom/jkt_aging_master/jkt_mpq/jkt_buffer_master/jkt_itemType_master + CuringSchedule.csv + jkt_demand.csv) instead of the deleted per-SKU CTP_Routing_<SKU>.xlsx workbooks. USE_DB_SOURCE=True. The consolidated routing (6.6MB, key finished_product) + bom (7.6MB, key Super_parent) are read ONCE, grouped, and module-cached; the 4 global masters (aging/mpq/buffer/itemtype) load ONCE as shared dicts. load_sku() slices the cache and REMAPS the DB bom columns to the legacy BOM schema the engine expects (Output<-Parent, output qty<-Parent_qty, unit<-Parent_unit, input code<-child, qty<-child_quantity, unit.1<-child_Unit, Input ItemType<-child_description); routing cols already match the old Routing sheet. load_drum() detects a CuringSchedule.csv-format file and maps machineName/sizeCode/startTime/endTime/scheduleQuantity/description/runTime/remarks -> Machine/SKUCode/StartTime/EndTime/Qty/SKU_Description/CycleTime_min/Remarks (legacy Curing_Sch_PCR.csv path still works). New load_demand() loader returns the real demand frame (drum stays the scheduling anchor). MPQ/Transfer plant masters, BUILD_CYCLE_OVERRIDE (op-195/200 GTCT stamp), GT_CUREBY_OVERRIDE, mpq_uom capture, proc_to_minutes UOM layer all preserved. Determinism (C8): sorted groupby, no RNG/wall-clock. ~250 SKUs available. PRIOR: 1.16.0  # PLANT MPQ + TRANSFER MASTERS are now the SOLE source of MPQ + transfer (USE_MPQ_TRANSFER_MASTERS=True, gated/A-B-able). New module ctp_scheduler/masters.py loads data/MPQ_master_corrected.(csv/xlsx: MPQ/Mixer_Batches/Calender_Roll_Lengths) + data/Transfer_master_corrected.csv READ-ONLY (zero master values altered). MPQ resolved per (line, item-type): MAX = UNBOUNDED for ALL types (mpq_max=0.0 -> C5 never caps a lot above; plant: produce to demand). MIN rules: numeric (Steel Belt 230 MTR / Ply 350 MTR / Bead 1000 Nos canonicalised via units.py); COMPOUND 'N batches' -> N x batch_kg of the item's op-10/20 mixing machine from Mixer_Batches (smallest runnable batch in the eligible pool; e.g. on 430-2 -> 3x350=1050 KG, full master pool min 270M -> 3x180=540 KG; unknown machine -> 3x230); CALANDARED ROLL -> item_code length_m from Calender_Roll_Lengths (steel spool 3500-12600 / fabric 500-3000), else default steel 6000 / fabric 2000 MTR. When masters ON the per-SKU MPQ sheet + MPQ_TYPE_DEFAULTS + DEFAULT_MPQ/DEFAULT_MPQ_MAX(99999) + flat TRANSFER_MIN are SUPERSEDED/IGNORED (mpq_source='master'); calender/compound pooling_exempt preserved. TRANSFER resolved per (line, item-type) from the Transfer master (PCR col for PCR, TBR for TBR; absent types -> 10, logged): e.g. IL=10/15(PCR/TBR), Master Compound=4, Cap Strip=5, Tread=15, Bead=7. LATENT-BUG FIX: schedule rows now carry transfer_min (SCHEMA_SCHEDULE) so Phase-8 validate re-derives C1/precedence + aging-min with the SAME transfer the commit-test used (was a flat-10 default that phantom-failed any lot whose master transfer != 10, e.g. bead=7 -> -3 gap). MEASURED: 5-pilot 100% OTIF, phase8 violations=0, C5-C8 PASS. C1-C8 + determinism preserved; app.py untouched. PRIOR: 1.15.0  # TBM DOUBLE-CHARGE FIX (CARCASS_FOLD_CHARGES_TBM, default False). GTCT evidence (reference/GTCT TBR PCR 1.xlsx): build_cycle_sec is the COMPLETE per-tyre build rate ("Cycle Time Per Tyre (Sec)"; "No of Tyres/Shift" = 28800/cycle EXACTLY for every row), carcass plies built inside that one cycle - NO separate stage-1 cycle exists. ingest stamps the full cycle onto BOTH op-195+op-200, so sizing's carcass fold (proc_min += carc_min) charged the TBM pool ~2x (~96s/tyre vs ~48s), halving modeled building capacity (~9,157 GT/day vs config's plant-validated ~16,160@100%/~13,500@84%). FIX: gate the add behind CARCASS_FOLD_CHARGES_TBM (default False = carcass adds 0 TBM minutes, already inside the per-tyre rate); carcass still folded for mass-balance/pegging, stays non-dispatched. A/B-able. C1-C8 + determinism preserved; ingest op-200 override UNCHANGED. PRIOR: 1.14.0  # PLANT COMPOUND-AGING-MAX OVERRIDE = 72h (COMPOUND_AGING_MAX_OVERRIDE_H): all compound item-types (final/master compound, small chemical) hold 72h MAX even over a tighter measured 24h row (MIN untouched). MEASURED on Curing_Sch_SYNTH_clean: OTIF 47.5%->54.3%, INFEASIBLE_AGING 1091->115, C2 violations=0. Build leveling (FIX-A) confirmed net-NEGATIVE even with 72h compounds (54.3 ALAP vs 52.0 leveled) -> stays default OFF, ALAP is correct. PRIOR: 1.13.0 ENGINE: FIX-A BUILD LEVEL-LOADER (gated, default OFF on evidence) + FIX-B CANONICAL UOM. FIX-A: green-tyre BUILD op (op-200, drum-fed) can be placed by LEAST-LOADED-TBM + EARLIEST-FEASIBLE-SLOT within its cure-by window (BUILD_LEVEL_BACKOFF_FRAC early-edge clamp), replacing consumer-first ALAP; mechanism fully wired + A/B-able via BUILD_LEVELING_ENABLED. MEASURED on Curing_Sch_SYNTH_clean: the proven build-spike premise does NOT bind here (Building pool peaks ~86-88% PLACED, never saturated; dominant failure is UPSTREAM input-chain over-aging/UNREACHED ~39.5k, not CUREBY ~3.6k), so leveling is net-negative (47.46% ALAP vs 44-47% leveled - pulling builds earlier over-ages their inputs). Per the 'clamp conservatively if C2 at risk' rule we DEFAULT IT OFF (ALAP = best measured OTIF) and ship it ready for a schedule where the TBM pool saturates. Commit-test still binds cure-by both-sided at every setting (phase-8 violations=0). FIX-B: new units.py canonical resolver (length->METRE MM/1000, mass->KG MT*1000, count->NOS; UNKNOWN raises); MPQ sheet UOM captured at ingest (SkuData.mpq_uom); sizing._mpq floor + validate C5 now reduce lot qty AND MPQ [min,max] to the same canonical dimension before comparing, so the MPQ floor BINDS for length items (was 50 MTR vs ~73018 MM non-binding -> now binds) and KG/MT mass items normalise 1000x correctly; displayed/stored lot UOM unchanged; calender pooling_exempt intact. C1-C8 + determinism preserved.\n# PRIOR: 1.12.0  # EXTERNAL-VALIDATOR FIXES (reporting/emit only; engine math UNCHANGED): FIX-1 folded carcass (op-195) rows NO LONGER emitted into schedule.csv (cleared 6,183 false Duration/Aging/Precedence FAILs) - routed to dispatch.produced_component_rows -> outputs/produced_components.csv + Produced_Components sheet (sku,item,item_type,qty,consumer_lot_id,source=carcass_fold); the dead is_folded special-casing deleted from validate/capacity/outputs/pipeline/app. FIX-2 CHANGEOVER/MOULD_CLEAN drum-admin rows excluded from the exported schedule.csv curing/feed rows (humanise_schedule drops NON_PRODUCTION_SKUS) - they keep L6 press-occupancy on the timeline (C6 immutable) but never count as demanded/unfed presses (-118). FIX-3 quarantined-SKU RESERVED presses routed to uncovered_demand.csv (reason QUARANTINED_SKU), removed from the unfed-press denominator (-441), counted ONCE. FIX-4 infeasibility classified ROOT (CUREBY/INFEASIBLE_AGING/QUARANTINE/no-machine/no-slot) vs CASCADE (UNREACHED orphans); infeasible_class column + KPI infeasible_root/infeasible_cascade (label only, hard counts unchanged). FIX-5 pooled calender/mother-roll lots stamped pooling_exempt=True (within-SKU + cross-SKU calender pools) so C5 distinguishes intentional pooling from a real MPQ breach; Bead Apex (NOS) NOT exempt. C1-C8 + determinism preserved; TBM busy-min/OTIF/placement UNCHANGED.\n# PRIOR: 1.11.0  # BUG FIXES (independent validation): BUG-1 FRACTIONAL NOS LOTS - sizing.make_lots now integer-partitions discrete-piece (NOS/PCS/EA) campaign qtys into balanced WHOLE-number lots (sizing._discrete_lot_qtys / _integer_partition / _is_discrete_uom), so 0 fractional NOS lots remain (was ~4185 full-drum / 2128 in the report scope), SUM still >= demand (e.g. BA13001 80174 demand -> 80174 produced), every lot within [mpq_min, mpq_max] (C5). Continuous KG/MTR sizing UNCHANGED. BUG-2 FOLDED CARCASS 0% PRODUCED - dispatch emits a 0-duration, machine-blank, is_folded=True carcass companion row at the GT build end (carried via sizing's carcass_item on the GT lot), so the folded op-195 carcass is COUNTED as produced for mass-balance/pegging WITHOUT re-charging the TBM pool; validate.py / capacity.py / outputs.py exclude is_folded rows from C4/C7 non-overlap, C1/C2, capacity and placed-lot counts. C1-C8 + determinism preserved. PRIOR: 1.10.0 REGRESSION FIX (full-drum OTIF 0.8% -> recovered): CROSS_SKU_POOL_ENABLED master flag (Approach B) defaults OFF - the FIX-5/6 cross-SKU compound pool anchored each shared batch to ONE consumer, so an anchor miss orphaned the whole batch (R14 poisoned ~24.5 blocks each) and collapsed multi-SKU OTIF; with the flag OFF the bulk compounds revert to per-SKU sizing (the ~63% state), within-SKU cap-ply pooling (FIX-1) untouched. outputs.binding_reason now names the dropped POOL lot id + item ("shared compound batch dropped: ...") instead of the false hardcoded "calender 901" string. PRIOR: 1.9.0 UI-SIMPLIFY + DEMAND-QTY OTIF: headline OTIF is now demand-quantity based (otif_demand_pct = GT qty on fulfilled curing blocks / GT qty demanded); dashboard simplified for non-expert plant users (6 tabs, plain-language captions, why-shortfall panel). PRIOR: 1.8.0 AUDIT FIXES L2/L3/L6/L13/L15/L16 + R4/R5/R14: L2 cross-SKU calendered mother-roll pooling (cap strip + CALANDARED ROLL family pooled across SKUs, one mother roll per shelf-window, C2/C5 preserved); L3 per-item-type shelf-life table (FINAL 48h/MASTER 72h/SMALL CHEMICAL 72h, ESTIMATED - item's own Aging-Master row always wins, so this dataset's measured 24h compound rows are PRESERVED, see deliverable flag); L6 CHANGEOVER+MOULD_CLEAN press occupancy pinned RESERVED (no build mid-changeover); L13 _lift_consumer refuses a lift that would over-age a committed sibling producer (C2); L15 compute_windows resolves merged/pooled consumer LST + drops transfer on the EST max side; L16 validate CUREBY real cure_max / app.py NO_CONSUMER on C1 / timeline changeover charged once / pin.py dup import removed. R4 uncovered_demand.csv + plant_fulfilment_pct + scope stamp; R5 quarantined RESERVED presses in unfulfilled_presses.csv; R14 pooled-lot UNREACHED expands to ALL consuming blocks.\n# PRIOR: 1.7.0 POOL-AWARE WASTE MATRIX - cross-SKU pooled bulk compounds (final/master compound, small chemical) reported ONCE at plant level (sku="POOL", required+produced summed across all SKUs, deduped on lot_id) instead of inflated-positive on the anchor SKU + negative ghost rows on every other consuming SKU; run.py now emits waste_matrix.csv with a skus=<n> header stamp. D3b: (A) C5-C8 INDEPENDENTLY re-proven in validate.py + dashboard scorecard; (B) MPQ_TYPE_DEFAULTS bounded fallback for 5 item-types missing from per-SKU MPQ sheets (small chemical / steel belt edge strip / slitted / pre-cut roll / apex) - replaces non-binding [1,99999] default. FIX-5: CROSS-SKU COMPOUND POOLING; FIX-1..4: cap-ply mother-roll pooling, multi-builder spread, non-binding early-build, GT cure-by override; FIX-3a: multi-consumer items sized TOTAL-then-anchor

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PKG_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(PKG_DIR)
DATA_DIR = os.path.join(ROOT_DIR, "data")
CORRECTED_DIR = os.path.join(DATA_DIR, "corrected")
OUTPUT_DIR = os.path.join(ROOT_DIR, "outputs")

ROUTING_FILE_PREFIX = "CTP_Routing_"
ROUTING_FILE_SUFFIX = " BOM_Final.xlsx"

# ---------------------------------------------------------------------------
# DB-EXPORT DATA SOURCE (migration from per-SKU xlsx workbooks -> consolidated
# database CSVs). The plant now exports a small set of consolidated CSVs from a
# database; the 137 per-SKU CTP_Routing_<SKU>.xlsx workbooks are GONE. When
# USE_DB_SOURCE is True, ingest.py reads exclusively from FROM_DB_DIR:
#   jkt_routing.csv         (all SKUs; key finished_product) -> per-SKU Routing
#   jkt_bom.csv             (all SKUs; key Super_parent)     -> per-SKU BOM
#   jkt_aging_master.csv    (GLOBAL)                         -> Aging Master
#   jkt_mpq.csv             (GLOBAL)                         -> MPQ
#   jkt_buffer_master.csv   (GLOBAL)                         -> Buffer Master
#   jkt_itemType_master.csv (GLOBAL)                         -> ItemType Master
#   CuringSchedule.csv      (the DRUM)                       -> curing schedule
#   jkt_demand.csv          (NEW real demand)                -> load_demand()
# The 4 global masters are loaded ONCE, cached at module scope, and shared by
# every SKU slice. The 7.6MB BOM + 6.6MB routing are read ONCE and grouped, not
# re-read per SKU. Determinism (C8) preserved: sorted groupby, no RNG/wall-clock.
FROM_DB_DIR = os.path.join(DATA_DIR, "from_db")
USE_DB_SOURCE = True

# Drum (curing schedule): USER-PROVIDED, NOT taken from the DB. The 6 recipe/
# master inputs (BOM/Routing/Aging/MPQ/Buffer/ItemType) come from the DB; the
# curing schedule is supplied by the user as a file (default Curing_Sch_PCR.csv,
# override at run time with `run.py --drum <path>`). load_drum() still
# auto-detects either format (legacy Curing_Sch_PCR.csv or a CuringSchedule.csv
# export), so the user may hand in either - it just never DEFAULTS to the DB one.
_DB_DRUM_CSV = os.path.join(FROM_DB_DIR, "CuringSchedule.csv")  # not used as the drum
_LEGACY_DRUM_CSV = os.path.join(DATA_DIR, "Curing_Sch_PCR.csv")
DEFAULT_DRUM_CSV = _LEGACY_DRUM_CSV   # curing is USER-PROVIDED, never auto-from-DB

# Consolidated DB CSV file paths.
DB_ROUTING_CSV = os.path.join(FROM_DB_DIR, "jkt_routing.csv")
DB_BOM_CSV = os.path.join(FROM_DB_DIR, "jkt_bom.csv")
DB_AGING_CSV = os.path.join(FROM_DB_DIR, "jkt_aging_master.csv")
DB_MPQ_CSV = os.path.join(FROM_DB_DIR, "jkt_mpq.csv")
DB_BUFFER_CSV = os.path.join(FROM_DB_DIR, "jkt_buffer_master.csv")
DB_ITEMTYPE_CSV = os.path.join(FROM_DB_DIR, "jkt_itemType_master.csv")
DB_DEMAND_CSV = os.path.join(FROM_DB_DIR, "jkt_demand.csv")

# ---------------------------------------------------------------------------
# PLANT MPQ + TRANSFER MASTERS (authoritative, SOLE source when enabled)
# ---------------------------------------------------------------------------
# Two plant-validated master files (created and certified outside the engine -
# the engine CONSUMES them READ-ONLY and never edits a value):
#   data/MPQ_master_corrected.csv  (+ .xlsx sheets MPQ / Mixer_Batches /
#                                    Calender_Roll_Lengths)
#   data/Transfer_master_corrected.csv
# When USE_MPQ_TRANSFER_MASTERS is True these become the SOLE source of MPQ
# (per line, per item-type) and inter-stage transfer time (per line, per
# item-type). The per-SKU MPQ sheet, MPQ_TYPE_DEFAULTS and DEFAULT_MPQ_MAX
# (99999) + the flat TRANSFER_MIN are SUPERSEDED (see ctp_scheduler/masters.py).
# MPQ MAX is UNBOUNDED for every type (plant: produce to demand, no cap), so C5
# never caps a lot above. Gated/A-B-able: flip False to revert to the legacy
# per-SKU MPQ sheet + MPQ_TYPE_DEFAULTS + DEFAULT_MPQ + flat transfer.
MPQ_MASTER_XLSX = os.path.join(DATA_DIR, "MPQ_master_corrected.xlsx")
MPQ_MASTER_CSV = os.path.join(DATA_DIR, "MPQ_master_corrected.csv")
TRANSFER_MASTER_CSV = os.path.join(DATA_DIR, "Transfer_master_corrected.csv")
# DB-ONLY for the 6 inputs: OFF so MPQ comes from the DB jkt_mpq (one of the 6),
# NOT the local MPQ_master_corrected.csv override. (Side effect: transfer reverts
# to the flat TRANSFER_MIN fallback - transfer is NOT one of the 6; wire DB
# routing.transfer_time_min separately if a per-type transfer is needed.)
USE_MPQ_TRANSFER_MASTERS = False

# ---------------------------------------------------------------------------
# BUILD CYCLE-TIME MASTER (authoritative TBM cycle time per SKU)
# ---------------------------------------------------------------------------
# Source of truth for the per-tyre building (TBM) cycle time is the plant
# enclosure "GTCT TBR PCR 1.xlsx" (PCR + TBR building cycle-time sheets),
# extracted once into data/build_cycle_master.csv (product_code, line_class,
# build_cycle_sec). When BUILD_CYCLE_OVERRIDE_ENABLED is True, ingest stamps the
# master value onto EVERY building op (ops 195 & 200, UOM=SEC) of EVERY SKU it
# loads. This makes the correction self-healing: any routing file that still
# carries a stale/placeholder building proc_time (e.g. the legacy "22") is
# transparently corrected at load time, so a rerun for ANY sku never re-surfaces
# the wrong building cycle. The override is logged into SkuData.audit.
BUILD_CYCLE_MASTER_CSV = os.path.join(DATA_DIR, "build_cycle_master.csv")
# DB-ONLY: OFF so the building cycle time comes from the DB jkt_routing op-195/200
# proc_time (real SEC values, 156/189 populated) - part of the Routing input (one
# of the 6) - instead of the local build_cycle_master.csv. The 33 ops with no DB
# proc_time fall back to the placeholder. Re-enable only if a local GTCT override
# is needed over the DB.
BUILD_CYCLE_OVERRIDE_ENABLED = False

# ---------------------------------------------------------------------------
# CARCASS FOLD - DOES IT ADD TBM MINUTES?  (default FALSE = correct)
# ---------------------------------------------------------------------------
# GTCT EVIDENCE (reference/GTCT TBR PCR 1.xlsx, sheets PCR+TBR): the building
# master column is literally "Cycle Time Per Tyre (Sec)" - ONE value per product
# - and the adjacent "No of Tyres/Shift/Machine 100%" column = 28800s / cycle
# EXACTLY for every row (49.92->576.92, 187->154.01, 189.2->152.22). That proves
# build_cycle_sec is the COMPLETE per-tyre build rate: one finished green tyre
# leaves the TBM per cycle, with the carcass plies (PLY1/PLY2/FLIPPER/CAP-STRIP/
# GSA - shown only as presence flags, NOT as separate cycles) built INSIDE that
# single cycle. There is NO separate stage-1 carcass cycle in the data.
#
# Because ingest's BUILD_CYCLE_OVERRIDE stamps that full per-tyre cycle onto BOTH
# op-195 (carcass) AND op-200 (GT), the sizing "carcass fold" doing
# proc_min += carc_min added the WHOLE per-tyre cycle a SECOND time -> every
# two-stage GT lot was charged ~2x cycle (~96s/tyre instead of ~48s), halving the
# modeled TBM building capacity (~9,157 GT/day modeled vs the config's own plant-
# validated ~16,160/day @100% / ~13,500 @84% OEE).
#
# CORRECT default = False: the carcass adds ZERO extra TBM minutes (it is already
# inside the per-tyre rate). The carcass is STILL folded for mass-balance /
# pegging / produced_components and remains a non-dispatched node - only the
# double TBM charge is removed.  Set True ONLY if the data is ever changed to
# provide a genuinely DISTINCT, shorter stage-1 carcass cycle on op-195.
CARCASS_FOLD_CHARGES_TBM = False  # FIXED: GTCT cycle is the complete per-tyre rate; carcass is inside it (no double-charge)

# ---------------------------------------------------------------------------
# Time / changeover defaults (data/corrected/_scheduler_config.md)
# ---------------------------------------------------------------------------
TRANSFER_MIN = 10.0          # inter-stage material move, applied per edge
CHANGEOVER_MIN = 15.0        # upstream stages only; reserved on SKU change
EFFICIENCY = 0.95            # plant standard OEE factor; proc_eff = proc / eff
SHIFT_MINUTES = 1440.0       # 3-shift = 24h available upstream (24x7 assumption)

# ---------------------------------------------------------------------------
# AGING CONVENTION (Round-2 decision, Devika MARGINAL C2 boundary fix)
# ---------------------------------------------------------------------------
# Scorch / cure-by life is a MATERIAL property: the compound (and the green
# tyre) age in WALL-CLOCK time, and they keep ageing while in transit between
# stages. Therefore the over-aging band (aging MAX, and the green-tyre cure-by
# max) is measured against the RAW wall-clock gap:
#       gap_aging = consumer_start - producer_end           (transfer NOT removed)
# The C1 precedence band and the aging MIN band describe AVAILABILITY (material
# in transit cannot yet be consumed), so they legitimately net out the transfer:
#       gap_precedence = consumer_start - producer_end - transfer
# AGING_SUBTRACTS_TRANSFER = False makes the MAX side wall-clock; flip to True
# only if the plant certifies that scorch life pauses during handling.
AGING_SUBTRACTS_TRANSFER = False

# Numerical tolerance for band comparisons (minutes). 1e-6 min ~ 60 microsec.
AGING_EPS_MIN = 1e-6

# Round each lot's effective processing time UP to whole minutes. M/MIN and
# SEC/BATCH proc math yields fractional minutes (e.g. 53.715 min); left raw they
# accumulate sub-minute drift that can push an ALAP-placed lot microscopically
# (~8 s) over a hard aging band. Ceil is deterministic and conservative (never
# under-states run time), so boundary lots no longer straddle the wall-clock max.
ROUND_PROC_TO_WHOLE_MIN = True

# L15: drop `transfer` from the EST/wall-clock-MAX side of compute_windows so the
# planning EST matches the ENFORCED aging-max gap (gap_aging = cs - end). This is
# priority/quality only (EST/slack feed the criticality PQ order; the commit-test
# is authoritative). Measured to PERTURB the dispatch order enough to starve some
# final-compound chains, so it is OFF by default; the merged/pooled-consumer LST
# resolution (the real L15 fix) is always on. Flip True only after plant sign-off.
WINDOWS_EST_DROP_TRANSFER = False

# BUG-03 RECOURSE: max times a single consumer lot may be LIFTED later to rescue
# an over-aged producer. Bounded so the dispatch loop always terminates (no
# infinite lift/retry cycle); deterministic. 2 lifts is enough to clear the
# small recoverable set without churn; the physical calender cap dominates.
RECOURSE_MAX_LIFTS = 2

# L13: guard the recourse lift so it never over-ages a committed SIBLING producer
# (C2). On by default (correctness). Diagnostic toggle.
RECOURSE_SIBLING_GUARD = True

# L2: pool the wide-sheet calendered mother rolls (cap strip + CALANDARED ROLL
# family) ACROSS SKUs (one mother roll per shelf-window for all).
#
# CONSERVATIVE DEFAULT = OFF (flagged). The mechanism is correct and C2/C5-safe
# (phase-8 stays 0; the roll keeps its tightest cross-SKU aging band and the MPQ
# MIN floor). It DOES remove the CALANDARED ROLL / Cap Strip family from UNREACHED
# (the audit's CAP66 target). BUT on this CERTIFIED 18-SKU dataset CAP66 is only
# ~0.5% of UNREACHED (not the plant-wide 47%), while pooling the calender rolls
# cross-SKU re-sequences the shared POOL- lot stream and perturbs the dispatch
# order enough to STARVE some final-compound chains - net in-scope fulfilment
# fell (2712 -> 1750 presses; FINAL COMPOUND INFEASIBLE_AGING 227 -> 1080). So on
# THIS dataset L2 is net-negative and is left OFF; it is retained and ready for
# the plant-wide run where CAP66 dominates UNREACHED and the trade-off reverses.
# Flip True to enable. When False the calender types stay WITHIN-SKU pooled
# (CALENDER_POOL_ITEM_TYPES), exactly as before this audit.
L2_CALENDER_CROSS_SKU_POOL = False

# R14: expand a dropped POOLED lot's broken-block attribution to ALL its
# consuming blocks (parent_lot_ids), minus blocks still served by a surviving
# pooled lot of the same item. On by default (honest OTIF). Diagnostic toggle.
R14_POOL_BROKEN_EXPANSION = True

# L6: pin CHANGEOVER (360 min) + MOULD_CLEAN drum rows as RESERVED press
# occupancy. On by default. Diagnostic toggle.
L6_NONPROD_OCCUPANCY = True

# ---------------------------------------------------------------------------
# FIX-3: NON-BINDING UPSTREAM EARLY-BUILD (build-ahead-as-stock, EST-directed)
# ---------------------------------------------------------------------------
# Consumer-first ALAP pulls every producer just-in-time (target = LST). On a
# CONTENDED single machine (the 4-Roll Calender, the lone tread extruder) the
# JIT slot is frequently taken, so the latest-feasible slot the dispatcher finds
# is far earlier than LST and the producer over-ages before its (far) consumer
# (GTs/compounds waited median 110h, then expired). Where the producer is
# NON-BINDING (not the bottleneck stage, and consumed by a real producer lot -
# NOT a drum-fed green tyre, whose press is immutable and cure-by hard), FIX-3
# lets it ANCHOR to the NEAREST consumer it can still feed within aging-max
# instead of being dropped: i.e. it is built earlier as STOCK toward EST and
# drawn by the closest feasible consumer in its own aging window. Bounded
# (single re-anchor attempt per lot) and deterministic (nearest feasible
# consumer chosen by sorted deadline). Still respects aging_min AND aging_max on
# BOTH sides (C2) - it only changes WHICH consumer edge the stock lot is matched
# to, never the aging band. Works WITH FIX-1 (pooled cap-ply made ahead).
FIX3_EARLY_BUILD_ENABLED = True

# proc_time == 22 is an ACCEPTED valid plant value (decision #2), but is still
# the pervasive placeholder on non-mixing/curing/building ops -> flag ESTIMATED.
PROC_PLACEHOLDER = 22.0
TREAD_PROC_PLACEHOLDER = 22.0   # extruder M/MIN placeholder (same numeric value)

# ---------------------------------------------------------------------------
# CALENDER MOTHER-ROLL RE-BASING (plant MES method - MESInterface.FOURROLL)
# ---------------------------------------------------------------------------
# THE FIX: the 4-Roll Calender (901) and the Roller-Head Calender (1101) are
# WIDE-SHEET machines. The plant does NOT run them for the narrow component's
# strip length (~51 m/tyre for a cap strip); it runs the full-width MOTHER ROLL
# and slits many tyres' worth of strip from one pass. The real machine-RUN
# length charged per tyre is therefore the WEIGHT the tyre takes off the sheet
# divided by the sheet's areal weight per running metre:
#
#   run_m_per_tyre = component_weight_kg_per_tyre / (Width_m * PerMeterWeight)
#
# where (Width_m * PerMeterWeight) = kg of sheet per running metre of the roll.
# Charging the calender by the narrow strip LENGTH over-states its load ~728x
# (cap ply: 51 m charged vs 0.07 m real) and turns it into a FALSE bottleneck.
#
# Width / PerMeterWeight are PLANT-PROVIDED process constants per calender
# product family. Until per-product values are extracted from MESInterface, we
# default ALL calender products to the plant cap-ply values below and FLAG the
# charge ESTIMATED so the exposure is auditable. CONFIGURABLE per product code
# via CALENDER_SHEET_SPEC.
#
# Provenance: Width=1.3851 m, PerMeterWeight=2.85 kg/m2 are the plant cap-ply
# (MESInterface.FOURROLL.O_Production) mother-roll constants. ESTIMATED for all
# other products until MES per-product specs are pulled.
CALENDER_REBASE_ENABLED = True
CALENDER_DEFAULT_WIDTH_M = 1.3851          # plant cap-ply mother-roll width (ESTIMATED for others)
CALENDER_DEFAULT_PERMETER_KG = 2.85        # plant cap-ply areal weight kg/m2 (ESTIMATED for others)

# Per-product (Width_m, PerMeterWeight_kg_per_m2) overrides. Key = calendered
# item code OR item-type. Empty -> all use the plant cap-ply default above.
# Mark provenance in comments when a real MES value is added.
CALENDER_SHEET_SPEC = {
    # "CAP 66": (1.3851, 2.85),   # example: real MES per-SKU spec goes here
}

# Calender machine ids (wide-sheet) that get the mother-roll re-base.
CALENDER_MACHINES = ("901", "1101")

# FIX-1: CAP-PLY / WIDE-SHEET MOTHER-ROLL POOLING (campaign-merge, not 1:1).
# The 4-Roll Calender (901) makes ONE wide MOTHER ROLL (~13 min, ~111 rolls/day,
# 97% idle) and many tyres are slit/drawn from it. Pinning the cap strip (and the
# wide-sheet "CALANDARED ROLL") 1:1 per curing block made a tiny ALAP lot for
# EVERY press, all serialised on the single calender, so they aged out in the 24h
# window before their green tyre built (thousands UNREACHED / INFEASIBLE_AGING).
# These item-types are therefore REMOVED from the per-block 1:1 set so Phase-3
# campaign-merge pools their demand across consecutive curing blocks WITHIN the
# item's own aging window (one mother-roll-sized lot feeds many tyres, made-ahead
# and drawn as stock). Green Tyre / Carcass / FG / Steel Belt / Tread stay 1:1.
# Lower-cased item-type match; scoped to the wide-sheet calender (901) products.
CALENDER_POOL_ITEM_TYPES = (
    "cap strip", "capstrip", "cap",
    "calandared roll", "calendared roll", "calandered roll",
)
# The per-tyre MPQ max (e.g. 214 MTR of cap strip) is a NARROW-component lot cap,
# NOT the wide mother-roll size, so re-splitting a pooled campaign back to it
# would un-do the pooling. For pooled calender types the campaign is sized by the
# AGING-WINDOW SPAN only (one mother-roll-scale run feeds every tyre in the
# window); its re-based proc (mother-roll method) stays tiny (~minutes) even for
# thousands of tyres. CALENDER_POOL_MAX_QTY is a deterministic safety cap on a
# single pooled lot's qty (0 = unbounded within the span). Left unbounded so the
# whole aging window pools into one run; the span budget already bounds it.
CALENDER_POOL_MAX_QTY = 0.0
# Stage labels that identify a calender op (department-derived).
CALENDER_STAGES = ("4 Roll Calender", "Roller Head Calender", "Calendering")

# Some BOM cap-strip weights are INFLATED vs the true MES sheet weight (the MES
# cap-ply is ~0.278 kg/tyre; the BOM-derived cap weight is ~34x that). Where we
# fall back to the BOM-derived weight we tag the charge ASSUMED/INFLATED so the
# data gap is visible. The TRUE per-SKU cap weight must be pulled from
# MESInterface.dbo.BOM.
CALENDER_MES_CAP_KG_PER_TYRE = 0.278       # plant MES reference (cap ply)
CALENDER_BOM_INFLATION_FLAG_RATIO = 5.0    # if BOM weight > ratio x MES ref -> flag INFLATED

# FIX-3 (de-inflate the calender cap charge). The BOM-derived cap-ply weight
# (B616M final compound + cord, ~3.7-9.5 kg/tyre depending on SKU) is ~13-34x
# the real MES cap-ply sheet weight (0.278 kg/tyre per MESInterface.dbo.BOM).
# Charging the wide-sheet 4-Roll Calender by that inflated weight over-states its
# mother-roll run per tyre. When CALENDER_CAP_MES_OVERRIDE_ENABLED is True we
# substitute the MES reference (or a per-SKU/per-item value from
# CALENDER_CAP_MES_KG) for the cap-ply item ONLY, so the cap run drops to the
# honest ~0.07 m/tyre and the WEIGHT_INFLATED flag clears. This is NON-BINDING
# (the calender already verdicts CLEAN); it only makes the charge honest and is
# scoped to cap-ply (item-type "Cap Strip" / item code prefix "CAP"), never the
# other calendered sheets (CPJ*/EG*/HTPOLY*/IL*) which carry real weights.
CALENDER_CAP_MES_OVERRIDE_ENABLED = True
# Item-types treated as cap-ply for the MES weight override (lower-cased match).
CALENDER_CAP_ITEM_TYPES = ("cap strip", "cap", "capstrip")
# Item-code prefixes treated as cap-ply (upper-cased startswith match).
CALENDER_CAP_ITEM_PREFIXES = ("CAP",)
# Optional per-SKU or per-item MES cap weight (kg/tyre). Key = item code OR sku;
# empty -> all cap-ply use CALENDER_MES_CAP_KG_PER_TYRE.
CALENDER_CAP_MES_KG = {
    # "CAP 66": 0.278,   # real MES per-item cap-ply weight goes here when pulled
}

# ---------------------------------------------------------------------------
# EXTRUDER PROFILE RE-BASING (tread under-charge fix)
# ---------------------------------------------------------------------------
# Extruders (Quintoplex 1002 tread, Triplex 1001 sidewall) extrude a PROFILE of
# real length per tyre. Sidewall already carries a BOM MM profile length (~1.45
# m) so it is charged correctly. TREAD is consumed "1 NOS" with no BOM profile
# length, so the M/MIN charge collapses to ~1 m/tyre - an UNDER-charge. Where
# the BOM gives no tread profile length we substitute a plant-typical profile
# length and FLAG it ESTIMATED.
EXTRUDER_MACHINES = ("1001", "1002")
TREAD_PROFILE_M_PER_TYRE = 1.4             # plant-typical PCR tread profile length (ESTIMATED)
TREAD_ITEM_TYPES = ("tread",)

# ---------------------------------------------------------------------------
# Aging injection / defaults (hours)
# ---------------------------------------------------------------------------
GREEN_TYRE_CUREBY_MIN_H = 0.0    # GT may cure immediately
GREEN_TYRE_CUREBY_MAX_H = 8.0    # GT cure-by window 8h (real PCR green-tyre hold 8-24h per plant/Vikram; 6h was over-tight, heavy SKU missed by ~5 min)

# FIX-4: CURE-BY CONFIG-DRIVEN (self-healing across all 137 SKUs).
# The per-file "Aging Master" row for a green-tyre item often still carries the
# legacy 6h cure-by max (only the 5 pilots were hand-patched to 8h). Because
# sizing._aging_for() returned the per-file aging row BEFORE this config could
# apply, GREEN_TYRE_CUREBY_MAX_H was DEAD CODE for the 14 un-patched SKUs. When
# GT_CUREBY_OVERRIDE_ENABLED is True, ingest STAMPS (GREEN_TYRE_CUREBY_MIN_H,
# GREEN_TYRE_CUREBY_MAX_H) onto every Green-Tyre item's aging row, overriding the
# per-file 6h/8h. Self-healing for all 137 SKUs (present and future); logged to
# SkuData.audit. Deterministic (pure config, no clock/RNG).
GT_CUREBY_OVERRIDE_ENABLED = True
# Item-types treated as a green tyre for the cure-by override (lower-cased).
GREEN_TYRE_ITEM_TYPES = ("green tyre", "green tyres")

# ---------------------------------------------------------------------------
# FIX-1: PRE-BUILD LEAD-IN (cold-start recovery)
# ---------------------------------------------------------------------------
# Green-tyre building (op-195/op-200) and the upstream compound/component chain
# may legitimately run BEFORE the drum horizon's first curing block: the plant
# pre-builds and ages stock so the very first presses can be fed. The ALAP
# machine model already permits negative-relative starts (latest_feasible_start
# has no horizon floor), so this constant documents and bounds that lead window
# and is the deterministic budget the dispatcher may reach back from the drum's
# first block start. The GT cure-by (<= GREEN_TYRE_CUREBY_MAX_H of its press)
# still binds, so a lead-in GT cannot scorch: it only converts the cold-start
# edge presses that previously had no feasible builder slot. Deterministic; no
# wall-clock (the lead is measured off the drum's own first block, an input).
PREBUILD_LEADIN_H = 48.0
PREBUILD_LEADIN_ENABLED = True

# ---------------------------------------------------------------------------
# FIX-2: MULTI-BUILDER SPREAD (no single-machine pinning)
# ---------------------------------------------------------------------------
# Building ops (195/200) share the PCR TBM pool; op-200 (GT) is typically
# eligible on >=2 TBMs (e.g. 3401,3402). The plain ALAP machine-choice key
# (changeover, latest-start, load, id) let the latest-start term dominate, so
# ALL of a heavy SKU's GT builds piled onto ONE TBM (3402) while an eligible TBM
# (3401) sat idle - and the congested single machine could not give the contested
# presses a slot near their deadline, scorching the GT (CUREBY_EXPIRED) and
# over-aging its compound chain (INFEASIBLE_AGING). When enabled, building ops
# are placed on the LEAST-LOADED eligible builder first (load primary), among
# machines whose latest-feasible start is within BUILD_SPREAD_TOL_MIN of the best
# available start, so ALAP correctness is preserved while load is balanced. Tie
# break stays machine_id (determinism). Non-building ops are unchanged.
BUILD_SPREAD_ENABLED = True
# FIX-2 (generalised): widened so the 11-TBM pool actually load-balances. With
# the old 600-min (10h) tolerance, a heavy SKU whose eligible builders' latest-
# feasible starts differed by >10h was NOT treated as equivalent, so its builds
# stacked on ONE TBM (HURL0 380/478 on 3401; SELS0 168 all on 3411) while the
# pool ran 59% avg and never >67%. The GT cure-by MAX (commit-test, wall-clock)
# still binds every placement, so widening the spread tolerance only re-routes a
# build to a LESS-LOADED eligible builder when that builder can still hit a slot
# the GT can cure from; it can never place an over-aged build. 1440 min (one full
# day) lets the whole eligible pool be considered for least-loaded balancing
# while ALAP correctness (and aging) is preserved by the commit-test. Tie-break
# stays machine_id (determinism).
BUILD_SPREAD_TOL_MIN = 1440.0

# ---------------------------------------------------------------------------
# FIX-A: BUILD LEVEL-LOADER (replace ALAP for the green-tyre build)
# ---------------------------------------------------------------------------
# PROVEN DIAGNOSIS: total building demand vs the 11-TBM pool is only ~75% in any
# 8h cure-by window (0 windows over 100% capacity) - the synthetic schedule is
# FEASIBLE. But the consumer-first ALAP machine model places EVERY green-tyre
# build As-Late-As-Possible (the last ~2h before its press cures), so all of a
# 2h slice's builds collapse onto the same instant -> 297% 2h spikes -> the TBM
# pool physically cannot build them -> CUREBY_EXPIRED / UNREACHED -> 47.5% OTIF
# on a buildable schedule.
#
# THE FIX: for the BUILD op ONLY (op-200 green tyre, drum-fed, stage Building),
# place each build by LEAST-LOADED-TBM + EARLIEST-FEASIBLE-SLOT within its OWN
# cure-by window, instead of latest-feasible-start. The cure DRUM stays the
# FIXED anchor (C6); mixing/components still subordinate to the build schedule.
# Spreading builds across the cure-by window AND the 11 TBMs converts the JIT
# spike into a level load so the feasible demand is actually built.
#
# WINDOW (per build feeding a curing block at cure_start):
#     [cure_start - amax, cure_start - amin]            (the GT's own aging band)
#   EARLY-EDGE CLAMPED to max(window_start, inputs_ready) so a build is never
#   placed before its inputs (C1) and never over-ages on EITHER side (C2: 0
#   too-fresh, 0 over-aged - the commit-test still binds both edges).
#   amin/amax are the GT item's OWN aging-master window (per-SKU/per-compound),
#   falling back to GREEN_TYRE_CUREBY_MAX_H only if no row (already resolved on
#   the lot as aging_min_h/aging_max_h by sizing._aging_for).
#
# DETERMINISM (total order): least machine busy-min -> lowest TBM id -> earliest
# feasible slot -> lowest GT lot_id. No dict/set/hash-order dependence; no RNG;
# no wall-clock. When False, the prior ALAP build placement is preserved exactly.
#
# MEASURED EVIDENCE (Curing_Sch_SYNTH_clean.csv, 32 SKUs, this build): the
# proven premise (build-time TBM contention is the OTIF lever) does NOT bind on
# this schedule. The Building pool peaks at ~86-88% PLACED (never saturated), and
# the dominant failure is UPSTREAM input-chain over-aging / UNREACHED (~39.5k),
# NOT CUREBY_EXPIRED build-spikes (~3.6k). Pulling builds earlier to spread them
# forces their compound/component inputs even earlier, where their shorter shelf
# windows over-age (C2) - so leveling is NET-NEGATIVE here:
#     ALAP (OFF)           otif_demand = 47.46%   (presses fulfilled 3221)
#     leveled backoff 0.0  otif_demand = 44.05%   (3035)  - inputs over-age
#     leveled backoff 0.5  otif_demand = 47.00%   (3212)
#     leveled backoff 0.7  otif_demand = 47.17%   (3212)
#     leveled backoff 0.85 otif_demand = 45.16%   (3125)
# Every setting keeps C1-C8 + determinism (phase-8 violations = 0): the
# commit-test still binds the cure-by band BOTH-sided, so a leveled build is
# never force-placed over-aged - at worst it is reported, never silently broken.
# Per the task's "clamp conservatively if leveling risks C2 over-aging" rule we
# DEFAULT THE LEVEL-LOADER OFF (ALAP preserved, the best measured OTIF) and ship
# the mechanism fully wired + A/B-able. The real OTIF lever on this dataset is the
# upstream aging chain (a separate fix), not build placement. Flip True (with
# BUILD_LEVEL_BACKOFF_FRAC) to A/B on a schedule where the TBM pool DOES saturate.
BUILD_LEVELING_ENABLED = False
# Conservative early-edge backoff (fraction of the cure-by window, 0..1) for the
# level target. 0.0 = pure earliest-feasible (max spread, but pulls inputs to the
# extreme early edge where their shorter shelf windows over-age); 1.0 = ALAP
# (no spread). The level-loader places at earliest-feasible AT/AFTER
# early_edge + frac*window, so builds still spread across the window + the 11
# TBMs while staying close enough to JIT that their compound/component inputs
# survive (C2). Tuned on the synthetic schedule. Deterministic.
BUILD_LEVEL_BACKOFF_FRAC = 0.5

DEFAULT_AGING_MIN_H = 0.0
DEFAULT_AGING_MAX_H = 168.0      # 1 week fallback

# ---------------------------------------------------------------------------
# L3: PER-ITEM-TYPE SHELF-LIFE (scorch / cure-by) TABLE  [ESTIMATED]
# ---------------------------------------------------------------------------
# The legacy blanket 24h (1440 min) provisional over-tightened the bulk mixed
# compounds: final-compound chains routinely needed ~24-25h between mix and
# consumption and expired by ~10 min (gap=1450 > amax=1440), so thousands of
# producer lots went INFEASIBLE_AGING / UNREACHED on a boundary that does not
# reflect real material life. The plant scorch/hold reality is per item-type:
#
#   FINAL COMPOUND   48h  - mixed-and-batched-off final stock holds ~2 days
#   MASTER COMPOUND  72h  - masters hold longer (no cure system in the master)
#   SMALL CHEMICAL   72h  - pre-weighed chemical sub-batch, stable
#
# RESOLUTION ORDER (sizing._aging_for): an item's OWN per-file "Aging Master"
# row (data.aging[item]) ALWAYS wins; then this per-item-type table; then the
# injected green-tyre cure-by; then DEFAULT_AGING_MAX_H. So a measured master
# row is never overridden - this only replaces the blanket 24h DEFAULT for the
# bulk types that have no per-item aging row.
#
#   !!! ESTIMATED - PLANT SIGN-OFF REQUIRED (assumed scorch life, not measured) !!!
#
# Keyed by lowercased item-type. (amin_h, amax_h).
SHELF_LIFE_BY_TYPE = {
    "final compound": (0.0, 48.0),    # ESTIMATED
    "master compound": (0.0, 72.0),   # ESTIMATED (masters hold longer)
    "small chemical": (0.0, 72.0),    # ESTIMATED
}

# COMPOUND AGING-MAX OVERRIDE (plant directive: real compound shelf life = 72h).
# When COMPOUND_AGING_MAX_OVERRIDE_H is not None, the MAX aging (scorch) band for
# every compound item-type below is forced to this value EVEN IF the item carries
# a (tighter, e.g. 24h) measured Aging-Master row - i.e. the plant has certified
# that all compounds genuinely hold 72h. The MIN-aging side is left untouched.
# Set to None to revert to the measured-row-wins behaviour.
COMPOUND_AGING_MAX_OVERRIDE_H = 72.0
COMPOUND_AGING_TYPES = ("final compound", "master compound", "small chemical")

# ---------------------------------------------------------------------------
# PLANT AGING MASTER (authoritative - from the plant aging screenshots)
# ---------------------------------------------------------------------------
# data/aging_master.csv holds the plant-certified (min_h, max_h) aging band per
# item-type (PCR column of the plant aging spec: GREEN_TYRE 15min-1day, TREAD/
# SIDEWALL 2h-3d, BELT/PLY/CAPSTRIP/CHAFFER 1d, INNERLINER 6h-3d, BEAD_APEX 3d,
# FINAL/MASTER COMPOUND ~3d, etc.). When USE_PLANT_AGING is True this table is the
# AUTHORITATIVE source and OVERRIDES the per-item measured rows + the L3 table +
# the compound-72h override + the 8h green-tyre cure-by, for every listed
# item-type. Unlisted types (raw materials) fall through to the old logic.
PLANT_AGING_CSV = os.path.join(DATA_DIR, "aging_master.csv")
# DB-ONLY for the 6 inputs: OFF so Aging comes from the DB jkt_aging_master (one
# of the 6), NOT the local aging_master.csv per-type override.
USE_PLANT_AGING = False


def _load_plant_aging(path):
    import csv as _csv
    out = {}
    if os.path.exists(path):
        with open(path, newline="") as f:
            for r in _csv.DictReader(f):
                try:
                    out[str(r["item_type"]).strip().lower()] = (
                        float(r["min_h"]), float(r["max_h"]))
                except (TypeError, ValueError, KeyError):
                    continue
    return out


PLANT_AGING_BY_TYPE = _load_plant_aging(PLANT_AGING_CSV)

# ---------------------------------------------------------------------------
# Default MPQ / buffer fallbacks
# ---------------------------------------------------------------------------
DEFAULT_MPQ_MIN = 1.0
DEFAULT_MPQ_MAX = 99999.0
DEFAULT_BUFFER_H = 0.0

# ---------------------------------------------------------------------------
# MPQ_TYPE_DEFAULTS - bounded per-item-type MPQ fallbacks (TASK B / data gap)
# ---------------------------------------------------------------------------
# Several item-types are PRODUCED as dispatchable lots but are ABSENT from every
# per-SKU "MPQ" sheet, so they used to fall back to the generic, NON-BINDING
# DEFAULT_MPQ [1, 99999]. That surfaced as the bogus "1 to 99999" rows in
# waste_matrix.csv. Enumerated across all 137 corrected workbooks (lots actually
# produced, item-type absent from that file's MPQ sheet):
#
#   item-type               uom   produced-lot occurrences (this run / plant-wide)
#   ---------------------   ---   ----------------------------------------------
#   SMALL CHEMICAL          KG    368 lots   (pre-weighed chemical sub-batch)
#   PRE CUT ROLL MATERIAL   MTR   69 lots    (calendered roll cut to length)
#   SLITTED MATERIAL        MTR   46 lots    (slit ply/fabric length stock)
#   STEEL BELT EDGE STRIP   MTR   46 lots    (narrow steel-belt edge length)
#   APEX                    KG    8  lots    (bead apex compound, mass-based)
#
# Bounds below are ENGINEERING ESTIMATES, chosen consistent with sibling types
# that DO carry a real MPQ (KG types track MASTER/FINAL COMPOUND 210..840 but
# capped lower for small sub-batches; length/MTR types track Steel Belt /
# SideWall / Cap Strip 20..250). They are deliberately BOUNDED (not 99999) so
# C5 becomes binding, but they remain VISIBLE: sizing flags every lot whose
# bounds came from here as ESTIMATED in the lot audit. Real plant master data
# must replace these.
#
#   !!! PLANT SIGN-OFF REQUIRED — these are assumed, not measured. !!!
#
# Keyed by lowercased item-type (same convention as SkuData.mpq). Consulted by
# sizing._mpq_for and waste._mpq_for BEFORE the generic [1,99999] default and
# only when the per-SKU MPQ sheet has no legitimate entry for the type (so real
# present values are never overridden).
MPQ_TYPE_DEFAULTS = {
    # KG / mass-based sub-batches
    "small chemical": (1.0, 840.0),    # KG - small pre-weighed chemical batch
    "apex": (1.0, 840.0),              # KG - bead apex compound (mass)
    # MTR / length-based stock (track other length items 20..250)
    "steel belt edge strip": (20.0, 250.0),   # MTR
    "slitted material": (20.0, 250.0),         # MTR
    "pre cut roll material": (20.0, 250.0),    # MTR
}

# ---------------------------------------------------------------------------
# Stage / operation model (canonical PCR routing)
# ---------------------------------------------------------------------------
OP_MASTER_MIX = 10
OP_FINAL_MIX = 20
OP_CARCASS_BUILD = 195       # 1st stage build
OP_BUILD = 200               # 2nd stage build -> green tyre
OP_CURING = 210              # the DRUM (fixed)

# Building ops share ONE TBM pool - never double-count their capacity.
BUILD_OPS = (OP_CARCASS_BUILD, OP_BUILD)

# ---------------------------------------------------------------------------
# FIX-5: CROSS-SKU COMPOUND POOLING (one batch per (item, shelf-window) for ALL)
# ---------------------------------------------------------------------------
# The bulk mixed compounds (Final / Master / Small-chemical) are produced on the
# SHARED mixer line and are PHYSICALLY IDENTICAL by item code regardless of which
# SKU's tyre eventually consumes them: a B278 final-compound batch is the same
# rubber whether it goes to TUNE0 or TUSP0. The per-SKU make_lots() previously
# campaigned each SKU's demand for B278 SEPARATELY, charging the 210 kg MPQ-min
# floor to EACH consuming SKU. With ~182 such compounds shared across 2-4 SKUs
# each, that minted ~7,461 extra KG batches and ~366,000 kg of avoidable floor
# over-production. POOLING merges the concatenated demand for each compound ITEM
# CODE across ALL SKUs into one campaign stream (sizing.merge_cross_sku_campaigns)
# so ONE batch per (item, shelf-window) feeds every consuming SKU's curing blocks.
#
# Pooled ONLY for these BULK item-types (lower-cased match). Per-block PHYSICAL
# items (Green Tyre / Carcass / FG / Steel Belt / Cap Strip / Tread) stay STRICTLY
# per-SKU 1:1 - they are tyre-specific geometry, not fungible bulk rubber.
#
# SAFETY (Devika re-audit): cross-SKU pooling is only sound when the shared item's
# MPQ band and aging band AGREE across every consuming SKU. The pool pass ASSERTS
# agreement; on any disagreement it takes the SAFE (tightest) bound - smallest
# aging-max (shortest shelf life binds), largest aging-min, and the per-SKU MPQ
# (which is keyed by item-type and verified identical in CTP data) - so no pooled
# lot is ever consumed outside ANY puller's aging band (C2 stays exact).
POOLED_CROSS_SKU_TYPES = ("final compound", "master compound", "small chemical")

# ---------------------------------------------------------------------------
# CROSS_SKU_POOL_ENABLED - master switch for FIX-5/6 cross-SKU compound pooling
# ---------------------------------------------------------------------------
# REGRESSION FIX (full-drum OTIF collapse 63% -> 0.8%). The cross-SKU compound
# pool mints ONE shared batch per (item, shelf-window) but the dispatcher anchors
# each pooled lot to a SINGLE consumer; when that anchor fails to place, the whole
# shared batch goes UNREACHED and R14 poisons EVERY block it fed. At multi-SKU
# scale (62% of POOL lots feed >1 SKU) this cascades: 2,149 dropped POOL lots ->
# 4,231/4,272 presses unfulfilled -> 0.8% OTIF. At 5-SKU the anchor holds (100%).
#
# Until the pooled lot can be drawn by ALL its consumers (Approach A: seed/release
# the shared batch against the UNION of its pullers' windows, bound each batch to
# its own aging window, place bottom-up from supply), this flag DISABLES the
# cross-SKU pool. When False, the bulk compounds (POOLED_CROSS_SKU_TYPES) revert
# to PER-SKU / per-block sizing in make_lots - the state that gave ~63% full-drum
# OTIF. The WITHIN-SKU cap-ply / calender mother-roll pooling (FIX-1,
# CALENDER_POOL_ITEM_TYPES) is INDEPENDENT and stays ON (it is sound).
#
# TRADE-OFF (honest): with pooling off the per-SKU MPQ-min compound floor is
# charged once per consuming SKU again, so the avoidable compound over-production
# returns (a PHYSICAL waste cost). OTIF is restored - the correct priority. Flip
# True only once Approach A makes a pooled lot reachable by every consumer.
CROSS_SKU_POOL_ENABLED = False


def _pooled_cross_sku_types():
    """Effective cross-SKU pooled item-types, honouring CROSS_SKU_POOL_ENABLED.

    Empty (pooling OFF) -> bulk compounds are sized PER-SKU in make_lots and
    merge_cross_sku_campaigns emits nothing. The L2 calender cross-SKU set is
    gated separately by L2_CALENDER_CROSS_SKU_POOL, so this only governs the bulk
    compound pool. Single source of truth for both sizing.make_lots' skip-guard
    and merge_cross_sku_campaigns' pool selection."""
    return tuple(POOLED_CROSS_SKU_TYPES) if CROSS_SKU_POOL_ENABLED else ()

# ---------------------------------------------------------------------------
# L2: CROSS-SKU CALENDERED-ROLL (MOTHER-ROLL) POOLING
# ---------------------------------------------------------------------------
# A wide-sheet calendered MOTHER ROLL (cap strip "CAP 66", and the CALANDARED
# ROLL family CPJ*/HTPOLY*/ECOPOLY*/EHT*/...) is the SAME physical sheet run on
# the single 4-Roll Calender (901) regardless of which SKU's tyre is eventually
# slit from it. Within-SKU pooling (CALENDER_POOL_ITEM_TYPES) already made one
# mother-roll-sized lot serve many curing blocks of ONE sku; but the SAME mother
# roll was re-made once PER SKU, serialising dozens of tiny ALAP runs on the lone
# calender so the later SKUs' rolls aged out (CAP66 dominated UNREACHED).
#
# These item-types are therefore added to the CROSS-SKU pool pass
# (sizing.merge_cross_sku_campaigns), so ONE mother-roll campaign per
# (item, shelf-window) feeds EVERY consuming SKU's blocks. They keep the CALENDER
# (mother-roll, span-only) sizing semantics inside the pool pass - NOT the bulk
# compound MPQ packing - so the run stays one tiny re-based lot per window.
#
# SCOPE (caution): only the calender RUN (the wide roll) is shared. The per-tyre
# CUT piece (Steel Belt, Steel Belt Edge Strip - tyre-specific geometry) is NOT
# in this set and stays STRICTLY per-SKU 1:1. C2 (the roll's own aging window,
# tightest across consuming SKUs) and C5 (MIN floor) are preserved exactly the
# same way the compound pool preserves them.
POOLED_CROSS_SKU_CALENDER_TYPES = (
    "cap strip", "capstrip", "cap",
    "calandared roll", "calendared roll", "calandered roll",
)

# Stage grouping for bottleneck / Gantt swimlanes.
STAGE_FINAL_MIX = "Final Mixing"
STAGE_MASTER_MIX = "Master Mixing"
STAGE_BUILDING = "Building"
STAGE_CURING = "Curing"

# Department -> canonical stage label, used to recover a real stage when an
# upstream routing row's operation_name is a placeholder ("Unknown"/blank).
# Round-2 COSMETIC fix (op-70 Bead/Apex Prep rows on 0301/201/202).
STAGE_BY_DEPARTMENT = {
    "bead/apex prep": "Bead Apexing",
    "bead apex prep": "Bead Apexing",
    "bead/apex": "Bead Apexing",
    "extrusion": "Extrusion",
    "calendering": "Calendering",
    "cutting": "Cutting",
}

# Final Mixers - the structural bottleneck (2 units).
FINAL_MIXERS = ("F270 F", "K310 F")
# PCR TBM building pool (ops 195 & 200 share this).
PCR_TBM_POOL = (
    "3401", "3402", "3403", "3404", "3405",
    "3406", "3407", "3408", "3409", "3410", "3411",
)

# Special drum rows that occupy press time but carry NO demand.
# DB CuringSchedule.csv labels its non-production admin blocks "C/O" (changeover)
# and "MOULD_CLEANING"; the legacy drum used CHANGEOVER / MOULD_CLEAN. Both label
# sets are excluded from demanded SKUs and the unfed-press denominator.
NON_PRODUCTION_SKUS = ("CHANGEOVER", "MOULD_CLEAN", "C/O", "MOULD_CLEANING")

# ---------------------------------------------------------------------------
# Label normalisation map (Buffer/ItemType label drift -> canonical BOM spelling)
# ---------------------------------------------------------------------------
LABEL_NORMALISE = {
    "side wall": "SideWall",
    "sidewall": "SideWall",
    "belt": "Steel Belt",
    "steel belt": "Steel Belt",
    "appex bead": "Bead Apex",
    "bead apex": "Bead Apex",
    "chaffer": "Chafer",
    "chafer": "Chafer",
    "bead wire": "Bead Wire",
    "cap strip": "Cap Strip",
    "capstrip": "Cap Strip",
}

# Buffer Master uses item-type names with parenthetical qualifiers; we fold
# those onto the base item-type for matching.
BUFFER_BASE = {
    "master compound (silica)": "MASTER COMPOUND",
    "final compound (silica)": "FINAL COMPOUND",
    "small chemical (master)": "SMALL CHEMICAL",
    "small chemical (final)": "SMALL CHEMICAL",
}

# ---------------------------------------------------------------------------
# DataFrame schemas (frozen between phases - design doc data contracts)
# ---------------------------------------------------------------------------
SCHEMA_DEMAND = [
    "item", "item_type", "required_qty", "uom",
    "source_curing_block", "curing_deadline", "line_class", "sku",
]
SCHEMA_LOT = [
    "lot_id", "sku", "item", "item_type", "qty", "uom",
    "op_seq", "stage", "machines", "proc_min", "proc_eff_min",
    "aging_min_h", "aging_max_h", "buffer_h", "transfer_min",
    "curing_deadline", "consumer_item", "is_bottleneck",
    "est", "lst", "slack_min", "infeasible_flag", "estimated_proc",
    "line_class", "parent_lot_ids",
    # TASK B audit: MPQ bounds applied + provenance ("sheet"/"estimated"/"default")
    "mpq_min", "mpq_max", "mpq_source",
    # FIX-5: intentional pooled mother-roll / cross-SKU pool lot that legitimately
    # exceeds per-tyre MPQ_max (calender mother roll, cross-SKU compound batch).
    "pooling_exempt",
]
SCHEMA_SCHEDULE = [
    "lot_id", "sku", "item", "item_type", "stage", "op_seq",
    "machine", "qty", "uom", "start", "end", "duration_min",
    "changeover_min", "is_curing", "consumer_item",
    "aging_min_h", "aging_max_h", "gap_to_consumer_min", "status",
    "source_curing_block", "consumer_lot_id",
    # MASTERS: exact transfer minutes (per-item-type, Transfer master) the lot was
    # committed with, so Phase-8 re-derives C1/aging with the same value.
    "transfer_min",
]
SCHEMA_VIOLATION = ["lot_id", "machine", "check_type", "detail", "severity"]

# ---------------------------------------------------------------------------
# WHAT-IF SCENARIO OVERRIDES (deterministic capacity perturbation)
# ---------------------------------------------------------------------------
# run_pipeline(..., config_overrides=dict) and capacity.analyse(..., overrides=)
# accept an OPTIONAL, fully-deterministic perturbation of plant capacity. Default
# None -> identity (no behaviour change, no regression, C1-C8 & determinism hold).
#
# Recognised keys (all optional):
#   machine_adds   : {stage_label: int}  extra machines added to that stage's
#                    eligible pool (e.g. {"Building": 2} -> +2 TBMs). Synthetic
#                    ids are appended deterministically to every lot's eligible
#                    machine cell for that stage, so dispatch can spread load.
#                    Curing (the drum) is NEVER expanded (fixed press schedule).
#   oee_factor     : float  multiplies the standard OEE (availability/quality).
#                    >1 = faster (proc_eff divided by factor); clamped (0.1, 3.0).
#   cycle_multiplier : float  scales every dispatchable lot's run-time
#                    (proc_eff_min *= cycle_multiplier); clamped (0.1, 5.0).
# Building-only and Final-Mixing-only machine adds are the spec what-if levers
# (+N Building TBM, +N Final Mixers); any stage label is accepted generically.
SCENARIO_STAGE_PREFIX = "SCN"   # synthetic added-machine id prefix


def normalise_overrides(overrides):
    """Return a canonical, validated overrides dict (or None). Pure/deterministic.

    Clamps numeric factors to safe ranges and drops no-op entries so a scenario
    that equals the baseline produces the identical dict (None) -> identical run.
    """
    if not overrides:
        return None
    out = {}
    adds = overrides.get("machine_adds") or {}
    clean_adds = {}
    for stage, n in adds.items():
        try:
            n = int(n)
        except (TypeError, ValueError):
            continue
        if stage == STAGE_CURING:        # the drum is fixed - never expand presses
            continue
        if n > 0:
            clean_adds[str(stage)] = n
    if clean_adds:
        out["machine_adds"] = clean_adds
    oee = overrides.get("oee_factor")
    if oee is not None:
        try:
            oee = float(oee)
            if abs(oee - 1.0) > 1e-9:
                out["oee_factor"] = min(3.0, max(0.1, oee))
        except (TypeError, ValueError):
            pass
    cyc = overrides.get("cycle_multiplier")
    if cyc is not None:
        try:
            cyc = float(cyc)
            if abs(cyc - 1.0) > 1e-9:
                out["cycle_multiplier"] = min(5.0, max(0.1, cyc))
        except (TypeError, ValueError):
            pass
    return out or None
