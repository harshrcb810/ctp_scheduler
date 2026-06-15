# CTP Production Scheduler

An end-to-end, constraint-based **production scheduler** for a radial tyre plant (JK Tyre — Chennai Tyre Plant, PCR/TBR). It takes the plant's **curing schedule (the drum)** plus master data (BOM, routing, MPQ, aging, buffers, item-types) and produces a **complete, machine-loaded, correct-by-construction schedule** for every upstream stage (Mixing → SFG Prep → Building → Curing), enforcing hard material-physics constraints.

> **Heuristic, deterministic, correct-by-construction.** No MILP/CP-SAT. Drum-Buffer-Rope backward-pull from the fixed curing schedule, As-Late-As-Possible (ALAP) placement, one shared machine timeline, one global priority queue across all SKUs and both PCR/TBR lines.

---

## What it does

1. Reads the **curing schedule** (the immutable "drum") and the **master data**.
2. Explodes demand backward through the multi-level **BOM**.
3. **Sizes lots** (MPQ minimums, mother-roll calendering, integer-piece partitioning).
4. **Places every lot** on a specific machine, just-in-time before its consumer, honouring:
   - BOM precedence & the green-tyre → cure coupling,
   - material **aging / scorch** windows (both sides),
   - machine **eligibility, non-overlap & changeover**,
   - the **cure-by** window (green tyres must reach the press before they scorch).
5. Emits the **schedule + KPIs + violations + infeasibility reasons + waste matrix**, and an interactive **dashboard**.

### The 8 hard constraints (C1–C8)
| | Constraint |
|---|---|
| C1 | BOM precedence / AND-join (all inputs ready before an operation) |
| C2 | Aging window both sides (min cure + max scorch) incl. green-tyre cure-by |
| C3 | Machine eligibility |
| C4 | Machine non-overlap + changeover |
| C5 | Minimum Production Quantity (MPQ) bounds |
| C6 | Curing schedule immutability (the drum is fixed) |
| C7 | DAG acyclicity / shared-machine integrity |
| C8 | Determinism (byte-identical reruns; no RNG / wall-clock) |

The scheduler **never** weakens a hard constraint: a lot that would scorch or violate precedence is reported `INFEASIBLE` with a reason code, not force-placed.

---

## Project structure

```
ctp_scheduler/        the engine (Python package)
  config.py           all constants, flags, schema contracts, version log
  ingest.py           input loading (DB-CSV or legacy workbooks)
  db_source.py        DB-source loaders (data/from_db/ consolidated CSVs)
  masters.py          MPQ + transfer plant masters (sole source)
  dag.py              BOM → operation DAG
  windows.py          ALAP time-window computation
  sizing.py           lot sizing (MPQ, mother-roll, integer-piece)
  pin.py              pin the curing drum (C6)
  dispatch.py         the placement engine (the core)
  timeline.py         per-machine interval timeline
  capacity.py         bottleneck / utilisation analysis
  validate.py         independent C1–C8 re-proof from placed reality
  outputs.py          schedule / KPI / handoff / waste emitters
  units.py            canonical UOM resolver
  pipeline.py / run.py  orchestration + CLI
app.py                Streamlit dashboard
requirements.txt      Python dependencies
.env.example          DB credentials template (copy to .env)
data/                 inputs (see "Data inputs" below)
outputs/              generated results (git-ignored)
```

---

## Installation

```bash
pip install -r requirements.txt
```
Python 3.10+ recommended.

---

## Configuration

The scheduler can read its input data **from a MySQL database** or **from local CSVs**.

### DB mode (default, `USE_DB_SOURCE=True` in `config.py`)
1. Copy the template and fill in your own credentials:
   ```bash
   cp .env.example .env      #  (Windows:  copy .env.example .env)
   ```
   ```
   DB_USER=...
   DB_PASSWORD=...
   DB_HOST=...
   DB_PORT=3306
   DB_NAME=jkplanning_CTP
   ```
   **`.env` is git-ignored — never commit it.**
2. Export the DB tables into `data/from_db/` as CSVs (one-time / refresh), or point `db_source.py` at your live DB.

Expected DB tables (per line, `_pcr` / `_tbr`):
`jkt_routing`, `jkt_bom`, `jkt_mpq`, `jkt_aging_master`, `jkt_buffer_master`, `jkt_itemType_master`, and `CuringSchedule` (the drum).

### File mode
Set `USE_DB_SOURCE=False` and provide the consolidated CSVs in `data/from_db/` (same column layout as the DB tables).

---

## Data inputs

| Input | Source | Notes |
|---|---|---|
| **Curing schedule (drum)** | `CuringSchedule` | the fixed scheduling anchor (C6) |
| **Routing** | `jkt_routing` | operations, machines, proc-time, transfer |
| **BOM** | `jkt_bom` | multi-level parent → child |
| **MPQ** | `jkt_mpq` + `data/MPQ_master_corrected.*` | min run qty per item-type |
| **Aging master** | `jkt_aging_master` | per-item min/max aging (scorch) |
| **Buffer master** | `jkt_buffer_master` | per item-type buffer hours |
| **ItemType master** | `jkt_itemType_master` | item code → type |
| **Build cycle (GTCT)** | `data/build_cycle_master.csv` | per-SKU green-tyre build cycle |
| **Transfer times** | `data/Transfer_master_corrected.csv` | per item-type, PCR/TBR |

> **⚠️ Plant data is proprietary.** The real BOM / routing / curing data (`data/from_db/`, the drum, the masters) is **not** included in this repo by default (see `.gitignore`). Bring your own DB / CSVs in the documented format.

---

## Running it

```bash
# schedule all SKUs present in the drum
python run.py --skus ALL

# only the SKUs that appear in the curing drum
python run.py --drum-skus

# a specific subset
python run.py --skus <SKU1> <SKU2> --outdir outputs

# use a specific curing file
python run.py --drum data/CuringSchedule.csv --skus ALL --outdir outputs
```

Dashboard:
```bash
streamlit run app.py      #  ->  http://localhost:8501
```

---

## Outputs (written to `outputs/`)

| File | Contents |
|---|---|
| `schedule.csv` | every placed lot (sku, item, stage, machine, start/end, qty) |
| `kpi_report.csv` | OTIF, plant fulfilment, lot placement, bottleneck, utilisation |
| `violations.csv` | hard-constraint violations (empty on a clean run) |
| `infeasibility.csv` | unplaced lots + reason codes (CUREBY_EXPIRED / INFEASIBLE_AGING / UNREACHED / …) |
| `unfulfilled_presses.csv` | curing blocks that couldn't be fed + binding reason |
| `waste_matrix.csv` | required vs produced per component (over-production / scrap) |
| `handoff_report.csv` | build→cure handoff timing |

---

## Known data gaps (current dataset)

The scheduler is correct; remaining shortfall traces to **input data**, not the engine:
- routing `proc_time` partially populated (mixing ops still being filled);
- `transfer_time_min` empty in the DB (supplied via the transfer master);
- build-cycle tables empty in the DB (supplied via `build_cycle_master.csv`);
- BOM zero-quantities, missing aging-maxes, and duplicates in the raw data (cleaned on ingest / flagged for the plant).

See `CTP_SCHEDULER_FORENSIC_ANALYSIS.md` for the full audit.

---

## License & data notice

Code: add your chosen license (e.g. MIT) as `LICENSE`.
**Data:** plant master data and curing schedules are JK Tyre confidential and are **not** distributed with this code.
