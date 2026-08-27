# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## How to maintain this file (ABL-574, following ABL-536)

This file auto-loads into every agent context, so its size is a per-turn tax on
every run that touches ingest. The dashboard repo let its copy reach 6,752 lines
and it killed runs outright.

- **Hard budget: 700 lines / 35 KB.** If an edit would cross it, move material
  to `docs/claude/` first. Enforced, not merely asked for:
  `tests/test_claude_md_budget.py` fails the suite when this file crosses either
  limit, and again if this sentence and `CLAUDE_MD_BUDGET` stop agreeing. Bytes
  are counted LF-normalised, as git stores the file (`core.autocrlf=true` gives
  this working tree CRLF, ~1 B per line heavier than the blob), so the verdict is
  the same on every platform. Raising the budget to fit an edit is not the
  remedy — moving the material is.
- **Durable rules only.** Commands, maps, invariants, gotchas — each stated once,
  tersely. Incident narratives, dated measurements and per-issue forensics go in
  the matching `docs/claude/` topic file; append there and reference it here in
  one line if a pointer is warranted.
- **Correct in place.** When a rule changes, rewrite it — never append a "this
  used to say…" paragraph. The history lives in git and in `docs/claude/`.
- **Baselines rot.** Record counts, coverage percentages, database size and
  date ranges have a shelf life measured in weeks; keep the query that
  re-measures, not the figure.
- After editing this file: `python -m pytest tests/test_claude_md_budget.py`
  must pass.

## Repository Overview

Ingest module for the Able energy stack. It fetches European energy market data
from the **ENTSO-E Transparency Platform** and weather from **Open-Meteo**, and
writes both into the shared SQLite database. It is the only writer; everything
downstream opens that file readonly.

**Reference documents** (all authoritative over this file on their own subject):

| Document | Covers |
|---|---|
| [`PIPELINE.md`](./PIPELINE.md) | Architecture, data flow, operations, troubleshooting |
| [`database_structure.md`](./database_structure.md) | Schema, indexes, views, query patterns |
| [`WEATHER_DB.md`](./WEATHER_DB.md) | `weather_observation` architecture + deploy runbook |
| [`EXTENDING.md`](./EXTENDING.md) | Step-by-step recipes for extending the weather DB |
| `docs/claude/` | This file's archived narrative (see Archive, below) |

`database_structure.md` predates `energy_generation`, `energy_generation_forecast`,
`net_position`, `crossborder_flows` and `weather_observation` — for those, read
`src/db.py`, `src/weather_schema.py` and `scripts/create_generation_table.py`.

**Consumers (all break if the schema breaks):**

- **`energy-dashboard-frontend`** — Express API on port 3001, readonly, serves the
  dashboard and `GET /api/weather/latest` to the LAN.
- **Heliocast production** — hourly Windows Task at `:45 UTC` reads the freshest
  forecast via `/api/weather/latest` and submits to Predico-Elia.
- **Helio research / `energy-forecast`** — backtests and D+2 model training read
  the workstation replica.

**Configuration:** `.env` holds `api_key_entsoe`. `python config.py` verifies the
API key, database path and country configuration.

## What this repo writes

Eight ENTSO-E data types (`ENTSOE_API_CONFIG`, `config.py:35`), four of which
carry `is_dayahead`:

| `--types` value | Table(s) written |
|---|---|
| `load` | `energy_load` |
| `price` | `energy_price` (day-ahead) |
| `renewable` | `energy_renewable` **and** `energy_generation` — one A75 fetch |
| `load_forecast_day_ahead` / `load_forecast_week_ahead` | `energy_load_forecast` |
| `wind_solar_forecast` | `energy_generation_forecast` (day-ahead) |
| `crossborder_flows` | `crossborder_flows` |
| `net_position` | `net_position` (day-ahead) |

Weather: `weather_data` (legacy, still serving dashboards) and
`weather_observation` + its `weather_location` / `weather_source` dimensions
(versioned, per-NWP-model, per-zone). Every run also writes `data_ingestion_log`.

**Code map:**

```
config.py                  # ENTSO-E endpoints, domain codes, PSR mappings, UPDATE_DAYS_BACK
utils.py                   # date chunking, logging setup, validation
src/entsoe_client.py       # API wrapper: rate limiting, retry, secret registration
src/fetch_*.py             # one fetcher per data type
src/published_points.py    # the unpublished-zero rule (see Rules that bite)
src/log_redaction.py       # central credential scrubbing (ABL-86)
src/db.py                  # upsert + logging; owns every CREATE TABLE but weather_observation
src/weather_schema.py      # weather_observation schema, LOCATIONS, OPEN_METEO_SOURCES
src/pipeline.py            # orchestrator: run_backfill / run_update
scripts/                   # entry points (below) + one-off migrations and probes
docker/crontab             # the authoritative schedule
tests/                     # pytest, colocated with nothing — all tests live here
```

## Commands

```bash
pip install -r requirements.txt      # see Testing for the interpreter that already has these
python config.py                     # verify configuration

python scripts/update.py                                    # routine pass: last 7 days, all types
python scripts/update.py --days 14 --types load,price --countries DE,FR
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types all --countries all
python scripts/backfill.py --use-defaults --types all --countries all
python scripts/catchup.py                                   # heal interior energy_load holes
python scripts/update_weather.py --forecast                 # legacy weather_data
python scripts/update_weather_observation_hourly.py         # versioned weather_observation
```

**Date chunking asymmetry.** Backfill splits a range into **90-day** chunks
(`src/pipeline.py:73`, `utils.py:129`) — ENTSO-E's own limit is ~1 year, but 90
days avoids year-boundary issues in some bidding zones. **Update does not chunk
at all:** `run_update` computes one `(start, end)` and calls `_fetch_data_chunk`
once per (country, data type). Widening `--days` therefore costs **zero** extra
ENTSO-E requests — only a bigger payload and more rows upserted.

**Idempotency.** Everything is `INSERT OR REPLACE` behind unique indexes, so any
pass can be re-run safely. That also means a re-run *rewrites* rows (see
`publication_timestamp_utc`, below).

## Schedule

`docker/crontab` is authoritative and carries the reasoning for each job; this is
the index, all times UTC.

| When | Job |
|---|---|
| `30 0,6,13,18` | `update.py` — full pass, trailing 7 days, all types |
| `15 11` and `15 12` | `update.py --types price --days 2` — D+1 price, right after the SDAC publication (ABL-54/ABL-51) |
| `0 15` | `update_weather.py --forecast` — legacy `weather_data` |
| `30 * * * *` | `update_weather_observation_hourly.py` — realtime NWP pull |
| `0 7`, `30 13`, `30 19` | `update_weather_observation.py` — adds Previous Runs (day1-3) |
| `0 5 * * 0` | `catchup.py` — weekly late-publisher catch-up (ABL-84/ABL-85) |

The two price lines are not redundancy for its own sake: the day-ahead auction
publishes at ~10:45 UTC (CEST) or ~11:45 UTC (CET), and without them the first
pass that can see tomorrow's price is 13:30 — the only one before evening.
`update.py`'s own docstring still says "hourly"; that has not been true since the
crontab was written.

## Rules that bite

**One upstream fetch per country per window.** `energy_renewable` and
`energy_generation` are both written from a single A75 document. Never add a
second request to fill one of them — `test_only_one_upstream_request_per_leg`
pins this.

**`NULL`, never a manufactured `0`.** entsoe-py 0.8.0 expands a sparse `Period`
by forward-filling the last published value across every missing position, and we
used to store that fill as measured data. `src/published_points.py` is the single
rule that refuses it, applied on all three paths that hit it — load (ABL-50), net
position (ABL-55) and generation (ABL-268). It drops a value only when it is
**both** a position the document carried no `Point` for **and** exactly `0.0`:

- **Unpublished alone is not enough.** Under `curveType=A03` — which every
  document sampled so far carries — an absent position is an *encoded repeat*,
  not an unknown. AL's hourly step function and PT's flat 500 MW interconnector
  are genuine data expressed as one Point.
- **Zero alone is not enough.** A published `0.0` is the TSO's number and is
  stored; keeping it off a chart is the dashboard's job.
- **The unit is a cell, not a row,** for generation: an A75 row carries up to 21
  independently measured types, so a blanked value becomes `NaN` → SQL `NULL`
  rather than deleting twenty real readings alongside one invented zero.
- **The published set is per sub-series, not per document.** A document-wide
  union reports every position as published as soon as any one series is
  complete. `published_timestamps_by_series` keys on (production type, Actual
  Aggregated | Actual Consumption).
- **Everything fails open.** Unparseable XML, an unrecognised resolution, or a
  position grid matching no returned row keeps every row and logs a warning. A
  parser that silently deleted real readings would be worse than the defect.

**"Store only genuinely published Points" is refuted** — it is the obvious
reading of that defect and it would have deleted more than half of PT's and ES's
real rows. Do not re-propose it. The rule is about provenance, not plausibility:
a pipeline that decides which zeros to believe by how plausible they look is the
one that wrote GR a year of measured-looking `0.0` MW net position.

The guard is **forward-only**. It changes what future passes write; no existing
row is backfilled or deleted, and what fraction of historical zeros are
fabricated is deliberately not estimated. Measurements, per-zone impact tables
and the reasoning: `docs/claude/09-data-quality-guidelines.md`.

**`energy_renewable` is frozen and deliberately unchanged by all of the above.**
Its columns are `DEFAULT 0` and it has never been able to express "not reported";
`_map_renewable_columns` `fillna(0)`s what it maps, so a blanked cell reaches it
as the same `0.0`. `test_energy_renewable_output_is_byte_identical` pins that.
Teaching it `NULL` mid-life would encode one condition two ways and would break a
consumer that trains models on it — a contract change for its owner to make
deliberately, not a side effect of a guard.

**`publication_timestamp_utc` records when we fetched, not when the value was
published.** ENTSO-E builds the document on request and stamps `createdDateTime`
with that moment (`src/entsoe_client.py`), and `INSERT OR REPLACE` overwrites the
column on every re-fetch — so one GB block from 2021 carries four different
values, and a backfilled row is stamped with the backfill date. **Do not use it
for publication delay, freshness, or revision tracking, and never backfill it.**
For "when did we first store this row", use `created_at`, which is write-once.

**The 7-day update window is shorter than the ~28-day revision horizon
(ABL-442).** `UPDATE_DAYS_BACK = 7` (`config.py:211`), but ENTSO-E keeps revising
a value for about 28 days. Every row therefore freezes on whichever vintage was
current ~7 days after delivery, and which vintage a given row holds is decided by
when someone last ran an ad-hoc backfill — not by policy. The revision is not
uniformly upward. `energy_renewable` is the one unrevised table (no ad-hoc
backfill writes it), which makes it a usable control *and* means the two
generation tables can disagree for reasons unrelated to column mapping.
`catchup.py` does not help: it targets instants we are *missing*, and a revision
is a value change on a row we already hold. **No behaviour has been changed** — a
weekly settle pass over a trailing 42 days is proposed on ABL-442 and awaits a
CEO decision, as does the one-time reconciliation, which rewrites published
history in both directions. Measurements:
`docs/claude/05-entso-e-data-gathering-pipeline.md`.

**A long outage is not healed by the routine pass.** A zone dark for more than 7
days is never re-requested once the outage slides out of the window, even though
ENTSO-E still holds the data. That is what `scripts/catchup.py` exists for
(ABL-84/ABL-85) — weekly, interior holes only, and **`energy_load` only**
(`catchup.py:71`). Extending it to another table is a deliberate change, not a
config tweak.

**Credentials must never reach a log** (ABL-86). entsoe-py puts the API key in
the request URL as `securityToken=`, and `requests` builds an `HTTPError`'s
message from the full URL — so every failed request handed us an exception whose
`str()` was our credential, at ~128 `logger.error(f"...: {e}")` sites.
`src/log_redaction.py` is the fix and is deliberately **central**, because a rule
at the call site would have to be remembered at all 128 and at the 129th. Four
entry points, none of which covers everything alone: `redact_secrets()` (pattern,
so it survives a rotation), `register_secret_value()` (literal, registered by
`ENTSOEClient.__init__`), `SecretRedactingFilter` installed on the **handlers**
of the root and `entsoe_pipeline` loggers (a handler filter sees entsoe-py's and
urllib3's records too), and `redact_exception()` at `_make_request`'s except
branches — the only one covering an **uncaught** traceback, which cron's `2>&1`
puts straight into the log file. **Keeping the rest of the URL is a requirement,
not an accident:** scrubbing the whole URL would close the hole by making the log
useless, and the log is how ABL-84 was diagnosed.
`tests/test_log_redaction.py` pins both halves. Two things this does not do, both
escalated to the Board: it does not scrub the log files already on prod, and it
does not rotate the key. **Treat existing prod logs as still containing the
credential.**

**Never DROP `weather_observation` or its dimension tables, and never
DROP-and-recreate to change a column.** Schema changes go through
`ALTER TABLE ... ADD COLUMN`, which SQLite does in O(1); column removal needs the
copy-table dance and a maintenance window. Every change is tested on a scratch
copy first, and a backup is taken before anything touches prod:

```bash
sqlite3 /home/clavain/energy-dashboard/data/energy_dashboard.db \
  ".backup /home/clavain/energy-dashboard/data/backup_$(date +%F).db"
cp data/energy_dashboard.db /tmp/scratch.db
ENERGY_DB_PATH=/tmp/scratch.db python scripts/<...>.py
```

Verify after every deploy with
`docker compose exec data-gathering python scripts/init_weather_observation.py --verify`
plus a `curl /api/weather/latest?...` smoke. Full recipes: `EXTENDING.md`.

**A seeded-but-unwired weather source is a silent dead end.** If you append to
`weather_schema.py::OPEN_METEO_SOURCES`, also add the model to the matching
fetcher tuple (`REALTIME_NWP_MODELS` or `NWP_MODELS`), or it produces no data and
reports no error.

**`weather_observation`'s hourly cron is the only Open-Meteo fetcher in the whole
stack.** Dashboards, Heliocast production inference and Helio backtests all read
the table rather than the API. Adding a second fetcher anywhere re-opens the
divergence this table was built to close.

**Do not change the schema, retire `energy_renewable`, or alter what is ingested
without escalating.** Those are cross-module contract changes with their own
approvals.

## Data the pipeline cannot give you

- **IS, MT and TR have no ENTSO-E data at all** and are skipped by design.
- **GB and UA stopped upstream**, not in ingest — their data ends in 2021 and
  2022 respectively.
- Several small Balkan zones (MD, MK, BA, CY, RS, ME) publish late, sparsely and
  holily. A short span there is usually upstream, and `catchup.py` is the first
  thing to try, not a bug report.
- **No forecast beyond the horizons actually stored.** Do not interpolate or
  extrapolate one to fill a visual gap.

Country-by-country quality analysis lives in `database_completeness.md`.

## Testing

```bash
python -m pytest -q          # from the repo root
```

**Use the miniconda3 interpreter** (`C:\Users\guill\miniconda3\python.exe` on the
workstation) — it already has every dependency. **Do not build a venv for this
repo:** a fresh one is missing deps and produces a fake red suite that looks like
your change broke something.

Baselines rot, so the durable half is the delta: re-measure on the merged tree
rather than trusting a count written here. Everything lives in `tests/`, one file
per subject, and `tests/conftest.py` provides the in-memory SQLite fixtures.

`tests/test_claude_md_budget.py` enforces this file's size budget and is the one
test to run after editing it.

## Archive

`docs/claude/` holds the full pre-2026-08-27 narrative this file was distilled
from (ABL-574), one file per former section — incident forensics, dated
measurements, worked SQL, design rationale. It is verbatim: the twenty files
round-trip to the original 51,430-byte document byte-for-byte. Figures and
citations in there are frozen at the archive date. Start with the matching topic
file whenever a rule here needs its evidence or history.

| File | Was |
|---|---|
| `01-repository-overview.md` | Database stats snapshot (2025-12-23), key files |
| `02-data-types.md` | Per-type coverage, granularity, date ranges |
| `03-countries-covered.md` | Country lists by region |
| `04-database-architecture.md` | Star schema, time-series patterns |
| `05-entso-e-data-gathering-pipeline.md` | Pipeline detail, ABL-442 revision-horizon measurements, ABL-86 redaction forensics |
| `06-weather-data-pipeline.md` | Legacy `weather_data` commands, forecast vintages, worked SQL |
| `07-versioned-weather-db-weather-observation-2026-04-22.md` | `weather_observation` consumers and extension rules |
| `08-common-database-operations.md` | Worked query and maintenance recipes |
| `09-data-quality-guidelines.md` | The full published-points narrative (ABL-50/55/268) with per-zone impact tables |
| `10-countries-with-complete-data-all-4-types.md` | The 23-country list |
| `11-database-schema.md` | Per-table column lists, index strategy, publication-timestamp forensics |
| `12`–`15-*-changes-*.md` | Dated changelogs, 2025-12-22 to 2026-01-11 |
| `16-working-with-this-repository.md` | Querying / modifying / analysing checklists |
| `17-known-issues-limitations.md` | Coverage gaps as of 2025-12 |
| `18-maintenance-tasks.md` | Cadence checklist |
| `19-performance-considerations.md` | Index and query-planner notes |
| `20-data-sources.md` | Upstream provider list |
