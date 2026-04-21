# Versioned Weather DB — architecture + runbook

This document describes the `weather_observation` subsystem: the
snapshot-versioned weather tables, the 3×/day ingest, the heliocast
write endpoint, the backfill, and the read helpers. All of it lives
inside the same canonical SQLite file as the rest of able
(`energy_dashboard.db` on prod, synced to the workstation daily).

> Scope today: **Belgium only**. The schema is country-extensible —
> adding Germany or France is a dimension-table insert + fetcher change,
> no schema migration.

## Why it exists

Before: weather was scattered across three systems — able's single-point
`weather_data` table (ERA5 only, 1×/day), heliocast's hourly per-run
`data/weather/*.csv` sidecars, and helio's bulk `tools/weather_nwp_*.csv`
flat files. None of them preserved "which forecast was available at
time T", which broke backtest fidelity.

After: one table, `weather_observation`, keyed by
`(source_id, location_id, valid_at, fetched_at)`. Every fetch creates
a new row even for the same target hour, so replay queries (what did
we know at 07:45 UTC last Tuesday?) are exact.

## Topology — single fetcher, many readers

```
                          Open-Meteo API
                               │
                               │ (THE ONLY outbound fetch path)
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│  QuietlyConfident   (prod, 192.168.86.36)                          │
│                                                                     │
│   energy-data-gathering                energy-dashboard-frontend   │
│    cron (Docker):                        express server :3001       │
│                                                                     │
│      00:30, 06:30, 13:30, 18:30 UTC      GET  /api/*        (RO)   │
│        → update.py (energy)              GET  /api/weather/latest  │
│                                                    (RO, public)    │
│      15:00 UTC                                                      │
│        → update_weather.py (legacy)      POST /api/weather/snapshot│
│                                                    (RW, token-auth,│
│      XX:30 UTC  (HOURLY — NEW)                     kept for back-  │
│        → update_weather_observation_                compat; will   │
│          hourly.py  (4 NWP, realtime)              be removed)     │
│                                                                     │
│      07:00, 13:30, 19:30 UTC                                        │
│        → update_weather_observation.py                              │
│          (Previous Runs day1+day3 archive)                          │
│                                                                     │
│         │                                      │                    │
│         ▼                                      ▼                    │
│   ┌─────────────────────────────────────────────────┐              │
│   │  energy_dashboard.db  (~3.2 GB, WAL mode)       │              │
│   │    weather_location / weather_source /          │              │
│   │    weather_observation  (fact, fetched_at PK)   │              │
│   └─────────────┬──────────────────┬────────────────┘              │
│                 │                  │                                │
│                 │                  │  GET /api/weather/latest       │
│                 │ nightly sqlite   │  (over LAN, port 3001)         │
│                 │ .backup() + scp  │                                │
└─────────────────┼──────────────────┼────────────────────────────────┘
                  │                  │
                  │ 07:00 local      │ :45 UTC hourly (live)
                  ▼                  ▼
       Workstation replica     heliocast runner.py
       (readonly, ≤ 24h lag)      ├─ fetch_weather_from_db()
               │                    ├─ features + predict + bias-correct
               │ SELECT              ├─ submit to Predico
               ▼                    └─ save forecast/submission sidecars
       helio research                   (NO local Open-Meteo fetch)
       notebooks · backtests
```

**The single-fetcher rule:** one Docker cron in able hits Open-Meteo.
Everything else (dashboards, heliocast inference, backtests) reads
from the DB. The workstation replica is for non-real-time research;
heliocast reads live from the prod DB over LAN for inference.

## Schema

Three tables. Full DDL lives in
[`src/weather_schema.py`](src/weather_schema.py). Summary:

### `weather_location` — dimension: where

| col | type | notes |
|---|---|---|
| `location_id` | INTEGER PK | auto |
| `country_code` | TEXT | `'BE'`, `'FR'`, ... |
| `zone_id` | TEXT | `'centroid'`, `'central'`, `'north'`, `'south'`, `'east'` |
| `lat`, `lon` | REAL | |
| `weight` | REAL | capacity share for zone aggregation (NULL for centroid) |
| `description` | TEXT | |
| `UNIQUE(country_code, zone_id)` | |

Seeded with 5 BE rows: centroid (legacy single-point) + 4 capacity-
weighted PV zones matching heliocast/helio's model definition (central
0.40, north 0.30, south 0.20, east 0.10).

### `weather_source` — dimension: who/what produced the forecast

| col | type | notes |
|---|---|---|
| `source_id` | INTEGER PK | |
| `provider` | TEXT | `open_meteo_archive`, `open_meteo_forecast`, `open_meteo_previous_runs` |
| `model_id` | TEXT | `era5`, `best_match`, `ecmwf_ifs025`, `icon_seamless`, `gfs_seamless` |
| `lead_time_hours` | INTEGER | `0` = observation, `-1` = real-time, `24` = day-1 lead, `72` = day-3 lead |
| `UNIQUE(provider, model_id, lead_time_hours)` | |

Seeded with 10 rows covering ERA5 historical + realtime best_match +
{4 NWP models × 2 leads}.

### `weather_observation` — fact

| col | type | notes |
|---|---|---|
| `source_id` | INTEGER FK | |
| `location_id` | INTEGER FK | |
| `valid_at` | TIMESTAMP | the target hour the values describe (ISO-8601 UTC) |
| `forecast_run_time` | TIMESTAMP | when the NWP was initialized (NULL — Open-Meteo doesn't expose this) |
| `fetched_at` | TIMESTAMP | **when we pulled the API — the replay key** |
| 25+ weather variables | REAL | `shortwave_radiation_wm2`, `temperature_2m_c`, `cloud_cover_frac`, ... |
| `PRIMARY KEY (source_id, location_id, valid_at, fetched_at)` | |

Unit suffixes (`_wm2`, `_c`, `_frac`, `_hpa`, `_ms`, `_mm`, `_s`)
match able's existing `weather_data` convention. Ingestors convert
Open-Meteo's raw response (Celsius temp, 0–100% humidity/cloud) to
SI fractions where needed — see
[`src/fetch_weather_observation.py:_convert_value`](src/fetch_weather_observation.py).

### Indexes

```sql
CREATE INDEX idx_wx_replay
  ON weather_observation(location_id, valid_at, source_id, fetched_at DESC);
CREATE INDEX idx_wx_source_latest
  ON weather_observation(source_id, location_id, fetched_at DESC, valid_at);
```

`idx_wx_replay` is the workhorse for `weather_as_of(t)` queries.

## Data flows

### A. Hourly real-time ingest (HOT PATH — serves heliocast inference)

`docker/crontab` runs `scripts/update_weather_observation_hourly.py`
at `XX:30 UTC` every hour. Pulls the `/v1/forecast` endpoint for
**all 4 NWP models** (best_match, ecmwf_ifs025, icon_seamless,
gfs_seamless) × **5 BE locations** (centroid + 4 capacity zones).
Each pull writes ~360 rows per model × 4 models = ~1.4K rows.
Wall time < 10 s. Heliocast reads at `XX:45 UTC`, so the freshest
row is ~15 min old when inference uses it.

### B. 3×/day heavy ingest (Previous Runs archive for backtesting)

Same script as before (`update_weather_observation.py`), now focused
on Previous Runs API. Runs at 07:00, 13:30, 19:30 UTC — timed after
each NWP publishing window so the 24h/72h lead-time stored runs are
fresh. Fetches for all 4 NWP models × {day1, day3} × 5 locations,
past 7 days rolling.

Also runs the real-time forecast for all 4 models (same as Phase A)
so the 3×/day slots double-confirm the hourly ingest.

### C. Heliocast read (pull, not push)

Every `runner.py :45 UTC` run fetches the current NWP forecast via
`GET http://192.168.86.36:3001/api/weather/latest` with
`provider=open_meteo_forecast`, `models=ecmwf_ifs025,icon_seamless,
gfs_seamless`, `zones=central,north,south,east`. The client
(`heliocast/src/weather_db_client.py`) reconstructs heliocast's
original weather-frame shape (15-min interpolated, zone-aggregated,
best-GHI merged, `ghi_model_std` + per-NWP-model diagnostic columns)
so downstream feature engineering and the model are unchanged.

**Staleness check:** client rejects data older than
`HELIO_WEATHER_MAX_STALE_MIN` minutes (default 120). On rejection or
network error, the legacy direct-Open-Meteo path takes over unless
`HELIO_WEATHER_SOURCE=db-strict`.

### D. Legacy heliocast push (`POST /api/weather/snapshot`)

Still implemented (`heliocast/src/weather_db.py`, Phase 5) and kept
for back-compat. Now redundant because able's hourly cron covers the
same data; scheduled for removal once the read path has been running
stably in prod for a week. Unset `HELIO_WRITE_TOKEN` in heliocast's
`.env` to disable it already.

### E. One-shot helio-CSV backfill

`scripts/backfill_weather_observation.py` reads helio's
`weather_nwp_{model}_day{1,3}_zones_*.csv` files and inserts historical
rows with `fetched_at = file mtime`. Scope: 2024-01-01 onwards. First
run: ~403K rows in ~60 s. Subsequent runs are no-ops (PK conflict).

Limitation: helio's day3 zones files only contain the `south` zone due
to a pre-existing upstream bug — day1 backfill is complete (4 zones),
day3 backfill is south-only. Prod cron (A) fills in day3 for other
zones going forward.

### D. Daily read-replica sync

Outside the scope of this subsystem, but it's what makes the workstation
copy usable: `sync-db-from-prod.ps1` runs via Windows Task Scheduler at
07:00 local, `sqlite3.backup()`'s prod, `scp`'s to the workstation,
atomic-swaps in place. See able's top-level `WORKFLOWS.md` for details.

## Read patterns

Two helpers land on both sides (able-side in
[`src/weather_read.py`](src/weather_read.py), helio-side in
`helioforge/src/data/weather_db_loader.py`). Identical SQL on
purpose.

### Latest-known snapshot

```python
from src.weather_read import resolve_location, resolve_source, latest_weather

loc = resolve_location("BE", "central")
src = resolve_source("open_meteo_forecast", "best_match", -1)
df = latest_weather(loc, src,
                    "2026-04-21T00:00:00Z", "2026-04-21T23:59:59Z")
```

Returns 24 rows (one per hour) with the freshest `fetched_at` for each
`valid_at`. Good for "what do we think now about today?".

### Replay (the key use case)

```python
from src.weather_read import weather_as_of

# What did heliocast see at its 07:45 UTC run for day 2026-04-19?
df = weather_as_of(
    loc, src,
    valid_from="2026-04-19T22:00:00Z",  # Predico day-ahead window
    valid_to="2026-04-20T21:45:00Z",
    at="2026-04-19T07:45:00Z",
)
```

Returns the weather that was in the DB with `fetched_at <= '07:45 UTC'`.
If heliocast had pushed a snapshot at 07:45:02 UTC, that's the row
returned — byte-identical to what the model saw.

### Raw SQL (same pattern)

```sql
SELECT valid_at, shortwave_radiation_wm2, temperature_2m_c, cloud_cover_frac
FROM weather_observation
WHERE location_id = :loc
  AND source_id   = :src
  AND valid_at BETWEEN :from AND :to
  AND fetched_at <= :at              -- omit for latest
GROUP BY valid_at HAVING MAX(fetched_at)
ORDER BY valid_at;
```

## Ops runbook

### Deploy to prod (first time)

```bash
ssh clavain@192.168.86.36

# 1. Pull the 2 prod repos
cd /home/clavain/energy-dashboard/energy-data-gathering && git pull
cd ../energy-dashboard-frontend && git pull

# 2. Bring up data-gathering (new hourly + 3×/day crons)
cd ../energy-data-gathering
docker compose up -d --build data-gathering

# 3. Bootstrap the schema (idempotent — also runs on every cron tick)
docker compose exec data-gathering python scripts/init_weather_observation.py

# 4. Warm-start one hourly pull to verify
docker compose exec data-gathering \
  python scripts/update_weather_observation_hourly.py

# 5. Bring up the frontend with the new GET /weather/latest endpoint
cd ../energy-dashboard-frontend
docker compose up -d --build frontend

# 6. Sanity checks
curl http://localhost:3001/api/health
curl "http://localhost:3001/api/weather/latest?country_code=BE&zones=central&provider=open_meteo_forecast&models=best_match&lead_time_hours=-1&valid_from=$(date -u +%FT%TZ)&valid_to=$(date -u -d '+3 hours' +%FT%TZ)" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('rows:', sum(len(r['rows']) for r in d['data']['results']))"
```

Then on the workstation, add to `C:\Code\heliocast\.env`:

```
HELIO_WEATHER_DB_URL=http://192.168.86.36:3001
# HELIO_WEATHER_SOURCE=db         # default behaviour
# HELIO_WEATHER_MAX_STALE_MIN=120 # default
```

The next heliocast `:45 UTC` run reads weather from the DB. The old
push token (`HELIO_WRITE_TOKEN`) is **no longer needed** — heliocast
doesn't push anymore.

### Daily observations

```sql
-- Rows ingested per source in the last 24 h
SELECT s.provider || '/' || s.model_id || '/lead=' || s.lead_time_hours as src,
       COUNT(*) as rows,
       MIN(fetched_at) as first, MAX(fetched_at) as last
FROM weather_observation wo
JOIN weather_source s ON wo.source_id = s.source_id
WHERE fetched_at > datetime('now', '-1 day')
GROUP BY s.source_id ORDER BY s.source_id;
```

### Missed a cron run?

Each tick fetches the past 7 days (Previous Runs) / past 1 day
(real-time). Missing one tick → next tick re-fetches that window.
No manual remediation needed.

### Backfill went sideways

Re-runnable safely — PK conflict handling via `INSERT OR IGNORE`.
If you want to blow it away and restart, `DROP TABLE weather_observation`
on prod then re-run `init_weather_observation.py` + the backfill.

### Schema change

No Alembic — we `CREATE TABLE IF NOT EXISTS`. To add a column:

1. Edit `src/weather_schema.py` — append to `WEATHER_VARIABLE_COLUMNS`
   and update `OPENMETEO_TO_DB`.
2. Add an explicit `ALTER TABLE` to `create_weather_observation_tables()`
   in `src/db.py` — SQLite handles `ALTER TABLE ADD COLUMN` online.
3. Rebuild + redeploy: `docker compose up -d --build data-gathering`.
4. `create_weather_observation_tables()` runs at startup via any fetch
   script import, so the ALTER lands on first cron tick.

Keep `helioforge/src/data/weather_db_loader.py::WEATHER_VARIABLE_COLUMNS`
in sync (it's a deliberately duplicated short list — no import
dependency between repos).

## Files in this subsystem

| Repo | Path | Role |
|---|---|---|
| energy-data-gathering | `src/weather_schema.py` | DDL + seeds + column list + Open-Meteo name map |
| energy-data-gathering | `src/db.py::create_weather_observation_tables` | idempotent bootstrap |
| energy-data-gathering | `src/fetch_weather_observation.py` | Open-Meteo client (realtime + Previous Runs) |
| energy-data-gathering | `src/weather_read.py` | SQL helpers (latest / as-of) |
| energy-data-gathering | `scripts/init_weather_observation.py` | schema init + verify CLI |
| energy-data-gathering | `scripts/update_weather_observation.py` | cron entry point |
| energy-data-gathering | `scripts/backfill_weather_observation.py` | one-shot helio CSV ingest |
| energy-data-gathering | `docker/crontab` | 3 new entries (07:00/13:30/19:30 UTC) |
| energy-dashboard-frontend | `server/src/config/writeDatabase.ts` | writable SQLite handle |
| energy-dashboard-frontend | `server/src/middleware/writeAuth.ts` | bearer-token auth |
| energy-dashboard-frontend | `server/src/routes/weather.ts` | POST /api/weather/snapshot |
| energy-dashboard-frontend | `docker/docker-compose.yml` | DB mount `:rw`, HELIO_WRITE_TOKEN env |
| heliocast | `src/weather_db.py` | HTTP client |
| heliocast | `runner.py` | step 7b best-effort push |
| heliocast | `.env.example` | documents HELIO_WEATHER_DB_URL / HELIO_WRITE_TOKEN |
| helioforge | `src/data/weather_db_loader.py` | read-only loader mirroring weather_read.py |

## Out-of-scope / future work

- Migrating `weather_data` consumers (dashboards, `energy_forecast`
  XGBoost) onto `weather_observation`. Dual-write stays for now;
  consumer-by-consumer switch in its own plan.
- Extending to other countries (FR, DE, NL). Add rows to
  `weather_location` + update `fetch_weather_observation.py` to fetch
  them.
- Engine migration (SQLite → DuckDB / Timescale). Plan when row count
  approaches ~500M (likely: 10 countries × wind forecasting × a year).
- Capturing Open-Meteo's true NWP-model run timestamp. API doesn't
  expose this today; `forecast_run_time` column exists but stays NULL.
