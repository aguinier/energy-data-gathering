# Extending the versioned weather DB

Step-by-step recipes to add a country, a new NWP source, a new
variable, or a new downstream consumer. For the architecture big
picture read [`WEATHER_DB.md`](WEATHER_DB.md) first.

These recipes assume Belgium-first scope is already in place (as of
2026-04-22 on prod). If you're starting from a fresh DB, run
`scripts/init_weather_observation.py` once.

## Before you start (every time)

```bash
# 1. Make sure you're on the latest main.
cd /c/Code/able/energy-data-gathering && git pull

# 2. Verify the current schema + dim seeds.
python scripts/init_weather_observation.py --verify

# 3. Make a scratch copy for testing. NEVER work against
#    data/energy_dashboard.db directly — that's the live workstation
#    replica and is overwritten by the daily prod→local sync anyway.
cp ../data/energy_dashboard.db /tmp/scratch.db

# 4. Point your test invocations at the scratch DB.
export ENERGY_DB_PATH=/tmp/scratch.db
```

For prod changes: back up the live DB first.

```bash
ssh clavain@192.168.86.36 \
  "sqlite3 /home/clavain/energy-dashboard/data/energy_dashboard.db \
   .backup /home/clavain/energy-dashboard/data/backup_$(date +%F).db"
```

---

## Recipe 1 — Add a country (e.g. Germany)

**Decide the spatial granularity first.** Three options:

1. Centroid only (simplest, 1 location row). Good for dashboards /
   demand forecasting.
2. Centroid + N capacity-weighted zones (like BE has 4 zones).
   Required for solar/wind physical-generation models.
3. A per-bidding-zone split if you care about DE-LU vs DE-AT splits.

### 1a. Extend the dimension seed

Edit `src/weather_schema.py` and append rows to the `LOCATIONS` list.
The 8-tuple shape is:

```
(country_code, zone_id, zone_type, lat, lon, weight, capacity_mw, description)
```

`zone_type` values: `'centroid'` (single-point), `'solar'` (capacity-weighted PV),
`'wind_onshore'`, `'wind_offshore'`. Example for Germany:

```python
LOCATIONS = [
    # ... existing BE rows ...
    ("DE", "centroid", "centroid", 51.2, 10.45, 1.00, None, "Germany centroid"),
    # ... optional capacity-weighted zones (weights must sum to 1.0) ...
]
```

`src/db.py::create_weather_observation_tables()` already iterates
`LOCATIONS` — no change needed there.

### 1b. Teach the fetcher about the new country

Edit `src/fetch_weather_observation.py`:

1. `_get_be_locations()` — parameterize it to accept a `country_code`
   argument (or add a `_get_locations(country_code)` helper).
2. The top-level ingest orchestrators (`run_hourly_realtime_ingest`,
   `run_ingest`) should loop over countries:
   ```python
   for country in ("BE", "DE"):
       for model in REALTIME_NWP_MODELS:
           fetch_realtime_forecast(model_id=model, country=country, ...)
   ```

### 1c. Test on scratch

```bash
ENERGY_DB_PATH=/tmp/scratch.db python scripts/init_weather_observation.py
ENERGY_DB_PATH=/tmp/scratch.db python scripts/update_weather_observation_hourly.py
# Verify observation counts:
sqlite3 /tmp/scratch.db "SELECT country_code, COUNT(*) FROM weather_observation wo
  JOIN weather_location l ON wo.location_id = l.location_id
  WHERE fetched_at > datetime('now', '-1 hour') GROUP BY country_code;"
```

Expected: one row per country, non-zero counts.

```bash
# Run coherence check on scratch:
python scripts/check_weather_coherence.py --db /tmp/scratch.db
```

Expected: `PASS: sources`, `PASS: locations`, `PASS: columns`, exit 0.

### 1d. Deploy

See [`weather-db-deploy`](#deploying-changes-to-prod) below or invoke
the `weather-db-deploy` skill.

---

## Recipe 2 — Add a new NWP source

"Source" = a `(provider, model_id, lead_time_hours)` triple. Typical
examples:

- New Open-Meteo real-time model: `(open_meteo_forecast, metno_nordic, -1)`
- New Previous Runs archive: `(open_meteo_previous_runs, knmi_harmonie, 24)`
- Different provider entirely: `(meteomatics, basic, -1)` — would
  also need a new fetcher module.

### 2a. Append to the source dimension

Edit `src/weather_schema.py::OPEN_METEO_SOURCES`:

```python
OPEN_METEO_SOURCES = [
    # ... existing 13 rows ...
    ("open_meteo_forecast", "metno_nordic", -1, "Real-time Met Norway Nordic"),
]
```

### 2b. Wire the fetcher

- **Real-time** (lead=-1): add the model_id to
  `src/fetch_weather_observation.py::REALTIME_NWP_MODELS`.
- **Previous Runs** (lead=24 or 72): add to `NWP_MODELS` + `LEAD_TIMES_HOURS`
  if a new lead time.
- **New provider entirely**: write a new
  `fetch_<provider>_*.py` module following the same pattern as
  `fetch_weather_observation.py` (including retry, rate-limit,
  free-tier fallback).

### 2c. Test + deploy

Same as Recipe 1c / 1d. The seed is idempotent — safe to re-run.

---

## Recipe 3 — Add a new variable (e.g. `soil_moisture_frac`)

### 3a. Schema + mapping

Edit `src/weather_schema.py`:

```python
WEATHER_VARIABLE_COLUMNS = [
    # ... existing ~28 columns ...
    "soil_moisture_0_to_10cm_frac",  # new
]

OPENMETEO_TO_DB = {
    # ... existing ~28 entries ...
    "soil_moisture_0_to_10cm": "soil_moisture_0_to_10cm_frac",
}
```

If the variable needs unit conversion, add a branch in
`src/fetch_weather_observation.py::_convert_value()`. Soil moisture
comes in fraction already — no conversion needed.

Include it in fetches: add to
`OPENMETEO_VARIABLES_FORECAST` (for real-time) and/or
`OPENMETEO_VARIABLES_PREVIOUS_RUNS` (for Previous Runs archive).

### 3b. Online ALTER on the existing table

`CREATE TABLE IF NOT EXISTS` in
`create_weather_observation_tables()` won't add a column to a
pre-existing table — it silently does nothing. You must run a one-
time `ALTER TABLE` against the live DB **before** redeploying the
container:

```bash
ssh clavain@192.168.86.36 \
  "sqlite3 /home/clavain/energy-dashboard/data/energy_dashboard.db \
   'ALTER TABLE weather_observation ADD COLUMN soil_moisture_0_to_10cm_frac REAL;'"
```

SQLite's `ALTER TABLE ADD COLUMN` is O(1) — no table rewrite, no
downtime.

### 3c. Test + deploy

Confirm on scratch (include the ALTER step) before prod.

### Variable removal is harder

SQLite before 3.35 didn't support `DROP COLUMN` at all. To remove a
column you need the copy-table dance:

```sql
BEGIN;
CREATE TABLE weather_observation_new (... columns except the one ...);
INSERT INTO weather_observation_new SELECT ... FROM weather_observation;
DROP TABLE weather_observation;
ALTER TABLE weather_observation_new RENAME TO weather_observation;
-- recreate indexes
COMMIT;
```

Don't do this on a 3 GB+ DB without a **verified backup** and a
maintenance window. Easier: leave the column, stop writing to it,
document it as deprecated.

---

## Recipe 4 — Add a new consumer application

Example: a wind-power forecaster that needs `wind_speed_100m_ms` for
Belgium + Germany.

### 4a. Pick the read path

| Access pattern | Use |
|---|---|
| Real-time inference (≤ 15 min staleness) | `GET http://192.168.86.36:3001/api/weather/latest` |
| Historical backtest / training | Workstation replica `C:\Code\able\data\energy_dashboard.db` via `src/weather_read.py` helpers |
| Replay ("what did forecast look like at time T?") | `weather_as_of(loc, src, valid_from, valid_to, at=T)` |

### 4b. Which sources?

| Use case | Sources |
|---|---|
| Live inference | `(open_meteo_forecast, best_match, -1)` + the 3 NWP models for disagreement |
| Training — retrospective realistic | `(open_meteo_previous_runs, {ecmwf,icon,gfs,best_match}, 24 or 72)` |
| Training — ERA5 truth | `(open_meteo_archive, era5, 0)` — ingest separately if not yet populated |

### 4c. Copy the heliocast reader pattern

`heliocast/src/weather_db_client.py` is the reference
implementation: HTTP GET, unit-reverse translation, per-zone
interpolation, best-GHI merge, per-model diagnostic columns. Fork
its top for a new consumer.

For a purely research/backtest consumer use
`helioforge/src/data/weather_db_loader.py` — direct SQLite read,
no HTTP.

---

## Deploying changes to prod

Only after tests pass on scratch:

```bash
# 1. Commit + push
cd /c/Code/able/energy-data-gathering
git add -p
git commit -m "feat: ..."
git push origin main

# 2. SSH + deploy
ssh clavain@192.168.86.36 \
  "cd /home/clavain/energy-dashboard && ./deploy.sh energy-data-gathering"

# 3. If you did an ALTER TABLE, run it on prod NOW before the next
#    cron tick (otherwise the fetcher tries to INSERT a column that
#    doesn't exist and fails):
ssh clavain@192.168.86.36 \
  "sqlite3 /home/clavain/energy-dashboard/data/energy_dashboard.db \
   'ALTER TABLE weather_observation ADD COLUMN <your_new_col> REAL;'"

# 4. Verify schema + seeds still consistent
ssh clavain@192.168.86.36 \
  "docker compose -f /home/clavain/energy-dashboard/repos/energy-data-gathering/docker/docker-compose.yml \
   exec data-gathering python scripts/init_weather_observation.py --verify"

# 5. Warm-start ingest
ssh clavain@192.168.86.36 \
  "docker compose -f /home/clavain/energy-dashboard/repos/energy-data-gathering/docker/docker-compose.yml \
   exec data-gathering python scripts/update_weather_observation_hourly.py"

# 6. Curl the read endpoint to confirm new data flows
curl "http://192.168.86.36:3001/api/weather/latest?country_code=BE&zones=central&provider=open_meteo_forecast&models=best_match&lead_time_hours=-1&valid_from=$(date -u +%FT%TZ)&valid_to=$(date -u -d '+3 hours' +%FT%TZ)"
```

See the `weather-db-deploy` Claude Code skill for an LLM-enforced
version of this sequence.

---

## What NEVER to do

The DB has **3 live consumers** (dashboards, heliocast production
Predico submission, helio research). A broken schema breaks all
three. Never:

1. `DROP TABLE weather_observation` or
   `DELETE FROM weather_observation` without a verified backup.
2. `ALTER TABLE` to rename or drop a column without the copy-table
   dance (SQLite doesn't support it cleanly pre-3.35).
3. Hand-edit the prod DB file by copying over it — always go through
   `sqlite3` with `BEGIN/COMMIT` or the ingest scripts.
4. Skip the scratch-DB dry run. Every code change must run through
   `ENERGY_DB_PATH=/tmp/scratch.db python scripts/...` first.
5. Deploy a schema change without running
   `init_weather_observation.py --verify` on prod after, and
   without curling `/api/weather/latest` to confirm the frontend
   is still serving.
6. Seed a new `weather_source` row without also adding the
   matching model to the fetcher's `REALTIME_NWP_MODELS` /
   `NWP_MODELS` tuple — seeding a dangling source leaves writes
   against it impossible and reads empty, confusing every downstream
   consumer.
7. `git push --force` on main. `deploy.sh` does `git pull origin main`
   and a force-push that rewrites main breaks pulls across all
   contributors' machines.
8. Run a multi-hour backfill during the :30 UTC cron window — the
   hourly cron fires on the hour and competing writes on a 3 GB
   SQLite file can stall both.
