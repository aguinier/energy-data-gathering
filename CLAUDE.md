# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

Energy Dashboard database containing energy market and weather data for 39 European countries.

**Database Stats:**
- **Type:** SQLite 3
- **Size:** ~507 MB
- **Records:** 1,946,857
- **Last Updated:** 2025-12-23

**Quick Stats:**
```
Countries: 39 European nations
├─ Complete data (4 types): 23 countries (59%)
├─ Partial data (1-3 types): 13 countries (33%)
└─ No data: 3 countries (8%)

Data Types:
├─ Energy Load:         279,880 records (36 countries)
├─ Energy Price:        928,533 records (28 countries)
├─ Renewable Energy:     90,636 records (29 countries)
├─ Load Forecasts:          672 records (1 country - growing)
├─ Weather Data:        741,288 records (28 countries)
└─ Weather Forecasts:     9,408 records (28 countries, 14-day horizon)
```

**Key Files:**
- `energy_dashboard.db` - Main SQLite database (~507 MB)
- `database_structure.md` - Complete schema, indexes, views, query patterns
- `database_completeness.md` - Country-by-country quality analysis, data gaps, priorities
- `PIPELINE.md` - **Complete pipeline documentation** (architecture, data flow, operations guide)
- `.env` - Environment configuration (contains sensitive data)

**Related Module:**
- `../energy_forecast/` - D+2 forecasting module using XGBoost (see `energy_forecast/CLAUDE.md`)

## Data Types

### 1. Energy Load (Demand)
Electricity consumption/demand in megawatts (MW).
- **Granularity:** Hourly
- **Coverage:** 36/39 countries
- **Date Range:** 2019-12-31 to 2025-11-26
- **Quality:** ⚠️ Variable (some countries <1 week data)

### 2. Energy Price
Electricity market prices in EUR/MWh.
- **Granularity:** Hourly
- **Coverage:** 28/39 countries
- **Date Range:** 2021-01-01 to 2025-11-28
- **Quality:** ✓ Good (4+ years for most countries)

### 3. Renewable Energy Generation
Breakdown by source type (solar, wind, hydro, biomass, etc.).
- **Granularity:** Varies (15-min to hourly)
- **Coverage:** 29/39 countries
- **Date Range:** 2021-12-31 to 2025-11-25
- **Sources:** Solar, Wind (onshore/offshore), Hydro, Biomass, Geothermal
- **Quality:** ⚠️ Moderate (some outdated entries)

### 4. Load Forecasts (ENTSO-E TSO Forecasts)
Official transmission system operator (TSO) load forecasts from ENTSO-E.
- **Granularity:** 15-minute intervals
- **Coverage:** Growing (currently 1 country, expandable to all ENTSO-E countries)
- **Date Range:** 2024-12-20 onwards (backfill to 2019-01-01 possible)
- **Forecast Types:**
  - **Day-Ahead (D+1):** Next day forecasts, published daily
  - **Week-Ahead (D+7):** 7-day ahead forecasts, published weekly
- **Quality:** ✓ Excellent (official TSO forecasts, 2-4% error typical)
- **Source:** ENTSO-E Transparency Platform
- **Table:** `energy_load_forecast`

### 5. Weather Data (Historical)
30 meteorological variables including temperature, wind, solar radiation, precipitation.
- **Granularity:** Hourly
- **Coverage:** 28 regions (26 countries + DK1/DK2 regional splits)
- **Date Range:** 2023-01-01 to present
- **Records:** 741,288 (~26,474 per region)
- **Quality:** ✓ Excellent (100% hourly coverage, no gaps)
- **Source:** Open-Meteo ERA5 reanalysis
- **Table:** `weather_data` with `data_quality='actual'`

### 6. Weather Forecasts
Up to 16-day weather forecasts for energy demand/price prediction.
- **Granularity:** Hourly
- **Coverage:** 28 regions (same as historical weather)
- **Forecast Horizon:** Up to 16 days ahead (default: 14 days)
- **Records:** 9,408 (336 per country × 28 countries)
- **History:** Multiple forecast vintages preserved for accuracy analysis
- **Source:** Open-Meteo Forecast API (GFS/ECMWF best_match model)
- **Table:** `weather_data` with `data_quality='forecast'`

**Key Fields:**
- `timestamp_utc`: Target time the forecast is FOR
- `forecast_run_time`: When the forecast was generated (6-hour model runs: 00, 06, 12, 18 UTC)
- `model_name`: 'best_match' (auto-selects optimal model per location)
- `data_quality`: 'forecast' (distinguishes from 'actual' historical data)

**Forecast Variables (same as historical):**
- Temperature, humidity, pressure
- Wind speed/direction at multiple heights (10m, 80m, 100m, 120m)
- Precipitation (rain, snow)
- Solar radiation (shortwave, direct, diffuse, DNI)

## Countries Covered

**Western Europe:** AT, BE, CH, DE, FR, LU, NL
**Northern Europe:** DK, EE, FI, IE, IS, LT, LV, NO, SE
**Southern Europe:** AL, BA, CY, ES, GR, HR, IT, ME, MK, MT, PT, RS, SI
**Eastern Europe:** BG, CZ, HU, MD, PL, RO, SK, TR, UA
**United Kingdom:** GB

**Countries with NO data:** IS (Iceland), MT (Malta), TR (Turkey)

## Database Architecture

### Star Schema Design
The database follows a star schema with `countries` as the central dimension table:
- **Dimension Table:** `countries` (39 European countries)
- **Fact Tables:** `energy_load`, `energy_price`, `energy_renewable`, `energy_load_forecast`, `weather_data`, `weather_point_data`
- **Operational Tables:** `data_ingestion_log`, `database_metadata`, `completeness_cache`
- **Views:** `energy_dashboard_data` (unified metrics), `latest_data_by_country` (freshness check)

All relationships use `country_code` as the linking field. The database uses logical relationships (enforced at application level) rather than physical foreign key constraints.

### Time-Series Data Patterns
All fact tables follow consistent patterns:
- Timestamps stored in UTC (`timestamp_utc` column)
- Composite indexes on `(country_code, timestamp_utc)` prevent duplicates
- `data_quality` field distinguishes actual vs forecast data
- `created_at` timestamp for record creation tracking

## ENTSO-E Data Gathering Pipeline

> **📖 For complete pipeline documentation, see [PIPELINE.md](./PIPELINE.md)**
>
> PIPELINE.md contains detailed information about:
> - Architecture diagrams and component details
> - Complete data flow with step-by-step execution
> - API integration specifics and error handling
> - Operations guide and troubleshooting
> - Configuration reference and best practices

### Overview
Automated pipeline for gathering energy data (load, price, renewable) from the ENTSO-E Transparency Platform API.

**Pipeline Features:**
- **Backfill Mode:** Fetch historical data for configurable date ranges
- **Update Mode:** Hourly updates fetching last 7 days (captures delayed uploads & revisions)
- **Coverage:** All 39 countries with ENTSO-E domain codes
- **Error Handling:** Retry with exponential backoff, per-country error isolation
- **Logging:** Comprehensive logging to `data_ingestion_log` table and log files

### Installation

```bash
# Install Python dependencies
pip install -r requirements.txt

# Verify configuration (checks API key, database, etc.)
python config.py
```

### Pipeline Commands

**Backfill historical data:**
```bash
# Backfill all data types for all countries from 2024 (includes load forecasts)
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types all --countries all

# Backfill only load data for Germany
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types load --countries DE

# Backfill ENTSO-E load forecasts (day-ahead and week-ahead)
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 \
  --types load_forecast_day_ahead,load_forecast_week_ahead --countries DE,FR,BE

# Backfill only day-ahead forecasts for all countries
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 \
  --types load_forecast_day_ahead --countries all

# Backfill high-priority countries only
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types all --priority 1

# Use default backfill periods from config.py (from 2019 for load & forecasts)
python scripts/backfill.py --use-defaults --types all --countries all
```

**Regular updates (for cron):**
```bash
# Standard update (last 7 days, all data types including forecasts)
python scripts/update.py

# Update last 14 days
python scripts/update.py --days 14

# Update only load and price data
python scripts/update.py --types load,price

# Update only load forecasts
python scripts/update.py --types load_forecast_day_ahead,load_forecast_week_ahead

# Update specific countries
python scripts/update.py --countries DE,FR,IT
```

**Setup hourly cron job:**
```bash
# Interactive setup for hourly updates
bash scripts/scheduler_setup.sh

# Manual cron entry (runs at minute 15 every hour)
# 15 * * * * cd /path/to/data_gathering && python3 scripts/update.py >> logs/cron_update.log 2>&1
```

### Pipeline Architecture

```
ENTSO-E API → ENTSOEClient → Data Fetchers → Database
                    ↓              ↓              ↓
              Rate Limiting   Transformers   Upsert Logic
              Retry Logic     Validators     Logging
```

**Components:**
- `config.py` - API configuration, ENTSO-E endpoints, PSR type mappings
- `utils.py` - Date handling, logging, validation
- `src/entsoe_client.py` - API wrapper with rate limiting & retry
- `src/fetch_*.py` - Data fetchers for load/price/renewable
- `src/db.py` - Database operations, upsert, logging
- `src/pipeline.py` - Main orchestrator

### Configuration

**API Key:** Set in `.env` file:
```
api_key_entsoe=your_api_key_here
```

**Backfill Defaults** (in `config.py`):
- Load: 2019-01-01 (5 years)
- Price: 2021-01-01 (4 years)
- Renewable: 2021-01-01 (4 years)
- Load Forecast (Day-Ahead): 2019-01-01 (matches load data)
- Load Forecast (Week-Ahead): 2019-01-01 (matches load data)

**Update Settings:**
- Days back: 7 (captures delayed uploads)
- Rate limit: 300 requests/minute (safe buffer)
- Max retries: 3 with exponential backoff

### Pipeline Behavior

**Idempotency:**
- Uses `INSERT OR REPLACE` for safe re-runs
- Unique indexes prevent duplicates
- Can safely re-run backfill without checking existing data

**Error Handling:**
- Failed country doesn't stop pipeline
- Errors logged to `data_ingestion_log` table
- Continues with remaining countries
- Retry 3 times with delays: 1s, 2s, 4s

**Date Chunking:**
- Large date ranges split into 365-day chunks
- Prevents ENTSO-E API timeout errors
- Allows resume after interruption

**Known Issues:**
- Countries IS, MT, TR have no ENTSO-E data (will be skipped)
- Some countries have delayed data publication (7-day update window handles this)

### Monitoring & Logs

**Log Files:**
- `logs/pipeline.log` - All pipeline activity
- `logs/cron_update.log` - Cron job output

**Database Logging:**
```sql
-- Check recent pipeline runs
SELECT pipeline_type, country_code, status,
       records_inserted, start_time, end_time
FROM data_ingestion_log
ORDER BY start_time DESC
LIMIT 20;

-- Check for failures
SELECT * FROM data_ingestion_log
WHERE status = 'failed'
ORDER BY start_time DESC;
```

**Data Freshness:**
```sql
-- Check latest data timestamps
SELECT * FROM latest_data_by_country
WHERE country_code = 'DE';
```

### Maintenance Tasks

**After backfill:**
```bash
# Update query statistics
sqlite3 energy_dashboard.db "ANALYZE;"

# Optimize database (if needed)
sqlite3 energy_dashboard.db "VACUUM;"
```

**Regular monitoring:**
- Check `data_ingestion_log` for failures
- Monitor log files for errors
- Verify `completeness_cache` is updated

## Weather Data Pipeline

### Overview
Automated pipeline for gathering historical weather data and forecasts from Open-Meteo API.

**Pipeline Features:**
- **Historical Mode:** Fetch ERA5 reanalysis data (last 7 days by default)
- **Forecast Mode:** Fetch up to 16-day weather forecasts
- **History Preservation:** Multiple forecast vintages stored for accuracy analysis
- **Coverage:** 28 regions with country centroid coordinates

### Weather Update Commands

**Update historical weather data:**
```bash
# Standard update (last 7 days, all countries)
python scripts/update_weather.py

# Update last 14 days
python scripts/update_weather.py --days 14

# Update specific countries
python scripts/update_weather.py --countries DE,FR,IT
```

**Fetch weather forecasts:**
```bash
# Historical + 14-day forecasts (recommended for daily updates)
python scripts/update_weather.py --forecast

# Forecasts only (skip historical)
python scripts/update_weather.py --forecast-only

# Custom forecast horizon (7 days)
python scripts/update_weather.py --forecast --forecast-days 7

# Forecasts for specific countries
python scripts/update_weather.py --forecast-only --countries DE,FR,IT
```

**Windows batch file (includes forecasts):**
```bash
# Double-click or run from command line
update_weather.bat
```

### Weather Data Architecture

```
Open-Meteo API
├─ Archive API (ERA5) ──→ Historical Data ──→ weather_data (data_quality='actual')
└─ Forecast API ────────→ Forecast Data ───→ weather_data (data_quality='forecast')
```

**Data Storage Strategy:**
- Historical and forecast data stored in same `weather_data` table
- Distinguished by `data_quality` field ('actual' vs 'forecast')
- Unique index: `(country_code, timestamp_utc, model_name, forecast_run_time)`
- Forecast history preserved: different `forecast_run_time` = separate records

**Forecast Run Times:**
- Rounded to 6-hour model runs (00, 06, 12, 18 UTC)
- Running script multiple times in same 6-hour window updates existing records
- Running in different 6-hour window creates new forecast vintage

### Weather Forecast Queries

**Get latest forecast for a country:**
```sql
SELECT timestamp_utc, temperature_2m_k, wind_speed_100m_ms
FROM weather_data
WHERE country_code = 'DE'
  AND data_quality = 'forecast'
  AND forecast_run_time = (
      SELECT MAX(forecast_run_time) FROM weather_data
      WHERE country_code = 'DE' AND data_quality = 'forecast'
  )
  AND timestamp_utc > datetime('now')
ORDER BY timestamp_utc;
```

**List all forecast vintages:**
```sql
SELECT
    country_code,
    forecast_run_time,
    COUNT(*) as records,
    MIN(timestamp_utc) as min_target,
    MAX(timestamp_utc) as max_target
FROM weather_data
WHERE data_quality = 'forecast'
GROUP BY country_code, forecast_run_time
ORDER BY country_code, forecast_run_time DESC;
```

**Compare forecast accuracy (forecast vs actual):**
```sql
SELECT
    a.timestamp_utc,
    a.temperature_2m_k AS actual_temp,
    f.temperature_2m_k AS forecast_temp,
    ROUND(ABS(a.temperature_2m_k - f.temperature_2m_k), 2) AS temp_error_k
FROM weather_data a
JOIN weather_data f ON
    a.country_code = f.country_code
    AND a.timestamp_utc = f.timestamp_utc
WHERE a.data_quality = 'actual'
  AND f.data_quality = 'forecast'
  AND a.country_code = 'DE'
  AND a.timestamp_utc >= datetime('now', '-3 days')
ORDER BY a.timestamp_utc;
```

### Migration (One-Time)

If upgrading from a version without forecast support:
```bash
# Preview migration changes
python scripts/migrate_weather_index.py --dry-run

# Run migration (updates index, sets forecast_run_time for existing records)
python scripts/migrate_weather_index.py
```

## Versioned Weather DB (weather_observation — 2026-04-22)

Since 2026-04-22 this repo owns a versioned per-NWP-model per-zone
weather table, `weather_observation`, alongside the legacy
`weather_data` table (which continues to serve dashboards). It is
populated by **one hourly Docker cron** at `XX:30 UTC` — the only
Open-Meteo fetcher in the whole stack. Every downstream consumer
(dashboards, heliocast production inference, helio research
backtests) reads from this table, not from Open-Meteo directly.

**Architecture + deploy runbook:** [`WEATHER_DB.md`](WEATHER_DB.md)
**Step-by-step extension recipes:** [`EXTENDING.md`](EXTENDING.md)

### Current consumers (all break if the schema breaks)

- **Dashboards** (`energy-dashboard-frontend` on `:3001`) — serves
  `GET /api/weather/latest` to anything on the LAN.
- **Heliocast production** — hourly `:45 UTC` Windows Task reads
  the freshest forecast from `/api/weather/latest` and submits to
  Predico-Elia.
- **Helio research** — reads the workstation replica at
  `C:\Code\able\data\energy_dashboard.db` for backtests via
  `helioforge/src/data/weather_db_loader.py`.

### Extension rules (always-on, applies to any code touching this table)

1. **Never DROP `weather_observation` or its dims.** Always back up
   first:
   ```bash
   sqlite3 /home/clavain/energy-dashboard/data/energy_dashboard.db \
     ".backup /home/clavain/energy-dashboard/data/backup_$(date +%F).db"
   ```
2. **Schema changes via `ALTER TABLE ... ADD COLUMN`, never DROP-and-
   recreate.** SQLite's online ALTER handles add-column in O(1).
   Column removal requires the copy-table dance and a maintenance
   window — see `EXTENDING.md` "What NEVER to do".
3. **Always test on a scratch copy first.** Every single change
   must go through
   ```bash
   cp data/energy_dashboard.db /tmp/scratch.db
   ENERGY_DB_PATH=/tmp/scratch.db python scripts/<...>.py
   ```
   before it lands on prod.
4. **Verify after every deploy.** Always run
   ```bash
   docker compose exec data-gathering \
     python scripts/init_weather_observation.py --verify
   ```
   plus a `curl /api/weather/latest?...` smoke to confirm the
   frontend is still serving.
5. **Wire fetchers when adding a source.** If you append to
   `weather_schema.py::OPEN_METEO_SOURCES`, also add the model to
   the matching fetcher tuple (`REALTIME_NWP_MODELS` or
   `NWP_MODELS`). A seeded-but-unwired source is a silent dead end
   that produces no data.

### Workflows (Claude Code skills)

If you're an LLM driving this repo, three skills enforce the above
rules on specific workflows. They auto-invoke on intent match:

| Skill | Triggers on |
|---|---|
| `weather-db-extend` | "add a country / source / variable to the weather DB" |
| `weather-db-query` | "query the weather DB", "how do I read weather for X" |
| `weather-db-deploy` | "deploy weather DB change to prod", "ship weather DB" |

## Common Database Operations

### Connecting to the Database

```python
import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('energy_dashboard.db')

# Query example
df = pd.read_sql_query("""
    SELECT * FROM energy_dashboard_data
    WHERE country_code = 'DE'
    LIMIT 100
""", conn)

conn.close()
```

### Essential Queries

**Check data availability:**
```sql
SELECT * FROM latest_data_by_country
WHERE country_code = 'DE';
```

**Get time-series data:**
```sql
SELECT timestamp_utc, load_mw, price_eur_mwh
FROM energy_dashboard_data
WHERE country_code = 'FR'
  AND timestamp_utc >= datetime('now', '-30 days')
ORDER BY timestamp_utc;
```

**Analyze renewable percentage:**
```sql
SELECT country_name, AVG(renewable_percentage) as avg_pct
FROM energy_dashboard_data
WHERE timestamp_utc >= datetime('now', '-30 days')
GROUP BY country_code, country_name
ORDER BY avg_pct DESC;
```

**Check forecast coverage:**
```sql
SELECT
    forecast_type,
    COUNT(*) as records,
    MIN(target_timestamp_utc) as earliest,
    MAX(target_timestamp_utc) as latest,
    COUNT(DISTINCT country_code) as countries
FROM energy_load_forecast
GROUP BY forecast_type;
```

**Compare forecast vs actual load:**
```sql
SELECT
    a.country_code,
    a.timestamp_utc,
    a.load_mw as actual_load,
    f.forecast_value_mw as day_ahead_forecast,
    ROUND(ABS(a.load_mw - f.forecast_value_mw), 2) as absolute_error_mw,
    ROUND(ABS(a.load_mw - f.forecast_value_mw) / a.load_mw * 100, 2) as error_pct
FROM energy_load a
JOIN energy_load_forecast f
    ON a.country_code = f.country_code
    AND a.timestamp_utc = f.target_timestamp_utc
    AND f.forecast_type = 'day_ahead'
WHERE a.country_code = 'DE'
    AND a.timestamp_utc >= '2024-12-01'
ORDER BY a.timestamp_utc
LIMIT 20;
```

**Calculate forecast accuracy by country:**
```sql
SELECT
    a.country_code,
    COUNT(*) as samples,
    ROUND(AVG(ABS(a.load_mw - f.forecast_value_mw)), 2) as avg_error_mw,
    ROUND(AVG(ABS(a.load_mw - f.forecast_value_mw) / a.load_mw * 100), 2) as avg_error_pct
FROM energy_load a
JOIN energy_load_forecast f
    ON a.country_code = f.country_code
    AND a.timestamp_utc = f.target_timestamp_utc
    AND f.forecast_type = 'day_ahead'
WHERE a.timestamp_utc >= datetime('now', '-7 days')
GROUP BY a.country_code
ORDER BY avg_error_pct;
```

### Database Maintenance Commands

**List all tables:**
```bash
sqlite3 energy_dashboard.db ".tables"
```

**Check database size:**
```bash
sqlite3 energy_dashboard.db "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size();"
```

**Optimize database:**
```bash
sqlite3 energy_dashboard.db "VACUUM;"
sqlite3 energy_dashboard.db "ANALYZE;"
```

**Export table to CSV:**
```bash
sqlite3 energy_dashboard.db ".headers on" ".mode csv" ".output data.csv" "SELECT * FROM countries;" ".quit"
```

## Data Quality Guidelines

### Critical Data Quality Issues (as of 2025-12-23)

1. **Short Data Spans:** 6 countries have <1 week of load data (MD, MK, BA, CY, RS, ME)
2. **Outdated Data:** GB and UA have data from 2019-2022 only
3. **Missing Countries:** IS, MT, TR have no data at all

See `database_completeness.md` for detailed analysis.

### Data Validation Rules

When importing or modifying data:
1. **No duplicate timestamps per country** - Unique indexes enforce this
2. **UTC timestamps only** - All timestamps must be in UTC
3. **Valid country codes** - Must exist in `countries` table
4. **No negative energy values** - Energy metrics should be >= 0
5. **Renewable totals must match sum of components** - Validate calculations

### Completeness Cache

The `completeness_cache` table stores pre-computed data quality metrics:
- **Last Updated:** 2025-11-22 (may be outdated)
- **Recommendation:** Recompute after any bulk data operations
- Provides fast lookups for data coverage without scanning fact tables

## Countries with Complete Data (all 4 types)

23 countries: AT, BE, BG, CH, CZ, DE, EE, ES, FI, FR, GR, HR, HU, IT, LT, LV, NL, NO, PL, PT, RO, SE, SI, SK

## Database Schema

### Tables Overview

**Main Tables:**
- `countries` - 39 European countries (dimension table)
- `energy_load` - Electricity demand (279,880 records)
- `energy_price` - Market prices (928,533 records)
- `energy_renewable` - Renewable generation (90,636 records)
- `energy_load_forecast` - ENTSO-E TSO load forecasts (672 records - growing)
- `weather_data` - Meteorological data (723,864 records)
- `weather_point_data` - Point-specific weather with lat/lon

**Helper Views:**
- `energy_dashboard_data` - Combined energy metrics with renewable percentage calculations
- `latest_data_by_country` - Most recent data timestamps per country

**Operational:**
- `data_ingestion_log` - ETL pipeline audit trail
- `completeness_cache` - Pre-computed data quality metrics
- `database_metadata` - System configuration

### Key Schema Details

**countries** - Central dimension table
- Primary key: `country_code` (TEXT)
- Flags: `has_load_data`, `has_price_data`, `has_renewable_data`, `has_weather_data`
- `priority` field: 1=high, 2=medium, 3=low

**energy_renewable** - Renewable source breakdown
- Columns: `solar_mw`, `wind_onshore_mw`, `wind_offshore_mw`, `hydro_run_mw`, `hydro_reservoir_mw`, `biomass_mw`, `geothermal_mw`, `other_renewable_mw`
- `total_renewable_mw` calculated from sum of components
- Unique index on `(country_code, timestamp_utc)`

**energy_load_forecast** - ENTSO-E TSO load forecasts
- Columns: `country_code`, `target_timestamp_utc`, `forecast_value_mw`, `forecast_type`, `forecast_run_time`, `horizon_hours`, `data_quality`, `created_at`, `publication_timestamp_utc`
- `forecast_type`: 'day_ahead' or 'week_ahead'
- `target_timestamp_utc`: The time the forecast is FOR (enables joining with actuals)
- `forecast_run_time` and `horizon_hours`: Currently NULL (ENTSO-E API limitation)
- `publication_timestamp_utc`: When ENTSO-E published this forecast
- `data_quality`: Always 'forecast'
- Unique index on `(country_code, target_timestamp_utc, forecast_type)`

**weather_data** - 30 meteorological variables (historical + forecasts)
- **Core Meteorological (4):** temperature_2m_k, dew_point_2m_k, relative_humidity_2m_frac, pressure_msl_hpa
- **Wind (7):** wind speeds/directions at 10m, 80m, 100m, 120m heights
- **Precipitation (3):** precip_mm, rain_mm, snowfall_mm
- **Solar Radiation (12):** shortwave, direct, diffuse, DNI, GHI, clear-sky models, pv_poa_wm2, pv_cell_temp_c
- **Metadata:** `forecast_run_time`, `model_name`, `data_quality`
- **Data Quality Values:**
  - `'actual'` + `model_name='era5'` → Historical observations
  - `'forecast'` + `model_name='best_match'` → Weather forecasts
- Unique index on `(country_code, timestamp_utc, model_name, forecast_run_time)`

### Index Strategy

All fact tables use composite indexes for:
1. **Uniqueness:** Prevent duplicate time-series entries via `(country_code, timestamp_utc)`
2. **Performance:** Fast filtering by country and time range
3. **ML workloads:** `(country_code, timestamp_utc, data_quality)` indexes

### Publication Timestamps

All ENTSO-E data tables include `publication_timestamp_utc` to track when ENTSO-E published or last updated the data:

**Coverage:**
- `energy_load`: 99.4% coverage
- `energy_price`: 89.67% coverage
- `energy_renewable`: 100% coverage
- `energy_load_forecast`: 100% coverage

**What it represents:**
The `createdDateTime` from ENTSO-E's XML responses - indicates when ENTSO-E's system last generated/refreshed the data document.

**Use cases:**
- Understanding data revision patterns
- Analyzing publication delays
- Tracking when ENTSO-E updates historical data
- Identifying data freshness

**Note:** For backfilled historical data, this timestamp reflects when the data was last updated in ENTSO-E's systems, not the original publication date.

**Backfilling:** Use `scripts/backfill_publication_timestamps.py` to populate missing timestamps for existing data.

## Recent Changes (2026-01-11)

**Weather Forecast Integration:**
- Added weather forecast fetching from Open-Meteo Forecast API
- Forecasts stored in same `weather_data` table with `data_quality='forecast'`
- Support for up to 16-day forecast horizon (default: 14 days)
- Forecast history preservation: multiple vintages stored for accuracy analysis
- New unique index: `(country_code, timestamp_utc, model_name, forecast_run_time)`
- New CLI flags: `--forecast`, `--forecast-only`, `--forecast-days`
- Updated `update_weather.bat` to include forecasts by default
- Migration script: `scripts/migrate_weather_index.py`
- Initial data: 9,408 forecast records (28 countries × 336 hours)

**Files Added/Modified:**
- `src/fetch_weather.py` - Added `fetch_weather_forecast()` and `fetch_weather_forecast_from_api()`
- `src/db.py` - Added `upsert_weather_forecast_data()`, updated `upsert_weather_data()`
- `scripts/update_weather.py` - Added forecast CLI flags
- `scripts/migrate_weather_index.py` - New migration script
- `update_weather.bat` - Now includes `--forecast` flag

## Previous Changes (2025-12-29)

**ENTSO-E Load Forecast Integration:**
- Added new `energy_load_forecast` table for TSO load forecasts
- Supports both day-ahead (D+1) and week-ahead (D+7) forecast types
- New data type options: `load_forecast_day_ahead` and `load_forecast_week_ahead`
- Integrated into existing ENTSO-E pipeline (backfill.py and update.py)
- Added `query_load_forecast()` method to ENTSOEClient
- Added `upsert_load_forecast_data()` to database operations
- Created new fetch module: `src/fetch_load_forecast.py`
- Updated configuration with backfill defaults (2019-01-01)
- Validated with test data: 672 records (Germany, Dec 20-26, 2024)
- Typical forecast accuracy: 2-4% error (excellent performance)

## Previous Changes (2025-12-23)

Weather data completeness improvements:
- Backfilled all weather data gaps: 75,768 new records added
- Fixed 4,032 records with null temperature values
- Weather data increased from 647,808 to 723,864 records
- All 28 regions now have 100% hourly coverage (no gaps)
- Removed unused columns: cloud_cover_frac, pv_kw_per_kwp, wind_kw_per_turbine
- Added weather backfill script: `scripts/backfill_weather.py`
- Added weather fetcher module: `src/fetch_weather.py`

Price data backfill completed:
- Italy price data restored: 35,065 records (2021-2025)
- Total price records increased from 828,878 to 928,533
- All 28 countries now have 4+ years of price data coverage

## Previous Changes (2025-12-22)

Database cleanup performed:
- Removed all ML/forecasting related tables
- Fixed 1,500 calculation errors in renewable totals
- Deleted 17,211 orphaned "MO" country records
- Removed 26,280 stale records (>2 years old)
- Renewable table reduced from 134,127 to 90,636 records
- Database validated and optimized

**Validation Results:**
- Zero orphaned country codes
- Zero calculation errors
- Zero duplicate records
- All totals validated

## Working with This Repository

### When Querying Data

1. **Always filter by country_code** to leverage indexes
2. **Use timestamp ranges** rather than scanning full tables
3. **Prefer views** (`energy_dashboard_data`, `latest_data_by_country`) for common queries
4. **Check completeness_cache** before expensive aggregations
5. **Filter by data_quality early** to reduce result sets

### When Modifying Data

1. **Never bypass unique constraints** - they prevent data corruption
2. **Always use UTC timestamps** - no local time conversions in database
3. **Update completeness_cache** after bulk operations
4. **Run ANALYZE** after large imports to update query planner statistics
5. **Log operations** in `data_ingestion_log` table

### When Analyzing Data Quality

1. **Consult `database_completeness.md`** for current quality assessment
2. **Check `latest_data_by_country` view** for freshness
3. **Validate renewable totals** match sum of components
4. **Look for gaps** in timestamp continuity
5. **Compare record counts** against expected hourly granularity

## Known Issues & Limitations

### Critical Issues
1. **Short Data Spans:** 6 countries have <1 week of load data (MD, MK, BA, CY, RS, ME)
2. **Outdated Data:** GB and UA have data from 2019-2022 only

### Missing Data
- **3 countries** have no data at all (IS, MT, TR)
- **11 countries** missing price data
- **10 countries** missing renewable data
- **12 countries** missing weather data (though 28 have complete coverage)

See `database_completeness.md` for detailed analysis and recommendations.

## Maintenance Tasks

**Regular:**
- **Weekly:** Update completeness_cache
- **Monthly:** Run VACUUM to optimize database
- **After imports:** Run ANALYZE to update query statistics
- **Quarterly:** Review and archive old data

**Data Validation:**
- Check for orphaned country codes
- Verify `total_renewable_mw` calculations match sum of components
- Monitor for duplicate timestamps (unique index violations)
- Validate timestamp continuity (detect gaps)
- Check for negative energy values

## Performance Considerations

The database is optimized for analytical workloads:
- Star schema enables simple, fast joins
- Composite indexes on `(country_code, timestamp_utc)` accelerate time-series queries
- Pre-computed views avoid repetitive complex joins
- Completeness cache avoids expensive gap analysis queries

**For large queries:**
- Use LIMIT when exploring data
- Index on timestamp enables efficient time-range filtering
- Consider partitioning by year if dataset grows significantly (currently ~507 MB)

## Data Sources

- **Energy Data:** ENTSO-E Transparency Platform
- **Weather Historical:** Open-Meteo Archive API (ERA5 reanalysis model)
- **Weather Forecasts:** Open-Meteo Forecast API (GFS/ECMWF best_match model)
