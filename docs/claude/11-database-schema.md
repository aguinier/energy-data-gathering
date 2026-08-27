> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

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
The `createdDateTime` from ENTSO-E's XML responses, parsed verbatim (`src/entsoe_client.py:307-333`).

**Critical caveat — this is our fetch time, not ENTSO-E's publication time.** ENTSO-E builds the
document on request and stamps `createdDateTime` with the generation moment, so the value records
when our cron last fetched the row. Two concrete measurements (replica, 2026-08-12):

- A GB `energy_load` reading for `2021-03-01T00:00:00` carries
  `publication_timestamp_utc = 2025-12-29 10:52:44` — four years after the measurement it
  describes, because that is when a recent cron pass re-fetched it.
- The BE day-ahead auction for the 2026-08-12 market day was published ~11:45Z on 2026-08-11; the
  stored rows are stamped `2026-08-12 00:32:38` — our 00:30 cron pass, ~12h45m after the real
  publication. The column cannot measure publication delay even where a real publication time exists.

**It is overwritten on every re-fetch.** A single GB 2021-03 block of 1,486 rows carries 4 distinct
`publication_timestamp_utc` values, drifting up to 34 days past their `created_at`. If you want
"when did we first store this row", use `created_at` — that is write-once.

**Do not use this column for:**
- Analyzing publication delays (it records fetch time, not publication time)
- Identifying data freshness (use `data_ingestion_log` or the dashboard's `/api/data-freshness` rules)
- Tracking when ENTSO-E updates historical data (rewriting on every fetch makes this unrecoverable)

**Note:** For backfilled historical data, `publication_timestamp_utc` is stamped with the date the
backfill ran — not the original publication date and not the measurement date. This makes it worse
than `NULL` for any provenance question about when the value was first published.

**Backfilling:** Use `scripts/backfill_publication_timestamps.py` to populate missing timestamps for existing data.
