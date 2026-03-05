# Energy Dashboard Database Structure

## Overview

The Energy Dashboard Database (`energy_dashboard.db`) is a SQLite database designed for storing and analyzing European energy market data. It follows a **star schema** architecture with `countries` as the central dimension table, surrounded by fact tables containing time-series data for energy metrics and weather.

**Database File:** `energy_dashboard.db`
**Database Type:** SQLite 3
**Total Size:** ~527 MB
**Total Records:** 2,022,913

---

## Architecture Pattern

The database implements a **star schema** optimized for time-series analysis:

```
                    ┌─────────────────┐
                    │   countries     │
                    │   (dimension)   │
                    └────────┬────────┘
                            │
            ┌───────────────┼───────────────┐
            │               │               │
     ┌──────▼─────┐  ┌──────▼─────┐  ┌──────▼─────┐
     │energy_load │  │energy_price│  │energy_     │
     │  (fact)    │  │  (fact)    │  │renewable   │
     └────────────┘  └────────────┘  │  (fact)    │
                                     └────────────┘
            ┌───────────────┼───────────────┐
            │               │               │
     ┌──────▼─────┐  ┌──────▼─────┐  ┌──────▼─────┐
     │weather_data│  │weather_    │  │data_       │
     │  (fact)    │  │point_data  │  │ingestion   │
     └────────────┘  │  (fact)    │  │_log        │
                     └────────────┘  └────────────┘
```

---

## Table Categories

### 1. Master/Dimension Tables

#### `countries`
Central reference table for all European countries.

**Columns:**
- `country_code` TEXT PRIMARY KEY - ISO 2-letter country code
- `country_name` TEXT NOT NULL - Full country name
- `entsoe_domain` TEXT - ENTSO-E transparency platform domain code
- `has_load_data` BOOLEAN - Flag indicating availability of load data
- `has_price_data` BOOLEAN - Flag indicating availability of price data
- `has_renewable_data` BOOLEAN - Flag indicating availability of renewable data
- `has_weather_data` BOOLEAN - Flag indicating availability of weather data
- `priority` INTEGER DEFAULT 2 - Data fetching priority (1=high, 2=medium, 3=low)
- `notes` TEXT - Additional notes
- `created_at` TIMESTAMP - Record creation timestamp
- `updated_at` TIMESTAMP - Record update timestamp

**Indexes:**
- `idx_countries_priority` ON (priority)

**Records:** 39 European countries

---

### 2. Time-Series Fact Tables

All fact tables share common design patterns:
- Reference `countries.country_code` (logical foreign key)
- Include `timestamp_utc` for temporal data
- Include `data_quality` field ('actual', 'forecast', etc.)
- Have unique composite indexes to prevent duplicates
- Store timestamps in UTC

#### `energy_load`
Electricity demand/consumption data.

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `country_code` TEXT NOT NULL - References countries
- `timestamp_utc` TIMESTAMP NOT NULL - UTC timestamp of measurement
- `load_mw` REAL NOT NULL - Electrical load in megawatts
- `data_quality` TEXT DEFAULT 'actual' - Data quality indicator
- `created_at` TIMESTAMP - Record creation timestamp
- `publication_timestamp_utc` TIMESTAMP - When ENTSO-E published/updated this data

**Indexes:**
- `idx_load_country_time` UNIQUE ON (country_code, timestamp_utc)
- `idx_load_time` ON (timestamp_utc)
- `idx_load_quality` ON (data_quality)
- `idx_load_publication` ON (publication_timestamp_utc)

**Records:** 279,880
**Date Range:** 2019-12-31 to 2025-11-26
**Granularity:** Hourly

---

#### `energy_price`
Electricity market prices.

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `country_code` TEXT NOT NULL - References countries
- `timestamp_utc` TIMESTAMP NOT NULL - UTC timestamp
- `price_eur_mwh` REAL NOT NULL - Price in EUR per megawatt-hour
- `data_quality` TEXT DEFAULT 'actual' - Data quality indicator
- `created_at` TIMESTAMP - Record creation timestamp
- `publication_timestamp_utc` TIMESTAMP - When ENTSO-E published/updated this data

**Indexes:**
- `idx_price_country_time` UNIQUE ON (country_code, timestamp_utc)
- `idx_price_time` ON (timestamp_utc)
- `idx_price_quality` ON (data_quality)
- `idx_price_publication` ON (publication_timestamp_utc)

**Records:** 928,533
**Date Range:** 2021-01-01 to 2025-11-28
**Granularity:** Hourly

---

#### `energy_renewable`
Renewable energy generation broken down by source type.

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `country_code` TEXT NOT NULL - References countries
- `timestamp_utc` TIMESTAMP NOT NULL - UTC timestamp
- `solar_mw` REAL DEFAULT 0 - Solar generation in MW
- `wind_onshore_mw` REAL DEFAULT 0 - Onshore wind in MW
- `wind_offshore_mw` REAL DEFAULT 0 - Offshore wind in MW
- `hydro_run_mw` REAL DEFAULT 0 - Run-of-river hydro in MW
- `hydro_reservoir_mw` REAL DEFAULT 0 - Reservoir hydro in MW
- `biomass_mw` REAL DEFAULT 0 - Biomass generation in MW
- `geothermal_mw` REAL DEFAULT 0 - Geothermal generation in MW
- `other_renewable_mw` REAL DEFAULT 0 - Other renewable sources in MW
- `total_renewable_mw` REAL - Total renewable generation (calculated)
- `data_quality` TEXT DEFAULT 'actual' - Data quality indicator
- `fetched_at` TIMESTAMP - When data was fetched (for tracking revisions)
- `publication_timestamp_utc` TIMESTAMP - When ENTSO-E published/updated this data

**Indexes:**
- `idx_renewable_country_time` UNIQUE ON (country_code, timestamp_utc)
- `idx_renewable_time` ON (timestamp_utc)
- `idx_renewable_quality` ON (data_quality)
- `idx_renewable_latest_revision` ON (country_code, timestamp_utc, fetched_at DESC)
- `idx_renewable_publication` ON (publication_timestamp_utc)

**Records:** 90,636
**Date Range:** 2021-12-31 to 2025-11-25
**Granularity:** Varies (15-min to hourly)

---

#### `energy_load_forecast`
ENTSO-E transmission system operator (TSO) load forecasts.

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `country_code` TEXT NOT NULL - References countries
- `target_timestamp_utc` TIMESTAMP NOT NULL - When the forecast is FOR
- `forecast_value_mw` REAL NOT NULL - Forecasted load in megawatts
- `forecast_type` TEXT NOT NULL - Type: 'day_ahead' (D+1) or 'week_ahead' (D+7)
- `forecast_run_time` TIMESTAMP - Forecast initialization time (currently NULL - ENTSO-E API limitation)
- `horizon_hours` INTEGER - Hours ahead (currently NULL - ENTSO-E API limitation)
- `data_quality` TEXT DEFAULT 'forecast' - Always 'forecast'
- `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP - Record creation timestamp
- `publication_timestamp_utc` TIMESTAMP - When ENTSO-E published this forecast
- `forecast_min_mw` REAL - Week-ahead only: daily minimum forecast
- `forecast_max_mw` REAL - Week-ahead only: daily maximum forecast

**Indexes:**
- `idx_load_forecast_country_time_type` UNIQUE ON (country_code, target_timestamp_utc, forecast_type)
- `idx_load_forecast_time` ON (target_timestamp_utc)
- `idx_load_forecast_type` ON (forecast_type)
- `idx_load_forecast_country_type` ON (country_code, forecast_type)
- `idx_forecast_publication` ON (publication_timestamp_utc)

**Records:** 1,915,680
**Date Range:** 2024-12-20 onwards (backfillable to 2019-01-01)
**Granularity:** 15-minute intervals
**Forecast Types:**
- Day-Ahead (D+1): Next day forecasts, published daily
- Week-Ahead (D+7): 7-day ahead forecasts, published weekly

**Quality:** ✓ Excellent (official TSO forecasts, typical accuracy 2-4% error)

---

#### `energy_generation_forecast`
ENTSO-E transmission system operator (TSO) generation forecasts for solar and wind.

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `country_code` TEXT NOT NULL - References countries
- `target_timestamp_utc` TIMESTAMP NOT NULL - When the forecast is FOR
- `solar_mw` REAL - Forecasted solar generation (MW)
- `wind_onshore_mw` REAL - Forecasted onshore wind generation (MW)
- `wind_offshore_mw` REAL - Forecasted offshore wind generation (MW)
- `total_forecast_mw` REAL - Total forecasted generation (MW)
- `forecast_type` TEXT DEFAULT 'day_ahead' - Forecast type
- `data_quality` TEXT DEFAULT 'forecast' - Always 'forecast'
- `publication_timestamp_utc` TIMESTAMP - When ENTSO-E published this forecast
- `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP - Record creation timestamp

**Indexes:**
- `UNIQUE(country_code, target_timestamp_utc, forecast_type)`
- `idx_gen_forecast_country_ts` ON (country_code, target_timestamp_utc)
- `idx_gen_forecast_ts` ON (target_timestamp_utc)
- `idx_gen_forecast_country_time` ON (country_code, target_timestamp_utc)

**Records:** ~2.5M
**Granularity:** Hourly
**Coverage:** 35 countries

---

**Example Query - Compare Forecast vs Actual:**
```sql
SELECT
    a.timestamp_utc,
    a.load_mw AS actual,
    f.forecast_value_mw AS day_ahead_forecast,
    ROUND(ABS(a.load_mw - f.forecast_value_mw), 2) AS error_mw,
    ROUND(ABS(a.load_mw - f.forecast_value_mw) / a.load_mw * 100, 2) AS error_pct
FROM energy_load a
JOIN energy_load_forecast f
    ON a.country_code = f.country_code
    AND a.timestamp_utc = f.target_timestamp_utc
    AND f.forecast_type = 'day_ahead'
WHERE a.country_code = 'DE'
    AND a.timestamp_utc >= datetime('now', '-7 days')
ORDER BY a.timestamp_utc;
```

---

#### `weather_data`
Country-aggregated weather data with 30 meteorological variables from Open-Meteo ERA5 reanalysis.

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `country_code` TEXT NOT NULL - References countries (includes DK1, DK2 regional splits)
- `timestamp_utc` TIMESTAMP NOT NULL - UTC timestamp
- `forecast_run_time` TIMESTAMP - Forecast initialization time (if forecast data)
- **Core Meteorological (4 variables):**
  - `temperature_2m_k` REAL - Temperature at 2m height (Kelvin)
  - `dew_point_2m_k` REAL - Dew point at 2m (Kelvin)
  - `relative_humidity_2m_frac` REAL - Relative humidity (fraction 0-1)
  - `pressure_msl_hpa` REAL - Mean sea level pressure (hPa)
- **Wind Variables (7 variables):**
  - `wind_speed_10m_ms` REAL - Wind speed at 10m (m/s)
  - `wind_gusts_10m_ms` REAL - Wind gusts at 10m (m/s)
  - `wind_direction_10m_deg` REAL - Wind direction at 10m (degrees)
  - `wind_speed_100m_ms` REAL - Wind speed at 100m (m/s)
  - `wind_direction_100m_deg` REAL - Wind direction at 100m (degrees)
  - `wind_speed_80m_ms` REAL - Wind speed at 80m (m/s)
  - `wind_speed_120m_ms` REAL - Wind speed at 120m (m/s)
- **Precipitation (3 variables):**
  - `precip_mm` REAL - Total precipitation (mm)
  - `rain_mm` REAL - Rainfall (mm)
  - `snowfall_mm` REAL - Snowfall (mm water equivalent)
- **Solar Radiation (14 variables):**
  - `shortwave_radiation_wm2` REAL - Shortwave radiation (W/m²)
  - `direct_radiation_wm2` REAL - Direct radiation (W/m²)
  - `direct_normal_irradiance_wm2` REAL - DNI (W/m²)
  - `diffuse_radiation_wm2` REAL - Diffuse radiation (W/m²)
  - `ghi_cs_wm2` REAL - Global Horizontal Irradiance clear-sky (W/m²)
  - `dni_cs_wm2` REAL - DNI clear-sky (W/m²)
  - `dhi_cs_wm2` REAL - Diffuse Horizontal Irradiance clear-sky (W/m²)
  - `ghi_est_wm2` REAL - GHI estimated (W/m²)
  - `dni_est_wm2` REAL - DNI estimated (W/m²)
  - `dhi_est_wm2` REAL - DHI estimated (W/m²)
  - `pv_poa_wm2` REAL - PV plane-of-array irradiance (W/m²)
  - `pv_cell_temp_c` REAL - PV cell temperature (°C)
- **Metadata:**
  - `model_name` TEXT DEFAULT 'era5' - Weather model used
  - `n_sampling_points` INTEGER - Number of geographic points aggregated
  - `data_quality` TEXT DEFAULT 'actual' - Data quality indicator
  - `created_at` TIMESTAMP - Record creation timestamp

**Indexes:**
- `idx_weather_country_time_model` UNIQUE ON (country_code, timestamp_utc, model_name)
- `idx_weather_time` ON (timestamp_utc)
- `idx_weather_forecast_run` ON (forecast_run_time)
- `idx_weather_quality` ON (data_quality)
- `idx_weather_ml` ON (country_code, timestamp_utc, data_quality)

**Records:** 723,864
**Date Range:** 2023-01-01 to 2025-11-29
**Granularity:** Hourly
**Coverage:** 28 regions (26 countries + DK1/DK2)

---

#### `weather_point_data`
Point-specific weather measurements (geographic coordinates).

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `point_id` TEXT NOT NULL - Unique point identifier
- `country_code` TEXT NOT NULL - References countries
- `lat` REAL NOT NULL - Latitude
- `lon` REAL NOT NULL - Longitude
- `timestamp_utc` TIMESTAMP NOT NULL - UTC timestamp
- `forecast_run_time` TIMESTAMP - Forecast initialization time
- `temperature_2m_k` REAL - Temperature (K)
- `relative_humidity_2m_frac` REAL - Relative humidity (0-1)
- `pressure_msl_hpa` REAL - Pressure (hPa)
- `cloud_cover_frac` REAL - Cloud cover (0-1)
- `wind_speed_10m_ms` REAL - Wind speed at 10m (m/s)
- `wind_speed_100m_ms` REAL - Wind speed at 100m (m/s)
- `wind_direction_10m_deg` REAL - Wind direction at 10m (degrees)
- `wind_direction_100m_deg` REAL - Wind direction at 100m (degrees)
- `shortwave_radiation_wm2` REAL - Shortwave radiation (W/m²)
- `direct_radiation_wm2` REAL - Direct radiation (W/m²)
- `diffuse_radiation_wm2` REAL - Diffuse radiation (W/m²)
- `precip_mm` REAL - Precipitation (mm)
- `model_name` TEXT DEFAULT 'era5' - Weather model
- `data_quality` TEXT DEFAULT 'actual' - Data quality
- `created_at` TIMESTAMP - Record creation timestamp

**Indexes:**
- `idx_weather_point_unique` UNIQUE ON (point_id, timestamp_utc, COALESCE(forecast_run_time, ''))
- `idx_weather_point_country` ON (country_code)
- `idx_weather_point_timestamp` ON (timestamp_utc)
- `idx_weather_point_location` ON (lat, lon)
- `idx_weather_point_composite` ON (country_code, timestamp_utc)
- `idx_weather_point_ml` ON (point_id, timestamp_utc, data_quality)

**Granularity:** Hourly per geographic point

---

### 3. Operational/Metadata Tables

#### `data_ingestion_log`
ETL pipeline execution audit trail.

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `pipeline_type` TEXT NOT NULL - Type of pipeline (e.g., 'load', 'price', 'renewable', 'weather')
- `country_code` TEXT - Country being processed
- `start_time` TIMESTAMP NOT NULL - Pipeline start time
- `end_time` TIMESTAMP - Pipeline completion time
- `status` TEXT NOT NULL - Execution status ('running', 'completed', 'failed')
- `records_inserted` INTEGER DEFAULT 0 - Number of records inserted
- `records_updated` INTEGER DEFAULT 0 - Number of records updated
- `records_failed` INTEGER DEFAULT 0 - Number of failed records
- `error_message` TEXT - Error details if failed
- `created_at` TIMESTAMP - Record creation timestamp

**Indexes:**
- `idx_ingestion_log_pipeline` ON (pipeline_type, start_time DESC)
- `idx_ingestion_log_status` ON (status)

**Records:** Currently empty (audit trail)

---

#### `database_metadata`
System-level configuration and metadata.

**Columns:**
- `key` TEXT PRIMARY KEY - Metadata key
- `value` TEXT NOT NULL - Metadata value
- `updated_at` TIMESTAMP - Last update timestamp

**Records:** 3 configuration entries

---

#### `completeness_cache`
Pre-computed data quality and completeness metrics per country.

**Columns:**
- `country_code` TEXT PRIMARY KEY - References countries
- `overview_json` TEXT - JSON with completeness overview
- `detail_json` TEXT - JSON with detailed gap analysis
- `computed_at` TIMESTAMP NOT NULL - Cache computation timestamp
- `computation_time_ms` INTEGER - Time taken to compute (milliseconds)

**Indexes:**
- `idx_completeness_cache_time` ON (computed_at)

**Records:** 33 countries
**Last Updated:** 2025-11-22 18:31:33 (Note: May be outdated)

**Example JSON Structure:**
```json
{
  "country_code": "AT",
  "country_name": "Austria",
  "priority": 1,
  "data_types": {
    "load": {
      "has_data": true,
      "record_count": 7546,
      "completeness": 22.0,
      "earliest": "2025-09-05T22:00:00",
      "latest": "2025-11-22T17:45:00+01:00"
    },
    "price": {...},
    "renewable": {...},
    "weather": {...}
  },
  "overall_completeness": 46.975
}
```

---

#### `forecasts`
D+2 energy forecasts generated by the forecasting module (`../energy_forecast/`).

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `country_code` TEXT NOT NULL - References countries
- `forecast_type` TEXT NOT NULL - Type of forecast ('load', 'price', 'renewable')
- `target_timestamp_utc` TIMESTAMP NOT NULL - Timestamp being forecasted
- `generated_at` TIMESTAMP NOT NULL - When the forecast was generated
- `horizon_hours` INTEGER NOT NULL - Hours ahead (typically 30-54 for D+2)
- `forecast_value` REAL NOT NULL - Predicted value
- `model_name` TEXT NOT NULL - Model used (e.g., 'xgboost')
- `model_version` TEXT - Model version identifier
- `renewable_type` TEXT - For individual renewable forecasts (solar, wind_onshore, etc.)

**Indexes:**
- `idx_forecasts_lookup` ON (country_code, forecast_type, target_timestamp_utc)
- `idx_forecasts_generated` ON (generated_at)

**Unique Constraint:**
- `UNIQUE(country_code, forecast_type, target_timestamp_utc, horizon_hours, generated_at)`

**Records:** Generated daily by `energy_forecast/scripts/forecast_daily.py`

**Example Query - Compare Forecast vs Actual:**
```sql
SELECT
    f.target_timestamp_utc,
    f.forecast_value AS predicted,
    l.load_mw AS actual,
    ABS(f.forecast_value - l.load_mw) AS error
FROM forecasts f
JOIN energy_load l
    ON f.country_code = l.country_code
    AND f.target_timestamp_utc = l.timestamp_utc
WHERE f.forecast_type = 'load'
    AND f.country_code = 'DE';
```

---

#### `completeness_cache_meta`
Metadata about the completeness cache itself.

**Columns:**
- `key` TEXT PRIMARY KEY - Metadata key
- `value` TEXT NOT NULL - Metadata value
- `updated_at` TIMESTAMP - Last update timestamp

**Records:** 2 entries (last refresh time and duration)

---

#### `training_jobs`
Tracks ML model training job execution.

**Columns:**
- `id` TEXT PRIMARY KEY - Unique job identifier
- `countries` TEXT NOT NULL - Countries being trained (JSON array)
- `forecast_types` TEXT NOT NULL - Forecast types (JSON array)
- `algorithm` TEXT NOT NULL - Algorithm used (e.g., 'xgboost')
- `status` TEXT NOT NULL DEFAULT 'pending' - Job status ('pending', 'running', 'completed', 'failed')
- `started_at` TEXT - Job start timestamp
- `completed_at` TEXT - Job completion timestamp
- `error` TEXT - Error message if failed
- `metrics` TEXT - Training metrics (JSON)
- `hyperparams` TEXT - Hyperparameters used (JSON)
- `grid_search_enabled` INTEGER DEFAULT 0 - Whether grid search was used
- `grid_search_results` TEXT - Grid search results (JSON)
- `created_at` TEXT - Record creation timestamp

**Indexes:**
- `idx_training_jobs_status` ON (status)

---

#### `forecast_jobs`
Tracks ML forecast generation job execution.

**Columns:**
- `id` TEXT PRIMARY KEY - Unique job identifier
- `countries` TEXT NOT NULL - Countries for forecast (JSON array)
- `forecast_types` TEXT - Forecast types (JSON array)
- `horizon_days` INTEGER - Forecast horizon in days
- `status` TEXT NOT NULL DEFAULT 'pending' - Job status
- `started_at` TEXT - Job start timestamp
- `completed_at` TEXT - Job completion timestamp
- `forecasts_generated` INTEGER DEFAULT 0 - Number of forecasts created
- `error` TEXT - Error message if failed
- `created_at` TEXT - Record creation timestamp

---

#### `model_evaluations`
Stores ML model evaluation metrics and baseline comparisons.

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `country_code` TEXT NOT NULL - Country code
- `forecast_type` TEXT NOT NULL - Forecast type
- `model_version` TEXT NOT NULL - Model version identifier
- `evaluation_date` TEXT NOT NULL - Date of evaluation
- `mae` REAL - Mean Absolute Error
- `rmse` REAL - Root Mean Square Error
- `mape` REAL - Mean Absolute Percentage Error
- `smape` REAL - Symmetric MAPE
- `mase` REAL - Mean Absolute Scaled Error
- `directional_accuracy` REAL - Directional accuracy percentage
- `skill_vs_persistence` REAL - Skill score vs persistence baseline
- `skill_vs_seasonal` REAL - Skill score vs seasonal baseline
- `skill_vs_tso` REAL - Skill score vs TSO forecast
- `training_samples` INTEGER - Number of training samples
- `test_samples` INTEGER - Number of test samples
- `evaluation_periods` TEXT - Evaluation periods (JSON)
- `is_baseline` BOOLEAN DEFAULT FALSE - Whether this is a baseline model
- `model_location` TEXT - Path to model file
- `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP

**Indexes:**
- `idx_evaluations_lookup` ON (country_code, forecast_type, model_version)

**Unique Constraint:**
- `UNIQUE(country_code, forecast_type, model_version, evaluation_date)`

---

#### `deployed_models`
Tracks production model deployments and rollback history.

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `country_code` TEXT NOT NULL - Country code
- `forecast_type` TEXT NOT NULL - Forecast type
- `model_version` TEXT NOT NULL - Deployed model version
- `deployed_at` TIMESTAMP NOT NULL - Deployment timestamp
- `deployed_by` TEXT DEFAULT 'system' - Who/what deployed the model
- `previous_version` TEXT - Previous model version (for rollback)
- `deployment_reason` TEXT - Reason for deployment
- `status` TEXT DEFAULT 'active' - Deployment status ('active', 'rolled_back')
- `mae_at_deployment` REAL - MAE at time of deployment
- `mape_at_deployment` REAL - MAPE at time of deployment
- `skill_score_at_deployment` REAL - Skill score at deployment
- `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP

**Indexes:**
- `idx_deployed_lookup` ON (country_code, forecast_type, status)

---

#### `forecast_runs`
Tracks daily forecast generation runs for monitoring and auditing.

**Columns:**
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `run_timestamp` TIMESTAMP NOT NULL - When the run was executed
- `run_type` TEXT NOT NULL - Type of run ('daily', 'manual', 'backfill')
- `trigger_source` TEXT - What triggered the run
- `status` TEXT NOT NULL - Run status ('running', 'completed', 'failed')
- `countries_requested` TEXT - Countries requested (JSON array)
- `countries_completed` TEXT - Countries completed (JSON array)
- `types_requested` TEXT - Forecast types requested (JSON array)
- `types_completed` TEXT - Forecast types completed (JSON array)
- `forecasts_generated` INTEGER DEFAULT 0 - Total forecasts generated
- `execution_time_seconds` REAL - Total execution time
- `error_message` TEXT - Error message if failed
- `log_file_path` TEXT - Path to log file
- `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP

**Indexes:**
- `idx_forecast_runs_timestamp` ON (run_timestamp DESC)

---

### 4. Views (Virtual Tables)

#### `energy_dashboard_data`
Unified view joining all energy metrics with calculated renewable percentage.

**Columns:**
- `timestamp_utc` - Timestamp
- `country_code` - Country code
- `country_name` - Country name
- `load_mw` - Electrical load
- `price_eur_mwh` - Electricity price
- `solar_mw` - Solar generation
- `wind_onshore_mw` - Onshore wind
- `wind_offshore_mw` - Offshore wind
- `total_wind_mw` - Total wind (calculated)
- `total_renewable_mw` - Total renewable (calculated)
- `renewable_percentage` - Renewable as % of load (calculated)

**Definition:**
```sql
SELECT
    l.timestamp_utc,
    l.country_code,
    c.country_name,
    l.load_mw,
    p.price_eur_mwh,
    r.solar_mw,
    r.wind_onshore_mw,
    r.wind_offshore_mw,
    (r.wind_onshore_mw + r.wind_offshore_mw) as total_wind_mw,
    (r.solar_mw + r.wind_onshore_mw + r.wind_offshore_mw +
     r.hydro_run_mw + r.hydro_reservoir_mw + r.biomass_mw +
     r.geothermal_mw + r.other_renewable_mw) as total_renewable_mw,
    ROUND((r.solar_mw + ...) * 100.0 / NULLIF(l.load_mw, 0), 2) as renewable_percentage
FROM energy_load l
JOIN countries c ON l.country_code = c.country_code
LEFT JOIN energy_price p ON l.country_code = p.country_code
    AND l.timestamp_utc = p.timestamp_utc
LEFT JOIN energy_renewable r ON l.country_code = r.country_code
    AND l.timestamp_utc = r.timestamp_utc
```

---

#### `latest_data_by_country`
Quick reference for most recent data timestamp per country and data type.

**Columns:**
- `country_code` - Country code
- `country_name` - Country name
- `latest_load_time` - Most recent load data timestamp
- `latest_price_time` - Most recent price data timestamp
- `latest_renewable_time` - Most recent renewable data timestamp
- `latest_weather_time` - Most recent weather data timestamp

**Definition:**
```sql
SELECT
    c.country_code,
    c.country_name,
    MAX(l.timestamp_utc) as latest_load_time,
    MAX(p.timestamp_utc) as latest_price_time,
    MAX(r.timestamp_utc) as latest_renewable_time,
    MAX(w.timestamp_utc) as latest_weather_time
FROM countries c
LEFT JOIN energy_load l ON c.country_code = l.country_code
LEFT JOIN energy_price p ON c.country_code = p.country_code
LEFT JOIN energy_renewable r ON c.country_code = r.country_code
LEFT JOIN weather_data w ON c.country_code = w.country_code
GROUP BY c.country_code, c.country_name
```

---

## Relationships

The database uses **logical relationships** (enforced at application level) rather than physical foreign key constraints:

```
countries (1) ----< (∞) energy_load
countries (1) ----< (∞) energy_price
countries (1) ----< (∞) energy_renewable
countries (1) ----< (∞) weather_data
countries (1) ----< (∞) weather_point_data
countries (1) ----< (∞) data_ingestion_log
countries (1) ----< (1) completeness_cache
```

All relationships use `country_code` as the linking field.

---

## Indexing Strategy

### 1. Unique Composite Indexes
Prevent duplicate time-series data:
- `(country_code, timestamp_utc)` on load/price/renewable tables
- `(country_code, timestamp_utc, model_name)` on weather_data
- `(point_id, timestamp_utc, forecast_run_time)` on weather_point_data

### 2. Performance Indexes
Optimize common queries:
- Single-column timestamp indexes for time-range queries
- Data quality indexes for filtering actual vs forecast data
- Composite indexes for ML workloads (country + time + quality)

### 3. Specialized Indexes
- `idx_renewable_latest_revision` - Fetch most recent data revision
- `idx_weather_point_location` - Geospatial queries on (lat, lon)
- `idx_ingestion_log_pipeline` - Pipeline monitoring queries

---

## Data Quality Features

### Duplicate Prevention
- Unique indexes on all fact tables prevent duplicate time-series entries
- Composite keys ensure data integrity

### Data Revision Tracking
- `fetched_at` timestamp in renewable data tracks when data was retrieved
- `forecast_run_time` in weather data tracks forecast initialization time
- Allows multiple revisions of same timestamp (e.g., forecast updates)

### Data Quality Indicators
- `data_quality` field in all fact tables
- Values: 'actual', 'forecast', 'estimated', etc.
- Enables filtering between measured and predicted data

### Completeness Monitoring
- `completeness_cache` table stores pre-computed metrics
- Tracks data coverage, gaps, and quality per country
- Cached for performance (recomputed periodically)

---

## Time Handling

### UTC Standardization
- All timestamps stored in UTC
- Handles daylight saving time transitions correctly
- Local time conversion done at application layer

### Temporal Granularity
- **Load/Price/Weather:** Hourly (24 records/day)
- **Renewable:** Varies (15-min to hourly, depending on source)
- **Point Weather:** Hourly per geographic point

### Date Ranges
- **Historical:** Earliest data from 2019-12-31
- **Current:** Most recent data through 2025-11-29
- **Span:** Up to ~5 years of historical data

---

## Design Patterns

### Star Schema Benefits
1. **Simple queries:** Easy joins from facts to dimension
2. **Query performance:** Optimized for analytical workloads
3. **Scalability:** Easy to add new countries or data types
4. **Maintainability:** Clear separation of concerns

### Time-Series Optimization
1. **Partitioning-ready:** Can partition by year if needed
2. **Index strategy:** Optimized for time-range queries
3. **Compression-ready:** Numeric data types for efficient storage
4. **Aggregation-friendly:** Pre-calculated totals and percentages

### ETL Support
1. **Audit trail:** data_ingestion_log tracks all pipeline runs
2. **Idempotency:** Unique indexes prevent duplicate inserts
3. **Error tracking:** Status and error fields in ingestion log
4. **Performance monitoring:** Computation time tracked in cache

---

## Storage Statistics

| Table | Records | Approx Size | % of Total |
|-------|---------|-------------|------------|
| energy_price | 928,533 | ~170 MB | 32% |
| weather_data | 723,864 | ~260 MB | 49% |
| energy_load | 279,880 | ~80 MB | 15% |
| energy_renewable | 90,636 | ~30 MB | 6% |
| Other tables | Various | ~7 MB | 1% |
| **Total** | **2,022,913** | **~547 MB** | **100%** |

---

## Query Optimization Tips

### Common Query Patterns

**1. Get latest data for a country:**
```sql
SELECT * FROM latest_data_by_country
WHERE country_code = 'DE';
```

**2. Time-series data for specific period:**
```sql
SELECT * FROM energy_load
WHERE country_code = 'FR'
  AND timestamp_utc BETWEEN '2025-01-01' AND '2025-01-31'
ORDER BY timestamp_utc;
```

**3. Dashboard view (all metrics):**
```sql
SELECT * FROM energy_dashboard_data
WHERE country_code = 'IT'
  AND timestamp_utc >= datetime('now', '-7 days')
ORDER BY timestamp_utc DESC;
```

**4. Renewable percentage analysis:**
```sql
SELECT country_name,
       AVG(renewable_percentage) as avg_renewable_pct
FROM energy_dashboard_data
WHERE timestamp_utc >= datetime('now', '-30 days')
GROUP BY country_code, country_name
ORDER BY avg_renewable_pct DESC;
```

### Performance Considerations

1. **Always filter by country_code** - Uses indexes effectively
2. **Use timestamp ranges** - Leverages temporal indexes
3. **Prefer views for complex joins** - Pre-optimized queries
4. **Use completeness_cache** - Avoid expensive aggregations
5. **Filter by data_quality early** - Reduces result set size

---

## Maintenance Recommendations

### Regular Tasks
1. **Update completeness_cache** - Weekly recomputation
2. **VACUUM database** - Monthly to reclaim space
3. **ANALYZE tables** - After bulk imports to update statistics
4. **Archive old data** - Move data >2 years to archive tables
5. **Monitor ingestion_log** - Track pipeline failures

### Data Quality Checks
1. Check for orphaned country codes (countries not in dimension table)
2. Verify total_renewable_mw calculations match sum of components
3. Monitor for duplicate timestamps (unique index violations)
4. Validate timestamp continuity (detect gaps)
5. Check for negative energy values (data quality issue)

### Backup Strategy
1. **Daily:** Incremental backup of new/changed records
2. **Weekly:** Full database backup
3. **Monthly:** Archive to long-term storage
4. **Before migrations:** Always backup before schema changes

---

## Future Enhancements

### Potential Improvements
1. Add explicit foreign key constraints for referential integrity
2. Partition large tables by year for better performance
3. Add materialized views for complex aggregations
4. Implement row-level compression for historical data
5. Add audit triggers for change tracking
6. Create summary tables for monthly/yearly aggregations

### Scalability Considerations
- Current design supports millions of records efficiently
- If > 10M records, consider partitioning by time period
- Point weather data may need separate optimization if scaled
- Consider time-series database (TimescaleDB) if real-time analysis needed

---

## Version History

- **2026-01-15:** Documentation update - added energy_generation_forecast table, ML operational tables (training_jobs, forecast_jobs, model_evaluations, deployed_models, forecast_runs), updated energy_load_forecast with min/max columns, added renewable_type to forecasts table
- **2025-12-23:** Weather data maintenance - backfilled 76,056 gap records, fixed null temperature records, removed 3 empty columns (cloud_cover_frac, pv_kw_per_kwp, wind_kw_per_turbine), total weather records increased to 723,864
- **2025-12-23:** Price data backfill - Italy price data restored (35,065 records), total price records increased to 928,533
- **2025-12-22:** Database cleanup - removed ML/forecasting tables, fixed data quality issues
- **2025-11-22:** Added completeness caching
- **Earlier:** Initial schema creation with energy and weather data support
