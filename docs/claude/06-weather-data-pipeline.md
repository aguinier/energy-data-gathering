> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

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
