> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

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
