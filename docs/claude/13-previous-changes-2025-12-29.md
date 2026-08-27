> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

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
