> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

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
