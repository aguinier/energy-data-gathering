> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

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
