> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

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
