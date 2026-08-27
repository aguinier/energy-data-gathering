> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

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
