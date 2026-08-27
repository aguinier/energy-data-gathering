> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

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
