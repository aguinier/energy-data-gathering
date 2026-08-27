> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

## Performance Considerations

The database is optimized for analytical workloads:
- Star schema enables simple, fast joins
- Composite indexes on `(country_code, timestamp_utc)` accelerate time-series queries
- Pre-computed views avoid repetitive complex joins
- Completeness cache avoids expensive gap analysis queries

**For large queries:**
- Use LIMIT when exploring data
- Index on timestamp enables efficient time-range filtering
- Consider partitioning by year if dataset grows significantly (currently ~507 MB)
