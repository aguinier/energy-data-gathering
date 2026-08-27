> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

## Maintenance Tasks

**Regular:**
- **Weekly:** Update completeness_cache
- **Monthly:** Run VACUUM to optimize database
- **After imports:** Run ANALYZE to update query statistics
- **Quarterly:** Review and archive old data

**Data Validation:**
- Check for orphaned country codes
- Verify `total_renewable_mw` calculations match sum of components
- Monitor for duplicate timestamps (unique index violations)
- Validate timestamp continuity (detect gaps)
- Check for negative energy values
