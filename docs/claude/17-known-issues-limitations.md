> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

## Known Issues & Limitations

### Critical Issues
1. **Short Data Spans:** 6 countries have <1 week of load data (MD, MK, BA, CY, RS, ME)
2. **Outdated Data:** GB and UA have data from 2019-2022 only

### Missing Data
- **3 countries** have no data at all (IS, MT, TR)
- **11 countries** missing price data
- **10 countries** missing renewable data
- **12 countries** missing weather data (though 28 have complete coverage)

See `database_completeness.md` for detailed analysis and recommendations.
