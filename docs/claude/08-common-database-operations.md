> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

## Common Database Operations

### Connecting to the Database

```python
import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('energy_dashboard.db')

# Query example
df = pd.read_sql_query("""
    SELECT * FROM energy_dashboard_data
    WHERE country_code = 'DE'
    LIMIT 100
""", conn)

conn.close()
```

### Essential Queries

**Check data availability:**
```sql
SELECT * FROM latest_data_by_country
WHERE country_code = 'DE';
```

**Get time-series data:**
```sql
SELECT timestamp_utc, load_mw, price_eur_mwh
FROM energy_dashboard_data
WHERE country_code = 'FR'
  AND timestamp_utc >= datetime('now', '-30 days')
ORDER BY timestamp_utc;
```

**Analyze renewable percentage:**
```sql
SELECT country_name, AVG(renewable_percentage) as avg_pct
FROM energy_dashboard_data
WHERE timestamp_utc >= datetime('now', '-30 days')
GROUP BY country_code, country_name
ORDER BY avg_pct DESC;
```

**Check forecast coverage:**
```sql
SELECT
    forecast_type,
    COUNT(*) as records,
    MIN(target_timestamp_utc) as earliest,
    MAX(target_timestamp_utc) as latest,
    COUNT(DISTINCT country_code) as countries
FROM energy_load_forecast
GROUP BY forecast_type;
```

**Compare forecast vs actual load:**
```sql
SELECT
    a.country_code,
    a.timestamp_utc,
    a.load_mw as actual_load,
    f.forecast_value_mw as day_ahead_forecast,
    ROUND(ABS(a.load_mw - f.forecast_value_mw), 2) as absolute_error_mw,
    ROUND(ABS(a.load_mw - f.forecast_value_mw) / a.load_mw * 100, 2) as error_pct
FROM energy_load a
JOIN energy_load_forecast f
    ON a.country_code = f.country_code
    AND a.timestamp_utc = f.target_timestamp_utc
    AND f.forecast_type = 'day_ahead'
WHERE a.country_code = 'DE'
    AND a.timestamp_utc >= '2024-12-01'
ORDER BY a.timestamp_utc
LIMIT 20;
```

**Calculate forecast accuracy by country:**
```sql
SELECT
    a.country_code,
    COUNT(*) as samples,
    ROUND(AVG(ABS(a.load_mw - f.forecast_value_mw)), 2) as avg_error_mw,
    ROUND(AVG(ABS(a.load_mw - f.forecast_value_mw) / a.load_mw * 100), 2) as avg_error_pct
FROM energy_load a
JOIN energy_load_forecast f
    ON a.country_code = f.country_code
    AND a.timestamp_utc = f.target_timestamp_utc
    AND f.forecast_type = 'day_ahead'
WHERE a.timestamp_utc >= datetime('now', '-7 days')
GROUP BY a.country_code
ORDER BY avg_error_pct;
```

### Database Maintenance Commands

**List all tables:**
```bash
sqlite3 energy_dashboard.db ".tables"
```

**Check database size:**
```bash
sqlite3 energy_dashboard.db "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size();"
```

**Optimize database:**
```bash
sqlite3 energy_dashboard.db "VACUUM;"
sqlite3 energy_dashboard.db "ANALYZE;"
```

**Export table to CSV:**
```bash
sqlite3 energy_dashboard.db ".headers on" ".mode csv" ".output data.csv" "SELECT * FROM countries;" ".quit"
```
