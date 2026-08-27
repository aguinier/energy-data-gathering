> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

## ENTSO-E Data Gathering Pipeline

> **📖 For complete pipeline documentation, see [PIPELINE.md](./PIPELINE.md)**
>
> PIPELINE.md contains detailed information about:
> - Architecture diagrams and component details
> - Complete data flow with step-by-step execution
> - API integration specifics and error handling
> - Operations guide and troubleshooting
> - Configuration reference and best practices

### Overview
Automated pipeline for gathering energy data (load, price, renewable) from the ENTSO-E Transparency Platform API.

**Pipeline Features:**
- **Backfill Mode:** Fetch historical data for configurable date ranges
- **Update Mode:** Four passes daily (00:30/06:30/13:30/18:30 UTC, `docker/crontab`)
  fetching the last 7 days. Captures new publications and delayed uploads. **It does
  NOT capture revisions** — see "The 7-day window is shorter than the revision horizon"
  below. (`update.py`'s docstring still says "hourly"; that has not been true since the
  crontab was written.)
- **Coverage:** All 39 countries with ENTSO-E domain codes
- **Error Handling:** Retry with exponential backoff, per-country error isolation
- **Logging:** Comprehensive logging to `data_ingestion_log` table and log files

### Installation

```bash
# Install Python dependencies
pip install -r requirements.txt

# Verify configuration (checks API key, database, etc.)
python config.py
```

### Running the tests

```bash
C:\Users\guill\miniconda3\python.exe -m pytest -q tests/
```

**Name that interpreter explicitly.** This repo has no `.venv`, and two of the three
Pythons on the workstation cannot run the suite at all. Measured 2026-08-14 against
`origin/main` at `08358b0`:

| interpreter | result |
|---|---|
| `C:\Users\guill\miniconda3\python.exe` (3.11.4) | **287 passed in 28.96s** |
| `python` first on `PATH` (`C:\Python314`, 3.14.3) | `No module named pytest` — and no `entsoe` or `requests` either, so `python config.py` in Installation above does not run as written on this box |
| `../energy-forecast/.venv` | **10 collection errors**, `ModuleNotFoundError: No module named 'entsoe'` / `'requests'` |

**The `.venv` failure is the dangerous one, because it looks like a red suite rather
than a wrong command.** It reports 10 of the 17 test files as `ERROR` — among them
`test_published_points.py`, `test_generation_published_points.py` and
`test_net_position_published_points.py`, the three guards this file spends its
longest section on. Reaching for that `.venv` is the natural mistake, because the
neighbouring `energy-forecast` repo really does keep its dependencies there.

The tell that separates it from a genuine regression: **a collection error names a
module, never an assertion**, and it is counted in the `N errors` line rather than
in `N failed`. If no test name appears in the output, you have the wrong interpreter.

`requirements.txt` pins a floor, not a version (`entsoe-py>=0.7.8`), so the resolved
library differs by environment: **0.7.11** under miniconda here, against the **0.8.0**
whose sparse-document forward-fill is the entire reason for the published-points
guards below. The suite is fixture-driven and asserts against stored documents rather
than against the library's parsing, so this does not change a result today — but a
test written against entsoe-py's own behaviour would pass here and prove nothing
about production.

### Pipeline Commands

**Backfill historical data:**
```bash
# Backfill all data types for all countries from 2024 (includes load forecasts)
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types all --countries all

# Backfill only load data for Germany
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types load --countries DE

# Backfill ENTSO-E load forecasts (day-ahead and week-ahead)
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 \
  --types load_forecast_day_ahead,load_forecast_week_ahead --countries DE,FR,BE

# Backfill only day-ahead forecasts for all countries
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 \
  --types load_forecast_day_ahead --countries all

# Backfill high-priority countries only
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types all --priority 1

# Use default backfill periods from config.py (from 2019 for load & forecasts)
python scripts/backfill.py --use-defaults --types all --countries all
```

**Regular updates (for cron):**
```bash
# Standard update (last 7 days, all data types including forecasts)
python scripts/update.py

# Update last 14 days
python scripts/update.py --days 14

# Update only load and price data
python scripts/update.py --types load,price

# Update only load forecasts
python scripts/update.py --types load_forecast_day_ahead,load_forecast_week_ahead

# Update specific countries
python scripts/update.py --countries DE,FR,IT
```

**Setup hourly cron job:**
```bash
# Interactive setup for hourly updates
bash scripts/scheduler_setup.sh

# Manual cron entry (runs at minute 15 every hour)
# 15 * * * * cd /path/to/data_gathering && python3 scripts/update.py >> logs/cron_update.log 2>&1
```

### Pipeline Architecture

```
ENTSO-E API → ENTSOEClient → Data Fetchers → Database
                    ↓              ↓              ↓
              Rate Limiting   Transformers   Upsert Logic
              Retry Logic     Validators     Logging
```

**Components:**
- `config.py` - API configuration, ENTSO-E endpoints, PSR type mappings
- `utils.py` - Date handling, logging, validation
- `src/entsoe_client.py` - API wrapper with rate limiting & retry
- `src/fetch_*.py` - Data fetchers for load/price/renewable
- `src/db.py` - Database operations, upsert, logging
- `src/pipeline.py` - Main orchestrator

### Configuration

**API Key:** Set in `.env` file:
```
api_key_entsoe=your_api_key_here
```

**Backfill Defaults** (in `config.py`):
- Load: 2019-01-01 (5 years)
- Price: 2021-01-01 (4 years)
- Renewable: 2021-01-01 (4 years)
- Load Forecast (Day-Ahead): 2019-01-01 (matches load data)
- Load Forecast (Week-Ahead): 2019-01-01 (matches load data)

**Update Settings:**
- Days back: 7 (captures delayed uploads)
- Rate limit: 300 requests/minute (safe buffer)
- Max retries: 3 with exponential backoff

### Pipeline Behavior

**Idempotency:**
- Uses `INSERT OR REPLACE` for safe re-runs
- Unique indexes prevent duplicates
- Can safely re-run backfill without checking existing data

**Error Handling:**
- Failed country doesn't stop pipeline
- Errors logged to `data_ingestion_log` table
- Continues with remaining countries
- Retry 3 times with delays: 1s, 2s, 4s

**Date Chunking:**
- **Backfill** splits large ranges into **90-day** chunks (`pipeline.py:73`;
  `utils.get_date_range`'s default, `utils.py:129`) — not 365, as this file used to say.
  ENTSO-E's own limit is ~1 year; 90 days is chosen to avoid year-boundary issues in
  some bidding zones (`utils.py:116`).
- **Update does not chunk at all.** `pipeline.run_update` computes one `(start, end)` and
  calls `_fetch_data_chunk` once per (country, data type). Widening `--days` therefore
  costs **zero** extra ENTSO-E requests — only a bigger payload and more rows upserted.
- Allows resume after interruption

**Known Issues:**
- Countries IS, MT, TR have no ENTSO-E data (will be skipped)
- Some countries have delayed data publication. The 7-day update window does **not**
  handle this on its own — a zone dark for longer than 7 days is never re-requested once
  the outage slides out of the window. That is what `scripts/catchup.py` exists for
  (ABL-84/ABL-85), weekly, and it covers `energy_load` only.

### The 7-day window is shorter than the revision horizon (ABL-442)

**`UPDATE_DAYS_BACK = 7` (`config.py:211`), but ENTSO-E keeps revising a value for about
28 days.** So the routine job re-fetches inside a window where the upstream number has not
settled, and then never looks again. Every row freezes on whichever vintage was current
~7 days after delivery, and which vintage a given row holds is decided by **when someone
last ran an ad-hoc backfill** — not by policy.

This is the same constant as ABL-85, one defect over: that one was a coverage hole, this
one is a silent value change on rows we already have.

Measured read-only on the replica 2026-08-14 (`scripts/abl442_revision_horizon_probe.py`,
full write-up in `reports/abl_442_revision_horizon.md`):

- The boundary is **28.00 days**, located from the data. `energy_generation`'s stored
  level is flat against age at fetch through 28 days and then steps.
- **98,582 rows (3.10%) sit on the unrevised side** — every target from **2026-07-01**
  onward, i.e. the window every gate and accuracy read uses.
- **10 (country, column) pairs confirm** against an independent third series (the TSO
  day-ahead forecast). The revision is **not uniformly upward**: NL `wind_onshore` is
  2.15x, CY `solar` is **0.19x**.
- **`energy_load` revises too**, at +2.3% (largest placebo movement 0.63%).
  `energy_price` is bounded, not measured — no same-quantity control exists.
- **`energy_renewable` is 100% unrevised**, because no ad-hoc backfill writes it. That is
  what makes it usable as a control, and it means the two generation tables can disagree
  for a reason that is nothing to do with their column mapping.

**Two traps this leaves for anyone reading these tables:**

1. **`fetched_at` / `created_at` date the LAST write, not the first.** Both are set to
   `CURRENT_TIMESTAMP` inside an `INSERT OR REPLACE`, which deletes and re-inserts. So
   `fetched_at - timestamp_utc` is "how old was this instant when we last asked about
   it" — which is exactly the diagnostic, but is *not* a provenance record of first
   capture.
2. **`scripts/catchup.py` does not help here.** It is `energy_load`-only
   (`catchup.py:71`) and targets interior holes — instants we are *missing* — so it never
   re-fetches a row we already hold. A revision is a value change on a present row.

**No behaviour has been changed.** A weekly settle pass over a trailing 42 days
(`+1.3%` requests, `+11%` writes) is proposed on ABL-442 and awaits a CEO decision, as
does the one-time reconciliation of the already-frozen rows — which **rewrites published
history in both directions** and therefore follows the ABL-85 norm.

### Monitoring & Logs

**Log Files:**
- `logs/pipeline.log` - All pipeline activity
- `logs/cron_update.log` - Cron job output

**Credentials are scrubbed out of both, and that is not optional** (ABL-86).
entsoe-py puts the API key in the request URL as `securityToken=`, and
`requests` builds an `HTTPError`'s message out of the **full** URL — so every
failed request handed us an exception whose `str()` was our credential, and
each of the ~128 `logger.error(f"...: {e}")` sites wrote it verbatim into these
two files. On prod they are ~215 MB and ~70 MB, owned by root, rotate slowly,
and exist to be tailed and pasted into diagnostics. The key was at rest in
cleartext and travelled into every shared excerpt.

`src/log_redaction.py` is the fix, and it is deliberately **central** — the
string we must not print is one we never construct, so a rule at the call site
would have to be remembered at all 128 of them and at the 129th. Four ways in,
because no single one covers everything:

- `redact_secrets()` rewrites `<name>=<value>` for any name in
  `SECRET_QUERY_PARAMS`, plus the quoted mapping form entsoe-py logs at DEBUG.
  Needs no knowledge of the key, so it works in a test and after a rotation.
- `register_secret_value()` scrubs a literal value in *any* shape.
  `ENTSOEClient.__init__` registers the key it was handed
  (`src/entsoe_client.py:233`).
- `SecretRedactingFilter`, installed by `install_secret_redaction()` on the
  **handlers** of the root and `entsoe_pipeline` loggers — a handler filter
  sees every record reaching it, including entsoe-py's and urllib3's, which a
  logger filter would not. Called from `utils.setup_logging()` and from
  `ENTSOEClient.__init__`, because the scripts configure logging three
  different ways (`setup_logging`, `logging.basicConfig`, not at all).
- `redact_exception()` at `_make_request`'s four except branches
  (`src/entsoe_client.py:288`, `:293`, `:298`, `:303`), rewriting `args` in
  place — every ENTSO-E request in the module funnels through there. The only
  thing covering an **uncaught** exception: the interpreter prints that
  traceback to stderr itself, and cron's `2>&1` puts it in the same file. It
  walks `__cause__`/`__context__` too, since `raise ... from e` prints the
  original `requests.HTTPError` underneath ours.

Verified 2026-08-09 with a deliberately invalid token against the live API: the
401 reaches `logs/`, stdout, the raised exception, its chained cause and the
uncaught 16-frame traceback as `securityToken=<redacted>`, with
`documentType`, `outBiddingZone_Domain` and `periodStart` all intact. **Keeping
the rest of the URL is a requirement, not an accident** — scrubbing the whole
URL would close the hole by making the log useless, and the log is how ABL-84
was diagnosed. `tests/test_log_redaction.py` pins both halves.

Two things this does **not** do, both escalated to the Board on ABL-86: it does
not scrub the log files already written on prod, and it does not rotate the
key. Treat the existing prod logs as still containing the credential.

**Database Logging:**
```sql
-- Check recent pipeline runs
SELECT pipeline_type, country_code, status,
       records_inserted, start_time, end_time
FROM data_ingestion_log
ORDER BY start_time DESC
LIMIT 20;

-- Check for failures
SELECT * FROM data_ingestion_log
WHERE status = 'failed'
ORDER BY start_time DESC;
```

**Data Freshness:**
```sql
-- Check latest data timestamps
SELECT * FROM latest_data_by_country
WHERE country_code = 'DE';
```

### Maintenance Tasks

**After backfill:**
```bash
# Update query statistics
sqlite3 energy_dashboard.db "ANALYZE;"

# Optimize database (if needed)
sqlite3 energy_dashboard.db "VACUUM;"
```

**Regular monitoring:**
- Check `data_ingestion_log` for failures
- Monitor log files for errors
- Verify `completeness_cache` is updated
