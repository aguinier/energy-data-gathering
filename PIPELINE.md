# ENTSO-E Data Pipeline Documentation

**Version:** 1.0.0
**Last Updated:** 2025-12-22

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Component Details](#component-details)
4. [Data Flow](#data-flow)
5. [API Integration](#api-integration)
6. [Error Handling & Resilience](#error-handling--resilience)
7. [Configuration Guide](#configuration-guide)
8. [Operations Guide](#operations-guide)
9. [Troubleshooting](#troubleshooting)
10. [Best Practices](#best-practices)

---

## Overview

### Purpose

The ENTSO-E Data Pipeline is an automated system designed to:
- Fetch electricity market data from the ENTSO-E Transparency Platform API
- Store data in a SQLite database for analysis
- Handle both historical backfill and regular updates
- Process data for 39 European countries across three data types

### Data Types

| Data Type | ENTSO-E Doc Type | Description | Update Frequency |
|-----------|------------------|-------------|------------------|
| **Load** | A65 | Actual total electricity demand (MW) | Hourly |
| **Price** | A44 | Day-ahead market prices (EUR/MWh) | Daily |
| **Renewable** | A75 | Generation by production type (MW) | 15-min to hourly |

### Key Features

- ✅ **Idempotent:** Safe to re-run without creating duplicates
- ✅ **Resilient:** Automatic retry with exponential backoff
- ✅ **Scalable:** Processes 39 countries in parallel-capable chunks
- ✅ **Monitored:** Comprehensive logging to database and files
- ✅ **Configurable:** Flexible date ranges and country selection

---

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         User Interface                           │
│                    (CLI Scripts: backfill.py, update.py)        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Pipeline Orchestrator                       │
│                        (pipeline.py)                             │
│                                                                   │
│  • Manages country iteration                                     │
│  • Coordinates data type fetching                                │
│  • Tracks progress and statistics                                │
│  • Updates completeness cache                                    │
└────────────┬────────────────┬────────────────┬───────────────────┘
             │                │                │
             ▼                ▼                ▼
    ┌────────────┐   ┌────────────┐   ┌────────────┐
    │ Load       │   │ Price      │   │ Renewable  │
    │ Fetcher    │   │ Fetcher    │   │ Fetcher    │
    └─────┬──────┘   └─────┬──────┘   └─────┬──────┘
          │                │                │
          └────────────────┴────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │   ENTSO-E API Client   │
              │  (entsoe_client.py)    │
              │                        │
              │  • Rate limiting       │
              │  • Retry logic         │
              │  • Response parsing    │
              └───────────┬────────────┘
                          │
                          ▼
              ┌────────────────────────┐
              │    ENTSO-E API         │
              │  (Transparency Platform)│
              └────────────────────────┘

              ┌────────────────────────┐
              │   Database Layer       │
              │      (db.py)           │
              │                        │
              │  • Upsert operations   │
              │  • Ingestion logging   │
              │  • Cache management    │
              └───────────┬────────────┘
                          │
                          ▼
              ┌────────────────────────┐
              │   SQLite Database      │
              │ (energy_dashboard.db)  │
              └────────────────────────┘
```

### Component Layers

1. **User Interface Layer** - CLI scripts for interaction
2. **Orchestration Layer** - Pipeline coordinator
3. **Fetcher Layer** - Data-type-specific fetchers
4. **Client Layer** - ENTSO-E API wrapper
5. **Database Layer** - Data persistence
6. **Support Layer** - Configuration, utilities, logging

---

## Component Details

### 1. Configuration (`config.py`)

**Purpose:** Centralized configuration for all pipeline components.

**Key Configurations:**

```python
# API Configuration
ENTSOE_API_CONFIG = {
    'load': {
        'document_type': 'A65',      # ENTSO-E document type
        'entsoe_method': 'query_load',
        'table': 'energy_load',
        'value_column': 'load_mw'
    },
    # ... price and renewable configs
}

# Rate Limiting
REQUESTS_PER_MINUTE = 300  # Conservative (ENTSO-E allows ~400)
REQUEST_DELAY_SECONDS = 0.2

# Retry Configuration
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = [1, 2, 4]

# Update Configuration
UPDATE_DAYS_BACK = 7  # Fetch last 7 days for updates
```

**Functions:**
- `get_api_config(data_type)` - Get configuration for specific data type
- `validate_config()` - Validate configuration on startup
- `validate_value(value, data_type)` - Validate data within reasonable limits

---

### 2. Utilities (`utils.py`)

**Purpose:** Common utility functions used across the pipeline.

**Key Functions:**

```python
# Date/Time Utilities
parse_date(date_str) → datetime
to_utc(dt, timezone) → datetime (UTC)
get_date_range(start, end, chunk_days) → List[Tuple[datetime, datetime]]
get_recent_date_range(days_back) → Tuple[datetime, datetime]

# Data Validation
validate_dataframe(df, required_columns) → bool
validate_energy_value(value, data_type) → bool
remove_outliers(df, value_column, data_type) → DataFrame

# Data Transformation
calculate_renewable_total(row) → float
ensure_timezone_aware(series, timezone) → Series

# Logging
setup_logging(log_level, log_file) → logger

# Progress Tracking
ProgressTracker(total, description) - Simple progress tracker
```

**Date Chunking Strategy:**
- Large date ranges split into 365-day chunks
- Prevents ENTSO-E API timeout errors
- Each chunk is an independent API request

---

### 3. ENTSO-E API Client (`src/entsoe_client.py`)

**Purpose:** Wrapper around the `entsoe-py` library with enhanced features.

**Enhanced Features:**

1. **Rate Limiting:**
   ```python
   def _rate_limit(self):
       """Enforce delay between requests"""
       elapsed = time.time() - self.last_request_time
       if elapsed < self.request_delay:
           time.sleep(self.request_delay - elapsed)
   ```

2. **Retry Logic:**
   ```python
   @retry(
       stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=1, max=10),
       retry=retry_if_exception_type((ConnectionError, TimeoutError))
   )
   def _make_request(self, method, *args, **kwargs):
       # Rate limit, then make request
   ```

3. **Error Handling:**
   - `NoMatchingDataError` → `ENTSOENoDataError` (graceful skip)
   - `InvalidPSRTypeError` → Logged and raised
   - `ConnectionError` → Retried with backoff

**Key Methods:**

```python
query_load(country_code, start, end) → DataFrame
    # Fetches actual total load (A65)

query_day_ahead_prices(country_code, start, end) → DataFrame
    # Fetches day-ahead prices (A44)

query_generation_per_type(country_code, start, end) → DataFrame
    # Fetches generation by production type (A75)
    # Maps ENTSO-E PSR types to our renewable columns
```

**PSR Type Mapping (Renewable):**

ENTSO-E returns generation data with PSR (Production Source Register) types. The client maps these to our database columns:

```python
'B16': 'solar_mw',              # Solar
'B19': 'wind_onshore_mw',       # Wind Onshore
'B18': 'wind_offshore_mw',      # Wind Offshore
'B10': 'hydro_run_mw',          # Hydro Run-of-river
'B11': 'hydro_reservoir_mw',    # Hydro Reservoir
'B01': 'biomass_mw',            # Biomass
'B09': 'geothermal_mw',         # Geothermal
```

---

### 4. Data Fetchers (`src/fetch_*.py`)

**Purpose:** Data-type-specific logic for fetching and storing data.

**Pattern (all fetchers follow this):**

```python
def fetch_<type>_data(client, country_code, start, end, log_id):
    """
    1. Query ENTSO-E API via client
    2. Validate returned DataFrame
    3. Upsert to database
    4. Return statistics
    """
    try:
        # Query API
        df = client.query_<type>(country_code, start, end)

        if df is None or df.empty:
            return 0, 0, 0  # No data

        # Upsert to database
        inserted, updated = db.upsert_<type>_data(df, country_code)

        return inserted, updated, 0  # Success

    except ENTSOENoDataError:
        return 0, 0, 0  # No data available (not an error)

    except Exception as e:
        log_error(e)
        return 0, 0, 1  # Failed
```

**Fetcher-Specific Logic:**

- **Load Fetcher:** Direct mapping, validates MW values
- **Price Fetcher:** Allows negative prices (renewable surplus scenarios)
- **Renewable Fetcher:**
  - Maps multiple PSR types to columns
  - Calculates `total_renewable_mw`
  - Sets `fetched_at` timestamp for revision tracking

---

### 5. Database Layer (`src/db.py`)

**Purpose:** All database interactions and data persistence.

**Key Operations:**

**Connection Management:**
```python
@contextmanager
def get_connection():
    """Context manager for safe database connections"""
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        yield conn
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()
```

**Upsert Operations:**
```python
def upsert_load_data(df, country_code):
    """
    Uses INSERT OR REPLACE for idempotency
    Unique index on (country_code, timestamp_utc) prevents duplicates
    """
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO energy_load
            (country_code, timestamp_utc, load_mw, data_quality)
            VALUES (?, ?, ?, 'actual')
        """, (country_code, row['timestamp_utc'], row['load_mw']))
```

**Ingestion Logging:**
```python
def log_ingestion_start(pipeline_type, country_code):
    """Create log entry, return log_id"""

def log_ingestion_complete(log_id, records_inserted, error_message):
    """Update log entry with results"""
```

**Completeness Cache:**
```python
def update_completeness_cache():
    """
    Updates cache with latest data quality metrics
    Simplified version - records counts and latest timestamps
    """
```

---

### 6. Pipeline Orchestrator (`src/pipeline.py`)

**Purpose:** Coordinates the entire data fetching process.

**Class: ENTSOEPipeline**

```python
class ENTSOEPipeline:
    def __init__(self):
        self.client = ENTSOEClient()
        self.stats = {
            'total_records': 0,
            'successful_countries': 0,
            'failed_countries': 0,
            'by_data_type': {}
        }

    def run_backfill(start_date, end_date, data_types, countries):
        """Backfill mode: historical data"""

    def run_update(days_back, data_types, countries):
        """Update mode: recent data"""
```

**Backfill Mode Workflow:**

1. **Parse Arguments**
   - Date range: start_date to end_date
   - Data types: load, price, renewable (or all)
   - Countries: specific codes or all

2. **Get Countries**
   ```python
   countries = db.get_countries(priority=None)  # or filtered
   ```

3. **Split Date Range**
   ```python
   date_chunks = utils.get_date_range(start_date, end_date, chunk_days=365)
   # Example: 2021-01-01 to 2024-12-31 → 4 chunks
   ```

4. **Process Each Country**
   ```python
   for country in countries:
       for data_type in data_types:
           for start, end in date_chunks:
               fetch_data_chunk(data_type, country_code, start, end)
   ```

5. **Update Completeness Cache**
   ```python
   db.update_completeness_cache()
   ```

6. **Print Summary**
   - Total countries processed
   - Records inserted
   - Success/failure breakdown

**Update Mode Workflow:**

1. **Calculate Recent Range**
   ```python
   start, end = utils.get_recent_date_range(days_back=7)
   # Example: 2024-12-15 to 2024-12-22
   ```

2. **Process All Countries**
   - Same as backfill but with recent date range
   - No date chunking needed (7 days is small)

3. **Update & Summarize**

---

### 7. CLI Scripts

**`scripts/backfill.py`**

Command-line interface for historical data backfill.

```bash
python scripts/backfill.py \
    --start 2024-01-01 \
    --end 2024-12-31 \
    --types load,price,renewable \
    --countries all \
    --priority 1 \
    --log-level INFO
```

**Arguments:**
- `--start`, `--end`: Date range (YYYY-MM-DD)
- `--types`: Comma-separated data types or "all"
- `--countries`: Comma-separated country codes or "all"
- `--priority`: Filter by priority (1=high, 2=medium, 3=low)
- `--use-defaults`: Use default backfill periods from config
- `--log-level`: DEBUG, INFO, WARNING, ERROR

**`scripts/update.py`**

Regular update script for cron execution.

```bash
python scripts/update.py \
    --days 7 \
    --types all \
    --countries all \
    --log-level INFO
```

**Arguments:**
- `--days`: Number of days to go back (default: 7)
- `--types`: Data types to update (default: all)
- `--countries`: Countries to update (default: all)
- `--log-level`: Logging level

**`scripts/scheduler_setup.sh`**

Interactive setup for cron job.

**Features:**
- Checks Python installation
- Verifies dependencies
- Creates cron entry for hourly execution
- Optional log rotation setup
- Backs up existing crontab

---

## Data Flow

### Complete Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. USER INITIATES                                                │
│    python scripts/backfill.py --start 2024-01-01 --end 2024-12-31│
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. ARGUMENT PARSING & VALIDATION                                │
│    • Parse dates (2024-01-01 → datetime)                        │
│    • Validate data types (load, price, renewable)               │
│    • Expand "all" → all 39 countries                            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. PIPELINE INITIALIZATION                                       │
│    pipeline = ENTSOEPipeline()                                   │
│    • Initialize ENTSO-E client with API key                      │
│    • Setup logging                                               │
│    • Initialize statistics tracking                              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. GET COUNTRIES FROM DATABASE                                   │
│    countries = db.get_countries()                                │
│    • Query: SELECT country_code, entsoe_domain FROM countries   │
│    • Returns: List of 39 countries with ENTSO-E domains          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. DATE RANGE CHUNKING                                           │
│    date_chunks = get_date_range('2024-01-01', '2024-12-31')    │
│    • Splits into 365-day chunks                                  │
│    • Returns: [(2024-01-01, 2024-12-31)]  # 1 chunk             │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. COUNTRY ITERATION LOOP                                        │
│    for country in countries:                                     │
│        ├─ Check if no-data country (IS, MT, TR) → Skip          │
│        ├─ Check if problematic (IT, MD, etc.) → Warn            │
│        └─ Process country                                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 7. DATA TYPE ITERATION LOOP                                      │
│    for data_type in ['load', 'price', 'renewable']:            │
│        └─ Process data type for current country                  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 8. DATE CHUNK ITERATION LOOP                                     │
│    for (start, end) in date_chunks:                             │
│        └─ Fetch data for this chunk                              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 9. FETCH DATA CHUNK                                              │
│    fetch_load.fetch_load_data(client, 'DE', start, end)        │
│    ├─ Call appropriate fetcher based on data type               │
│    └─ Return (inserted, updated, failed)                         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 10. QUERY ENTSO-E API                                            │
│     client.query_load('DE', start, end)                         │
│     ├─ Rate limit check (wait if needed)                        │
│     ├─ Make API request with retry logic                        │
│     ├─ Parse XML response → pandas DataFrame                    │
│     └─ Return DataFrame with columns: [timestamp_utc, load_mw]  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 11. DATA VALIDATION & TRANSFORMATION                             │
│     • Ensure timestamps are UTC                                  │
│     • Remove outliers (e.g., load_mw > 100,000)                 │
│     • For renewable: calculate total_renewable_mw                │
│     • For renewable: map PSR types to columns                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 12. UPSERT TO DATABASE                                           │
│     db.upsert_load_data(df, 'DE')                               │
│     ├─ Convert timestamps to SQLite format                       │
│     ├─ For each row:                                             │
│     │   INSERT OR REPLACE INTO energy_load                       │
│     │   (country_code, timestamp_utc, load_mw, data_quality)    │
│     │   VALUES ('DE', '2024-01-01 00:00:00', 45000.0, 'actual') │
│     └─ Return count of records affected                          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 13. LOG TO data_ingestion_log                                    │
│     • Start: log_ingestion_start('load', 'DE')                  │
│     • End: log_ingestion_complete(log_id, inserted=24, failed=0)│
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 14. UPDATE STATISTICS                                            │
│     stats['total_records'] += inserted                           │
│     stats['by_data_type']['load']['success'] += 1               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 15. CONTINUE TO NEXT CHUNK/TYPE/COUNTRY                          │
│     • Repeat steps 8-14 for next chunk                          │
│     • Repeat steps 7-14 for next data type                      │
│     • Repeat steps 6-14 for next country                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 16. UPDATE COMPLETENESS CACHE                                    │
│     db.update_completeness_cache()                               │
│     • Calculate latest timestamps for each country/data type     │
│     • Update completeness_cache table                            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 17. PRINT SUMMARY                                                │
│     ✓ Backfill complete!                                        │
│     Total countries: 39 (38 successful, 1 failed)               │
│     Total records inserted: 850,320                              │
│     By data type:                                                │
│       load: 38 successful, 1 failed                             │
│       price: 37 successful, 2 failed                            │
│       renewable: 35 successful, 4 failed                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## API Integration

### ENTSO-E API Basics

**Base URL:** `https://web-api.tp.entsoe.eu/api`

**Authentication:** API key in query parameter
```
?securityToken=your_api_key_here
```

**Request Format:**
```
GET /api?
    documentType=A65&
    processType=A16&
    outBiddingZone_Domain=10Y1001A1001A83F&  # Germany
    periodStart=202401010000&
    periodEnd=202401020000&
    securityToken=your_key
```

**Response Format:** XML

### Document Types Used

| Data Type | Doc Type | Process Type | Description |
|-----------|----------|--------------|-------------|
| Load | A65 | A16 | Actual total load (realised) |
| Price | A44 | A01 | Day ahead prices |
| Renewable | A75 | A16 | Actual generation per type |

### Rate Limiting

**ENTSO-E Limits:**
- ~400 requests per minute
- No daily limit documented

**Our Implementation:**
- 300 requests per minute (safe buffer)
- 0.2 seconds between requests
- Client-side throttling

### Error Responses

**Common Errors:**
1. **NoMatchingDataError** - Data not available for date range
   - **Our handling:** Log warning, return None, continue

2. **401 Unauthorized** - Invalid API key
   - **Our handling:** Raise error, stop pipeline

3. **429 Too Many Requests** - Rate limit exceeded
   - **Our handling:** Exponential backoff, retry

4. **500 Server Error** - ENTSO-E server issue
   - **Our handling:** Retry up to 3 times

### Response Parsing

**entsoe-py library handles:**
- XML parsing
- Timezone conversion
- Data structure normalization
- Period handling (start/end)

**We handle:**
- PSR type mapping (renewable data)
- Column renaming to match our schema
- UTC timezone enforcement
- Outlier removal

---

## Error Handling & Resilience

### Multi-Layer Error Handling

**1. API Level (entsoe_client.py)**

```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10)
)
def _make_request(method, *args, **kwargs):
    try:
        result = method(*args, **kwargs)
        return result
    except NoMatchingDataError:
        raise ENTSOENoDataError("No data available")
    except ConnectionError:
        # Will be retried by decorator
        raise
```

**Retry Schedule:**
- Attempt 1: Immediate
- Attempt 2: Wait 1 second
- Attempt 3: Wait 2 seconds
- Attempt 4: Wait 4 seconds
- Give up after 3 retries

**2. Fetcher Level (fetch_*.py)**

```python
def fetch_load_data(client, country_code, start, end):
    try:
        df = client.query_load(country_code, start, end)
        # ... process ...
        return inserted, updated, 0  # Success
    except ENTSOENoDataError:
        return 0, 0, 0  # No data, not an error
    except Exception as e:
        logger.error(f"Failed: {e}")
        return 0, 0, 1  # Failed
```

**Key Decision:** Failed country doesn't stop pipeline

**3. Pipeline Level (pipeline.py)**

```python
for country in countries:
    try:
        success = fetch_data_chunk(data_type, country_code, start, end)
        if success:
            successful_countries += 1
        else:
            failed_countries += 1
    except Exception as e:
        logger.error(f"Country {country_code} failed: {e}")
        failed_countries += 1
        continue  # Continue with next country
```

**4. Database Level (db.py)**

```python
@contextmanager
def get_connection():
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        yield conn
        conn.commit()  # Success
    except Exception as e:
        if conn:
            conn.rollback()  # Rollback on error
        raise
    finally:
        if conn:
            conn.close()  # Always close
```

### Error Isolation Strategy

**Per-Country Isolation:**
- Country A fails → Countries B-Z continue
- Logged to `data_ingestion_log` with error details
- Final summary shows success/failure breakdown

**Per-Data-Type Isolation:**
- Load fetch fails → Price and Renewable still attempted
- Each data type logged separately

**Per-Chunk Isolation:**
- Chunk 1 (2024-01-01 to 2024-12-31) fails
- Chunk 2 (2025-01-01 to 2025-12-31) still runs
- Allows partial backfill success

### Logging Strategy

**Three Logging Destinations:**

1. **Console Output**
   - Real-time progress
   - INFO level and above
   - Formatted with timestamps

2. **File Logging** (`logs/pipeline.log`)
   - All levels (DEBUG to ERROR)
   - Persistent record
   - Rotation recommended

3. **Database Logging** (`data_ingestion_log`)
   - Per-country, per-data-type tracking
   - Start/end timestamps
   - Records inserted/updated/failed
   - Error messages

**Example Log Entry:**
```sql
INSERT INTO data_ingestion_log
(pipeline_type, country_code, start_time, end_time, status,
 records_inserted, records_updated, records_failed, error_message)
VALUES
('load', 'DE', '2024-12-22 10:00:00', '2024-12-22 10:05:23', 'completed',
 8760, 0, 0, NULL)
```

---

## Configuration Guide

### Environment Configuration

**`.env` file:**
```bash
# ENTSO-E API Key (required)
api_key_entsoe=your_api_key_here
```

**Obtaining API Key:**
1. Register at https://transparency.entsoe.eu/
2. Navigate to Account Settings
3. Generate new API key
4. Add to `.env` file

### Pipeline Configuration

**`config.py` - Key Settings:**

```python
# Database Path
DATABASE_PATH = BASE_DIR / "energy_dashboard.db"

# Rate Limiting (adjust if needed)
REQUESTS_PER_MINUTE = 300  # Increase up to 400 if stable
REQUEST_DELAY_SECONDS = 0.2

# Retry Configuration
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = [1, 2, 4]

# Update Settings
UPDATE_DAYS_BACK = 7  # Increase to 14 for more conservative updates

# Backfill Defaults
BACKFILL_DEFAULTS = {
    'load': '2019-01-01',
    'price': '2021-01-01',
    'renewable': '2021-01-01'
}

# Validation Limits (adjust based on data analysis)
VALIDATION_LIMITS = {
    'load_mw': {'min': 0, 'max': 100000},
    'price_eur_mwh': {'min': -500, 'max': 3000},
    'renewable_mw': {'min': 0, 'max': 50000}
}
```

### Logging Configuration

**Adjust in `config.py`:**
```python
LOG_LEVEL = 'INFO'  # DEBUG, INFO, WARNING, ERROR
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
```

**Adjust in scripts:**
```bash
python scripts/backfill.py --log-level DEBUG
```

---

## Operations Guide

### Initial Setup

**1. Install Dependencies:**
```bash
cd /path/to/data_gathering
pip install -r requirements.txt
```

**2. Configure API Key:**
```bash
echo "api_key_entsoe=your_key_here" > .env
```

**3. Verify Setup:**
```bash
python config.py
# Should output: ✓ Configuration validation passed!
```

### Running Backfill

**Step 1: Test with Single Country**
```bash
python scripts/backfill.py \
    --start 2024-12-20 \
    --end 2024-12-21 \
    --types load \
    --countries DE
```

**Step 2: Small Batch**
```bash
python scripts/backfill.py \
    --start 2024-12-01 \
    --end 2024-12-31 \
    --types load,price \
    --countries DE,FR,IT
```

**Step 3: Full Backfill**
```bash
python scripts/backfill.py \
    --use-defaults \
    --types all \
    --countries all
```

**Estimated Time:**
- Single country, single day: ~10 seconds
- All countries, 1 year, all types: ~6-8 hours
- Full backfill (5 years): ~24-36 hours

**Monitoring During Backfill:**
```bash
# Terminal 1: Run backfill
python scripts/backfill.py --use-defaults --types all --countries all

# Terminal 2: Monitor logs
tail -f logs/pipeline.log

# Terminal 3: Monitor database
watch -n 5 'sqlite3 energy_dashboard.db "SELECT COUNT(*) FROM energy_load"'
```

### Running Regular Updates

**Manual Update:**
```bash
python scripts/update.py
```

**Setup Cron:**
```bash
bash scripts/scheduler_setup.sh
```

**Verify Cron:**
```bash
crontab -l | grep update.py
```

**Monitor Cron Logs:**
```bash
tail -f logs/cron_update.log
```

### Resuming Failed Backfill

**Scenario:** Backfill stopped at country IT after processing DE, FR

**Solution:**
```bash
# Resume from IT onwards
python scripts/backfill.py \
    --start 2024-01-01 \
    --end 2024-12-31 \
    --types all \
    --countries IT,NL,ES,PT,...  # List remaining countries
```

**Alternative:** Re-run all countries (idempotent)
```bash
# Already processed countries will be updated (INSERT OR REPLACE)
python scripts/backfill.py \
    --start 2024-01-01 \
    --end 2024-12-31 \
    --types all \
    --countries all
```

### Database Maintenance

**After Large Backfill:**
```bash
# Update query statistics
sqlite3 energy_dashboard.db "ANALYZE;"

# Optimize database
sqlite3 energy_dashboard.db "VACUUM;"
```

**Regular Maintenance:**
```bash
# Monthly cleanup
sqlite3 energy_dashboard.db <<EOF
-- Remove very old ingestion logs (keep 90 days)
DELETE FROM data_ingestion_log
WHERE start_time < datetime('now', '-90 days');

-- Optimize
VACUUM;
ANALYZE;
EOF
```

---

## Troubleshooting

### Common Issues

**1. API Key Not Found**
```
ERROR: ENTSOE_API_KEY not found in environment variables
```

**Solution:**
```bash
# Check .env file exists
ls -la .env

# Check content
cat .env

# Should contain: api_key_entsoe=...
```

**2. No Data Returned for Country**
```
WARNING: No load data for MT (2024-01-01 to 2024-12-31)
```

**Explanation:** Country not in ENTSO-E network (IS, MT, TR)

**Action:** None needed, pipeline will skip

**3. Rate Limit Exceeded**
```
ERROR: 429 Too Many Requests
```

**Solution:**
```python
# In config.py, reduce rate
REQUESTS_PER_MINUTE = 200  # More conservative
REQUEST_DELAY_SECONDS = 0.3
```

**4. Connection Timeout**
```
ERROR: ConnectionError: Failed to fetch data
```

**Solution:**
- Check internet connection
- Verify ENTSO-E API status: https://transparency.entsoe.eu
- Retry will happen automatically (up to 3 times)

**5. Database Locked**
```
ERROR: database is locked
```

**Cause:** Another process is writing to database

**Solution:**
```bash
# Check for other Python processes
ps aux | grep python

# Kill competing process or wait for it to finish
```

**6. Outliers Removed**
```
WARNING: Removed 15 outliers from load_mw
```

**Explanation:** Values outside validation limits

**Action:**
- Check `VALIDATION_LIMITS` in config.py
- Adjust if legitimate values are being filtered
- Review ENTSO-E data quality for that country

### Debugging Tips

**Enable Debug Logging:**
```bash
python scripts/backfill.py --log-level DEBUG ...
```

**Check Database State:**
```sql
-- Latest data
SELECT * FROM latest_data_by_country WHERE country_code = 'DE';

-- Recent pipeline runs
SELECT * FROM data_ingestion_log
ORDER BY start_time DESC LIMIT 10;

-- Failed runs
SELECT * FROM data_ingestion_log
WHERE status = 'failed';

-- Record counts
SELECT
    (SELECT COUNT(*) FROM energy_load) as load_count,
    (SELECT COUNT(*) FROM energy_price) as price_count,
    (SELECT COUNT(*) FROM energy_renewable) as renewable_count;
```

**Test API Client Directly:**
```python
from src.entsoe_client import ENTSOEClient
import pytz
from datetime import datetime

client = ENTSOEClient()

# Test single query
start = pytz.UTC.localize(datetime(2024, 12, 20))
end = pytz.UTC.localize(datetime(2024, 12, 21))

df = client.query_load('DE', start, end)
print(df.head())
```

---

## Best Practices

### Development

1. **Test with Single Country First**
   - Always test changes with one country and short date range
   - Prevents large-scale issues

2. **Use Version Control**
   - Commit config changes
   - Tag releases (v1.0.0, v1.1.0, etc.)

3. **Monitor Logs During Development**
   - Keep `tail -f logs/pipeline.log` running
   - Catch issues early

### Production

1. **Incremental Backfill**
   - Start with recent data (2024)
   - Then expand backwards (2023, 2022, etc.)
   - Allows faster verification

2. **High-Priority Countries First**
   ```bash
   python scripts/backfill.py --priority 1 ...
   ```

3. **Regular Monitoring**
   - Check `data_ingestion_log` daily
   - Set up alerts for failures
   - Monitor disk space (database growth)

4. **Backup Before Major Operations**
   ```bash
   cp energy_dashboard.db energy_dashboard.db.backup.$(date +%Y%m%d)
   ```

### Performance

1. **Rate Limiting Balance**
   - Start conservative (300 req/min)
   - Increase if stable (up to 400 req/min)
   - Monitor for 429 errors

2. **Date Chunking**
   - Keep at 365 days (current setting)
   - Smaller chunks = more API calls but more resilient
   - Larger chunks = fewer calls but risk timeout

3. **Parallel Processing** (Future)
   - Current: Sequential country processing
   - Future: Process multiple countries in parallel
   - Requires careful rate limiting coordination

### Data Quality

1. **Validate After Backfill**
   ```sql
   -- Check for duplicates (should be 0)
   SELECT country_code, timestamp_utc, COUNT(*)
   FROM energy_load
   GROUP BY country_code, timestamp_utc
   HAVING COUNT(*) > 1;

   -- Check renewable totals match sum
   SELECT * FROM energy_renewable
   WHERE ABS(total_renewable_mw - (
       solar_mw + wind_onshore_mw + wind_offshore_mw +
       hydro_run_mw + hydro_reservoir_mw + biomass_mw +
       geothermal_mw + other_renewable_mw
   )) > 0.1;
   ```

2. **Update Completeness Cache Regularly**
   ```bash
   # After backfill
   sqlite3 energy_dashboard.db "SELECT 1"  # Trigger update via pipeline
   ```

3. **Review Data Gaps**
   - Check `database_completeness.md` regularly
   - Prioritize filling critical gaps (Italy price data)

---

## Appendix

### File Structure Reference

```
data_gathering/
├── .env                      # API key configuration
├── config.py                 # Pipeline configuration
├── utils.py                  # Utility functions
├── requirements.txt          # Python dependencies
├── energy_dashboard.db       # SQLite database
├── CLAUDE.md                 # Claude Code guidance
├── PIPELINE.md              # This document
├── database_structure.md     # Database schema
├── database_completeness.md  # Data quality analysis
├── README.md                 # User documentation
│
├── src/
│   ├── __init__.py
│   ├── db.py                 # Database operations
│   ├── entsoe_client.py      # API client
│   ├── fetch_load.py         # Load data fetcher
│   ├── fetch_price.py        # Price data fetcher
│   ├── fetch_renewable.py    # Renewable data fetcher
│   └── pipeline.py           # Main orchestrator
│
├── scripts/
│   ├── backfill.py           # Backfill CLI
│   ├── update.py             # Update CLI
│   └── scheduler_setup.sh    # Cron setup
│
└── logs/
    ├── .gitkeep
    ├── pipeline.log          # Main pipeline logs
    └── cron_update.log       # Cron job logs
```

### API Endpoints Reference

**ENTSO-E API Documentation:**
https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html

**Document Types:**
- A65: Actual Total Load
- A44: Day Ahead Prices
- A75: Actual Generation per Production Type

**Process Types:**
- A01: Day ahead
- A16: Realised

**PSR Types (Production):**
- B01: Biomass
- B09: Geothermal
- B10: Hydro Run-of-river
- B11: Hydro Water Reservoir
- B12: Hydro Pumped Storage
- B16: Solar
- B18: Wind Offshore
- B19: Wind Onshore

### Database Schema Quick Reference

**energy_load:**
```sql
CREATE TABLE energy_load (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code TEXT NOT NULL,
    timestamp_utc TIMESTAMP NOT NULL,
    load_mw REAL NOT NULL,
    data_quality TEXT DEFAULT 'actual',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    publication_timestamp_utc TIMESTAMP,
    UNIQUE(country_code, timestamp_utc)
);
```

**energy_price:**
```sql
CREATE TABLE energy_price (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code TEXT NOT NULL,
    timestamp_utc TIMESTAMP NOT NULL,
    price_eur_mwh REAL NOT NULL,
    data_quality TEXT DEFAULT 'actual',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    publication_timestamp_utc TIMESTAMP,
    UNIQUE(country_code, timestamp_utc)
);
```

**energy_renewable:**
```sql
CREATE TABLE energy_renewable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code TEXT NOT NULL,
    timestamp_utc TIMESTAMP NOT NULL,
    solar_mw REAL DEFAULT 0,
    wind_onshore_mw REAL DEFAULT 0,
    wind_offshore_mw REAL DEFAULT 0,
    hydro_run_mw REAL DEFAULT 0,
    hydro_reservoir_mw REAL DEFAULT 0,
    biomass_mw REAL DEFAULT 0,
    geothermal_mw REAL DEFAULT 0,
    other_renewable_mw REAL DEFAULT 0,
    total_renewable_mw REAL,
    data_quality TEXT DEFAULT 'actual',
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    publication_timestamp_utc TIMESTAMP,
    UNIQUE(country_code, timestamp_utc)
);
```

**energy_load_forecast:**
```sql
CREATE TABLE energy_load_forecast (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code TEXT NOT NULL,
    target_timestamp_utc TIMESTAMP NOT NULL,
    forecast_value_mw REAL NOT NULL,
    forecast_type TEXT NOT NULL,
    forecast_run_time TIMESTAMP,
    horizon_hours INTEGER,
    data_quality TEXT DEFAULT 'forecast',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    publication_timestamp_utc TIMESTAMP,
    UNIQUE(country_code, target_timestamp_utc, forecast_type)
);
```

---

**Document Version:** 1.0.0
**Last Updated:** 2025-12-22
**Maintained By:** Data Pipeline Team
