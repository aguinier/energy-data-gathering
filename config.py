"""
Configuration for ENTSO-E Energy Data Pipeline
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============================================================================
# PATHS
# ============================================================================
BASE_DIR = Path(__file__).parent
DATABASE_PATH = Path(os.getenv("ENERGY_DB_PATH", str(BASE_DIR / "energy_dashboard.db")))
LOGS_DIR = Path(os.getenv("ENERGY_LOGS_DIR", str(BASE_DIR / "logs")))

# ============================================================================
# API CONFIGURATION
# ============================================================================
ENTSOE_API_KEY = os.getenv("api_key_entsoe")

# API Rate Limiting
REQUESTS_PER_MINUTE = 300  # Conservative limit (ENTSO-E allows ~400/min)
REQUEST_DELAY_SECONDS = 60 / REQUESTS_PER_MINUTE  # ~0.2 seconds between requests

# Retry Configuration -- PER REQUEST, and narrower than it looks.
#
# MAX_RETRIES is tenacity's stop_after_attempt in ENTSOEClient._make_request
# (src/entsoe_client.py:255-260), whose `retry_if_exception_type` covers
# ConnectionError and TimeoutError ONLY. An HTTP 503 is neither, so the 484
# HTTP 503s of 2026-08-06 13:30 UTC were retried exactly zero times.
#
# RETRY_BACKOFF_SECONDS is read by nothing (`grep -rn RETRY_BACKOFF_SECONDS`
# returns this line, PIPELINE.md and nothing else) -- the waits actually used
# are tenacity's wait_exponential(min=1, max=10). Tuning it changes nothing;
# it is kept only because PIPELINE.md documents it.
#
# Neither knob addresses a multi-minute upstream outage: three attempts seconds
# apart all land inside it. That is what PASS_RETRY_DELAYS_SECONDS below is for.
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = [1, 2, 4]  # unreferenced; see above

# ============================================================================
# ENTSO-E API ENDPOINT CONFIGURATION
# ============================================================================
ENTSOE_API_CONFIG = {
    'load': {
        'name': 'Actual Total Load',
        'document_type': 'A65',
        'process_type': 'A16',  # Realised
        'table': 'energy_load',
        'value_column': 'load_mw',
        'entsoe_method': 'query_load',  # Method name in entsoe-py client
        'description': 'Electricity demand/consumption in megawatts'
    },
    'price': {
        'name': 'Day-Ahead Prices',
        'document_type': 'A44',
        'process_type': 'A01',  # Day ahead
        'table': 'energy_price',
        'value_column': 'price_eur_mwh',
        'entsoe_method': 'query_day_ahead_prices',
        'description': 'Day-ahead market prices in EUR/MWh',
        'is_dayahead': True  # Flag to indicate this data type supports D+1 fetching
    },
    'renewable': {
        'name': 'Generation per Production Type',
        'document_type': 'A75',
        'process_type': 'A16',  # Realised
        'table': 'energy_renewable',
        'entsoe_method': 'query_generation',
        'description': 'Renewable energy generation by source type',
        # PSR Type mappings (Production Source Register)
        # Maps ENTSO-E PSR types to our database columns
        'psr_types': {
            'B01': 'biomass_mw',           # Biomass
            'B09': 'geothermal_mw',        # Geothermal
            'B10': 'hydro_run_mw',         # Hydro Run-of-river and poundage
            'B11': 'hydro_reservoir_mw',   # Hydro Water Reservoir
            'B12': 'hydro_reservoir_mw',   # Hydro Pumped Storage (map to reservoir)
            'B16': 'solar_mw',             # Solar
            'B18': 'wind_offshore_mw',     # Wind Offshore
            'B19': 'wind_onshore_mw',      # Wind Onshore
        },
        # Renewable source columns in database
        'renewable_columns': [
            'solar_mw',
            'wind_onshore_mw',
            'wind_offshore_mw',
            'hydro_run_mw',
            'hydro_reservoir_mw',
            'biomass_mw',
            'geothermal_mw',
            'other_renewable_mw'
        ]
    },
    'load_forecast_day_ahead': {
        'name': 'Day-Ahead Load Forecast',
        'document_type': 'A65',
        'process_type': 'A01',  # Day ahead
        'table': 'energy_load_forecast',
        'value_column': 'forecast_value_mw',
        'forecast_type': 'day_ahead',
        'entsoe_method': 'query_load_forecast',
        'description': 'TSO day-ahead load forecasts (D+1)',
        'is_dayahead': True  # Flag to indicate this data type supports D+1 fetching
    },
    'load_forecast_week_ahead': {
        'name': 'Week-Ahead Load Forecast',
        'document_type': 'A65',
        'process_type': 'A31',  # Week ahead
        'table': 'energy_load_forecast',
        'value_column': 'forecast_value_mw',
        'forecast_type': 'week_ahead',
        'entsoe_method': 'query_load_forecast',
        'description': 'TSO week-ahead load forecasts (D+7)'
    },
    'wind_solar_forecast': {
        'name': 'Wind & Solar Generation Forecast',
        'document_type': 'A69',
        'process_type': 'A01',  # Day ahead
        'table': 'energy_generation_forecast',
        'entsoe_method': 'query_wind_and_solar_forecast',
        'description': 'Day-ahead wind and solar generation forecasts',
        'is_dayahead': True,  # Flag to indicate this data type supports D+1 fetching
        'forecast_columns': ['solar_mw', 'wind_onshore_mw', 'wind_offshore_mw']
    },
    'crossborder_flows': {
        'name': 'Cross-Border Physical Flows',
        'document_type': 'A11',
        'process_type': None,
        'table': 'crossborder_flows',
        'value_column': 'flow_mw',
        'entsoe_method': 'query_physical_crossborder_allborders',
        'description': 'Physical electricity flows between interconnected countries (MW)',
    },
    'net_position': {
        # DAY-AHEAD, not realized: fetch_net_position calls the client with
        # dayahead=True. Published per BIDDING ZONE, which is not always the
        # 2-letter code -- DE maps to DE_LU (the Core CCR zone covering DE+LU);
        # see ENTSOEClient.NET_POSITION_BIDDING_ZONES.
        'name': 'Day-Ahead Net Position',
        'document_type': 'A25',
        'process_type': None,
        'table': 'net_position',
        'value_column': 'net_position_mw',
        'entsoe_method': 'query_net_position',
        'description': 'Day-ahead net position per bidding zone (MW). Positive = exporter.',
        # Without this the pipeline caps the fetch window at now (pipeline.py's
        # `fetch_end = end` branch), so the D+1 market-coupling result -- the
        # whole point of an A25 day-ahead document -- was never requested. The
        # series stopped at the current hour and the dashboard had nothing to
        # show for tomorrow.
        'is_dayahead': True  # Flag to indicate this data type supports D+1 fetching
    },
}

# ============================================================================
# FULL A75 GENERATION MAPPING (energy_generation)
# ============================================================================
# A75 ("Generation per production type") is already fetched with
# psr_type=None -- the whole document, not just renewables -- but the
# 'renewable' config block above (and _map_renewable_columns) only keeps 8
# columns and silently drops the rest (nuclear, fossil, waste, other).
#
# This map is the *complete* ENTSO-E column name -> energy_generation column
# mapping, alongside (not replacing) the renewable one. It is a straight 1:1
# mapping -- no folding -- so hydro_pumped_mw, marine_mw and
# energy_storage_mw get their own columns instead of being combined into
# hydro_reservoir_mw / other_renewable_mw the way the renewable mapping
# combines them.
#
# The 21 ENTSO-E column names below were measured live against the real API
# across DE/PL/ES (2026-07-29) -- do not guess additional ones; if ENTSO-E
# adds a new production type, entsoe_client._map_generation_columns logs it
# at WARNING rather than silently dropping it.
GENERATION_COLUMN_MAP = {
    'Biomass': 'biomass_mw',
    'Energy storage': 'energy_storage_mw',
    'Fossil Brown coal/Lignite': 'fossil_brown_coal_mw',
    'Fossil Coal-derived gas': 'fossil_coal_derived_gas_mw',
    'Fossil Gas': 'fossil_gas_mw',
    'Fossil Hard coal': 'fossil_hard_coal_mw',
    'Fossil Oil': 'fossil_oil_mw',
    'Fossil Oil shale': 'fossil_oil_shale_mw',
    'Fossil Peat': 'fossil_peat_mw',
    'Geothermal': 'geothermal_mw',
    'Hydro Pumped Storage': 'hydro_pumped_mw',
    'Hydro Run-of-river and poundage': 'hydro_run_mw',
    'Hydro Water Reservoir': 'hydro_reservoir_mw',
    'Marine': 'marine_mw',
    'Nuclear': 'nuclear_mw',
    'Other': 'other_mw',
    'Other renewable': 'other_renewable_mw',
    'Solar': 'solar_mw',
    'Waste': 'waste_mw',
    'Wind Offshore': 'wind_offshore_mw',
    'Wind Onshore': 'wind_onshore_mw',
}

# ============================================================================
# BACKFILL DEFAULT PERIODS
# ============================================================================
# These are default start dates for backfilling historical data
# Users can override these via command-line arguments
BACKFILL_DEFAULTS = {
    'load': '2019-01-01',      # 5 years to match existing data range
    'price': '2021-01-01',     # 4 years to match existing data range
    'renewable': '2021-01-01', # 4 years
    'load_forecast_day_ahead': '2019-01-01',  # Match load actual data
    'load_forecast_week_ahead': '2019-01-01',  # Match load actual data
    'wind_solar_forecast': '2021-01-01',  # Match renewable data range
    'crossborder_flows': '2023-01-01',
    'net_position': '2023-01-01',
}

# ============================================================================
# UPDATE CONFIGURATION
# ============================================================================
# For regular updates, fetch data from this many days ago
# This ensures we capture delayed uploads and data revisions
UPDATE_DAYS_BACK = 7

# ============================================================================
# PASS SUPERVISION (ABL-61)
# ============================================================================
# scripts/update.py used to exit 0 on a total upstream outage. These are the
# numbers behind the verdict in src/pass_verdict.py; the rules are there.

# Whole-pass retry, applied only when a multi-country pass stored NOTHING.
# Minutes, not seconds: the failure to survive is an upstream outage lasting
# minutes (2026-08-06: 484 HTTP 503s), which a 1/2/4-second per-request backoff
# sits entirely inside. Two retries, so a full pass plus its retries stays well
# under the 6-hour gap to the next scheduled pass even at the 55-minute end of
# the pass-duration range.
PASS_RETRY_DELAYS_SECONDS = [300, 900]  # 5 min, then 15 min

# How many recent healthy full passes form the volume baseline. 12 = three days
# at four passes a day -- long enough that one quiet pass cannot move the median,
# short enough to follow a real change in what we fetch.
PASS_BASELINE_PASSES = 12

# Refuse to judge volume on less history than this. A freshly deployed container
# has no pass rows and must not alarm on its first pass.
PASS_MIN_BASELINE_PASSES = 4

# Stored under this fraction of the baseline = collapsed. ABL-630 fell to ~2% of
# normal for four days; the moderate days on either side of it (75%, 45%) are
# deliberately NOT alarms -- a pass that stores three quarters of normal is a
# quiet day upstream, and an alarm that fires on those gets muted.
PASS_COLLAPSE_FRACTION = 0.25

# ============================================================================
# COUNTRY CONFIGURATION
# ============================================================================
# This will be loaded from the database dynamically
# But we define some constants for reference

# Countries with known data issues (from database_completeness.md)
PROBLEMATIC_COUNTRIES = {
    'IT': 'Only 1 day of price data instead of 4 years',
    'MD': 'Only 6 days of load data',
    'MK': 'Only 5 days of load data',
    'BA': 'Only 7 days of load data',
    'CY': 'Only 7 days of load data',
    'RS': 'Only 7 days of load data',
    'ME': 'Only 7 days of load data',
    'GB': 'Outdated data (last update June 2021)',
    'UA': 'Outdated data (ended Feb 2022, war-related)',
}

# Countries with no ENTSO-E data (likely not in network)
NO_DATA_COUNTRIES = ['IS', 'MT', 'TR']

# ============================================================================
# DATA QUALITY SETTINGS
# ============================================================================
# Data quality markers
DATA_QUALITY_ACTUAL = 'actual'
DATA_QUALITY_FORECAST = 'forecast'
DATA_QUALITY_ESTIMATED = 'estimated'

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
LOG_FILE = LOGS_DIR / "pipeline.log"
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = 'INFO'

# ============================================================================
# VALIDATION SETTINGS
# ============================================================================
# Minimum and maximum reasonable values for data validation
VALIDATION_LIMITS = {
    'load_mw': {
        'min': 0,
        'max': 100000  # 100 GW (reasonable max for a country)
    },
    'price_eur_mwh': {
        'min': -500,   # Negative prices can occur (renewable surplus)
        'max': 3000    # Extreme price events can reach this
    },
    'renewable_mw': {
        'min': 0,
        'max': 50000   # 50 GW (reasonable max for renewable generation)
    }
}

# ============================================================================
# DATABASE SCHEMA REFERENCE
# ============================================================================
# Table schemas for reference (defined in database, but documented here)
SCHEMA = {
    'energy_load': {
        'columns': ['id', 'country_code', 'timestamp_utc', 'load_mw', 'data_quality', 'created_at'],
        'unique_constraint': ['country_code', 'timestamp_utc']
    },
    'energy_price': {
        'columns': ['id', 'country_code', 'timestamp_utc', 'price_eur_mwh', 'data_quality', 'created_at'],
        'unique_constraint': ['country_code', 'timestamp_utc']
    },
    'energy_renewable': {
        'columns': [
            'id', 'country_code', 'timestamp_utc',
            'solar_mw', 'wind_onshore_mw', 'wind_offshore_mw',
            'hydro_run_mw', 'hydro_reservoir_mw',
            'biomass_mw', 'geothermal_mw', 'other_renewable_mw',
            'total_renewable_mw', 'data_quality', 'fetched_at'
        ],
        'unique_constraint': ['country_code', 'timestamp_utc']
    },
    'energy_generation': {
        # Complete A75 document -- one column per production type, all
        # NULL-default (no DEFAULT 0; see create_generation_table() in
        # src/db.py). energy_renewable above is unchanged and unrelated.
        'columns': [
            'id', 'country_code', 'timestamp_utc',
            'solar_mw', 'wind_onshore_mw', 'wind_offshore_mw',
            'hydro_run_mw', 'hydro_reservoir_mw', 'hydro_pumped_mw',
            'biomass_mw', 'geothermal_mw', 'marine_mw',
            'other_renewable_mw', 'energy_storage_mw',
            'nuclear_mw', 'fossil_gas_mw', 'fossil_hard_coal_mw',
            'fossil_brown_coal_mw', 'fossil_oil_mw', 'fossil_oil_shale_mw',
            'fossil_peat_mw', 'fossil_coal_derived_gas_mw',
            'waste_mw', 'other_mw',
            'data_quality', 'fetched_at', 'publication_timestamp_utc'
        ],
        'unique_constraint': ['country_code', 'timestamp_utc']
    },
    'data_ingestion_log': {
        'columns': [
            'id', 'pipeline_type', 'country_code',
            'start_time', 'end_time', 'status',
            'records_inserted', 'records_updated', 'records_failed',
            'error_message', 'created_at'
        ]
    }
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_api_config(data_type):
    """Get API configuration for a specific data type"""
    if data_type not in ENTSOE_API_CONFIG:
        raise ValueError(f"Unknown data type: {data_type}. Valid types: {list(ENTSOE_API_CONFIG.keys())}")
    return ENTSOE_API_CONFIG[data_type]


def get_table_name(data_type):
    """Get database table name for a data type"""
    return get_api_config(data_type)['table']


def get_renewable_columns():
    """Get list of renewable energy columns"""
    return ENTSOE_API_CONFIG['renewable']['renewable_columns']


def get_generation_columns():
    """Get list of energy_generation columns (all 21 A75 production types).

    Order matches GENERATION_COLUMN_MAP's insertion order. Unlike
    get_renewable_columns(), this is a 1:1 mapping so every value is
    distinct -- no column receives more than one ENTSO-E source name.
    """
    return list(GENERATION_COLUMN_MAP.values())


def is_dayahead_data_type(data_type: str) -> bool:
    """Check if a data type supports day-ahead (D+1) fetching"""
    if data_type not in ENTSOE_API_CONFIG:
        return False
    return ENTSOE_API_CONFIG[data_type].get('is_dayahead', False)


def get_dayahead_data_types() -> list:
    """Get list of all data types that support day-ahead (D+1) fetching"""
    return [
        data_type for data_type, conf in ENTSOE_API_CONFIG.items()
        if conf.get('is_dayahead', False)
    ]


def validate_value(value, data_type):
    """Validate if a value is within reasonable limits"""
    column_type = ENTSOE_API_CONFIG[data_type].get('value_column')
    if column_type and column_type in VALIDATION_LIMITS:
        limits = VALIDATION_LIMITS[column_type]
        return limits['min'] <= value <= limits['max']
    elif data_type == 'renewable':
        limits = VALIDATION_LIMITS['renewable_mw']
        return limits['min'] <= value <= limits['max']
    return True


# ============================================================================
# STARTUP VALIDATION
# ============================================================================

def validate_config():
    """Validate configuration on startup"""
    errors = []

    # Check API key
    if not ENTSOE_API_KEY:
        errors.append("ENTSOE_API_KEY not found in environment variables. Check .env file.")

    # Check database exists
    if not DATABASE_PATH.exists():
        errors.append(f"Database not found at {DATABASE_PATH}")

    # Check logs directory exists
    if not LOGS_DIR.exists():
        LOGS_DIR.mkdir(parents=True, exist_ok=True)

    if errors:
        raise ValueError(f"Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors))

    return True


if __name__ == "__main__":
    # Test configuration
    print("Configuration loaded successfully!")
    print(f"Database: {DATABASE_PATH}")
    print(f"API Key: {'[OK] Set' if ENTSOE_API_KEY else '[X] Not set'}")
    print(f"Data types: {list(ENTSOE_API_CONFIG.keys())}")
    print(f"Logs directory: {LOGS_DIR}")

    try:
        validate_config()
        print("\n[OK] Configuration validation passed!")
    except ValueError as e:
        print(f"\n[FAIL] Configuration validation failed:\n{e}")
