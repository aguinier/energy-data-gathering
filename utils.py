"""
Utility functions for ENTSO-E Energy Data Pipeline
"""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
import pytz
from typing import Optional, Tuple, List
import pandas as pd

import config


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(log_level: str = None, log_file: Path = None):
    """
    Setup logging configuration for the pipeline

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (defaults to config.LOG_FILE)

    Returns:
        logger: Configured logger instance
    """
    if log_level is None:
        log_level = config.LOG_LEVEL

    if log_file is None:
        log_file = config.LOG_FILE

    # Ensure logs directory exists
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Create logger
    logger = logging.getLogger('entsoe_pipeline')
    logger.setLevel(getattr(logging, log_level.upper()))

    # Clear existing handlers
    logger.handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_formatter = logging.Formatter(config.LOG_FORMAT)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)  # Always log DEBUG to file
    file_formatter = logging.Formatter(config.LOG_FORMAT)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger


# ============================================================================
# DATE/TIME UTILITIES
# ============================================================================

def parse_date(date_str: str) -> datetime:
    """
    Parse date string to datetime object

    Args:
        date_str: Date in format 'YYYY-MM-DD'

    Returns:
        datetime object
    """
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")


def to_utc(dt: datetime, timezone: str = 'Europe/Brussels') -> datetime:
    """
    Convert datetime to UTC

    Args:
        dt: Datetime object (can be naive or aware)
        timezone: Timezone string (default: Europe/Brussels for ENTSO-E)

    Returns:
        Timezone-aware datetime in UTC
    """
    tz = pytz.timezone(timezone)

    # If naive, localize to specified timezone
    if dt.tzinfo is None:
        dt = tz.localize(dt)

    # Convert to UTC
    return dt.astimezone(pytz.UTC)


def get_date_range(start_date: str, end_date: str, chunk_days: int = None) -> List[Tuple[datetime, datetime]]:
    """
    Generate date range chunks for API requests

    ENTSO-E API has limits on date range per request (typically 1 year).
    This function splits large date ranges into manageable chunks.
    Using 90-day chunks to avoid year-boundary issues with some bidding zones.

    Args:
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        chunk_days: Number of days per chunk (default: 90)

    Returns:
        List of (start_datetime, end_datetime) tuples
    """
    if chunk_days is None:
        chunk_days = 90  # 3-month chunks to avoid year-boundary issues

    start = parse_date(start_date)
    end = parse_date(end_date)

    if start > end:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")

    chunks = []
    current = start

    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        chunks.append((
            pytz.UTC.localize(current),
            pytz.UTC.localize(chunk_end)
        ))
        current = chunk_end

    return chunks


def get_recent_date_range(days_back: int = 7) -> Tuple[datetime, datetime]:
    """
    Get date range for recent data updates

    Args:
        days_back: Number of days to go back from now

    Returns:
        Tuple of (start_datetime, end_datetime) in UTC
    """
    end = datetime.now(pytz.UTC)
    start = end - timedelta(days=days_back)
    return start, end


def get_dayahead_end_date() -> datetime:
    """
    Get the end date for fetching day-ahead data (D+1).

    Day-ahead prices and forecasts are published around 12:57 CET for the next day.
    This function returns the end of tomorrow (D+1) at 23:59 UTC.

    Returns:
        datetime: End of D+1 in UTC
    """
    now = datetime.now(pytz.UTC)
    tomorrow = now + timedelta(days=1)
    # Return end of tomorrow
    return tomorrow.replace(hour=23, minute=59, second=59, microsecond=0)


def format_timestamp_for_db(dt: datetime) -> str:
    """
    Format datetime for database storage

    Args:
        dt: Datetime object

    Returns:
        Formatted string for SQLite (ISO 8601 format)
    """
    if dt.tzinfo is not None:
        dt = dt.astimezone(pytz.UTC)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def parse_timestamp_from_db(ts_str: str) -> datetime:
    """
    Parse timestamp from database

    Args:
        ts_str: Timestamp string from database

    Returns:
        UTC datetime object
    """
    dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
    return pytz.UTC.localize(dt)


# ============================================================================
# DATA VALIDATION
# ============================================================================

def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Validate DataFrame has required columns

    Args:
        df: DataFrame to validate
        required_columns: List of required column names

    Returns:
        True if valid

    Raises:
        ValueError if validation fails
    """
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"DataFrame missing required columns: {missing_columns}")

    return True


def validate_energy_value(value: float, data_type: str) -> bool:
    """
    Validate if an energy value is reasonable

    Args:
        value: Energy value to validate
        data_type: Type of data ('load', 'price', 'renewable')

    Returns:
        True if valid, False otherwise
    """
    if pd.isna(value):
        return False

    return config.validate_value(value, data_type)


def remove_outliers(df: pd.DataFrame, value_column: str, data_type: str) -> pd.DataFrame:
    """
    Remove outliers from DataFrame based on validation limits

    Args:
        df: DataFrame with data
        value_column: Column name containing values to check
        data_type: Type of data for validation limits

    Returns:
        DataFrame with outliers removed
    """
    initial_count = len(df)
    df_clean = df[df[value_column].apply(lambda x: validate_energy_value(x, data_type))]
    removed_count = initial_count - len(df_clean)

    if removed_count > 0:
        logger = logging.getLogger('entsoe_pipeline')
        logger.warning(f"Removed {removed_count} outliers from {value_column}")

    return df_clean


# ============================================================================
# DATA TRANSFORMATION
# ============================================================================

def calculate_renewable_total(row: pd.Series) -> float:
    """
    Calculate total renewable generation from individual sources

    Args:
        row: DataFrame row with renewable source columns

    Returns:
        Total renewable generation in MW
    """
    renewable_columns = config.get_renewable_columns()
    total = 0.0

    for col in renewable_columns:
        if col in row and pd.notna(row[col]):
            total += row[col]

    return total


def ensure_timezone_aware(series: pd.Series, timezone: str = 'UTC') -> pd.Series:
    """
    Ensure datetime series is timezone-aware

    Args:
        series: Pandas datetime series
        timezone: Target timezone (default: UTC)

    Returns:
        Timezone-aware datetime series
    """
    if series.dt.tz is None:
        # Naive datetime, localize
        return series.dt.tz_localize(timezone)
    else:
        # Already timezone-aware, convert to target timezone
        return series.dt.tz_convert(timezone)


# ============================================================================
# COUNTRY UTILITIES
# ============================================================================

def is_problematic_country(country_code: str) -> bool:
    """
    Check if country has known data quality issues

    Args:
        country_code: ISO 2-letter country code

    Returns:
        True if country has known issues
    """
    return country_code in config.PROBLEMATIC_COUNTRIES


def is_no_data_country(country_code: str) -> bool:
    """
    Check if country is known to have no ENTSO-E data

    Args:
        country_code: ISO 2-letter country code

    Returns:
        True if country has no data
    """
    return country_code in config.NO_DATA_COUNTRIES


def get_country_issue(country_code: str) -> Optional[str]:
    """
    Get description of data issue for a country

    Args:
        country_code: ISO 2-letter country code

    Returns:
        Issue description or None
    """
    return config.PROBLEMATIC_COUNTRIES.get(country_code)


# ============================================================================
# PROGRESS TRACKING
# ============================================================================

class ProgressTracker:
    """Simple progress tracker for pipeline operations"""

    def __init__(self, total: int, description: str = "Processing"):
        self.total = total
        self.current = 0
        self.description = description
        self.logger = logging.getLogger('entsoe_pipeline')

    def update(self, increment: int = 1, item: str = ""):
        """Update progress"""
        self.current += increment
        percentage = (self.current / self.total) * 100 if self.total > 0 else 0
        self.logger.info(f"{self.description}: {self.current}/{self.total} ({percentage:.1f}%) {item}")

    def finish(self):
        """Mark progress as complete"""
        self.logger.info(f"{self.description}: Complete ({self.total} items)")


# ============================================================================
# ERROR FORMATTING
# ============================================================================

def format_error(error: Exception, context: str = "") -> str:
    """
    Format error message for logging

    Args:
        error: Exception object
        context: Additional context string

    Returns:
        Formatted error message
    """
    error_type = type(error).__name__
    error_msg = str(error)

    if context:
        return f"{context}: {error_type}: {error_msg}"
    else:
        return f"{error_type}: {error_msg}"


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    # Test utilities
    print("Testing utilities...")

    # Test logging
    logger = setup_logging(log_level='INFO')
    logger.info("Logging test successful")

    # Test date parsing
    dt = parse_date('2024-01-01')
    print(f"Parsed date: {dt}")

    # Test date range
    ranges = get_date_range('2024-01-01', '2024-03-01', chunk_days=30)
    print(f"Date ranges: {len(ranges)} chunks")
    for start, end in ranges:
        print(f"  {start} -> {end}")

    # Test recent date range
    start, end = get_recent_date_range(days_back=7)
    print(f"Recent range: {start} -> {end}")

    # Test timezone conversion
    now = datetime.now()
    utc_now = to_utc(now)
    print(f"Local: {now} -> UTC: {utc_now}")

    print("\n[OK] All utility tests passed!")
