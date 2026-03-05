"""
Fetch historical weather data from Open-Meteo API (ERA5 reanalysis data)
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, List
import time

import pandas as pd
import numpy as np
import requests

from . import db
import utils
import config

logger = logging.getLogger('entsoe_pipeline')


# Open-Meteo API endpoints
OPENMETEO_HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"
OPENMETEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# Default forecast days (max 16 supported by Open-Meteo)
DEFAULT_FORECAST_DAYS = 14

# API rate limiting (Open-Meteo allows 10,000 requests/day for free tier)
REQUEST_DELAY_SECONDS = 0.5  # Be conservative to avoid hitting limits

# Country centroid coordinates for weather data
# These are approximate geographic centers for each country/region
COUNTRY_COORDINATES = {
    'AT': {'lat': 47.7, 'lon': 13.35, 'name': 'Austria'},
    'BE': {'lat': 50.5, 'lon': 4.45, 'name': 'Belgium'},
    'BG': {'lat': 42.7, 'lon': 25.5, 'name': 'Bulgaria'},
    'CH': {'lat': 46.8, 'lon': 8.2, 'name': 'Switzerland'},
    'CZ': {'lat': 49.85, 'lon': 15.5, 'name': 'Czech Republic'},
    'DE': {'lat': 51.2, 'lon': 10.45, 'name': 'Germany'},
    'DK': {'lat': 56.2, 'lon': 10.0, 'name': 'Denmark'},
    'DK1': {'lat': 56.2, 'lon': 9.5, 'name': 'Denmark West'},
    'DK2': {'lat': 55.7, 'lon': 12.5, 'name': 'Denmark East'},
    'EE': {'lat': 58.6, 'lon': 25.0, 'name': 'Estonia'},
    'ES': {'lat': 39.85, 'lon': -2.5, 'name': 'Spain'},
    'FI': {'lat': 64.95, 'lon': 25.35, 'name': 'Finland'},
    'FR': {'lat': 46.2, 'lon': 2.25, 'name': 'France'},
    'GB': {'lat': 54.0, 'lon': -2.0, 'name': 'Great Britain'},
    'GR': {'lat': 38.25, 'lon': 24.5, 'name': 'Greece'},
    'HR': {'lat': 44.45, 'lon': 16.45, 'name': 'Croatia'},
    'HU': {'lat': 47.15, 'lon': 19.5, 'name': 'Hungary'},
    'IE': {'lat': 53.4, 'lon': -8.25, 'name': 'Ireland'},
    'IT': {'lat': 41.85, 'lon': 12.55, 'name': 'Italy'},
    'LT': {'lat': 55.2, 'lon': 23.85, 'name': 'Lithuania'},
    'LU': {'lat': 49.8, 'lon': 6.1, 'name': 'Luxembourg'},
    'LV': {'lat': 56.9, 'lon': 24.55, 'name': 'Latvia'},
    'NL': {'lat': 52.2, 'lon': 5.3, 'name': 'Netherlands'},
    'NO': {'lat': 64.55, 'lon': 17.95, 'name': 'Norway'},
    'PL': {'lat': 51.9, 'lon': 19.15, 'name': 'Poland'},
    'PT': {'lat': 39.5, 'lon': -7.85, 'name': 'Portugal'},
    'RO': {'lat': 45.95, 'lon': 25.0, 'name': 'Romania'},
    'SE': {'lat': 62.2, 'lon': 17.6, 'name': 'Sweden'},
    'SI': {'lat': 46.15, 'lon': 15.0, 'name': 'Slovenia'},
    'SK': {'lat': 48.65, 'lon': 19.7, 'name': 'Slovakia'},
    # Countries currently without weather data
    'AL': {'lat': 41.0, 'lon': 20.0, 'name': 'Albania'},
    'BA': {'lat': 43.9, 'lon': 17.7, 'name': 'Bosnia Herzegovina'},
    'CY': {'lat': 35.0, 'lon': 33.0, 'name': 'Cyprus'},
    'MD': {'lat': 47.0, 'lon': 28.5, 'name': 'Moldova'},
    'ME': {'lat': 42.5, 'lon': 19.3, 'name': 'Montenegro'},
    'MK': {'lat': 41.5, 'lon': 21.5, 'name': 'North Macedonia'},
    'RS': {'lat': 44.0, 'lon': 21.0, 'name': 'Serbia'},
    'UA': {'lat': 49.0, 'lon': 32.0, 'name': 'Ukraine'},
}

# Open-Meteo hourly variables to fetch
# Note: cloud_cover removed - column no longer in database
HOURLY_VARIABLES = [
    'temperature_2m',
    'dew_point_2m',
    'relative_humidity_2m',
    'pressure_msl',
    'wind_speed_10m',
    'wind_gusts_10m',
    'wind_direction_10m',
    'wind_speed_100m',
    'wind_direction_100m',
    'wind_speed_80m',
    'wind_speed_120m',
    'precipitation',
    'rain',
    'snowfall',
    'shortwave_radiation',
    'direct_radiation',
    'direct_normal_irradiance',
    'diffuse_radiation',
]

# Mapping from Open-Meteo variables to database columns
# Note: cloud_cover mapping removed - column no longer in database
VARIABLE_MAPPING = {
    'temperature_2m': ('temperature_2m_k', lambda x: x + 273.15),  # Celsius to Kelvin
    'dew_point_2m': ('dew_point_2m_k', lambda x: x + 273.15),  # Celsius to Kelvin
    'relative_humidity_2m': ('relative_humidity_2m_frac', lambda x: x / 100.0),  # % to fraction
    'pressure_msl': ('pressure_msl_hpa', lambda x: x),  # Already in hPa
    'wind_speed_10m': ('wind_speed_10m_ms', lambda x: x),  # Already in m/s
    'wind_gusts_10m': ('wind_gusts_10m_ms', lambda x: x),
    'wind_direction_10m': ('wind_direction_10m_deg', lambda x: x),
    'wind_speed_100m': ('wind_speed_100m_ms', lambda x: x),
    'wind_direction_100m': ('wind_direction_100m_deg', lambda x: x),
    'wind_speed_80m': ('wind_speed_80m_ms', lambda x: x),
    'wind_speed_120m': ('wind_speed_120m_ms', lambda x: x),
    'precipitation': ('precip_mm', lambda x: x),
    'rain': ('rain_mm', lambda x: x),
    'snowfall': ('snowfall_mm', lambda x: x),
    'shortwave_radiation': ('shortwave_radiation_wm2', lambda x: x),
    'direct_radiation': ('direct_radiation_wm2', lambda x: x),
    'direct_normal_irradiance': ('direct_normal_irradiance_wm2', lambda x: x),
    'diffuse_radiation': ('diffuse_radiation_wm2', lambda x: x),
}


def fetch_weather_from_api(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str
) -> Optional[pd.DataFrame]:
    """
    Fetch weather data from Open-Meteo Historical API

    Args:
        lat: Latitude
        lon: Longitude
        start_date: Start date (YYYY-MM-DD format)
        end_date: End date (YYYY-MM-DD format)

    Returns:
        DataFrame with weather data or None if failed
    """
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_date,
        'end_date': end_date,
        'hourly': ','.join(HOURLY_VARIABLES),
        'timezone': 'UTC',
    }

    try:
        response = requests.get(OPENMETEO_HISTORICAL_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if 'hourly' not in data:
            logger.warning(f"No hourly data in response for lat={lat}, lon={lon}")
            return None

        # Create DataFrame from hourly data
        hourly_data = data['hourly']
        df = pd.DataFrame({
            'timestamp_utc': pd.to_datetime(hourly_data['time'], utc=True),
        })

        # Add and transform variables
        for api_var, (db_col, transform) in VARIABLE_MAPPING.items():
            if api_var in hourly_data:
                values = hourly_data[api_var]
                # Handle None values
                df[db_col] = [transform(v) if v is not None else None for v in values]
            else:
                df[db_col] = None

        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing weather data: {e}")
        return None


def fetch_weather_data(
    country_code: str,
    start: datetime,
    end: datetime,
    log_id: Optional[int] = None
) -> Tuple[int, int, int]:
    """
    Fetch and store weather data for a country

    Args:
        country_code: ISO 2-letter country code or regional code (DK1, DK2)
        start: Start datetime (UTC)
        end: End datetime (UTC)
        log_id: Optional ingestion log ID

    Returns:
        Tuple of (records_inserted, records_updated, records_failed)
    """
    if country_code not in COUNTRY_COORDINATES:
        logger.warning(f"No coordinates defined for country {country_code}")
        return 0, 0, 1

    coords = COUNTRY_COORDINATES[country_code]
    lat, lon = coords['lat'], coords['lon']

    start_str = start.strftime('%Y-%m-%d')
    end_str = end.strftime('%Y-%m-%d')

    logger.info(f"Fetching weather data for {country_code} ({coords['name']}): {start_str} to {end_str}")

    try:
        # Add delay to respect rate limits
        time.sleep(REQUEST_DELAY_SECONDS)

        # Fetch from API
        df = fetch_weather_from_api(lat, lon, start_str, end_str)

        if df is None or df.empty:
            logger.warning(f"No weather data returned for {country_code}")
            return 0, 0, 0

        # Upsert data to database
        records_inserted, records_updated = db.upsert_weather_data(df, country_code)

        logger.info(f"Successfully stored {records_inserted} weather records for {country_code}")
        return records_inserted, records_updated, 0

    except Exception as e:
        error_msg = f"Error fetching weather for {country_code}: {e}"
        logger.error(error_msg)

        if log_id:
            db.log_ingestion_complete(
                log_id,
                records_failed=1,
                error_message=str(e)
            )

        return 0, 0, 1


def fetch_weather_for_country(
    country_code: str,
    start: datetime,
    end: datetime
) -> bool:
    """
    Fetch weather data for a single country (convenience function)

    Args:
        country_code: ISO 2-letter country code
        start: Start datetime (UTC)
        end: End datetime (UTC)

    Returns:
        True if successful, False otherwise
    """
    # Log start
    log_id = db.log_ingestion_start('weather', country_code)

    try:
        # Split into chunks of max 1 year (API limitation)
        total_inserted = 0
        total_updated = 0
        total_failed = 0

        current_start = start
        while current_start < end:
            # Max 1 year per request
            chunk_end = min(current_start + timedelta(days=365), end)

            inserted, updated, failed = fetch_weather_data(
                country_code, current_start, chunk_end, log_id
            )

            total_inserted += inserted
            total_updated += updated
            total_failed += failed

            current_start = chunk_end + timedelta(days=1)

        # Log completion
        db.log_ingestion_complete(
            log_id,
            records_inserted=total_inserted,
            records_updated=total_updated,
            records_failed=total_failed
        )

        return total_failed == 0

    except Exception as e:
        logger.error(f"Failed to fetch weather data for {country_code}: {e}")
        db.log_ingestion_complete(log_id, records_failed=1, error_message=str(e))
        return False


def get_weather_countries() -> List[str]:
    """Get list of country codes that have weather data in the database"""
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT country_code
            FROM weather_data
            ORDER BY country_code
        """)
        return [row[0] for row in cursor.fetchall()]


def get_weather_gaps(country_code: str) -> List[Tuple[datetime, datetime]]:
    """
    Find gaps in weather data for a country

    Args:
        country_code: Country code

    Returns:
        List of (gap_start, gap_end) tuples
    """
    import pytz

    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            WITH hourly_series AS (
                SELECT timestamp_utc,
                       LAG(timestamp_utc) OVER (ORDER BY timestamp_utc) as prev_timestamp
                FROM weather_data
                WHERE country_code = ?
            )
            SELECT prev_timestamp, timestamp_utc
            FROM hourly_series
            WHERE (JULIANDAY(timestamp_utc) - JULIANDAY(prev_timestamp)) * 24 > 1.1
              AND prev_timestamp IS NOT NULL
            ORDER BY timestamp_utc
        """, (country_code,))

        gaps = []
        for row in cursor.fetchall():
            # Parse timestamps - handle both tz-aware and naive timestamps
            gap_start = pd.to_datetime(row[0])
            gap_end = pd.to_datetime(row[1])

            # Ensure timezone awareness
            if gap_start.tzinfo is None:
                gap_start = gap_start.tz_localize('UTC')
            else:
                gap_start = gap_start.tz_convert('UTC')

            if gap_end.tzinfo is None:
                gap_end = gap_end.tz_localize('UTC')
            else:
                gap_end = gap_end.tz_convert('UTC')

            gaps.append((gap_start, gap_end))

        return gaps


def fetch_weather_forecast_from_api(
    lat: float,
    lon: float,
    forecast_days: int = DEFAULT_FORECAST_DAYS
) -> Optional[Tuple[pd.DataFrame, datetime]]:
    """
    Fetch weather forecast from Open-Meteo Forecast API

    Args:
        lat: Latitude
        lon: Longitude
        forecast_days: Number of days to forecast (max 16)

    Returns:
        Tuple of (DataFrame with forecast data, forecast_run_time) or None if failed
    """
    import pytz

    params = {
        'latitude': lat,
        'longitude': lon,
        'hourly': ','.join(HOURLY_VARIABLES),
        'forecast_days': min(forecast_days, 16),  # API max is 16
        'timezone': 'UTC',
    }

    try:
        response = requests.get(OPENMETEO_FORECAST_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if 'hourly' not in data:
            logger.warning(f"No hourly data in forecast response for lat={lat}, lon={lon}")
            return None

        # Create DataFrame from hourly data
        hourly_data = data['hourly']
        df = pd.DataFrame({
            'timestamp_utc': pd.to_datetime(hourly_data['time'], utc=True),
        })

        # Add and transform variables
        for api_var, (db_col, transform) in VARIABLE_MAPPING.items():
            if api_var in hourly_data:
                values = hourly_data[api_var]
                # Handle None values
                df[db_col] = [transform(v) if v is not None else None for v in values]
            else:
                df[db_col] = None

        # Determine forecast run time from API response or current time
        # Open-Meteo doesn't provide explicit model run time, use current UTC hour rounded down
        now_utc = datetime.now(pytz.UTC)
        # Round to nearest 6-hour model run (00, 06, 12, 18 UTC)
        hour_rounded = (now_utc.hour // 6) * 6
        forecast_run_time = now_utc.replace(hour=hour_rounded, minute=0, second=0, microsecond=0)

        return df, forecast_run_time

    except requests.exceptions.RequestException as e:
        logger.error(f"Forecast API request failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing forecast data: {e}")
        return None


def fetch_weather_forecast(
    country_code: str,
    forecast_days: int = DEFAULT_FORECAST_DAYS,
    log_id: Optional[int] = None
) -> Tuple[int, int, int]:
    """
    Fetch and store weather forecast for a country

    Args:
        country_code: ISO 2-letter country code or regional code (DK1, DK2)
        forecast_days: Number of days to forecast (default 14, max 16)
        log_id: Optional ingestion log ID

    Returns:
        Tuple of (records_inserted, records_updated, records_failed)
    """
    if country_code not in COUNTRY_COORDINATES:
        logger.warning(f"No coordinates defined for country {country_code}")
        return 0, 0, 1

    coords = COUNTRY_COORDINATES[country_code]
    lat, lon = coords['lat'], coords['lon']

    logger.info(f"Fetching weather forecast for {country_code} ({coords['name']}): {forecast_days} days ahead")

    try:
        # Add delay to respect rate limits
        time.sleep(REQUEST_DELAY_SECONDS)

        # Fetch from API
        result = fetch_weather_forecast_from_api(lat, lon, forecast_days)

        if result is None:
            logger.warning(f"No forecast data returned for {country_code}")
            return 0, 0, 0

        df, forecast_run_time = result

        if df.empty:
            logger.warning(f"Empty forecast data for {country_code}")
            return 0, 0, 0

        # Upsert data to database
        records_inserted, records_updated = db.upsert_weather_forecast_data(
            df, country_code, forecast_run_time
        )

        logger.info(f"Successfully stored {records_inserted} forecast records for {country_code}")
        return records_inserted, records_updated, 0

    except Exception as e:
        error_msg = f"Error fetching forecast for {country_code}: {e}"
        logger.error(error_msg)

        if log_id:
            db.log_ingestion_complete(
                log_id,
                records_failed=1,
                error_message=str(e)
            )

        return 0, 0, 1


if __name__ == "__main__":
    # Test weather fetcher
    import pytz

    print("Testing weather data fetcher...")
    utils.setup_logging()

    # Test fetching a small date range
    start = pytz.UTC.localize(datetime(2024, 12, 1))
    end = pytz.UTC.localize(datetime(2024, 12, 2))

    # Test API call directly
    print("\nTesting API call for Germany...")
    df = fetch_weather_from_api(51.2, 10.45, '2024-12-01', '2024-12-02')
    if df is not None:
        print(f"Retrieved {len(df)} records")
        print(f"Columns: {list(df.columns)}")
        print(f"\nSample data:")
        print(df.head())
    else:
        print("API call failed")

    # Test forecast API
    print("\n" + "=" * 50)
    print("Testing FORECAST API for Germany...")
    result = fetch_weather_forecast_from_api(51.2, 10.45, forecast_days=3)
    if result is not None:
        df_forecast, run_time = result
        print(f"Forecast run time: {run_time}")
        print(f"Retrieved {len(df_forecast)} forecast records")
        print(f"Time range: {df_forecast['timestamp_utc'].min()} to {df_forecast['timestamp_utc'].max()}")
        print(f"\nSample forecast data:")
        print(df_forecast.head())
    else:
        print("Forecast API call failed")
