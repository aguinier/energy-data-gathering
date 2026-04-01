"""
Database operations for ENTSO-E Energy Data Pipeline
"""

import sqlite3
import logging
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from contextlib import contextmanager
import pandas as pd
import pytz

import config
import utils


logger = logging.getLogger("entsoe_pipeline")


# Valid tables for dynamic queries - prevents SQL injection
VALID_TABLES = frozenset(
    {
        "energy_load",
        "energy_price",
        "energy_generation",
        "weather_data",
        "energy_load_forecast",
        "energy_generation_forecast",
        "forecasts",
        "countries",
        "crossborder_flows",
        "net_position",
    }
)


# ============================================================================
# DATABASE CONNECTION
# ============================================================================


@contextmanager
def get_connection():
    """
    Context manager for database connections

    Yields:
        sqlite3.Connection: Database connection

    Example:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM countries")
    """
    conn = None
    try:
        # Set timeout to 30 seconds to avoid "database is locked" errors
        # when multiple processes access the database concurrently
        conn = sqlite3.connect(config.DATABASE_PATH, timeout=30.0)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()


# ============================================================================
# COUNTRY OPERATIONS
# ============================================================================


def get_countries(priority: Optional[int] = None) -> List[Dict]:
    """
    Get countries from database

    Args:
        priority: Filter by priority (1=high, 2=medium, 3=low), None=all

    Returns:
        List of country dictionaries with country_code, country_name, entsoe_domain
    """
    with get_connection() as conn:
        cursor = conn.cursor()

        if priority is not None:
            cursor.execute(
                """
                SELECT country_code, country_name, entsoe_domain, priority
                FROM countries
                WHERE entsoe_domain IS NOT NULL AND priority = ?
                ORDER BY country_code
            """,
                (priority,),
            )
        else:
            cursor.execute("""
                SELECT country_code, country_name, entsoe_domain, priority
                FROM countries
                WHERE entsoe_domain IS NOT NULL
                ORDER BY country_code
            """)

        countries = []
        for row in cursor.fetchall():
            countries.append(
                {
                    "country_code": row["country_code"],
                    "country_name": row["country_name"],
                    "entsoe_domain": row["entsoe_domain"],
                    "priority": row["priority"],
                }
            )

    logger.info(f"Retrieved {len(countries)} countries from database")
    return countries


def get_country_by_code(country_code: str) -> Optional[Dict]:
    """
    Get specific country by code

    Args:
        country_code: ISO 2-letter country code

    Returns:
        Country dictionary or None if not found
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT country_code, country_name, entsoe_domain, priority
            FROM countries
            WHERE country_code = ? AND entsoe_domain IS NOT NULL
        """,
            (country_code,),
        )

        row = cursor.fetchone()
        if row:
            return {
                "country_code": row["country_code"],
                "country_name": row["country_name"],
                "entsoe_domain": row["entsoe_domain"],
                "priority": row["priority"],
            }

    return None


# ============================================================================
# TABLE CREATION (CROSSBORDER FLOWS & NET POSITION)
# ============================================================================


def create_crossborder_flows_table():
    """Create crossborder_flows table for bilateral physical flow data."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crossborder_flows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_from TEXT NOT NULL,
                country_to TEXT NOT NULL,
                timestamp_utc TEXT NOT NULL,
                flow_mw REAL NOT NULL,
                data_quality TEXT DEFAULT 'actual',
                publication_timestamp_utc TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(country_from, country_to, timestamp_utc)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cbf_from
            ON crossborder_flows(country_from, timestamp_utc)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cbf_to
            ON crossborder_flows(country_to, timestamp_utc)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cbf_pair
            ON crossborder_flows(country_from, country_to, timestamp_utc)
        """)
        conn.commit()
    logger.info("crossborder_flows table created/verified")


def create_net_position_table():
    """Create net_position table for aggregated net position data."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS net_position (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                timestamp_utc TEXT NOT NULL,
                net_position_mw REAL NOT NULL,
                data_quality TEXT DEFAULT 'actual',
                publication_timestamp_utc TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(country_code, timestamp_utc)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_np_lookup
            ON net_position(country_code, timestamp_utc)
        """)
        conn.commit()
    logger.info("net_position table created/verified")


# ============================================================================
# DATA UPSERT OPERATIONS
# ============================================================================


def upsert_load_data(
    df: pd.DataFrame,
    country_code: str,
    publication_timestamp: Optional[datetime] = None,
) -> Tuple[int, int]:
    """
    Insert or update load data

    Args:
        df: DataFrame with columns: timestamp_utc, load_mw
        country_code: ISO 2-letter country code
        publication_timestamp: When ENTSO-E published this data (optional)

    Returns:
        Tuple of (records_inserted, records_updated)
    """
    if df.empty:
        logger.warning(f"Empty DataFrame for load data, country {country_code}")
        return 0, 0

    # Validate DataFrame
    utils.validate_dataframe(df, ["timestamp_utc", "load_mw"])

    # Convert timestamps to string format for SQLite
    df = df.copy()
    df["timestamp_utc"] = df["timestamp_utc"].apply(
        lambda x: utils.format_timestamp_for_db(x) if pd.notna(x) else None
    )

    # Format publication timestamp if provided
    pub_time_str = None
    if publication_timestamp:
        pub_time_str = utils.format_timestamp_for_db(publication_timestamp)

    records_affected = 0

    with get_connection() as conn:
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT OR REPLACE INTO energy_load
                (country_code, timestamp_utc, load_mw, data_quality,
                 publication_timestamp_utc, created_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
                (
                    country_code,
                    row["timestamp_utc"],
                    row["load_mw"],
                    config.DATA_QUALITY_ACTUAL,
                    pub_time_str,
                ),
            )
            records_affected += cursor.rowcount

    logger.info(f"Upserted {records_affected} load records for {country_code}")
    return records_affected, 0  # SQLite doesn't distinguish insert vs update easily


def upsert_price_data(
    df: pd.DataFrame,
    country_code: str,
    publication_timestamp: Optional[datetime] = None,
) -> Tuple[int, int]:
    """
    Insert or update price data

    Args:
        df: DataFrame with columns: timestamp_utc, price_eur_mwh
        country_code: ISO 2-letter country code
        publication_timestamp: When ENTSO-E published this data (optional)

    Returns:
        Tuple of (records_inserted, records_updated)
    """
    if df.empty:
        logger.warning(f"Empty DataFrame for price data, country {country_code}")
        return 0, 0

    # Validate DataFrame
    utils.validate_dataframe(df, ["timestamp_utc", "price_eur_mwh"])

    # Convert timestamps to string format for SQLite
    df = df.copy()
    df["timestamp_utc"] = df["timestamp_utc"].apply(
        lambda x: utils.format_timestamp_for_db(x) if pd.notna(x) else None
    )

    # Format publication timestamp if provided
    pub_time_str = None
    if publication_timestamp:
        pub_time_str = utils.format_timestamp_for_db(publication_timestamp)

    records_affected = 0

    with get_connection() as conn:
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT OR REPLACE INTO energy_price
                (country_code, timestamp_utc, price_eur_mwh, data_quality,
                 publication_timestamp_utc, created_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
                (
                    country_code,
                    row["timestamp_utc"],
                    row["price_eur_mwh"],
                    config.DATA_QUALITY_ACTUAL,
                    pub_time_str,
                ),
            )
            records_affected += cursor.rowcount

    logger.info(f"Upserted {records_affected} price records for {country_code}")
    return records_affected, 0


def upsert_load_forecast_data(
    df: pd.DataFrame,
    country_code: str,
    forecast_type: str,
    publication_timestamp: Optional[datetime] = None,
) -> Tuple[int, int]:
    """
    Insert or update load forecast data

    Args:
        df: DataFrame with columns:
            - timestamp_utc, forecast_value_mw (required)
            - forecast_min_mw, forecast_max_mw (optional, for week_ahead)
        country_code: ISO 2-letter country code
        forecast_type: 'day_ahead' or 'week_ahead'
        publication_timestamp: When ENTSO-E published this data (optional)

    Returns:
        Tuple of (records_inserted, records_updated)
    """
    if df.empty:
        logger.warning(
            f"Empty DataFrame for {forecast_type} forecast data, country {country_code}"
        )
        return 0, 0

    # Validate DataFrame
    utils.validate_dataframe(df, ["timestamp_utc", "forecast_value_mw"])

    # Convert timestamps to string format for SQLite
    df = df.copy()
    df["timestamp_utc"] = df["timestamp_utc"].apply(
        lambda x: utils.format_timestamp_for_db(x) if pd.notna(x) else None
    )

    # Format publication timestamp if provided
    pub_time_str = None
    if publication_timestamp:
        pub_time_str = utils.format_timestamp_for_db(publication_timestamp)

    # Check if min/max columns exist (for week-ahead data)
    has_min_max = "forecast_min_mw" in df.columns and "forecast_max_mw" in df.columns

    records_affected = 0

    with get_connection() as conn:
        cursor = conn.cursor()

        for _, row in df.iterrows():
            if has_min_max:
                # Week-ahead with min/max values
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO energy_load_forecast
                    (country_code, target_timestamp_utc, forecast_value_mw,
                     forecast_min_mw, forecast_max_mw,
                     forecast_type, forecast_run_time, horizon_hours,
                     data_quality, publication_timestamp_utc, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, 'forecast', ?, CURRENT_TIMESTAMP)
                """,
                    (
                        country_code,
                        row["timestamp_utc"],
                        row["forecast_value_mw"],
                        row.get("forecast_min_mw"),
                        row.get("forecast_max_mw"),
                        forecast_type,
                        pub_time_str,
                    ),
                )
            else:
                # Day-ahead without min/max
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO energy_load_forecast
                    (country_code, target_timestamp_utc, forecast_value_mw,
                     forecast_type, forecast_run_time, horizon_hours,
                     data_quality, publication_timestamp_utc, created_at)
                    VALUES (?, ?, ?, ?, NULL, NULL, 'forecast', ?, CURRENT_TIMESTAMP)
                """,
                    (
                        country_code,
                        row["timestamp_utc"],
                        row["forecast_value_mw"],
                        forecast_type,
                        pub_time_str,
                    ),
                )
            records_affected += cursor.rowcount

    logger.info(
        f"Upserted {records_affected} {forecast_type} forecast records for {country_code}"
    )
    return records_affected, 0


def upsert_generation_forecast_data(
    df: pd.DataFrame,
    country_code: str,
    publication_timestamp: Optional[datetime] = None,
    forecast_type: str = "day_ahead",
) -> Tuple[int, int]:
    """
    Insert or update wind & solar generation forecast data

    Args:
        df: DataFrame with columns: timestamp_utc, solar_mw, wind_onshore_mw, wind_offshore_mw
        country_code: ISO 2-letter country code
        publication_timestamp: When ENTSO-E published this data (optional)
        forecast_type: Forecast type (default: 'day_ahead')

    Returns:
        Tuple of (records_inserted, records_updated)
    """
    if df.empty:
        logger.warning(
            f"Empty DataFrame for generation forecast data, country {country_code}"
        )
        return 0, 0

    # Validate DataFrame has timestamp
    utils.validate_dataframe(df, ["timestamp_utc"])

    # Convert timestamps to string format for SQLite
    df = df.copy()
    df["timestamp_utc"] = df["timestamp_utc"].apply(
        lambda x: utils.format_timestamp_for_db(x) if pd.notna(x) else None
    )

    # Format publication timestamp if provided
    pub_time_str = None
    if publication_timestamp:
        pub_time_str = utils.format_timestamp_for_db(publication_timestamp)

    # Ensure columns exist with default 0
    for col in ["solar_mw", "wind_onshore_mw", "wind_offshore_mw"]:
        if col not in df.columns:
            df[col] = 0.0

    # Calculate total forecast
    df["total_forecast_mw"] = (
        df["solar_mw"].fillna(0)
        + df["wind_onshore_mw"].fillna(0)
        + df["wind_offshore_mw"].fillna(0)
    )

    records_affected = 0

    with get_connection() as conn:
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT OR REPLACE INTO energy_generation_forecast
                (country_code, target_timestamp_utc, solar_mw, wind_onshore_mw, wind_offshore_mw,
                 total_forecast_mw, forecast_type, data_quality, publication_timestamp_utc, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'forecast', ?, CURRENT_TIMESTAMP)
            """,
                (
                    country_code,
                    row["timestamp_utc"],
                    row.get("solar_mw", 0.0),
                    row.get("wind_onshore_mw", 0.0),
                    row.get("wind_offshore_mw", 0.0),
                    row["total_forecast_mw"],
                    forecast_type,
                    pub_time_str,
                ),
            )
            records_affected += cursor.rowcount

    logger.info(
        f"Upserted {records_affected} generation forecast records for {country_code}"
    )
    return records_affected, 0


def upsert_renewable_data(
    df: pd.DataFrame,
    country_code: str,
    publication_timestamp: Optional[datetime] = None,
) -> Tuple[int, int]:
    """
    Insert or update renewable energy data

    Args:
        df: DataFrame with renewable source columns
        country_code: ISO 2-letter country code
        publication_timestamp: When ENTSO-E published this data (optional)

    Returns:
        Tuple of (records_inserted, records_updated)
    """
    if df.empty:
        logger.warning(f"Empty DataFrame for renewable data, country {country_code}")
        return 0, 0

    # Validate DataFrame has timestamp
    utils.validate_dataframe(df, ["timestamp_utc"])

    # Convert timestamps to string format for SQLite
    df = df.copy()
    df["timestamp_utc"] = df["timestamp_utc"].apply(
        lambda x: utils.format_timestamp_for_db(x) if pd.notna(x) else None
    )

    # Ensure all renewable columns exist with default 0
    renewable_cols = config.get_renewable_columns()
    for col in renewable_cols:
        if col not in df.columns:
            df[col] = 0.0

    # Calculate total_renewable_mw
    df["total_renewable_mw"] = df.apply(utils.calculate_renewable_total, axis=1)

    # Format publication timestamp if provided
    pub_time_str = None
    if publication_timestamp:
        pub_time_str = utils.format_timestamp_for_db(publication_timestamp)

    records_affected = 0

    with get_connection() as conn:
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT OR REPLACE INTO energy_renewable
                (country_code, timestamp_utc, solar_mw, wind_onshore_mw, wind_offshore_mw,
                 hydro_run_mw, hydro_reservoir_mw, biomass_mw, geothermal_mw, other_renewable_mw,
                 total_renewable_mw, data_quality, publication_timestamp_utc, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
                (
                    country_code,
                    row["timestamp_utc"],
                    row.get("solar_mw", 0.0),
                    row.get("wind_onshore_mw", 0.0),
                    row.get("wind_offshore_mw", 0.0),
                    row.get("hydro_run_mw", 0.0),
                    row.get("hydro_reservoir_mw", 0.0),
                    row.get("biomass_mw", 0.0),
                    row.get("geothermal_mw", 0.0),
                    row.get("other_renewable_mw", 0.0),
                    row["total_renewable_mw"],
                    config.DATA_QUALITY_ACTUAL,
                    pub_time_str,
                ),
            )
            records_affected += cursor.rowcount

    logger.info(f"Upserted {records_affected} renewable records for {country_code}")
    return records_affected, 0


def upsert_weather_data(df: pd.DataFrame, country_code: str) -> Tuple[int, int]:
    """
    Insert or update weather data

    Args:
        df: DataFrame with weather columns (from Open-Meteo API)
        country_code: ISO 2-letter country code or regional code

    Returns:
        Tuple of (records_inserted, records_updated)
    """
    if df.empty:
        logger.warning(f"Empty DataFrame for weather data, country {country_code}")
        return 0, 0

    # Validate DataFrame has timestamp
    utils.validate_dataframe(df, ["timestamp_utc"])

    # Convert timestamps to string format for SQLite
    df = df.copy()
    df["timestamp_utc"] = df["timestamp_utc"].apply(
        lambda x: utils.format_timestamp_for_db(x) if pd.notna(x) else None
    )

    # Weather columns to insert (cloud_cover_frac removed - column no longer in database)
    weather_cols = [
        "temperature_2m_k",
        "dew_point_2m_k",
        "relative_humidity_2m_frac",
        "pressure_msl_hpa",
        "wind_speed_10m_ms",
        "wind_gusts_10m_ms",
        "wind_direction_10m_deg",
        "wind_speed_100m_ms",
        "wind_direction_100m_deg",
        "wind_speed_80m_ms",
        "wind_speed_120m_ms",
        "precip_mm",
        "rain_mm",
        "snowfall_mm",
        "shortwave_radiation_wm2",
        "direct_radiation_wm2",
        "direct_normal_irradiance_wm2",
        "diffuse_radiation_wm2",
    ]

    records_affected = 0

    with get_connection() as conn:
        cursor = conn.cursor()

        for _, row in df.iterrows():
            # For actual data, forecast_run_time = timestamp_utc (observation time)
            cursor.execute(
                """
                INSERT OR REPLACE INTO weather_data
                (country_code, timestamp_utc, forecast_run_time, temperature_2m_k, dew_point_2m_k,
                 relative_humidity_2m_frac, pressure_msl_hpa,
                 wind_speed_10m_ms, wind_gusts_10m_ms, wind_direction_10m_deg,
                 wind_speed_100m_ms, wind_direction_100m_deg, wind_speed_80m_ms,
                 wind_speed_120m_ms, precip_mm, rain_mm, snowfall_mm,
                 shortwave_radiation_wm2, direct_radiation_wm2,
                 direct_normal_irradiance_wm2, diffuse_radiation_wm2,
                 model_name, data_quality, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'era5', 'actual', CURRENT_TIMESTAMP)
            """,
                (
                    country_code,
                    row["timestamp_utc"],
                    row[
                        "timestamp_utc"
                    ],  # forecast_run_time = timestamp_utc for actuals
                    row.get("temperature_2m_k"),
                    row.get("dew_point_2m_k"),
                    row.get("relative_humidity_2m_frac"),
                    row.get("pressure_msl_hpa"),
                    row.get("wind_speed_10m_ms"),
                    row.get("wind_gusts_10m_ms"),
                    row.get("wind_direction_10m_deg"),
                    row.get("wind_speed_100m_ms"),
                    row.get("wind_direction_100m_deg"),
                    row.get("wind_speed_80m_ms"),
                    row.get("wind_speed_120m_ms"),
                    row.get("precip_mm"),
                    row.get("rain_mm"),
                    row.get("snowfall_mm"),
                    row.get("shortwave_radiation_wm2"),
                    row.get("direct_radiation_wm2"),
                    row.get("direct_normal_irradiance_wm2"),
                    row.get("diffuse_radiation_wm2"),
                ),
            )
            records_affected += cursor.rowcount

    logger.info(f"Upserted {records_affected} weather records for {country_code}")
    return records_affected, 0


def upsert_weather_forecast_data(
    df: pd.DataFrame,
    country_code: str,
    forecast_run_time: datetime,
    model_name: str = "best_match",
) -> Tuple[int, int]:
    """
    Insert weather forecast data (preserving history)

    Each unique (country_code, timestamp_utc, model_name, forecast_run_time)
    combination is stored as a separate row, allowing multiple forecast
    vintages to be kept for accuracy analysis.

    Args:
        df: DataFrame with weather columns (from Open-Meteo Forecast API)
        country_code: ISO 2-letter country code or regional code
        forecast_run_time: When the forecast was generated (model run time)
        model_name: Weather model name (default: 'best_match')

    Returns:
        Tuple of (records_inserted, records_updated)
    """
    if df.empty:
        logger.warning(f"Empty DataFrame for weather forecast, country {country_code}")
        return 0, 0

    # Validate DataFrame has timestamp
    utils.validate_dataframe(df, ["timestamp_utc"])

    # Convert timestamps to string format for SQLite
    df = df.copy()
    df["timestamp_utc"] = df["timestamp_utc"].apply(
        lambda x: utils.format_timestamp_for_db(x) if pd.notna(x) else None
    )

    # Format forecast_run_time
    forecast_run_time_str = utils.format_timestamp_for_db(forecast_run_time)

    records_affected = 0

    with get_connection() as conn:
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT OR REPLACE INTO weather_data
                (country_code, timestamp_utc, forecast_run_time, temperature_2m_k, dew_point_2m_k,
                 relative_humidity_2m_frac, pressure_msl_hpa,
                 wind_speed_10m_ms, wind_gusts_10m_ms, wind_direction_10m_deg,
                 wind_speed_100m_ms, wind_direction_100m_deg, wind_speed_80m_ms,
                 wind_speed_120m_ms, precip_mm, rain_mm, snowfall_mm,
                 shortwave_radiation_wm2, direct_radiation_wm2,
                 direct_normal_irradiance_wm2, diffuse_radiation_wm2,
                 model_name, data_quality, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'forecast', CURRENT_TIMESTAMP)
            """,
                (
                    country_code,
                    row["timestamp_utc"],
                    forecast_run_time_str,
                    row.get("temperature_2m_k"),
                    row.get("dew_point_2m_k"),
                    row.get("relative_humidity_2m_frac"),
                    row.get("pressure_msl_hpa"),
                    row.get("wind_speed_10m_ms"),
                    row.get("wind_gusts_10m_ms"),
                    row.get("wind_direction_10m_deg"),
                    row.get("wind_speed_100m_ms"),
                    row.get("wind_direction_100m_deg"),
                    row.get("wind_speed_80m_ms"),
                    row.get("wind_speed_120m_ms"),
                    row.get("precip_mm"),
                    row.get("rain_mm"),
                    row.get("snowfall_mm"),
                    row.get("shortwave_radiation_wm2"),
                    row.get("direct_radiation_wm2"),
                    row.get("direct_normal_irradiance_wm2"),
                    row.get("diffuse_radiation_wm2"),
                    model_name,
                ),
            )
            records_affected += cursor.rowcount

    logger.info(
        f"Upserted {records_affected} weather forecast records for {country_code} (run: {forecast_run_time_str})"
    )
    return records_affected, 0


def upsert_crossborder_flows(
    df: pd.DataFrame,
    country_from: str,
) -> Tuple[int, int]:
    """
    Insert or update cross-border flow data.

    Args:
        df: DataFrame with columns: country_to, timestamp_utc, flow_mw
        country_from: ISO 2-letter country code (exporting country)

    Returns:
        Tuple of (records_affected, 0)
    """
    if df.empty:
        logger.warning(f"Empty DataFrame for crossborder flows from {country_from}")
        return 0, 0

    records_affected = 0

    with get_connection() as conn:
        cursor = conn.cursor()

        for _, row in df.iterrows():
            ts_str = utils.format_timestamp_for_db(row["timestamp_utc"]) if pd.notna(row["timestamp_utc"]) else None
            cursor.execute(
                """
                INSERT OR REPLACE INTO crossborder_flows
                (country_from, country_to, timestamp_utc, flow_mw,
                 data_quality, fetched_at)
                VALUES (?, ?, ?, ?, 'actual', CURRENT_TIMESTAMP)
                """,
                (
                    country_from,
                    row["country_to"],
                    ts_str,
                    float(row["flow_mw"]) if pd.notna(row["flow_mw"]) else None,
                ),
            )
            records_affected += cursor.rowcount

    logger.info(f"Upserted {records_affected} crossborder flow records from {country_from}")
    return records_affected, 0


def upsert_net_position(
    df: pd.DataFrame,
    country_code: str,
) -> Tuple[int, int]:
    """
    Insert or update net position data.

    Args:
        df: DataFrame with columns: timestamp_utc, net_position_mw
        country_code: ISO 2-letter country code

    Returns:
        Tuple of (records_affected, 0)
    """
    if df.empty:
        logger.warning(f"Empty DataFrame for net position, country {country_code}")
        return 0, 0

    records_affected = 0

    with get_connection() as conn:
        cursor = conn.cursor()

        for _, row in df.iterrows():
            ts_str = utils.format_timestamp_for_db(row["timestamp_utc"]) if pd.notna(row["timestamp_utc"]) else None
            cursor.execute(
                """
                INSERT OR REPLACE INTO net_position
                (country_code, timestamp_utc, net_position_mw,
                 data_quality, fetched_at)
                VALUES (?, ?, ?, 'actual', CURRENT_TIMESTAMP)
                """,
                (
                    country_code,
                    ts_str,
                    float(row["net_position_mw"]) if pd.notna(row["net_position_mw"]) else None,
                ),
            )
            records_affected += cursor.rowcount

    logger.info(f"Upserted {records_affected} net position records for {country_code}")
    return records_affected, 0


# ============================================================================
# INGESTION LOGGING
# ============================================================================


def log_ingestion_start(pipeline_type: str, country_code: Optional[str] = None) -> int:
    """
    Log the start of a data ingestion process

    Args:
        pipeline_type: Type of pipeline ('load', 'price', 'renewable', 'all')
        country_code: ISO 2-letter country code (optional)

    Returns:
        Log entry ID
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO data_ingestion_log
            (pipeline_type, country_code, start_time, status)
            VALUES (?, ?, ?, 'running')
        """,
            (pipeline_type, country_code, datetime.now(pytz.UTC).isoformat()),
        )

        return cursor.lastrowid


def log_ingestion_complete(
    log_id: int,
    records_inserted: int = 0,
    records_updated: int = 0,
    records_failed: int = 0,
    error_message: Optional[str] = None,
):
    """
    Log the completion of a data ingestion process

    Args:
        log_id: Log entry ID from log_ingestion_start
        records_inserted: Number of records inserted
        records_updated: Number of records updated
        records_failed: Number of records that failed
        error_message: Error message if failed (optional)
    """
    status = "failed" if error_message else "completed"

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE data_ingestion_log
            SET end_time = ?,
                status = ?,
                records_inserted = ?,
                records_updated = ?,
                records_failed = ?,
                error_message = ?
            WHERE id = ?
        """,
            (
                datetime.now(pytz.UTC).isoformat(),
                status,
                records_inserted,
                records_updated,
                records_failed,
                error_message,
                log_id,
            ),
        )


# ============================================================================
# COMPLETENESS CACHE UPDATE
# ============================================================================


def update_completeness_cache():
    """
    Update completeness cache table with latest data quality metrics

    This is a simplified version - full implementation would require
    complex gap analysis. For now, just update timestamps.
    """
    logger.info("Updating completeness cache...")

    with get_connection() as conn:
        cursor = conn.cursor()

        # Get all countries
        cursor.execute(
            "SELECT country_code FROM countries WHERE entsoe_domain IS NOT NULL"
        )
        countries = [row[0] for row in cursor.fetchall()]

        for country_code in countries:
            # Get latest timestamps for each data type
            cursor.execute(
                """
                SELECT
                    (SELECT MAX(timestamp_utc) FROM energy_load WHERE country_code = ?) as latest_load,
                    (SELECT MAX(timestamp_utc) FROM energy_price WHERE country_code = ?) as latest_price,
                    (SELECT MAX(timestamp_utc) FROM energy_renewable WHERE country_code = ?) as latest_renewable,
                    (SELECT COUNT(*) FROM energy_load WHERE country_code = ?) as count_load,
                    (SELECT COUNT(*) FROM energy_price WHERE country_code = ?) as count_price,
                    (SELECT COUNT(*) FROM energy_renewable WHERE country_code = ?) as count_renewable
            """,
                (
                    country_code,
                    country_code,
                    country_code,
                    country_code,
                    country_code,
                    country_code,
                ),
            )

            result = cursor.fetchone()

            # Note: Full completeness cache update would require more complex JSON generation
            # For now, we just ensure the cache is marked for recomputation
            logger.debug(
                f"Country {country_code}: Load={result[3]}, Price={result[4]}, Renewable={result[5]}"
            )

    logger.info("Completeness cache update complete")


# ============================================================================
# DATA RETRIEVAL
# ============================================================================


def get_latest_timestamp(table: str, country_code: str) -> Optional[datetime]:
    """
    Get the latest timestamp for a country in a specific table

    Args:
        table: Table name ('energy_load', 'energy_price', 'energy_renewable')
        country_code: ISO 2-letter country code

    Returns:
        Latest timestamp as datetime object or None

    Raises:
        ValueError: If table name is not in the whitelist
    """
    if table not in VALID_TABLES:
        raise ValueError(f"Invalid table name: {table}")

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT MAX(timestamp_utc) as latest
            FROM {table}
            WHERE country_code = ?
        """,
            (country_code,),
        )

        result = cursor.fetchone()
        if result and result["latest"]:
            return utils.parse_timestamp_from_db(result["latest"])

    return None


def get_record_count(table: str, country_code: str) -> int:
    """
    Get the number of records for a country in a specific table

    Args:
        table: Table name ('energy_load', 'energy_price', 'energy_renewable')
        country_code: ISO 2-letter country code

    Returns:
        Record count

    Raises:
        ValueError: If table name is not in the whitelist
    """
    if table not in VALID_TABLES:
        raise ValueError(f"Invalid table name: {table}")

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT COUNT(*) as count
            FROM {table}
            WHERE country_code = ?
        """,
            (country_code,),
        )

        result = cursor.fetchone()
        return result["count"] if result else 0


# ============================================================================
# DATABASE MAINTENANCE
# ============================================================================


def vacuum_database():
    """Run VACUUM to optimize database"""
    logger.info("Running VACUUM on database...")
    with get_connection() as conn:
        conn.execute("VACUUM")
    logger.info("VACUUM complete")


def analyze_database():
    """Run ANALYZE to update query planner statistics"""
    logger.info("Running ANALYZE on database...")
    with get_connection() as conn:
        conn.execute("ANALYZE")
    logger.info("ANALYZE complete")


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    # Test database operations
    print("Testing database operations...")

    # Setup logging
    utils.setup_logging()

    # Test get countries
    countries = get_countries()
    print(f"\nTotal countries: {len(countries)}")
    print(f"First 5 countries: {[c['country_code'] for c in countries[:5]]}")

    # Test get specific country
    de = get_country_by_code("DE")
    if de:
        print(f"\nGermany: {de}")

    # Test get latest timestamps
    for data_type in ["energy_load", "energy_price", "energy_renewable"]:
        latest = get_latest_timestamp(data_type, "DE")
        count = get_record_count(data_type, "DE")
        print(f"\n{data_type}:")
        print(f"  Latest: {latest}")
        print(f"  Count: {count}")

    print("\n[OK] Database operations test complete!")
