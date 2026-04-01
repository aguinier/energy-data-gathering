"""
Fetch realized net position data from ENTSO-E.

Net position = aggregated import/export balance per country.
Positive = net exporter, Negative = net importer.
"""

import logging
from datetime import datetime
from typing import Tuple, Optional

import pandas as pd

from src import db, utils
from src.entsoe_client import ENTSOEClient

logger = logging.getLogger("energy_data_gathering.fetch_net_position")


def fetch_net_position_data(
    client: ENTSOEClient,
    country_code: str,
    start: datetime,
    end: datetime,
    log_id: Optional[int] = None,
) -> Tuple[int, int, int]:
    """
    Fetch and store net position data for a country.

    Args:
        client: ENTSO-E client instance
        country_code: ISO 2-letter country code
        start: Start datetime (UTC)
        end: End datetime (UTC)
        log_id: Optional ingestion log ID

    Returns:
        Tuple of (records_inserted, records_updated, records_failed)
    """
    logger.info(f"Fetching net position for {country_code}: {start.date()} to {end.date()}")

    try:
        db.create_net_position_table()

        series = client.query_net_position_data(country_code, start, end, dayahead=True)

        if series is None or series.empty:
            logger.warning(f"No net position data for {country_code}")
            return 0, 0, 0

        # Convert Series to DataFrame
        df = series.to_frame(name="net_position_mw")
        df.index.name = "timestamp_utc"
        df = df.reset_index()

        # Ensure timestamps are tz-aware UTC
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

        # Resample to hourly
        df = df.set_index("timestamp_utc")
        df = df["net_position_mw"].resample("h").mean().reset_index()

        # Drop NaN
        df = df.dropna(subset=["net_position_mw"])

        if df.empty:
            logger.warning(f"No valid net position data after resampling for {country_code}")
            return 0, 0, 0

        records_inserted, _ = db.upsert_net_position(df, country_code)

        logger.info(f"Stored {records_inserted} net position records for {country_code}")
        return records_inserted, 0, 0

    except Exception as e:
        logger.error(f"Error fetching net position for {country_code}: {e}")
        if log_id:
            db.log_ingestion_complete(log_id, records_failed=1, error_message=str(e))
        return 0, 0, 1
