"""
Fetch cross-border physical flow data from ENTSO-E.

Uses query_physical_crossborder_allborders() which returns a wide DataFrame
with one column per neighbor. Normalizes to long format (country_from,
country_to, flow_mw) for storage.
"""

import logging
from datetime import datetime
from typing import Tuple, Optional

import pandas as pd

from . import db
import utils
from .entsoe_client import ENTSOEClient, normalize_zone_to_country

logger = logging.getLogger("energy_data_gathering.fetch_crossborder_flows")


def _normalize_wide_to_long(
    wide_df: pd.DataFrame,
    country_from: str,
) -> pd.DataFrame:
    """Convert wide DataFrame (columns=neighbors) to long format.

    Args:
        wide_df: DataFrame from query_physical_crossborder_allborders,
                 index=timestamps, columns=neighbor zone names + 'sum'
        country_from: ISO 2-letter code of the exporting country

    Returns:
        DataFrame with columns: timestamp_utc, country_to, flow_mw
    """
    # Drop the 'sum' column if present
    cols_to_melt = [c for c in wide_df.columns if c != "sum"]

    if not cols_to_melt:
        return pd.DataFrame(columns=["timestamp_utc", "country_to", "flow_mw"])

    df = wide_df[cols_to_melt].copy()
    df.index.name = "timestamp_utc"
    df = df.reset_index()

    # Ensure timestamps are tz-aware UTC
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    # Melt: wide -> long
    long_df = df.melt(
        id_vars=["timestamp_utc"],
        var_name="zone_name",
        value_name="flow_mw",
    )

    # Drop NaN flow values
    long_df = long_df.dropna(subset=["flow_mw"])

    # Map bidding zone names to 2-letter country codes
    long_df["country_to"] = long_df["zone_name"].apply(normalize_zone_to_country)

    # Aggregate multiple zones for same country (e.g., IT_NORD + IT_CSUD -> IT)
    long_df = (
        long_df.groupby(["timestamp_utc", "country_to"], as_index=False)["flow_mw"]
        .sum()
    )

    # Resample to hourly
    if not long_df.empty:
        result_frames = []
        for country_to, group in long_df.groupby("country_to"):
            hourly = group.set_index("timestamp_utc")["flow_mw"].resample("h").mean()
            hourly_df = hourly.reset_index()
            hourly_df["country_to"] = country_to
            result_frames.append(hourly_df)
        long_df = pd.concat(result_frames, ignore_index=True)

    return long_df[["timestamp_utc", "country_to", "flow_mw"]]


def fetch_crossborder_flows_data(
    client: ENTSOEClient,
    country_code: str,
    start: datetime,
    end: datetime,
    log_id: Optional[int] = None,
) -> Tuple[int, int, int]:
    """
    Fetch and store cross-border physical flows for a country (exports).

    Args:
        client: ENTSO-E client instance
        country_code: ISO 2-letter country code
        start: Start datetime (UTC)
        end: End datetime (UTC)
        log_id: Optional ingestion log ID

    Returns:
        Tuple of (records_inserted, records_updated, records_failed)
    """
    logger.info(f"Fetching crossborder flows for {country_code}: {start.date()} to {end.date()}")

    try:
        db.create_crossborder_flows_table()

        wide_df = client.query_crossborder_all(country_code, start, end, export=True)

        if wide_df is None or wide_df.empty:
            logger.warning(f"No crossborder flow data for {country_code}")
            return 0, 0, 0

        long_df = _normalize_wide_to_long(wide_df, country_code)

        if long_df.empty:
            logger.warning(f"No valid flow data after normalization for {country_code}")
            return 0, 0, 0

        records_inserted, _ = db.upsert_crossborder_flows(long_df, country_code)

        logger.info(
            f"Stored {records_inserted} crossborder flow records for {country_code} "
            f"({long_df['country_to'].nunique()} neighbors)"
        )
        return records_inserted, 0, 0

    except Exception as e:
        logger.error(f"Error fetching crossborder flows for {country_code}: {e}")
        if log_id:
            db.log_ingestion_complete(log_id, records_failed=1, error_message=str(e))
        return 0, 0, 1
