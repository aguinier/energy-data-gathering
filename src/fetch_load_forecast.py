"""
Fetch electricity load forecast data from ENTSO-E API
"""

import logging
from datetime import datetime
from typing import Optional, Tuple

from . import db
from .entsoe_client import ENTSOEClient, ENTSOENoDataError
import utils


logger = logging.getLogger('entsoe_pipeline')


def fetch_load_forecast_data(
    client: ENTSOEClient,
    country_code: str,
    start: datetime,
    end: datetime,
    forecast_type: str,
    log_id: Optional[int] = None
) -> Tuple[int, int, int]:
    """
    Fetch and store load forecast data for a country

    Args:
        client: ENTSO-E client instance
        country_code: ISO 2-letter country code
        start: Start datetime (UTC)
        end: End datetime (UTC)
        forecast_type: 'day_ahead' or 'week_ahead'
        log_id: Optional ingestion log ID

    Returns:
        Tuple of (records_inserted, records_updated, records_failed)
    """
    logger.info(f"Fetching {forecast_type} load forecast for {country_code}: {start.date()} to {end.date()}")

    try:
        # Map forecast_type to process_type
        process_type_map = {
            'day_ahead': 'A01',
            'week_ahead': 'A31'
        }
        process_type = process_type_map[forecast_type]

        # Query ENTSO-E API with metadata
        df, publication_time = client.query_load_forecast_with_metadata(
            country_code, start, end, process_type=process_type
        )

        if df is None or df.empty:
            logger.warning(f"No {forecast_type} forecast data returned for {country_code}")
            return 0, 0, 0

        # Upsert data to database with publication timestamp
        records_inserted, records_updated = db.upsert_load_forecast_data(
            df, country_code, forecast_type, publication_timestamp=publication_time
        )

        logger.info(f"Successfully stored {records_inserted} {forecast_type} forecast records for {country_code}")
        return records_inserted, records_updated, 0

    except ENTSOENoDataError as e:
        logger.warning(f"No {forecast_type} forecast data available for {country_code}: {e}")
        return 0, 0, 0

    except Exception as e:
        error_msg = utils.format_error(e, f"fetch_load_forecast_data({country_code}, {forecast_type})")
        logger.error(error_msg)

        if log_id:
            db.log_ingestion_complete(
                log_id,
                records_failed=1,
                error_message=str(e)
            )

        return 0, 0, 1


def fetch_load_forecast_for_country(
    country_code: str,
    start: datetime,
    end: datetime,
    forecast_type: str
) -> bool:
    """
    Fetch load forecast data for a single country (convenience function)

    Args:
        country_code: ISO 2-letter country code
        start: Start datetime (UTC)
        end: End datetime (UTC)
        forecast_type: 'day_ahead' or 'week_ahead'

    Returns:
        True if successful, False otherwise
    """
    # Initialize client
    client = ENTSOEClient()

    # Log start
    log_id = db.log_ingestion_start(f'load_forecast_{forecast_type}', country_code)

    try:
        # Fetch data
        inserted, updated, failed = fetch_load_forecast_data(
            client, country_code, start, end, forecast_type, log_id
        )

        # Log completion
        db.log_ingestion_complete(
            log_id,
            records_inserted=inserted,
            records_updated=updated,
            records_failed=failed
        )

        return failed == 0

    except Exception as e:
        logger.error(f"Failed to fetch {forecast_type} forecast data for {country_code}: {e}")
        db.log_ingestion_complete(log_id, records_failed=1, error_message=str(e))
        return False


if __name__ == "__main__":
    # Test load forecast fetcher
    import pytz
    from datetime import datetime

    print("Testing load forecast data fetcher...")
    utils.setup_logging()

    # Test single day
    start = pytz.UTC.localize(datetime(2024, 12, 20))
    end = pytz.UTC.localize(datetime(2024, 12, 21))

    # Test day-ahead forecast
    print("\n--- Testing Day-Ahead Forecast (Germany) ---")
    success = fetch_load_forecast_for_country('DE', start, end, 'day_ahead')
    print(f"Day-ahead fetch {'successful' if success else 'failed'}")

    # Test week-ahead forecast
    print("\n--- Testing Week-Ahead Forecast (Germany) ---")
    success = fetch_load_forecast_for_country('DE', start, end, 'week_ahead')
    print(f"Week-ahead fetch {'successful' if success else 'failed'}")
