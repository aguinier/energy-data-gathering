"""
Fetch renewable energy generation data from ENTSO-E API
"""

import logging
from datetime import datetime
from typing import Optional, Tuple

from . import db
from .entsoe_client import ENTSOEClient, ENTSOENoDataError
import utils


logger = logging.getLogger('entsoe_pipeline')


def fetch_renewable_data(
    client: ENTSOEClient,
    country_code: str,
    start: datetime,
    end: datetime,
    log_id: Optional[int] = None
) -> Tuple[int, int, int]:
    """
    Fetch and store renewable energy generation data for a country

    Args:
        client: ENTSO-E client instance
        country_code: ISO 2-letter country code
        start: Start datetime (UTC)
        end: End datetime (UTC)
        log_id: Optional ingestion log ID

    Returns:
        Tuple of (records_inserted, records_updated, records_failed)
    """
    logger.info(f"Fetching renewable data for {country_code}: {start.date()} to {end.date()}")

    try:
        # Query ENTSO-E API with metadata
        df, publication_time = client.query_generation_per_type_with_metadata(country_code, start, end)

        if df is None or df.empty:
            logger.warning(f"No renewable data returned for {country_code}")
            return 0, 0, 0

        # Upsert data to database with publication timestamp
        records_inserted, records_updated = db.upsert_renewable_data(
            df, country_code, publication_timestamp=publication_time
        )

        logger.info(f"Successfully stored {records_inserted} renewable records for {country_code}")
        return records_inserted, records_updated, 0

    except ENTSOENoDataError as e:
        logger.warning(f"No renewable data available for {country_code}: {e}")
        return 0, 0, 0

    except Exception as e:
        error_msg = utils.format_error(e, f"fetch_renewable_data({country_code})")
        logger.error(error_msg)

        if log_id:
            db.log_ingestion_complete(
                log_id,
                records_failed=1,
                error_message=str(e)
            )

        return 0, 0, 1


def fetch_renewable_for_country(
    country_code: str,
    start: datetime,
    end: datetime
) -> bool:
    """
    Fetch renewable data for a single country (convenience function)

    Args:
        country_code: ISO 2-letter country code
        start: Start datetime (UTC)
        end: End datetime (UTC)

    Returns:
        True if successful, False otherwise
    """
    # Initialize client
    client = ENTSOEClient()

    # Log start
    log_id = db.log_ingestion_start('renewable', country_code)

    try:
        # Fetch data
        inserted, updated, failed = fetch_renewable_data(
            client, country_code, start, end, log_id
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
        logger.error(f"Failed to fetch renewable data for {country_code}: {e}")
        db.log_ingestion_complete(log_id, records_failed=1, error_message=str(e))
        return False


if __name__ == "__main__":
    # Test renewable fetcher
    import pytz
    from datetime import datetime

    print("Testing renewable data fetcher...")
    utils.setup_logging()

    # Test single day
    start = pytz.UTC.localize(datetime(2024, 12, 20))
    end = pytz.UTC.localize(datetime(2024, 12, 21))

    success = fetch_renewable_for_country('DE', start, end)
    print(f"\nFetch {'successful' if success else 'failed'}")
