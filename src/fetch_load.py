"""
Fetch electricity load (demand) data from ENTSO-E API
"""

import logging
from datetime import datetime
from typing import Optional, Tuple

from . import db
from .entsoe_client import ENTSOEClient, ENTSOENoDataError
from .fetch_result import FetchResult
import utils


logger = logging.getLogger('entsoe_pipeline')


def fetch_load_data(
    client: ENTSOEClient,
    country_code: str,
    start: datetime,
    end: datetime,
    log_id: Optional[int] = None
) -> Tuple[int, int, int]:
    """
    Fetch and store load data for a country

    Args:
        client: ENTSO-E client instance
        country_code: ISO 2-letter country code
        start: Start datetime (UTC)
        end: End datetime (UTC)
        log_id: Optional ingestion log ID

    Returns:
        Tuple of (records_inserted, records_updated, records_failed)
    """
    logger.info(f"Fetching load data for {country_code}: {start.date()} to {end.date()}")

    try:
        # Query ENTSO-E API with metadata
        df, publication_time = client.query_load_with_metadata(country_code, start, end)

        if df is None or df.empty:
            logger.warning(f"No load data returned for {country_code}")
            return 0, 0, 0

        # Upsert data to database with publication timestamp
        records_inserted, records_updated = db.upsert_load_data(
            df, country_code, publication_timestamp=publication_time
        )

        logger.info(f"Successfully stored {records_inserted} load records for {country_code}")
        return records_inserted, records_updated, 0

    except ENTSOENoDataError as e:
        logger.warning(f"No load data available for {country_code}: {e}")
        return 0, 0, 0

    except Exception as e:
        error_msg = utils.format_error(e, f"fetch_load_data({country_code})")
        logger.error(error_msg)

        if log_id:
            db.log_ingestion_complete(
                log_id,
                records_failed=1,
                error_message=error_msg,
            )

        # The reason travels with the count. `pipeline._fetch_data_chunk` calls
        # this without a log_id, so before ABL-61 the branch above never ran and
        # the row went to the database as `records_failed = 1`, reason NULL.
        return FetchResult.failure(error_msg)


def fetch_load_for_country(
    country_code: str,
    start: datetime,
    end: datetime
) -> bool:
    """
    Fetch load data for a single country (convenience function)

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
    log_id = db.log_ingestion_start('load', country_code)

    try:
        # Fetch data
        inserted, updated, failed = fetch_load_data(
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
        logger.error(f"Failed to fetch load data for {country_code}: {e}")
        db.log_ingestion_complete(log_id, records_failed=1, error_message=str(e))
        return False


if __name__ == "__main__":
    # Test load fetcher
    import pytz
    from datetime import datetime

    print("Testing load data fetcher...")
    utils.setup_logging()

    # Test single day
    start = pytz.UTC.localize(datetime(2024, 12, 20))
    end = pytz.UTC.localize(datetime(2024, 12, 21))

    success = fetch_load_for_country('DE', start, end)
    print(f"\nFetch {'successful' if success else 'failed'}")
