"""
Fetch renewable energy generation data from ENTSO-E API
"""

import logging
from datetime import datetime
from typing import Optional, Tuple

from . import db
from .entsoe_client import ENTSOEClient, ENTSOENoDataError
from .fetch_result import FetchResult
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
    Fetch the A75 document ONCE and store BOTH energy_generation (the
    complete document) and energy_renewable (the frozen 8-column subset)
    from that single response.

    This used to call query_generation_per_type_with_metadata and write only
    energy_renewable. It now calls
    client.query_generation_and_renewable_with_metadata -- which itself
    issues exactly the same two _make_request calls (raw XML for the
    publication timestamp, pandas client for the structured frame) that
    query_generation_per_type_with_metadata always made -- and derives both
    output frames from that one fetch. There is no second ENTSO-E request to
    fill energy_generation; see that method's docstring and
    tests/test_generation_mapping.py for the equivalence proof that the
    derived energy_renewable frame is unchanged.

    Args:
        client: ENTSO-E client instance
        country_code: ISO 2-letter country code
        start: Start datetime (UTC)
        end: End datetime (UTC)
        log_id: Optional ingestion log ID

    Returns:
        Tuple of (records_inserted, records_updated, records_failed) --
        inserted/updated are summed across both tables (generation +
        renewable) so callers that track total records affected (e.g.
        Pipeline._fetch_data_chunk's self.stats['total_records']) keep
        seeing an accurate count without needing their own changes.
    """
    logger.info(f"Fetching generation data for {country_code}: {start.date()} to {end.date()}")

    try:
        # Query ENTSO-E API once; derive both output frames from it.
        generation_df, renewable_df, publication_time = (
            client.query_generation_and_renewable_with_metadata(country_code, start, end)
        )

        if (
            generation_df is None or generation_df.empty
            or renewable_df is None or renewable_df.empty
        ):
            logger.warning(f"No generation data returned for {country_code}")
            return 0, 0, 0

        # Upsert the complete document first...
        gen_inserted, gen_updated = db.upsert_generation_data(
            generation_df, country_code, publication_timestamp=publication_time
        )

        # ...then the frozen renewable subset, derived from the same fetch.
        ren_inserted, ren_updated = db.upsert_renewable_data(
            renewable_df, country_code, publication_timestamp=publication_time
        )

        records_inserted = gen_inserted + ren_inserted
        records_updated = gen_updated + ren_updated

        logger.info(
            f"Successfully stored {gen_inserted} generation + {ren_inserted} "
            f"renewable records for {country_code}"
        )
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
                error_message=error_msg,
            )

        return FetchResult.failure(error_msg)


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
