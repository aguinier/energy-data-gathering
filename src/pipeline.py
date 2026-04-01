"""
Main pipeline orchestrator for ENTSO-E data gathering

Coordinates data fetching across:
- Multiple countries
- Multiple data types (load, price, renewable)
- Date ranges (backfill and update modes)
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict
import pytz

import config
import utils
from . import db
from .entsoe_client import ENTSOEClient
from . import fetch_load, fetch_price, fetch_renewable, fetch_load_forecast, fetch_wind_solar_forecast
from . import fetch_crossborder_flows, fetch_net_position


logger = logging.getLogger('entsoe_pipeline')


# ============================================================================
# PIPELINE ORCHESTRATOR
# ============================================================================

class ENTSOEPipeline:
    """Main pipeline orchestrator for ENTSO-E data gathering"""

    def __init__(self):
        """Initialize pipeline"""
        self.client = ENTSOEClient()
        self.stats = {
            'total_records': 0,
            'total_countries': 0,
            'successful_countries': 0,
            'failed_countries': 0,
            'by_data_type': {}
        }

    def run_backfill(
        self,
        start_date: str,
        end_date: str,
        data_types: List[str],
        country_codes: Optional[List[str]] = None,
        priority: Optional[int] = None
    ):
        """
        Run backfill mode to fetch historical data

        Args:
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            data_types: List of data types to fetch ('load', 'price', 'renewable')
            country_codes: List of country codes to process (None = all)
            priority: Filter countries by priority (None = all)
        """
        logger.info("=" * 80)
        logger.info("ENTSO-E BACKFILL PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Data types: {', '.join(data_types)}")

        # Get countries
        countries = self._get_countries(country_codes, priority)
        logger.info(f"Countries to process: {len(countries)}")

        # Split date range into chunks
        date_chunks = utils.get_date_range(start_date, end_date, chunk_days=90)
        logger.info(f"Date range split into {len(date_chunks)} chunks (max 90 days each)")

        # Process each country
        progress = utils.ProgressTracker(len(countries), "Processing countries")

        for country in countries:
            country_code = country['country_code']
            country_name = country['country_name']

            logger.info(f"\n--- Processing {country_name} ({country_code}) ---")

            # Check for known issues
            if utils.is_no_data_country(country_code):
                logger.warning(f"Skipping {country_code}: Known to have no ENTSO-E data")
                progress.update(item=f"{country_code} (skipped)")
                continue

            if utils.is_problematic_country(country_code):
                issue = utils.get_country_issue(country_code)
                logger.warning(f"Known issue with {country_code}: {issue}")

            # Process each data type
            country_success = True
            for data_type in data_types:
                logger.info(f"Fetching {data_type} data for {country_code}...")

                type_success = True
                for start, end in date_chunks:
                    success = self._fetch_data_chunk(
                        data_type,
                        country_code,
                        start,
                        end
                    )
                    if not success:
                        type_success = False
                        country_success = False

                # Update stats
                if data_type not in self.stats['by_data_type']:
                    self.stats['by_data_type'][data_type] = {'success': 0, 'failed': 0}

                if type_success:
                    self.stats['by_data_type'][data_type]['success'] += 1
                else:
                    self.stats['by_data_type'][data_type]['failed'] += 1

            # Update country stats
            if country_success:
                self.stats['successful_countries'] += 1
            else:
                self.stats['failed_countries'] += 1

            progress.update(item=f"{country_code} ({'ok' if country_success else 'FAIL'})")

        progress.finish()

        # Update completeness cache
        logger.info("\nUpdating completeness cache...")
        db.update_completeness_cache()

        # Print summary
        self._print_summary()

    def run_update(
        self,
        days_back: int = 7,
        data_types: Optional[List[str]] = None,
        country_codes: Optional[List[str]] = None,
        include_dayahead: bool = False
    ):
        """
        Run update mode to fetch recent data

        Args:
            days_back: Number of days to go back from now
            data_types: List of data types to fetch (None = all)
            country_codes: List of country codes to process (None = all)
            include_dayahead: If True, extend end date to D+1 for day-ahead data types
        """
        logger.info("=" * 80)
        logger.info("ENTSO-E UPDATE PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Fetching last {days_back} days of data")
        if include_dayahead:
            logger.info("Day-ahead mode: Will fetch D+1 data for applicable types")

        # Default to all data types
        if data_types is None:
            data_types = ['load', 'price', 'renewable']
        logger.info(f"Data types: {', '.join(data_types)}")

        # Get date range
        start, end = utils.get_recent_date_range(days_back)

        # Calculate extended end date for day-ahead data types
        if include_dayahead:
            end_dayahead = utils.get_dayahead_end_date()
            logger.info(f"Date range: {start.date()} to {end.date()} (D+1 end: {end_dayahead.date()})")
        else:
            end_dayahead = end
            logger.info(f"Date range: {start.date()} to {end.date()}")

        # Get countries
        countries = self._get_countries(country_codes, priority=None)
        logger.info(f"Countries to process: {len(countries)}")

        # Process each country
        progress = utils.ProgressTracker(len(countries), "Updating countries")

        for country in countries:
            country_code = country['country_code']
            country_name = country['country_name']

            logger.info(f"\n--- Updating {country_name} ({country_code}) ---")

            # Skip known no-data countries
            if utils.is_no_data_country(country_code):
                logger.debug(f"Skipping {country_code}: Known to have no ENTSO-E data")
                progress.update(item=f"{country_code} (skipped)")
                continue

            # Process each data type
            country_success = True
            for data_type in data_types:
                # Use extended end date for day-ahead data types
                if include_dayahead and config.is_dayahead_data_type(data_type):
                    fetch_end = end_dayahead
                else:
                    fetch_end = end

                success = self._fetch_data_chunk(
                    data_type,
                    country_code,
                    start,
                    fetch_end
                )
                if not success:
                    country_success = False

                # Update stats
                if data_type not in self.stats['by_data_type']:
                    self.stats['by_data_type'][data_type] = {'success': 0, 'failed': 0}

                if success:
                    self.stats['by_data_type'][data_type]['success'] += 1
                else:
                    self.stats['by_data_type'][data_type]['failed'] += 1

            # Update country stats
            if country_success:
                self.stats['successful_countries'] += 1
            else:
                self.stats['failed_countries'] += 1

            progress.update(item=f"{country_code} ({'ok' if country_success else 'FAIL'})")

        progress.finish()

        # Update completeness cache
        logger.info("\nUpdating completeness cache...")
        db.update_completeness_cache()

        # Print summary
        self._print_summary()

    # ========================================================================
    # HELPER METHODS
    # ========================================================================

    def _get_countries(
        self,
        country_codes: Optional[List[str]],
        priority: Optional[int]
    ) -> List[Dict]:
        """Get list of countries to process"""
        if country_codes:
            # Specific countries requested
            countries = []
            for code in country_codes:
                country = db.get_country_by_code(code)
                if country:
                    countries.append(country)
                else:
                    logger.warning(f"Country not found: {code}")
            return countries
        else:
            # All countries (optionally filtered by priority)
            return db.get_countries(priority=priority)

    def _fetch_data_chunk(
        self,
        data_type: str,
        country_code: str,
        start: datetime,
        end: datetime
    ) -> bool:
        """
        Fetch data for a specific chunk

        Args:
            data_type: Type of data ('load', 'price', 'renewable')
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)

        Returns:
            True if successful, False otherwise
        """
        try:
            if data_type == 'load':
                inserted, updated, failed = fetch_load.fetch_load_data(
                    self.client, country_code, start, end
                )
            elif data_type == 'price':
                inserted, updated, failed = fetch_price.fetch_price_data(
                    self.client, country_code, start, end
                )
            elif data_type == 'renewable':
                inserted, updated, failed = fetch_renewable.fetch_renewable_data(
                    self.client, country_code, start, end
                )
            elif data_type == 'load_forecast_day_ahead':
                inserted, updated, failed = fetch_load_forecast.fetch_load_forecast_data(
                    self.client, country_code, start, end, 'day_ahead'
                )
            elif data_type == 'load_forecast_week_ahead':
                inserted, updated, failed = fetch_load_forecast.fetch_load_forecast_data(
                    self.client, country_code, start, end, 'week_ahead'
                )
            elif data_type == 'wind_solar_forecast':
                inserted, updated, failed = fetch_wind_solar_forecast.fetch_wind_solar_forecast_data(
                    self.client, country_code, start, end
                )
            elif data_type == 'crossborder_flows':
                inserted, updated, failed = fetch_crossborder_flows.fetch_crossborder_flows_data(
                    self.client, country_code, start, end
                )
            elif data_type == 'net_position':
                inserted, updated, failed = fetch_net_position.fetch_net_position_data(
                    self.client, country_code, start, end
                )
            else:
                logger.error(f"Unknown data type: {data_type}")
                return False

            # Update total records
            self.stats['total_records'] += inserted

            return failed == 0

        except Exception as e:
            logger.error(f"Error fetching {data_type} data for {country_code}: {e}")
            return False

    def _print_summary(self):
        """Print pipeline execution summary"""
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total countries processed: {self.stats['successful_countries'] + self.stats['failed_countries']}")
        logger.info(f"  Successful: {self.stats['successful_countries']}")
        logger.info(f"  Failed: {self.stats['failed_countries']}")
        logger.info(f"Total records inserted: {self.stats['total_records']}")

        logger.info("\nBy data type:")
        for data_type, stats in self.stats['by_data_type'].items():
            logger.info(f"  {data_type}: {stats['success']} successful, {stats['failed']} failed")

        logger.info("=" * 80)


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def backfill(
    start_date: str,
    end_date: str,
    data_types: List[str] = None,
    countries: List[str] = None,
    priority: int = None
):
    """
    Convenience function to run backfill pipeline

    Args:
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        data_types: List of data types to fetch (default: all)
        countries: List of country codes (default: all)
        priority: Filter by country priority (default: all)
    """
    if data_types is None:
        data_types = ['load', 'price', 'renewable']

    pipeline = ENTSOEPipeline()
    pipeline.run_backfill(start_date, end_date, data_types, countries, priority)


def update(
    days_back: int = 7,
    data_types: List[str] = None,
    countries: List[str] = None,
    include_dayahead: bool = False
):
    """
    Convenience function to run update pipeline

    Args:
        days_back: Number of days to go back (default: 7)
        data_types: List of data types to fetch (default: all)
        countries: List of country codes (default: all)
        include_dayahead: If True, extend end date to D+1 for day-ahead data types
    """
    if data_types is None:
        data_types = ['load', 'price', 'renewable']

    pipeline = ENTSOEPipeline()
    pipeline.run_update(days_back, data_types, countries, include_dayahead)


if __name__ == "__main__":
    # Test pipeline
    print("Testing pipeline orchestrator...")
    utils.setup_logging()

    # Test update mode with single country
    logger.info("Testing update mode with Germany (last 2 days)")
    update(days_back=2, countries=['DE'])

    print("\n[OK] Pipeline test complete!")
