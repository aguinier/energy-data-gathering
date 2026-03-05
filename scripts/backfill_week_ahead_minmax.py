#!/usr/bin/env python3
"""
Backfill week-ahead load forecast min/max values from ENTSO-E

This script re-fetches all historical week-ahead data to populate
the forecast_min_mw and forecast_max_mw columns.

Usage:
    python scripts/backfill_week_ahead_minmax.py --start 2024-01-01 --end 2024-12-31
    python scripts/backfill_week_ahead_minmax.py --start 2024-01-01 --end 2024-12-31 --countries DE,FR
    python scripts/backfill_week_ahead_minmax.py --countries all
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import time

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
import utils
import pytz
from src import db
from src.entsoe_client import ENTSOEClient


logger = utils.setup_logging()


def get_countries_with_week_ahead_data() -> list:
    """Get list of countries that have week-ahead forecast data"""
    query = """
        SELECT DISTINCT country_code
        FROM energy_load_forecast
        WHERE forecast_type = 'week_ahead'
        ORDER BY country_code
    """
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

    return [row['country_code'] for row in rows]


def get_date_range_for_country(country_code: str) -> tuple:
    """Get the date range of existing week-ahead data for a country"""
    query = """
        SELECT
            MIN(DATE(target_timestamp_utc)) as min_date,
            MAX(DATE(target_timestamp_utc)) as max_date
        FROM energy_load_forecast
        WHERE country_code = ? AND forecast_type = 'week_ahead'
    """
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (country_code,))
        row = cursor.fetchone()

    if row and row['min_date'] and row['max_date']:
        return (
            datetime.strptime(row['min_date'], '%Y-%m-%d'),
            datetime.strptime(row['max_date'], '%Y-%m-%d')
        )
    return None, None


def backfill_week_ahead_minmax(
    country_code: str,
    start_date: datetime,
    end_date: datetime,
    client: ENTSOEClient
) -> int:
    """
    Backfill week-ahead min/max for a country in the specified date range

    Args:
        country_code: ISO 2-letter country code
        start_date: Start date
        end_date: End date
        client: ENTSO-E API client

    Returns:
        Number of records updated
    """
    total_records = 0

    # Process in monthly chunks to avoid API timeouts
    current = start_date
    while current < end_date:
        # Calculate chunk end (30 days or end_date, whichever comes first)
        chunk_end = min(current + timedelta(days=30), end_date)

        try:
            logger.info(f"  Fetching {country_code} week-ahead: {current.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")

            # Fetch with min/max using the updated client method
            df, pub_time = client.query_load_forecast_with_metadata(
                country_code,
                current.replace(tzinfo=pytz.UTC),
                chunk_end.replace(tzinfo=pytz.UTC),
                process_type='A31'  # Week-ahead
            )

            if df is not None and not df.empty:
                # Check if we got min/max columns
                has_min_max = 'forecast_min_mw' in df.columns and 'forecast_max_mw' in df.columns

                if has_min_max:
                    # Upsert with min/max values
                    inserted, _ = db.upsert_load_forecast_data(
                        df, country_code, 'week_ahead', pub_time
                    )
                    total_records += inserted
                    logger.info(f"    Updated {inserted} records with min/max")
                else:
                    logger.warning(f"    No min/max columns in response (fallback to single value)")
            else:
                logger.warning(f"    No data returned")

        except Exception as e:
            logger.error(f"    Error: {e}")

        # Rate limiting - be conservative
        time.sleep(1.0)
        current = chunk_end

    return total_records


def main():
    parser = argparse.ArgumentParser(
        description='Backfill week-ahead load forecast min/max values from ENTSO-E'
    )
    parser.add_argument(
        '--start',
        help='Start date (YYYY-MM-DD). If not specified, uses earliest data date for each country.'
    )
    parser.add_argument(
        '--end',
        help='End date (YYYY-MM-DD). If not specified, uses today.'
    )
    parser.add_argument(
        '--countries',
        default='all',
        help='Comma-separated country codes or "all" (default: all)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )

    args = parser.parse_args()

    # Determine countries to process
    if args.countries == 'all':
        countries = get_countries_with_week_ahead_data()
        if not countries:
            # Fallback to high-priority countries from config
            countries = [c['country_code'] for c in config.COUNTRIES if c.get('priority', 99) <= 2]
    else:
        countries = [c.strip().upper() for c in args.countries.split(',')]

    logger.info(f"Countries to process: {', '.join(countries)}")

    # Parse dates if provided
    end_date = datetime.strptime(args.end, '%Y-%m-%d') if args.end else datetime.now()

    if args.dry_run:
        logger.info("DRY RUN - No changes will be made")
        for country in countries:
            min_date, max_date = get_date_range_for_country(country)
            if min_date:
                start = datetime.strptime(args.start, '%Y-%m-%d') if args.start else min_date
                logger.info(f"  {country}: Would backfill {start.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            else:
                logger.info(f"  {country}: No existing week-ahead data")
        return

    # Initialize client
    client = ENTSOEClient()

    total_all = 0

    for country in countries:
        logger.info(f"Processing {country}...")

        # Get date range
        if args.start:
            start_date = datetime.strptime(args.start, '%Y-%m-%d')
        else:
            min_date, _ = get_date_range_for_country(country)
            if min_date:
                start_date = min_date
            else:
                logger.warning(f"  No existing week-ahead data for {country}, skipping")
                continue

        # Backfill
        records = backfill_week_ahead_minmax(country, start_date, end_date, client)
        total_all += records
        logger.info(f"  {country}: Updated {records} records")

    logger.info(f"Total records updated: {total_all}")


if __name__ == "__main__":
    main()
