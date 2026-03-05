#!/usr/bin/env python3
"""
Backfill weather data gaps from Open-Meteo Historical API

This script identifies gaps in weather data and fills them using the
Open-Meteo Historical Weather API (ERA5 reanalysis data).

Usage:
    # Backfill all gaps for all countries
    python scripts/backfill_weather.py

    # Backfill specific country
    python scripts/backfill_weather.py --countries DE,FR

    # Backfill specific date range
    python scripts/backfill_weather.py --start 2025-01-01 --end 2025-11-30

    # Fix null records only
    python scripts/backfill_weather.py --fix-nulls

    # Dry run (show what would be done)
    python scripts/backfill_weather.py --dry-run
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple

import pytz

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import utils
from src import db
from src.fetch_weather import (
    fetch_weather_data,
    get_weather_countries,
    get_weather_gaps,
    COUNTRY_COORDINATES
)


def get_null_record_ranges(country_code: str) -> List[Tuple[datetime, datetime]]:
    """
    Find date ranges with null temperature records for a country

    Args:
        country_code: Country code

    Returns:
        List of (start, end) tuples for ranges with null records
    """
    import pandas as pd

    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT MIN(timestamp_utc) as start, MAX(timestamp_utc) as end
            FROM weather_data
            WHERE country_code = ? AND temperature_2m_k IS NULL
            GROUP BY DATE(timestamp_utc)
            ORDER BY start
        """, (country_code,))

        ranges = []
        for row in cursor.fetchall():
            # Parse timestamps - handle both tz-aware and naive timestamps
            start = pd.to_datetime(row[0])
            end = pd.to_datetime(row[1])

            # Ensure timezone awareness
            if start.tzinfo is None:
                start = start.tz_localize('UTC')
            else:
                start = start.tz_convert('UTC')

            if end.tzinfo is None:
                end = end.tz_localize('UTC')
            else:
                end = end.tz_convert('UTC')

            ranges.append((start, end + timedelta(hours=1)))

        return ranges


def delete_null_records(country_code: str) -> int:
    """
    Delete records with null temperature values (they will be re-fetched)

    Args:
        country_code: Country code

    Returns:
        Number of records deleted
    """
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM weather_data
            WHERE country_code = ? AND temperature_2m_k IS NULL
        """, (country_code,))
        deleted = cursor.rowcount
        conn.commit()
        return deleted


def consolidate_gaps(gaps: List[Tuple[datetime, datetime]]) -> List[Tuple[datetime, datetime]]:
    """
    Consolidate overlapping or adjacent gaps into larger ranges

    Args:
        gaps: List of (start, end) tuples

    Returns:
        Consolidated list of gaps
    """
    if not gaps:
        return []

    # Sort by start time
    sorted_gaps = sorted(gaps, key=lambda x: x[0])

    consolidated = [sorted_gaps[0]]

    for start, end in sorted_gaps[1:]:
        last_start, last_end = consolidated[-1]

        # If gap starts within 1 day of last gap end, merge them
        if start <= last_end + timedelta(days=1):
            consolidated[-1] = (last_start, max(last_end, end))
        else:
            consolidated.append((start, end))

    return consolidated


def main():
    parser = argparse.ArgumentParser(
        description='Backfill weather data gaps from Open-Meteo API'
    )
    parser.add_argument(
        '--countries',
        type=str,
        help='Comma-separated list of country codes (default: all with existing data)'
    )
    parser.add_argument(
        '--start',
        type=str,
        help='Start date (YYYY-MM-DD) for backfill range'
    )
    parser.add_argument(
        '--end',
        type=str,
        help='End date (YYYY-MM-DD) for backfill range'
    )
    parser.add_argument(
        '--fix-nulls',
        action='store_true',
        help='Fix records with null values (delete and re-fetch)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Verbose output'
    )

    args = parser.parse_args()

    # Setup logging
    utils.setup_logging()

    print("=" * 60)
    print("Weather Data Backfill")
    print("=" * 60)

    # Determine countries to process
    if args.countries:
        countries = [c.strip().upper() for c in args.countries.split(',')]
    else:
        countries = get_weather_countries()

    print(f"\nCountries to process: {len(countries)}")

    # Process each country
    total_inserted = 0
    total_failed = 0

    for country_code in countries:
        print(f"\n{'='*40}")
        print(f"Processing {country_code}")
        print(f"{'='*40}")

        if country_code not in COUNTRY_COORDINATES:
            print(f"  WARNING: No coordinates for {country_code}, skipping")
            continue

        # Get gaps for this country
        gaps = get_weather_gaps(country_code)

        # Add custom date range if specified
        if args.start and args.end:
            custom_start = pytz.UTC.localize(datetime.strptime(args.start, '%Y-%m-%d'))
            custom_end = pytz.UTC.localize(datetime.strptime(args.end, '%Y-%m-%d'))
            gaps.append((custom_start, custom_end))

        # Handle null records
        null_ranges = get_null_record_ranges(country_code)
        if null_ranges:
            print(f"  Found {len(null_ranges)} date ranges with null records")
            if args.fix_nulls or not args.dry_run:
                gaps.extend(null_ranges)

        # Consolidate gaps
        gaps = consolidate_gaps(gaps)

        if not gaps:
            print(f"  No gaps found for {country_code}")
            continue

        print(f"  Found {len(gaps)} gap(s) to fill:")
        total_hours = 0
        for gap_start, gap_end in gaps:
            hours = (gap_end - gap_start).total_seconds() / 3600
            total_hours += hours
            print(f"    - {gap_start.date()} to {gap_end.date()} ({int(hours)} hours)")

        print(f"  Total: {int(total_hours)} hours to backfill")

        if args.dry_run:
            print("  [DRY RUN] Would fetch data for these gaps")
            continue

        # Delete null records first if fixing
        if args.fix_nulls and null_ranges:
            deleted = delete_null_records(country_code)
            print(f"  Deleted {deleted} null records")

        # Fetch data for each gap
        for gap_start, gap_end in gaps:
            print(f"  Fetching {gap_start.date()} to {gap_end.date()}...")

            inserted, updated, failed = fetch_weather_data(
                country_code,
                gap_start,
                gap_end
            )

            total_inserted += inserted
            total_failed += failed

            if failed:
                print(f"    ERROR: {failed} records failed")
            else:
                print(f"    OK: {inserted} records inserted")

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total records inserted: {total_inserted}")
    print(f"Total failures: {total_failed}")

    if args.dry_run:
        print("\n[DRY RUN] No changes were made")


if __name__ == '__main__':
    main()
