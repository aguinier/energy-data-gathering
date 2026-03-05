#!/usr/bin/env python3
"""
Backfill publication timestamps for existing ENTSO-E data

This script re-queries ENTSO-E API to extract publication timestamps
and updates existing database records.

Usage:
    python scripts/backfill_publication_timestamps.py --table load --countries DE,FR
    python scripts/backfill_publication_timestamps.py --table price --start 2024-01-01 --end 2024-12-31
    python scripts/backfill_publication_timestamps.py --table all --countries all
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import sqlite3

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
import utils
import pytz
from src import db
from src.entsoe_client import ENTSOEClient


logger = utils.setup_logging()


def get_date_ranges_needing_backfill(table_name: str, country_code: str) -> list:
    """
    Get date ranges that have data but no publication timestamps
    
    Args:
        table_name: Table to check (energy_load, energy_price, etc.)
        country_code: Country code
        
    Returns:
        List of (start_date, end_date) tuples
    """
    timestamp_col = 'target_timestamp_utc' if table_name == 'energy_load_forecast' else 'timestamp_utc'
    
    query = f"""
        SELECT 
            DATE({timestamp_col}) as date,
            COUNT(*) as total_records,
            SUM(CASE WHEN publication_timestamp_utc IS NULL THEN 1 ELSE 0 END) as missing_pub_time
        FROM {table_name}
        WHERE country_code = ?
        GROUP BY DATE({timestamp_col})
        HAVING missing_pub_time > 0
        ORDER BY date
    """
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (country_code,))
        rows = cursor.fetchall()
    
    if not rows:
        logger.info(f"No records missing publication timestamps for {country_code} in {table_name}")
        return []
    
    # Group consecutive days into ranges
    ranges = []
    start_date = None
    prev_date = None
    
    for row in rows:
        date_str = row['date']
        date = datetime.strptime(date_str, '%Y-%m-%d').date()
        
        if start_date is None:
            start_date = date
            prev_date = date
        elif (date - prev_date).days > 1:
            # Gap detected, close current range
            ranges.append((start_date, prev_date))
            start_date = date
            prev_date = date
        else:
            prev_date = date
    
    # Close final range
    if start_date:
        ranges.append((start_date, prev_date))
    
    return ranges


def backfill_load_timestamps(
    client: ENTSOEClient,
    country_code: str,
    start_date: datetime,
    end_date: datetime
) -> int:
    """Backfill publication timestamps for load data"""
    
    logger.info(f"Backfilling load timestamps for {country_code}: {start_date.date()} to {end_date.date()}")
    
    try:
        # Query with metadata
        df, pub_time = client.query_load_with_metadata(country_code, start_date, end_date)
        
        if df is None or pub_time is None:
            logger.warning(f"No data or publication time returned")
            return 0
        
        # Update existing records with publication timestamp
        pub_time_str = utils.format_timestamp_for_db(pub_time)
        
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE energy_load
                SET publication_timestamp_utc = ?
                WHERE country_code = ?
                  AND timestamp_utc >= ?
                  AND timestamp_utc < ?
                  AND publication_timestamp_utc IS NULL
            """, (
                pub_time_str,
                country_code,
                utils.format_timestamp_for_db(start_date),
                utils.format_timestamp_for_db(end_date)
            ))
            updated = cursor.rowcount
        
        logger.info(f"  Updated {updated} load records with publication time: {pub_time}")
        return updated
    
    except Exception as e:
        logger.error(f"Error backfilling load timestamps: {e}")
        return 0


def backfill_price_timestamps(
    client: ENTSOEClient,
    country_code: str,
    start_date: datetime,
    end_date: datetime
) -> int:
    """Backfill publication timestamps for price data"""
    
    logger.info(f"Backfilling price timestamps for {country_code}: {start_date.date()} to {end_date.date()}")
    
    try:
        # Query with metadata
        df, pub_time = client.query_day_ahead_prices_with_metadata(country_code, start_date, end_date)
        
        if df is None or pub_time is None:
            logger.warning(f"No data or publication time returned")
            return 0
        
        # Update existing records with publication timestamp
        pub_time_str = utils.format_timestamp_for_db(pub_time)
        
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE energy_price
                SET publication_timestamp_utc = ?
                WHERE country_code = ?
                  AND timestamp_utc >= ?
                  AND timestamp_utc < ?
                  AND publication_timestamp_utc IS NULL
            """, (
                pub_time_str,
                country_code,
                utils.format_timestamp_for_db(start_date),
                utils.format_timestamp_for_db(end_date)
            ))
            updated = cursor.rowcount
        
        logger.info(f"  Updated {updated} price records with publication time: {pub_time}")
        return updated
    
    except Exception as e:
        logger.error(f"Error backfilling price timestamps: {e}")
        return 0


def backfill_renewable_timestamps(
    client: ENTSOEClient,
    country_code: str,
    start_date: datetime,
    end_date: datetime
) -> int:
    """Backfill publication timestamps for renewable data"""
    
    logger.info(f"Backfilling renewable timestamps for {country_code}: {start_date.date()} to {end_date.date()}")
    
    try:
        # Query with metadata
        df, pub_time = client.query_generation_per_type_with_metadata(country_code, start_date, end_date)
        
        if df is None or pub_time is None:
            logger.warning(f"No data or publication time returned")
            return 0
        
        # Update existing records with publication timestamp
        pub_time_str = utils.format_timestamp_for_db(pub_time)
        
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE energy_renewable
                SET publication_timestamp_utc = ?
                WHERE country_code = ?
                  AND timestamp_utc >= ?
                  AND timestamp_utc < ?
                  AND publication_timestamp_utc IS NULL
            """, (
                pub_time_str,
                country_code,
                utils.format_timestamp_for_db(start_date),
                utils.format_timestamp_for_db(end_date)
            ))
            updated = cursor.rowcount
        
        logger.info(f"  Updated {updated} renewable records with publication time: {pub_time}")
        return updated
    
    except Exception as e:
        logger.error(f"Error backfilling renewable timestamps: {e}")
        return 0


def backfill_load_forecast_timestamps(
    client: ENTSOEClient,
    country_code: str,
    start_date: datetime,
    end_date: datetime,
    forecast_type: str = 'day_ahead'
) -> int:
    """Backfill publication timestamps for load forecast data"""
    
    logger.info(f"Backfilling {forecast_type} forecast timestamps for {country_code}: {start_date.date()} to {end_date.date()}")
    
    try:
        # Map forecast_type to process_type
        process_type = 'A01' if forecast_type == 'day_ahead' else 'A31'
        
        # Query with metadata
        df, pub_time = client.query_load_forecast_with_metadata(
            country_code, start_date, end_date, process_type=process_type
        )
        
        if df is None or pub_time is None:
            logger.warning(f"No data or publication time returned")
            return 0
        
        # Update existing records with publication timestamp
        pub_time_str = utils.format_timestamp_for_db(pub_time)
        
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE energy_load_forecast
                SET publication_timestamp_utc = ?
                WHERE country_code = ?
                  AND target_timestamp_utc >= ?
                  AND target_timestamp_utc < ?
                  AND forecast_type = ?
                  AND publication_timestamp_utc IS NULL
            """, (
                pub_time_str,
                country_code,
                utils.format_timestamp_for_db(start_date),
                utils.format_timestamp_for_db(end_date),
                forecast_type
            ))
            updated = cursor.rowcount
        
        logger.info(f"  Updated {updated} {forecast_type} forecast records with publication time: {pub_time}")
        return updated
    
    except Exception as e:
        logger.error(f"Error backfilling load forecast timestamps: {e}")
        return 0


def backfill_table(
    table: str,
    country_code: str,
    start_date: str = None,
    end_date: str = None,
    chunk_days: int = 7
) -> dict:
    """
    Backfill publication timestamps for a table
    
    Args:
        table: Table name (load, price, renewable, load_forecast)
        country_code: Country code
        start_date: Optional start date (YYYY-MM-DD), defaults to auto-detect
        end_date: Optional end date (YYYY-MM-DD), defaults to auto-detect
        chunk_days: Number of days to process per API call
        
    Returns:
        Dict with statistics
    """
    client = ENTSOEClient()
    
    # Map table names to database table names
    table_map = {
        'load': 'energy_load',
        'price': 'energy_price',
        'renewable': 'energy_renewable',
        'load_forecast': 'energy_load_forecast'
    }
    
    # Map to backfill functions
    backfill_funcs = {
        'load': backfill_load_timestamps,
        'price': backfill_price_timestamps,
        'renewable': backfill_renewable_timestamps,
        'load_forecast': backfill_load_forecast_timestamps
    }
    
    db_table = table_map[table]
    backfill_func = backfill_funcs[table]
    
    logger.info(f"\n{'='*80}")
    logger.info(f"Backfilling {table} for {country_code}")
    logger.info(f"{'='*80}")
    
    # Get date ranges to backfill
    if start_date and end_date:
        # User-specified range
        ranges = [(
            datetime.strptime(start_date, '%Y-%m-%d').date(),
            datetime.strptime(end_date, '%Y-%m-%d').date()
        )]
    else:
        # Auto-detect ranges with missing publication timestamps
        ranges = get_date_ranges_needing_backfill(db_table, country_code)
    
    if not ranges:
        logger.info(f"No records need backfilling for {country_code}")
        return {'total_updated': 0, 'ranges_processed': 0}
    
    logger.info(f"Found {len(ranges)} date ranges to backfill")
    
    total_updated = 0
    ranges_processed = 0
    
    # Process each range in chunks
    for range_start, range_end in ranges:
        logger.info(f"\nProcessing range: {range_start} to {range_end}")
        
        current = range_start
        while current <= range_end:
            chunk_end = min(current + timedelta(days=chunk_days - 1), range_end)
            
            # Convert to datetime with UTC timezone
            start_dt = datetime.combine(current, datetime.min.time()).replace(tzinfo=pytz.UTC)
            end_dt = datetime.combine(chunk_end + timedelta(days=1), datetime.min.time()).replace(tzinfo=pytz.UTC)
            
            # Call appropriate backfill function
            if table == 'load_forecast':
                # Process both day-ahead and week-ahead
                updated_day = backfill_func(client, country_code, start_dt, end_dt, 'day_ahead')
                updated_week = backfill_func(client, country_code, start_dt, end_dt, 'week_ahead')
                updated = updated_day + updated_week
            else:
                updated = backfill_func(client, country_code, start_dt, end_dt)
            
            total_updated += updated
            
            current = chunk_end + timedelta(days=1)
        
        ranges_processed += 1
    
    logger.info(f"\n[OK] Backfilled {total_updated} records across {ranges_processed} ranges for {country_code}")
    
    return {
        'total_updated': total_updated,
        'ranges_processed': ranges_processed
    }


def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Backfill publication timestamps for existing ENTSO-E data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill load data for Germany and France
  python scripts/backfill_publication_timestamps.py --table load --countries DE,FR

  # Backfill price data for specific date range
  python scripts/backfill_publication_timestamps.py --table price --start 2024-01-01 --end 2024-12-31 --countries all

  # Backfill all tables for all countries (auto-detect ranges)
  python scripts/backfill_publication_timestamps.py --table all --countries all

  # Backfill with smaller chunks (for rate limiting)
  python scripts/backfill_publication_timestamps.py --table load --countries DE --chunk-days 3
        """
    )

    parser.add_argument(
        '--table',
        type=str,
        required=True,
        choices=['load', 'price', 'renewable', 'load_forecast', 'all'],
        help='Table to backfill'
    )

    parser.add_argument(
        '--countries',
        type=str,
        default='all',
        help='Country codes (comma-separated) or "all" (default: all)'
    )

    parser.add_argument(
        '--start',
        type=str,
        help='Start date (YYYY-MM-DD). If not specified, auto-detects ranges with missing timestamps'
    )

    parser.add_argument(
        '--end',
        type=str,
        help='End date (YYYY-MM-DD). If not specified, auto-detects ranges with missing timestamps'
    )

    parser.add_argument(
        '--chunk-days',
        type=int,
        default=7,
        help='Number of days to process per API call (default: 7)'
    )

    parser.add_argument(
        '--priority',
        type=int,
        choices=[1, 2, 3],
        help='Filter countries by priority (1=high, 2=medium, 3=low)'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_args()

    # Setup logging
    logger = utils.setup_logging(log_level=args.log_level)

    # Validate configuration
    try:
        config.validate_config()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    logger.info("ENTSO-E Publication Timestamp Backfill Script")
    logger.info("=" * 80)

    # Parse countries
    if args.countries.lower() == 'all':
        countries = db.get_countries(priority=args.priority)
        country_codes = [c['country_code'] for c in countries]
        logger.info(f"Countries: ALL ({len(country_codes)} countries)")
    else:
        country_codes = [c.strip().upper() for c in args.countries.split(',')]
        logger.info(f"Countries: {', '.join(country_codes)}")

    # Parse tables
    if args.table == 'all':
        tables = ['load', 'price', 'renewable', 'load_forecast']
    else:
        tables = [args.table]

    logger.info(f"Tables: {', '.join(tables)}")

    if args.start and args.end:
        logger.info(f"Date range: {args.start} to {args.end}")
    else:
        logger.info("Date range: Auto-detect (only backfill records missing timestamps)")

    logger.info(f"Chunk size: {args.chunk_days} days")

    # Process each combination
    total_stats = {
        'countries_processed': 0,
        'total_records_updated': 0,
        'failed_countries': []
    }

    for country_code in country_codes:
        for table in tables:
            try:
                stats = backfill_table(
                    table=table,
                    country_code=country_code,
                    start_date=args.start,
                    end_date=args.end,
                    chunk_days=args.chunk_days
                )
                total_stats['total_records_updated'] += stats['total_updated']
            except Exception as e:
                logger.error(f"Failed to backfill {table} for {country_code}: {e}")
                total_stats['failed_countries'].append(f"{country_code}/{table}")

        total_stats['countries_processed'] += 1

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("BACKFILL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Countries processed: {total_stats['countries_processed']}/{len(country_codes)}")
    logger.info(f"Total records updated: {total_stats['total_records_updated']}")

    if total_stats['failed_countries']:
        logger.warning(f"\nFailed countries/tables ({len(total_stats['failed_countries'])}):")
        for failed in total_stats['failed_countries']:
            logger.warning(f"  - {failed}")

    logger.info("\n[OK] Backfill complete!")


if __name__ == "__main__":
    main()
