#!/usr/bin/env python3
"""
Backfill historical energy data from ENTSO-E API

Usage:
    python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types load,price,renewable --countries all
    python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types load --countries DE,FR,IT
    python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types all --priority 1
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
import utils
from src import pipeline


def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Backfill historical energy data from ENTSO-E API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill all data types for all countries from 2024
  python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types all --countries all

  # Backfill only load data for Germany
  python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types load --countries DE

  # Backfill load and price for high-priority countries
  python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types load,price --priority 1

  # Backfill with default periods (from config.py)
  python scripts/backfill.py --use-defaults --types all --countries all

  # Fix Italy price data gap (critical issue)
  python scripts/backfill.py --start 2021-01-01 --end 2024-12-31 --types price --countries IT
        """
    )

    parser.add_argument(
        '--start',
        type=str,
        help='Start date (YYYY-MM-DD format). Required unless --use-defaults is specified.'
    )

    parser.add_argument(
        '--end',
        type=str,
        help='End date (YYYY-MM-DD format). Required unless --use-defaults is specified.'
    )

    parser.add_argument(
        '--types',
        type=str,
        required=True,
        help='Data types to fetch: load, price, renewable, all (comma-separated)'
    )

    parser.add_argument(
        '--countries',
        type=str,
        default='all',
        help='Country codes to process: DE,FR,IT or "all" (default: all)'
    )

    parser.add_argument(
        '--priority',
        type=int,
        choices=[1, 2, 3],
        help='Filter countries by priority (1=high, 2=medium, 3=low)'
    )

    parser.add_argument(
        '--use-defaults',
        action='store_true',
        help='Use default backfill periods from config.py'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    return parser.parse_args()


def validate_args(args):
    """Validate command-line arguments"""
    errors = []

    # Check start/end dates
    if not args.use_defaults:
        if not args.start:
            errors.append("--start is required unless --use-defaults is specified")
        if not args.end:
            errors.append("--end is required unless --use-defaults is specified")

        if args.start and args.end:
            try:
                start = datetime.strptime(args.start, '%Y-%m-%d')
                end = datetime.strptime(args.end, '%Y-%m-%d')
                if start > end:
                    errors.append(f"Start date ({args.start}) is after end date ({args.end})")
            except ValueError as e:
                errors.append(f"Invalid date format: {e}")

    # Validate data types
    valid_types = {'load', 'price', 'renewable', 'load_forecast_day_ahead', 'load_forecast_week_ahead', 'wind_solar_forecast', 'all'}
    requested_types = [t.strip() for t in args.types.split(',')]
    invalid_types = set(requested_types) - valid_types
    if invalid_types:
        errors.append(f"Invalid data types: {invalid_types}. Valid: {valid_types}")

    if errors:
        print("ERROR: Invalid arguments:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)


def main():
    """Main entry point"""
    args = parse_args()
    validate_args(args)

    # Setup logging
    logger = utils.setup_logging(log_level=args.log_level)

    # Validate configuration
    try:
        config.validate_config()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    logger.info("ENTSO-E Data Backfill Script")
    logger.info("=" * 80)

    # Parse data types
    if args.types == 'all':
        data_types = ['load', 'price', 'renewable', 'load_forecast_day_ahead', 'load_forecast_week_ahead', 'wind_solar_forecast']
    else:
        data_types = [t.strip() for t in args.types.split(',')]

    logger.info(f"Data types: {', '.join(data_types)}")

    # Parse countries
    if args.countries.lower() == 'all':
        country_codes = None  # None means all countries
        logger.info("Countries: ALL")
    else:
        country_codes = [c.strip().upper() for c in args.countries.split(',')]
        logger.info(f"Countries: {', '.join(country_codes)}")

    # Priority filter
    if args.priority:
        logger.info(f"Priority filter: {args.priority}")

    # Run backfill for each data type (with appropriate date ranges)
    if args.use_defaults:
        logger.info("Using default backfill periods from config")
        for data_type in data_types:
            start_date = config.BACKFILL_DEFAULTS.get(data_type, '2024-01-01')
            end_date = datetime.now().strftime('%Y-%m-%d')

            logger.info(f"\nBackfilling {data_type}: {start_date} to {end_date}")

            pipeline.backfill(
                start_date=start_date,
                end_date=end_date,
                data_types=[data_type],
                countries=country_codes,
                priority=args.priority
            )
    else:
        # Use specified date range for all types
        logger.info(f"Date range: {args.start} to {args.end}")

        pipeline.backfill(
            start_date=args.start,
            end_date=args.end,
            data_types=data_types,
            countries=country_codes,
            priority=args.priority
        )

    logger.info("\n[OK] Backfill complete!")


if __name__ == "__main__":
    main()
