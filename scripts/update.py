#!/usr/bin/env python3
"""
Regular update script for ENTSO-E energy data

Fetches recent data (last 7 days by default) to capture:
- New data publications
- Delayed uploads
- Data revisions

Designed to be run hourly via cron.

Usage:
    python scripts/update.py
    python scripts/update.py --days 14
    python scripts/update.py --types load,price
"""

import sys
import argparse
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
import utils
from src import pipeline


def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Update recent energy data from ENTSO-E API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard hourly update (last 7 days)
  python scripts/update.py

  # Update last 14 days
  python scripts/update.py --days 14

  # Update only load and price data
  python scripts/update.py --types load,price

  # Update specific countries
  python scripts/update.py --countries DE,FR,IT
        """
    )

    parser.add_argument(
        '--days',
        type=int,
        default=config.UPDATE_DAYS_BACK,
        help=f'Number of days to go back (default: {config.UPDATE_DAYS_BACK})'
    )

    parser.add_argument(
        '--types',
        type=str,
        default='all',
        help='Data types to fetch: load, price, renewable, all (comma-separated, default: all)'
    )

    parser.add_argument(
        '--countries',
        type=str,
        default='all',
        help='Country codes to process: DE,FR,IT or "all" (default: all)'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    parser.add_argument(
        '--include-dayahead',
        action='store_true',
        default=False,
        help='Fetch D+1 data (auto-enabled for price, load_forecast_day_ahead, wind_solar_forecast)'
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

    logger.info("ENTSO-E Data Update Script")
    logger.info("=" * 80)
    logger.info(f"Updating last {args.days} days of data")

    # Parse data types
    if args.types == 'all':
        data_types = ['load', 'price', 'renewable', 'load_forecast_day_ahead', 'load_forecast_week_ahead', 'wind_solar_forecast', 'crossborder_flows', 'net_position']
    else:
        data_types = [t.strip() for t in args.types.split(',')]

    logger.info(f"Data types: {', '.join(data_types)}")

    # Auto-enable day-ahead fetching when any selected type is published ahead
    # of delivery (~12:00 CET for D+1), so we always fetch tomorrow's data.
    #
    # Read the set from config rather than repeating it here. This used to be a
    # hardcoded literal, which meant a type could be flagged is_dayahead in
    # config and still be fetched with a now-capped window when selected on its
    # own (e.g. `--types net_position`) -- the flag is only consulted after this
    # block has already decided include_dayahead.
    dayahead_types = set(config.get_dayahead_data_types())
    selected_dayahead = dayahead_types & set(data_types)
    if selected_dayahead:
        args.include_dayahead = True
        logger.info(
            "Auto-enabled D+1 fetching for: %s", ', '.join(sorted(selected_dayahead))
        )

    # Parse countries
    if args.countries.lower() == 'all':
        country_codes = None  # None means all countries
        logger.info("Countries: ALL")
    else:
        country_codes = [c.strip().upper() for c in args.countries.split(',')]
        logger.info(f"Countries: {', '.join(country_codes)}")

    # Run update pipeline
    try:
        pipeline.update(
            days_back=args.days,
            data_types=data_types,
            countries=country_codes,
            include_dayahead=args.include_dayahead
        )

        logger.info("\n[OK] Update complete!")

    except Exception as e:
        logger.error(f"Update failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
