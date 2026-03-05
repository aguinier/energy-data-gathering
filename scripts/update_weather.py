#!/usr/bin/env python3
"""
Regular update script for weather data from Open-Meteo API

Fetches recent weather data (last 7 days by default) to capture:
- New data publications
- Fill any recent gaps
- Data revisions

Can also fetch weather forecasts (up to 16 days ahead) for energy forecasting.

Designed to be run regularly via cron (Linux/Mac) or Task Scheduler (Windows).

Usage:
    python scripts/update_weather.py
    python scripts/update_weather.py --days 14
    python scripts/update_weather.py --countries DE,FR,IT
    python scripts/update_weather.py --forecast
    python scripts/update_weather.py --forecast-only --forecast-days 14
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

import pytz

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

import utils
from src import db
from src.fetch_weather import (
    fetch_weather_data,
    fetch_weather_forecast,
    get_weather_countries,
    COUNTRY_COORDINATES,
    DEFAULT_FORECAST_DAYS
)

# Default days to go back for updates
DEFAULT_UPDATE_DAYS = 7


def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Update recent weather data and forecasts from Open-Meteo API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard update (last 7 days, all countries)
  python scripts/update_weather.py

  # Update last 14 days
  python scripts/update_weather.py --days 14

  # Update specific countries
  python scripts/update_weather.py --countries DE,FR,IT

  # Include weather forecasts (14 days ahead)
  python scripts/update_weather.py --forecast

  # Forecasts only (skip historical data)
  python scripts/update_weather.py --forecast-only

  # Custom forecast horizon
  python scripts/update_weather.py --forecast --forecast-days 7

  # Verbose output
  python scripts/update_weather.py --verbose
        """
    )

    parser.add_argument(
        '--days',
        type=int,
        default=DEFAULT_UPDATE_DAYS,
        help=f'Number of days to go back for historical data (default: {DEFAULT_UPDATE_DAYS})'
    )

    parser.add_argument(
        '--countries',
        type=str,
        default='all',
        help='Country codes to process: DE,FR,IT or "all" (default: all)'
    )

    parser.add_argument(
        '--forecast',
        action='store_true',
        help='Also fetch weather forecasts (up to 14 days ahead)'
    )

    parser.add_argument(
        '--forecast-only',
        action='store_true',
        help='Only fetch forecasts, skip historical data'
    )

    parser.add_argument(
        '--forecast-days',
        type=int,
        default=DEFAULT_FORECAST_DAYS,
        help=f'Number of forecast days (default: {DEFAULT_FORECAST_DAYS}, max: 16)'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )

    return parser.parse_args()


def update_historical(country_codes, start_date, end_date, args, logger):
    """Update historical weather data"""
    total_inserted = 0
    total_updated = 0
    total_failed = 0
    countries_processed = 0
    countries_failed = []

    logger.info(f"Updating historical data: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info("-" * 60)

    for country_code in country_codes:
        if country_code not in COUNTRY_COORDINATES:
            logger.warning(f"Skipping {country_code}: No coordinates defined")
            continue

        try:
            # Log start of country processing
            log_id = db.log_ingestion_start('weather_update', country_code)

            inserted, updated, failed = fetch_weather_data(
                country_code,
                start_date,
                end_date,
                log_id
            )

            # Log completion
            db.log_ingestion_complete(
                log_id,
                records_inserted=inserted,
                records_updated=updated,
                records_failed=failed
            )

            total_inserted += inserted
            total_updated += updated
            total_failed += failed
            countries_processed += 1

            if args.verbose:
                logger.info(f"  {country_code}: {inserted} inserted, {updated} updated")

        except Exception as e:
            logger.error(f"Failed to update weather for {country_code}: {e}")
            countries_failed.append(country_code)

    return {
        'inserted': total_inserted,
        'updated': total_updated,
        'failed': total_failed,
        'countries_processed': countries_processed,
        'countries_failed': countries_failed
    }


def update_forecasts(country_codes, forecast_days, args, logger):
    """Update weather forecasts"""
    total_inserted = 0
    total_updated = 0
    total_failed = 0
    countries_processed = 0
    countries_failed = []

    logger.info(f"Fetching forecasts: {forecast_days} days ahead")
    logger.info("-" * 60)

    for country_code in country_codes:
        if country_code not in COUNTRY_COORDINATES:
            logger.warning(f"Skipping {country_code}: No coordinates defined")
            continue

        try:
            # Log start of country processing
            log_id = db.log_ingestion_start('weather_forecast', country_code)

            inserted, updated, failed = fetch_weather_forecast(
                country_code,
                forecast_days,
                log_id
            )

            # Log completion
            db.log_ingestion_complete(
                log_id,
                records_inserted=inserted,
                records_updated=updated,
                records_failed=failed
            )

            total_inserted += inserted
            total_updated += updated
            total_failed += failed
            countries_processed += 1

            if args.verbose:
                logger.info(f"  {country_code}: {inserted} forecast records inserted")

        except Exception as e:
            logger.error(f"Failed to fetch forecast for {country_code}: {e}")
            countries_failed.append(country_code)

    return {
        'inserted': total_inserted,
        'updated': total_updated,
        'failed': total_failed,
        'countries_processed': countries_processed,
        'countries_failed': countries_failed
    }


def main():
    """Main entry point"""
    args = parse_args()

    # Setup logging
    logger = utils.setup_logging(log_level=args.log_level)

    # Print header
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("Weather Data Update Script (Open-Meteo API)")
    logger.info("=" * 60)
    logger.info(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Determine what to fetch
    fetch_historical = not args.forecast_only
    fetch_forecasts = args.forecast or args.forecast_only

    if fetch_historical:
        logger.info(f"Historical data: last {args.days} days")
    else:
        logger.info("Historical data: SKIPPED (--forecast-only)")

    if fetch_forecasts:
        logger.info(f"Forecasts: {args.forecast_days} days ahead")
    else:
        logger.info("Forecasts: SKIPPED (use --forecast to enable)")

    # Determine countries to process
    if args.countries.lower() == 'all':
        # Get countries that already have weather data
        country_codes = get_weather_countries()
        if not country_codes:
            # Fall back to all countries with coordinates
            country_codes = list(COUNTRY_COORDINATES.keys())
        logger.info(f"Countries: ALL ({len(country_codes)} countries)")
    else:
        country_codes = [c.strip().upper() for c in args.countries.split(',')]
        logger.info(f"Countries: {', '.join(country_codes)}")

    logger.info("=" * 60)

    # Results tracking
    historical_results = None
    forecast_results = None

    # Fetch historical data
    if fetch_historical:
        end_date = datetime.now(pytz.UTC)
        start_date = end_date - timedelta(days=args.days)
        historical_results = update_historical(country_codes, start_date, end_date, args, logger)

    # Fetch forecasts
    if fetch_forecasts:
        forecast_results = update_forecasts(country_codes, args.forecast_days, args, logger)

    # Print summary
    end_time = datetime.now()
    duration = end_time - start_time

    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)

    all_failed = []

    if historical_results:
        logger.info("Historical Data:")
        logger.info(f"  Countries processed: {historical_results['countries_processed']}")
        logger.info(f"  Records inserted: {historical_results['inserted']}")
        logger.info(f"  Records updated: {historical_results['updated']}")
        logger.info(f"  Records failed: {historical_results['failed']}")
        all_failed.extend(historical_results['countries_failed'])

    if forecast_results:
        logger.info("Forecasts:")
        logger.info(f"  Countries processed: {forecast_results['countries_processed']}")
        logger.info(f"  Records inserted: {forecast_results['inserted']}")
        logger.info(f"  Records updated: {forecast_results['updated']}")
        logger.info(f"  Records failed: {forecast_results['failed']}")
        all_failed.extend(forecast_results['countries_failed'])

    if all_failed:
        # Remove duplicates
        unique_failed = list(set(all_failed))
        logger.warning(f"Failed countries: {', '.join(unique_failed)}")

    logger.info("-" * 60)
    logger.info(f"Duration: {duration.total_seconds():.1f} seconds")
    logger.info(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Exit with error code if there were failures
    total_failed = 0
    if historical_results:
        total_failed += historical_results['failed']
    if forecast_results:
        total_failed += forecast_results['failed']

    if total_failed > 0 or all_failed:
        sys.exit(1)

    logger.info("[OK] Weather update complete!")


if __name__ == "__main__":
    main()
