#!/usr/bin/env python3
"""
Smart backfill for cross-border flows and net position.

Processes ONE country at a time through all its missing months before
moving to the next. Adds generous delays to avoid ENTSO-E throttling.
Skips months where data already exists.

Usage:
    # Backfill all supported countries
    python scripts/smart_backfill.py

    # Specific countries
    python scripts/smart_backfill.py --countries DE,FR,BE

    # Only crossborder flows (skip net position)
    python scripts/smart_backfill.py --skip-netpos

    # Dry run (show what would be fetched)
    python scripts/smart_backfill.py --dry-run
"""

import argparse
import logging
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from src.entsoe_client import ENTSOEClient
from src import fetch_crossborder_flows, fetch_net_position, db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("smart_backfill")

SUPPORTED = [
    'AT', 'BE', 'BG', 'CH', 'CZ', 'DE', 'EE', 'ES', 'FI', 'FR',
    'GR', 'HR', 'HU', 'IT', 'LT', 'LV', 'NL', 'NO', 'PL', 'PT',
    'RO', 'SE', 'SI', 'SK',
]

BACKFILL_START = "2023-01"
DELAY_BETWEEN_REQUESTS = 3    # seconds between API call batches
DELAY_BETWEEN_COUNTRIES = 10  # seconds between countries


def get_months(start_month: str, end_month: str) -> list[tuple[datetime, datetime, str]]:
    """Generate (month_start, month_end, month_key) tuples."""
    months = []
    current = pd.Timestamp(start_month + "-01")
    end = pd.Timestamp(end_month + "-01")
    while current <= end:
        month_start = current.to_pydatetime()
        month_end = (current + pd.offsets.MonthEnd(1) + pd.Timedelta(days=1)).to_pydatetime()
        month_key = current.strftime("%Y-%m")
        months.append((month_start, month_end, month_key))
        current += pd.offsets.MonthBegin(1)
    return months


def get_existing_months(country_code: str, data_type: str) -> set[str]:
    """Check which months already have data for a country."""
    conn = sqlite3.connect(str(config.DATABASE_PATH))
    try:
        if data_type == "crossborder_flows":
            rows = conn.execute(
                "SELECT DISTINCT strftime('%Y-%m', timestamp_utc) FROM crossborder_flows WHERE country_from = ?",
                (country_code,)
            ).fetchall()
        elif data_type == "net_position":
            rows = conn.execute(
                "SELECT DISTINCT strftime('%Y-%m', timestamp_utc) FROM net_position WHERE country_code = ?",
                (country_code,)
            ).fetchall()
        else:
            return set()
        return {r[0] for r in rows if r[0]}
    finally:
        conn.close()


def count_records(country_code: str, data_type: str) -> int:
    """Count total records for a country."""
    conn = sqlite3.connect(str(config.DATABASE_PATH))
    try:
        if data_type == "crossborder_flows":
            return conn.execute(
                "SELECT COUNT(*) FROM crossborder_flows WHERE country_from = ?", (country_code,)
            ).fetchone()[0]
        elif data_type == "net_position":
            return conn.execute(
                "SELECT COUNT(*) FROM net_position WHERE country_code = ?", (country_code,)
            ).fetchone()[0]
        return 0
    finally:
        conn.close()


def backfill_country(
    client: ENTSOEClient,
    country_code: str,
    months: list,
    data_type: str,
    dry_run: bool = False,
) -> tuple[int, int, int]:
    """Backfill one data type for one country across all missing months.

    Returns (months_fetched, records_inserted, errors)
    """
    existing = get_existing_months(country_code, data_type)
    missing = [(s, e, k) for s, e, k in months if k not in existing]

    if not missing:
        logger.info(f"  {country_code}/{data_type}: fully covered ({len(existing)} months)")
        return 0, 0, 0

    logger.info(f"  {country_code}/{data_type}: {len(missing)} months to fetch (have {len(existing)})")

    if dry_run:
        for _, _, mk in missing[:3]:
            logger.info(f"    [DRY] would fetch {mk}")
        if len(missing) > 3:
            logger.info(f"    [DRY] ... and {len(missing) - 3} more")
        return 0, 0, 0

    total_inserted = 0
    total_errors = 0
    months_done = 0

    for month_start, month_end, month_key in missing:
        try:
            if data_type == "crossborder_flows":
                inserted, _, failed = fetch_crossborder_flows.fetch_crossborder_flows_data(
                    client, country_code, month_start, month_end
                )
            elif data_type == "net_position":
                inserted, _, failed = fetch_net_position.fetch_net_position_data(
                    client, country_code, month_start, month_end
                )
            else:
                continue

            total_inserted += inserted
            total_errors += failed
            months_done += 1

            if inserted > 0:
                logger.info(f"    {month_key}: +{inserted} records")
            else:
                logger.debug(f"    {month_key}: no data")

        except Exception as e:
            logger.warning(f"    {month_key}: error - {e}")
            total_errors += 1

        # Delay between months to avoid throttling
        time.sleep(DELAY_BETWEEN_REQUESTS)

    return months_done, total_inserted, total_errors


def main():
    parser = argparse.ArgumentParser(description="Smart cross-border data backfill")
    parser.add_argument("--countries", default=None, help="Comma-separated codes (default: all 24)")
    parser.add_argument("--start-month", default=BACKFILL_START, help="Start YYYY-MM")
    parser.add_argument("--end-month", default=None, help="End YYYY-MM (default: current)")
    parser.add_argument("--skip-netpos", action="store_true", help="Skip net position")
    parser.add_argument("--skip-flows", action="store_true", help="Skip crossborder flows")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without fetching")
    args = parser.parse_args()

    countries = args.countries.split(",") if args.countries else SUPPORTED
    end_month = args.end_month or datetime.now().strftime("%Y-%m")
    months = get_months(args.start_month, end_month)

    data_types = []
    if not args.skip_flows:
        data_types.append("crossborder_flows")
    if not args.skip_netpos:
        data_types.append("net_position")

    # Ensure tables exist
    db.create_crossborder_flows_table()
    db.create_net_position_table()

    logger.info(f"=== Smart Backfill ===")
    logger.info(f"Countries: {len(countries)} | Months: {args.start_month} to {end_month} ({len(months)}) | Types: {data_types}")

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    client = None
    if not args.dry_run:
        client = ENTSOEClient()

    grand_total_records = 0
    grand_total_errors = 0

    for i, cc in enumerate(countries):
        logger.info(f"\n[{i+1}/{len(countries)}] === {cc} ===")
        before = sum(count_records(cc, dt) for dt in data_types)

        for dt in data_types:
            months_done, inserted, errors = backfill_country(
                client, cc, months, dt, dry_run=args.dry_run
            )
            grand_total_records += inserted
            grand_total_errors += errors

        after = sum(count_records(cc, dt) for dt in data_types)
        logger.info(f"  {cc} total: {before} -> {after} records (+{after - before})")

        # Longer delay between countries
        if not args.dry_run and i < len(countries) - 1:
            logger.info(f"  Waiting {DELAY_BETWEEN_COUNTRIES}s before next country...")
            time.sleep(DELAY_BETWEEN_COUNTRIES)

    logger.info(f"\n=== Done ===")
    logger.info(f"Records inserted: {grand_total_records}")
    logger.info(f"Errors: {grand_total_errors}")


if __name__ == "__main__":
    main()
