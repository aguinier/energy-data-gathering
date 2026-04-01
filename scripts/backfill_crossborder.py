#!/usr/bin/env python3
"""
Backfill cross-border flows and net position data from ENTSO-E.

Processes data month-by-month with progress checkpointing.
Can be interrupted and resumed — picks up from last completed month.

Usage:
    # Full backfill (2023-01 to present)
    python scripts/backfill_crossborder.py

    # Single country, single month (for testing)
    python scripts/backfill_crossborder.py --countries DE --start-month 2024-01 --end-month 2024-02

    # Resume interrupted backfill
    python scripts/backfill_crossborder.py --resume

    # Only net position (skip cross-border flows)
    python scripts/backfill_crossborder.py --types net_position
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from src.entsoe_client import ENTSOEClient
from src import fetch_crossborder_flows, fetch_net_position, db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("backfill_crossborder")

CHECKPOINT_FILE = Path(__file__).parent.parent / "backfill_crossborder_progress.json"


def load_checkpoint() -> dict:
    """Load progress checkpoint from disk."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {}


def save_checkpoint(progress: dict):
    """Save progress checkpoint to disk."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(progress, f, indent=2, default=str)


def get_months(start_month: str, end_month: str) -> list[tuple[datetime, datetime]]:
    """Generate list of (month_start, month_end) tuples."""
    months = []
    current = pd.Timestamp(start_month + "-01")
    end = pd.Timestamp(end_month + "-01")

    while current <= end:
        month_start = current.to_pydatetime()
        month_end = (current + pd.offsets.MonthEnd(1) + pd.Timedelta(days=1)).to_pydatetime()
        months.append((month_start, month_end))
        current += pd.offsets.MonthBegin(1)

    return months


def main():
    parser = argparse.ArgumentParser(description="Backfill cross-border and net position data")
    parser.add_argument("--countries", default="all", help="Comma-separated country codes or 'all'")
    parser.add_argument("--start-month", default="2023-01", help="Start month YYYY-MM (default: 2023-01)")
    parser.add_argument("--end-month", default=None, help="End month YYYY-MM (default: current month)")
    parser.add_argument("--types", default="all", help="Comma-separated: crossborder_flows,net_position or 'all'")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    args = parser.parse_args()

    # Parse countries — load from database when 'all'
    if args.countries.lower() == "all":
        countries_from_db = db.get_countries()
        countries = [c["country_code"] for c in countries_from_db]
        logger.info(f"Countries: ALL ({len(countries)} from database)")
    else:
        countries = [c.strip().upper() for c in args.countries.split(",")]

    if args.end_month is None:
        args.end_month = datetime.now().strftime("%Y-%m")

    if args.types == "all":
        data_types = ["crossborder_flows", "net_position"]
    else:
        data_types = [t.strip() for t in args.types.split(",")]

    months = get_months(args.start_month, args.end_month)

    # Load checkpoint for resume
    progress = load_checkpoint() if args.resume else {}
    last_completed = progress.get("last_completed_month", "")

    # Ensure tables exist
    db.create_crossborder_flows_table()
    db.create_net_position_table()

    # Initialize client
    client = ENTSOEClient()

    total_records = progress.get("total_records", 0)
    total_errors = 0

    logger.info(f"=== Cross-Border Data Backfill ===")
    logger.info(f"Countries: {len(countries)}")
    logger.info(f"Data types: {data_types}")
    logger.info(f"Months: {args.start_month} to {args.end_month} ({len(months)} months)")
    if last_completed:
        logger.info(f"Resuming from: {last_completed}")

    for month_start, month_end in months:
        month_key = month_start.strftime("%Y-%m")

        # Skip already completed months (resume mode)
        if args.resume and month_key <= last_completed:
            logger.info(f"Skipping {month_key} (already completed)")
            continue

        logger.info(f"\n--- {month_key} ---")

        for country in countries:
            for data_type in data_types:
                try:
                    if data_type == "crossborder_flows":
                        inserted, _, failed = fetch_crossborder_flows.fetch_crossborder_flows_data(
                            client, country, month_start, month_end
                        )
                    elif data_type == "net_position":
                        inserted, _, failed = fetch_net_position.fetch_net_position_data(
                            client, country, month_start, month_end
                        )
                    else:
                        continue

                    total_records += inserted
                    total_errors += failed

                except Exception as e:
                    logger.error(f"  {country}/{data_type}: {e}")
                    total_errors += 1

        # Save checkpoint after each month
        progress["last_completed_month"] = month_key
        progress["total_records"] = total_records
        progress["total_errors"] = total_errors
        progress["updated_at"] = datetime.now().isoformat()
        save_checkpoint(progress)

        logger.info(f"  Checkpoint saved: {month_key} ({total_records} total records)")

    logger.info(f"\n=== Backfill Complete ===")
    logger.info(f"Total records: {total_records}")
    logger.info(f"Total errors: {total_errors}")

    # Clean up checkpoint on successful completion
    if total_errors == 0 and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logger.info("Checkpoint file removed (clean completion)")


if __name__ == "__main__":
    main()
