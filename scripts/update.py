#!/usr/bin/env python3
"""
Regular update script for ENTSO-E energy data

Fetches recent data (last 7 days by default) to capture:
- New data publications
- Delayed uploads
- Data revisions

Run four times a day from `docker/crontab` (00:30, 06:30, 13:30, 18:30 UTC),
plus two price-only passes. The "hourly" this docstring used to claim has not
been true since the crontab was written.

**Exit codes (ABL-61).** This script used to exit 0 whatever happened short of
`pipeline.update()` raising, so a total upstream outage was indistinguishable
from a healthy pass to anything supervising the container:

    0  pass looks healthy
    1  configuration error, or the pipeline raised
    2  the pass stored NOTHING, and still stored nothing after its retries
    3  the pass stored an order of magnitude less than its recent baseline

Usage:
    python scripts/update.py
    python scripts/update.py --days 14
    python scripts/update.py --types load,price
    python scripts/update.py --no-pass-retry     # fail fast instead of retrying
"""

import sys
import time
import argparse
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
import utils
from src import db, pass_verdict, pipeline

#: Every data type a full pass covers. `--types all` means exactly this list,
#: and a pass that covers exactly this list is comparable with other passes.
ALL_DATA_TYPES = [
    'load',
    'price',
    'renewable',
    'load_forecast_day_ahead',
    'load_forecast_week_ahead',
    'wind_solar_forecast',
    'crossborder_flows',
    'net_position',
]


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

    parser.add_argument(
        '--no-pass-retry',
        action='store_true',
        default=False,
        help=(
            'Do not re-run the whole pass when it stores nothing. Exits 2 '
            'immediately instead. For interactive runs and tests.'
        )
    )

    return parser.parse_args()


def is_full_pass(args, data_types: Sequence[str], country_codes: Optional[List[str]]) -> bool:
    """Is this the routine pass, i.e. one comparable with other passes?

    Only a full pass records a `data_ingestion_log` pass row and only a full
    pass is volume-checked. A `--types price --days 2` run stores a small
    fraction of what a full pass stores; letting it into the baseline would
    drag the median down and letting it be judged against the baseline would
    alarm every day at 11:15 UTC.
    """
    return (
        country_codes is None
        and set(data_types) == set(ALL_DATA_TYPES)
        and args.days == config.UPDATE_DAYS_BACK
    )


def read_baseline(logger, full_pass: bool) -> List[int]:
    """Recent healthy pass totals, or an empty list.

    Never raises: the volume check is supervision, and supervision that can
    take the ingest down with it is worse than no supervision. A locked
    database (the replica is locked twice a day for the sync) costs the check,
    not the pass.
    """
    if not full_pass:
        return []
    try:
        return db.recent_pass_totals(config.PASS_BASELINE_PASSES)
    except Exception as e:
        logger.warning(
            "Could not read the pass baseline; volume check disabled for this pass: %s",
            utils.format_error(e),
        )
        return []


def _start_pass_row(logger, full_pass: bool) -> Optional[int]:
    """Open the pass-level `data_ingestion_log` row, or return None. Never raises."""
    if not full_pass:
        return None
    try:
        return db.log_ingestion_start(db.PASS_PIPELINE_TYPE)
    except Exception as e:
        logger.warning("Could not open the pass log row: %s", utils.format_error(e))
        return None


def _close_pass_row(logger, log_id: Optional[int], **kwargs) -> None:
    """Close the pass-level row. Never raises, for the same reason as above."""
    if log_id is None:
        return
    try:
        db.log_ingestion_complete(log_id, **kwargs)
    except Exception as e:
        logger.warning("Could not close the pass log row: %s", utils.format_error(e))


def run_one_pass(
    args,
    data_types: List[str],
    country_codes: Optional[List[str]],
    baseline: Sequence[int],
    full_pass: bool,
    logger,
) -> Tuple[dict, pass_verdict.PassVerdict]:
    """Run the pass once and judge it. Re-raises whatever the pipeline raises."""
    log_id = _start_pass_row(logger, full_pass)

    try:
        stats = pipeline.update(
            days_back=args.days,
            data_types=data_types,
            countries=country_codes,
            include_dayahead=args.include_dayahead,
        )
    except Exception as e:
        _close_pass_row(
            logger,
            log_id,
            records_failed=1,
            error_message=utils.format_error(e, "update pass raised"),
        )
        raise

    verdict = pass_verdict.classify_pass(
        stored_records=stats['total_records'],
        countries_processed=stats['total_countries'],
        baseline_totals=baseline,
        collapse_fraction=config.PASS_COLLAPSE_FRACTION,
        min_baseline_passes=config.PASS_MIN_BASELINE_PASSES,
    )

    # `error_message` on the pass row is what marks it unhealthy, and
    # `db.recent_pass_totals` filters on exactly that -- so a collapsed pass
    # never becomes part of the baseline the next pass is judged against.
    _close_pass_row(
        logger,
        log_id,
        records_inserted=stats['total_records'],
        records_failed=stats['failed_targets'],
        error_message=None if verdict.is_ok else f"{verdict.verdict}: {verdict.reason}",
    )

    return stats, verdict


def log_verdict(logger, verdict: pass_verdict.PassVerdict) -> None:
    """One greppable line per pass. `PASS VERDICT` is the marker to alert on."""
    line = f"PASS VERDICT [{verdict.verdict}] {verdict.reason}"
    if verdict.is_ok:
        logger.info(line)
    else:
        logger.error(line)


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
        data_types = list(ALL_DATA_TYPES)
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

    # Run update pipeline, judge it, and retry the WHOLE pass if it stored
    # nothing (ABL-61). The retry is whole-pass rather than per-request because
    # the failure it exists for is a multi-minute upstream outage -- on
    # 2026-08-06 13:30 UTC, 484 HTTP 503s and 0 of 30 countries stored -- which
    # three per-request attempts seconds apart all land inside.
    full_pass = is_full_pass(args, data_types, country_codes)
    baseline = read_baseline(logger, full_pass)
    delays = [] if args.no_pass_retry else list(config.PASS_RETRY_DELAYS_SECONDS)
    retries_used = 0

    while True:
        try:
            stats, verdict = run_one_pass(
                args, data_types, country_codes, baseline, full_pass, logger
            )
        except Exception as e:
            logger.error(f"Update failed: {utils.format_error(e)}", exc_info=True)
            sys.exit(pass_verdict.EXIT_ERROR)

        log_verdict(logger, verdict)

        if not verdict.should_retry or retries_used >= len(delays):
            break

        delay = delays[retries_used]
        retries_used += 1
        logger.warning(
            "Pass stored nothing -- re-running the whole pass in %ds "
            "(retry %d of %d)", delay, retries_used, len(delays)
        )
        time.sleep(delay)

    if verdict.is_ok:
        if retries_used:
            logger.info(
                "\n[OK] Update complete after %d whole-pass retr%s!",
                retries_used, "y" if retries_used == 1 else "ies",
            )
        else:
            logger.info("\n[OK] Update complete!")
    else:
        logger.error(
            "[FAIL] Update pass unhealthy after %d retr%s -- exiting %d",
            retries_used, "y" if retries_used == 1 else "ies", verdict.exit_code,
        )

    sys.exit(verdict.exit_code)


if __name__ == "__main__":
    main()
