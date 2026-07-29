#!/usr/bin/env python3
"""
Resumable full-history backfill for `energy_generation` (A75 full-generation
capture, Task 4).

`energy_renewable` already has 2021-01-01 -> now for 34 countries (Task 3
writes both tables going forward from a single A75 fetch, but that only
covers new ingest runs). This script fills in the *history* `energy_generation`
never had: for every (country, month) pair not yet present in
`energy_generation`, fetch the A75 document once via
`ENTSOEClient.query_generation_and_renewable_with_metadata` and upsert the
full-generation frame. The renewable frame that call also returns is
deliberately discarded here -- `energy_renewable` already covers this whole
window (see module docstring below), and re-upserting rows that already
exist would triple this script's write volume for zero benefit.

Modelled on scripts/smart_backfill.py's shape (one country at a time through
all its missing months, skip months that already have data) rather than
scripts/backfill_crossborder.py's global-checkpoint-file shape -- resumability
here comes from `energy_generation`'s own rows, not a side-car JSON file.

Rate limiting: relies entirely on ENTSOEClient._rate_limit (invoked inside
_make_request on every API call). Unlike smart_backfill.py, this script adds
no DELAY_BETWEEN_REQUESTS / DELAY_BETWEEN_COUNTRIES sleep of its own -- a
second throttle on top of the client's would just slow the run down without
protecting anything the client isn't already protecting.

Usage:
    # Show the plan without calling ENTSO-E or writing anything
    python scripts/backfill_generation.py --dry-run

    # Two countries, narrow window (for testing)
    python scripts/backfill_generation.py --countries FR,DE --start 2021-01-01 --end 2021-03-31

    # Full run: every country energy_renewable has A75 data for, 2021-01-01 -> now
    python scripts/backfill_generation.py

    # On prod, under nohup so it survives the SSH session closing:
    nohup python scripts/backfill_generation.py > backfill_generation.log 2>&1 &
    tail -20 backfill_generation.log   # check progress later, no need to attach

Point ENERGY_DB_PATH at a scratch copy to test before touching the replica or
prod -- never run this against the read-only replica, and never against prod
except from the deploy host:
    ENERGY_DB_PATH=/path/to/scratch.db python scripts/backfill_generation.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: E402
from src.entsoe_client import ENTSOEClient  # noqa: E402
from src import db  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("backfill_generation")

DEFAULT_START = "2021-01-01"

# How many country-months between progress summaries. Chosen so a `tail -20`
# on the log always catches at least one summary line alongside recent
# per-country-month lines, without spamming a summary after every request.
SUMMARY_EVERY = 20


# ============================================================================
# Planning: which (country, month) pairs need fetching
# ============================================================================

def get_months(start_date: str, end_date: str) -> list[tuple[datetime, datetime, str]]:
    """(month_start, month_end, month_key) tuples covering start_date..end_date,
    inclusive of both months. Same shape as smart_backfill.get_months."""
    months = []
    current = pd.Timestamp(start_date).to_period("M").to_timestamp()
    end = pd.Timestamp(end_date).to_period("M").to_timestamp()
    while current <= end:
        month_start = current.to_pydatetime()
        month_end = (current + pd.offsets.MonthEnd(1) + pd.Timedelta(days=1)).to_pydatetime()
        month_key = current.strftime("%Y-%m")
        months.append((month_start, month_end, month_key))
        current += pd.offsets.MonthBegin(1)
    return months


def resolve_countries(countries_arg: str) -> list[str]:
    """
    'ALL' resolves to the countries `energy_renewable` already has data for,
    not `db.get_countries()`.

    Why: `db.get_countries()` returns every country with an `entsoe_domain`
    configured -- 39 of them on the replica. But `energy_renewable` (written
    from the exact same A75 document `energy_generation` is filled from) only
    has data for 34 of those 39: GB, IS, MT, TR and UA have a configured
    domain but zero rows in `energy_renewable` (measured on the replica --
    IS/MT/TR match config.NO_DATA_COUNTRIES; GB and UA are flagged in
    config.PROBLEMATIC_COUNTRIES as outdated/war-related). Resolving 'ALL'
    against `db.get_countries()` would spend all 2,200+ requests re-proving
    "no A75 data" for those 5 countries across every one of ~66 months each,
    for a table this script exists to fill efficiently. energy_renewable's
    own country coverage is the measured, correct answer to "which countries
    does ENTSO-E actually return A75 data for".
    """
    if countries_arg and countries_arg.upper() != "ALL":
        return [c.strip().upper() for c in countries_arg.split(",") if c.strip()]

    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT country_code FROM energy_renewable ORDER BY country_code"
        ).fetchall()
    return [r[0] for r in rows]


def get_existing_months(country_code: str) -> set[str]:
    """
    Months this country already has in `energy_generation` -- NOT
    `energy_renewable`.

    This is the one decision in this script most likely to be gotten wrong
    silently: `energy_renewable` already covers 2021-01-01 -> now for every
    supported country (established fact, Task 3). If the skip check looked
    at `energy_renewable` instead of `energy_generation`, every month would
    read as "already covered" on the very first run, the backfill would skip
    everything, and `energy_generation` -- the table this script exists to
    populate -- would stay empty forever. `energy_generation` is the new
    table (Task 1); it starts empty and only grows as this script (or the
    live cron once deployed, per Task 7) writes rows into it, so checking its
    own coverage is the only check that can ever converge to "done".
    """
    try:
        with db.get_connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT strftime('%Y-%m', timestamp_utc) FROM energy_generation "
                "WHERE country_code = ?",
                (country_code,),
            ).fetchall()
        return {r[0] for r in rows if r[0]}
    except sqlite3.OperationalError as e:
        # energy_generation doesn't exist yet on this DB (e.g. a fresh
        # scratch copy of the replica before create_generation_table.py has
        # run against it). Nothing is "already covered" -- the caller will
        # create the table before any real fetch happens.
        if "no such table" in str(e):
            return set()
        raise


def build_plan(countries: list[str], months: list[tuple[datetime, datetime, str]]):
    """Skip-aware plan across every requested country: list of
    (country_code, month_start, month_end, month_key), plus the count of
    months already covered (for reporting)."""
    plan: list[tuple[str, datetime, datetime, str]] = []
    total_existing = 0
    for cc in countries:
        existing = get_existing_months(cc)
        total_existing += len(existing)
        missing = [(s, e, k) for s, e, k in months if k not in existing]
        logger.info(
            f"  {cc}: {len(existing)} months already in energy_generation, "
            f"{len(missing)} to fetch"
        )
        for s, e, k in missing:
            plan.append((cc, s, e, k))
    return plan, total_existing


# ============================================================================
# Fetch + upsert one country-month, distinguishing no-data from failure
# ============================================================================

class _FetchOutcome(logging.Handler):
    """
    Attached to the 'entsoe_pipeline' logger only for the duration of one
    fetch call.

    `ENTSOEClient.query_generation_and_renewable_with_metadata` swallows
    every exception internally and returns the same `(None, None, None)`
    whether ENTSO-E legitimately had nothing to report (its
    `except ENTSOENoDataError` branch, logged at WARNING) or something
    genuinely broke -- network, auth, malformed response (its
    `except Exception` branch, logged at ERROR). The return value alone
    cannot tell those two apart, and this backfill must: a no-data
    country-month is expected and should just be counted and skipped, a
    genuine failure must be logged and counted as a failure so it's visible
    without becoming a retry loop or aborting the run.

    Piggy-backing on the log level the client already emits recovers that
    distinction without duplicating its fetch/flatten logic or changing its
    behaviour (frozen, reviewed in Tasks 2-3) just to get a signal back out
    through the return value.
    """

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.max_level = 0
        self.last_message = ""

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno > self.max_level:
            self.max_level = record.levelno
            self.last_message = record.getMessage()


def fetch_and_upsert_month(
    client: ENTSOEClient,
    country_code: str,
    month_start: datetime,
    month_end: datetime,
) -> tuple[str, object]:
    """
    Fetch and upsert one country-month into energy_generation.

    Returns (outcome, detail):
      - ("ok", rows_affected)   -- fetched and upserted
      - ("no_data", message)    -- ENTSO-E legitimately had nothing; not an
                                    error, do not retry within this run
      - ("failed", message)     -- network/auth/malformed/DB error; logged
                                    and counted, run continues to the next
                                    country-month
    """
    outcome = _FetchOutcome()
    entsoe_logger = logging.getLogger("entsoe_pipeline")
    entsoe_logger.addHandler(outcome)
    try:
        generation_df, _renewable_df, publication_time = (
            client.query_generation_and_renewable_with_metadata(
                country_code, month_start, month_end
            )
        )
    except Exception as e:
        # Defensive: query_generation_and_renewable_with_metadata already
        # catches broadly and returns None instead of raising, but don't
        # depend on that never changing -- an escaped exception here is
        # unambiguously a failure, not silence-worthy no-data.
        return "failed", f"unhandled exception from client: {e}"
    finally:
        entsoe_logger.removeHandler(outcome)

    if generation_df is None or generation_df.empty:
        if outcome.max_level >= logging.ERROR:
            return "failed", outcome.last_message or "fetch error (see log above)"
        return "no_data", outcome.last_message or "ENTSO-E returned nothing for this window"

    try:
        inserted, updated = db.upsert_generation_data(
            generation_df, country_code, publication_timestamp=publication_time
        )
    except Exception as e:
        return "failed", f"upsert error: {e}"

    return "ok", inserted + updated


# ============================================================================
# Main loop
# ============================================================================

def run_backfill(
    plan: list[tuple[str, datetime, datetime, str]],
    client: ENTSOEClient,
) -> dict:
    """
    Execute the plan sequentially, one country-month at a time, committing
    each as it completes (db.upsert_generation_data opens and commits its
    own connection per call -- there is no long-held write transaction here,
    so a live dashboard reading the same SQLite file is never blocked for
    longer than a single month's upsert).

    Safe to interrupt at any point: only fully-committed country-months
    count as "existing" on the next run (get_existing_months), so re-running
    this function (or the whole script) picks up exactly where it left off,
    with no duplicate rows -- energy_generation's unique index on
    (country_code, timestamp_utc) plus upsert_generation_data's
    INSERT OR REPLACE guarantee that even a re-fetch of an already-written
    month overwrites in place rather than duplicating.
    """
    stats = {"done": 0, "no_data": 0, "failed": 0, "rows_written": 0}
    total = len(plan)
    start_time = time.time()

    for i, (cc, month_start, month_end, month_key) in enumerate(plan, start=1):
        outcome, detail = fetch_and_upsert_month(client, cc, month_start, month_end)

        if outcome == "ok":
            stats["done"] += 1
            stats["rows_written"] += detail
            logger.info(f"[{i}/{total}] {cc} {month_key}: +{detail} rows")
        elif outcome == "no_data":
            stats["no_data"] += 1
            logger.info(f"[{i}/{total}] {cc} {month_key}: no data ({detail})")
        else:
            stats["failed"] += 1
            logger.error(f"[{i}/{total}] {cc} {month_key}: FAILED - {detail}")

        if i % SUMMARY_EVERY == 0 or i == total:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = total - i
            eta_min = (remaining / rate / 60) if rate > 0 else float("nan")
            logger.info(
                f"--- progress: {i}/{total} country-months done "
                f"(ok={stats['done']} no_data={stats['no_data']} failed={stats['failed']}) "
                f"| now at {cc} {month_key} "
                f"| elapsed {elapsed / 60:.1f}m | ETA {eta_min:.1f}m ---"
            )

    stats["elapsed_seconds"] = time.time() - start_time
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--countries", default="ALL",
        help="Comma-separated country codes, or ALL (default: ALL -- every "
             "country energy_renewable has A75 data for; see resolve_countries)",
    )
    parser.add_argument("--start", default=DEFAULT_START, help="Start date YYYY-MM-DD (default: 2021-01-01)")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD (default: today, UTC)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the skip-aware plan and its count; make no ENTSO-E calls "
             "and no database writes (read-only checks against energy_generation "
             "only, to compute which months are already covered).",
    )
    args = parser.parse_args()

    end_date = args.end or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    months = get_months(args.start, end_date)
    countries = resolve_countries(args.countries)

    logger.info("=== energy_generation backfill ===")
    logger.info(f"Database: {config.DATABASE_PATH}")
    logger.info(f"Countries: {len(countries)} ({', '.join(countries)})")
    logger.info(f"Window: {args.start} -> {end_date} ({len(months)} months x {len(countries)} countries)")
    if args.dry_run:
        logger.info("[DRY RUN] no ENTSO-E calls, no database writes")

    if not args.dry_run:
        # Idempotent (CREATE TABLE/INDEX IF NOT EXISTS) -- never touches
        # energy_renewable. Skipped entirely in --dry-run, which must not
        # write anything, schema included.
        db.create_generation_table()

    plan, total_existing = build_plan(countries, months)
    logger.info(
        f"Plan: {len(plan)} country-months to fetch across {len(countries)} countries "
        f"({total_existing} already covered by energy_generation)"
    )

    if args.dry_run:
        for cc, _s, _e, k in plan:
            logger.info(f"  [DRY] {cc} {k}")
        logger.info(f"[DRY RUN] total country-months to fetch: {len(plan)}")
        return 0

    if not plan:
        logger.info("Nothing to do -- energy_generation already covers the full requested window.")
        return 0

    client = ENTSOEClient()
    stats = run_backfill(plan, client)

    logger.info("=== Backfill complete ===")
    logger.info(
        f"Country-months: {stats['done']} done, {stats['no_data']} no-data, "
        f"{stats['failed']} failed (of {len(plan)} planned)"
    )
    logger.info(f"Rows written: {stats['rows_written']}")
    logger.info(f"Elapsed: {stats['elapsed_seconds'] / 60:.1f} minutes")
    if stats["failed"] > 0:
        logger.warning(
            f"{stats['failed']} country-month(s) failed. Re-running the same "
            f"command retries only those -- months already written to "
            f"energy_generation are skipped automatically."
        )

    return 1 if stats["failed"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
