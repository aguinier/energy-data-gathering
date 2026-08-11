#!/usr/bin/env python3
"""Targeted catch-up pass for late-published gaps the 7-day update window cannot heal.

ABL-84/ABL-85: `scripts/update.py` only ever refetches the trailing
`UPDATE_DAYS_BACK` (7) days. An upstream zone that goes dark for longer than
that and later republishes is never re-requested once the outage slides out
of that window -- even though ENTSO-E is holding the data. Measured 2026-08-09,
probing ENTSO-E directly for windows prod has zero rows in: AL 2026-06-29..
07-08 (169 points now available) and 2026-01-05..01-20 (358 points), CY
2026-05-21..06-18 (1,344 points). MK and MD go dark over similar spans and
genuinely never publish the missing hours at all (0 points upstream) -- this
script cannot and does not need to special-case that; it just gets an empty
response back and moves on. Sizing and probe method are recorded on ABL-85 and
in CLAUDE.md ("Data the database does not have").

This script finds "interior" holes: gaps in a country's own `energy_load`
series bounded by real rows on BOTH sides. That shape is exactly what a
late-then-recovered publisher leaves behind. An ongoing or permanently-ended
outage -- data up to some point and nothing since -- has no "after" boundary
yet and is correctly invisible here; that is the ordinary update cron's job,
not this one's, and is already covered by the server's freshness verdicts
(energy-dashboard-frontend's `services/freshness.ts`). A country with no
interior holes costs this pass one bounded, indexed SELECT and zero ENTSO-E
requests -- there is no standing cost on a healthy zone.

Usage:
    # Report only, read-only, safe to run against a replica
    python scripts/catchup.py --dry-run

    # Scan the default 100-day lookback and fetch every hole found
    python scripts/catchup.py

    # One-off wider scan to reach AL's January window too
    python scripts/catchup.py --countries AL,CY --since 2025-12-01
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
import utils
from src.entsoe_client import ENTSOEClient
from src import fetch_load

logger = logging.getLogger("entsoe_pipeline")

# Below this, treat consecutive rows as ordinary cadence. Sized against the
# whole fleet on ABL-85/ABL-60: every healthy zone's own worst lag sits under
# 9.5h and the next value up is a genuine multi-day outage, so 6h has wide
# margin on both sides without being so wide it misses a real gap.
MIN_GAP_HOURS = 6.0

# How far back a routine (weekly, cron-scheduled) scan looks. Matches the
# window ABL-85 sized the fleet-wide gap total against (3,242h of energy_load
# gaps >6h over a trailing 90 days). A one-off wider `--since` is how the one
# known older window (AL's January gap, which sits outside 90 days) gets
# healed on an ad hoc run -- see the ABL-85 comment thread. Nothing in this
# script treats that window specially; a wide enough `--since` finds it the
# same way it finds everything else.
DEFAULT_LOOKBACK_DAYS = 100

TABLE = "energy_load"
TIMESTAMP_COLUMN = "timestamp_utc"


class Hole(NamedTuple):
    country: str
    last_seen: str  # last known-good timestamp before the gap (DB string form)
    next_seen: str  # first known-good timestamp after the gap (DB string form)
    gap_hours: float


# ============================================================================
# Mixed-separator-safe window bounds
#
# This database stores 'energy_load.timestamp_utc' with two separators in one
# column ('2026-05-01T00:00:00' and '2026-05-01 00:00:00'); SQLite compares
# them as plain strings and 'T' (0x54) sorts above ' ' (0x20), so a single-form
# bound silently drops rows on one side of it. Same defect class as ABL-21 in
# the sibling dashboard module; see that module's
# server/src/utils/timestamp.ts for the sizing measurements this mirrors.
# ============================================================================

def _normalize(ts: str) -> str:
    """Space-separated form of a timestamp, whichever separator it was stored with."""
    return ts.replace("T", " ")


def _to_t_form(normalized: str) -> str:
    """The 'T'-separated form of an already-normalized (space) timestamp."""
    if len(normalized) > 10 and normalized[10] == " ":
        return normalized[:10] + "T" + normalized[11:]
    return normalized


def _full_timestamp(value: str, end_of_day: bool) -> str:
    """Pad a bare 'YYYY-MM-DD' bound to a full timestamp.

    A bare date is a valid *lower* bound as a string (it's a strict prefix of
    any same-day timestamp, and a prefix sorts first) but NOT a valid upper
    bound -- '2026-08-11' sorts below '2026-08-11 00:15:00', which would
    silently exclude the entire day. Callers only ever pass a bare date for
    convenience; this makes both directions correct.
    """
    if len(value) <= 10:
        return f"{value} 23:59:59" if end_of_day else f"{value} 00:00:00"
    return value


def _range_clause(column: str) -> str:
    """Two-clause window predicate that stays index-friendly under mixed
    separators: a wide bare-column BETWEEN (drives the index) plus an exact
    REPLACE(...)-normalised re-check over just the rows the first clause found."""
    return f"({column} BETWEEN ? AND ? AND REPLACE({column}, 'T', ' ') BETWEEN ? AND ?)"


def _range_args(start: str, end: str) -> tuple[str, str, str, str]:
    norm_start = _normalize(start)
    norm_end = _normalize(end)
    return (norm_start, _to_t_form(norm_end), norm_start, norm_end)


# ============================================================================
# Hole detection (pure, colocated test: tests/test_catchup.py)
# ============================================================================

def find_interior_holes(
    conn: sqlite3.Connection,
    since: str,
    until: str,
    countries: Optional[list[str]] = None,
    min_gap_hours: float = MIN_GAP_HOURS,
    table: str = TABLE,
    timestamp_column: str = TIMESTAMP_COLUMN,
) -> list[Hole]:
    """Every gap > min_gap_hours between two real rows of `table`, per country.

    Only interior gaps count: both a `last_seen` row before the gap and a
    `next_seen` row after it must exist inside [since, until]. An ongoing
    outage -- data up to some point and nothing since -- has no `next_seen`
    inside the window and is correctly invisible here; that is the ordinary
    update cron's job. A gap that started before `since` is only found from
    its recovery point onward on this run (the front half is out of the
    query's view) -- for the routine weekly scan that self-heals as the window
    slides; a one-off wider `--since` covers it in one pass.
    """
    since = _full_timestamp(since, end_of_day=False)
    until = _full_timestamp(until, end_of_day=True)

    where = f"WHERE {_range_clause(timestamp_column)}"
    params: list[str] = list(_range_args(since, until))
    if countries:
        placeholders = ",".join("?" for _ in countries)
        where += f" AND country_code IN ({placeholders})"
        params.extend(countries)

    rows = conn.execute(
        f"SELECT DISTINCT country_code, {timestamp_column} FROM {table} {where}",
        params,
    ).fetchall()

    by_country: dict[str, list[datetime]] = {}
    for country_code, ts in rows:
        # [:19] drops a trailing offset suffix some 2025-11 rows carry
        # (e.g. '+02:00') -- known, unrelated data-quality wrinkle documented
        # in CLAUDE.md ("Data the database does not have"); out of scope here.
        parsed = datetime.strptime(_normalize(ts)[:19], "%Y-%m-%d %H:%M:%S")
        by_country.setdefault(country_code, []).append(parsed)

    holes: list[Hole] = []
    for country, stamps in by_country.items():
        stamps.sort()
        for prev, nxt in zip(stamps, stamps[1:]):
            gap_hours = (nxt - prev).total_seconds() / 3600.0
            if gap_hours > min_gap_hours:
                holes.append(
                    Hole(
                        country=country,
                        last_seen=prev.strftime("%Y-%m-%d %H:%M:%S"),
                        next_seen=nxt.strftime("%Y-%m-%d %H:%M:%S"),
                        gap_hours=gap_hours,
                    )
                )

    return sorted(holes, key=lambda h: (h.country, h.last_seen))


def fetch_window(hole: Hole) -> tuple[datetime, datetime]:
    """The (start, end) UTC datetimes to request for one hole.

    Includes the known-good boundary timestamps themselves rather than only
    the gap's interior -- harmless, since `db.upsert_load_data` is an upsert
    keyed on (country_code, timestamp_utc), and it keeps the request aligned
    to data ENTSO-E is known to have rather than guessing at the exact
    resolution boundary.
    """
    import pytz

    start = pytz.UTC.localize(datetime.strptime(hole.last_seen, "%Y-%m-%d %H:%M:%S"))
    end = pytz.UTC.localize(
        datetime.strptime(hole.next_seen, "%Y-%m-%d %H:%M:%S") + timedelta(hours=1)
    )
    return start, end


def count_rows(conn: sqlite3.Connection, country: str, start: str, end: str) -> int:
    where = _range_clause(TIMESTAMP_COLUMN)
    args = _range_args(start, end)
    return conn.execute(
        f"SELECT COUNT(*) FROM {TABLE} WHERE country_code = ? AND {where}",
        (country, *args),
    ).fetchone()[0]


# ============================================================================
# CLI
# ============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--since", default=None,
        help=f"Start of scan window, YYYY-MM-DD (default: {DEFAULT_LOOKBACK_DAYS} days ago)",
    )
    parser.add_argument("--until", default=None, help="End of scan window, YYYY-MM-DD (default: now)")
    parser.add_argument(
        "--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
        help=f"Days back from --until when --since is not given (default: {DEFAULT_LOOKBACK_DAYS})",
    )
    parser.add_argument("--countries", default=None, help="Comma-separated codes (default: all)")
    parser.add_argument("--min-gap-hours", type=float, default=MIN_GAP_HOURS)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report holes only; opens the database read-only and makes no ENTSO-E requests",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger_local = utils.setup_logging(log_level=args.log_level)

    now = datetime.now(timezone.utc)
    until = args.until or now.strftime("%Y-%m-%d %H:%M:%S")
    since = args.since or (now - timedelta(days=args.lookback_days)).strftime("%Y-%m-%d")
    countries = (
        [c.strip().upper() for c in args.countries.split(",")] if args.countries else None
    )

    logger_local.info("Catch-up scan: %s .. %s (countries: %s)", since, until, countries or "ALL")

    if args.dry_run:
        conn = sqlite3.connect(f"file:{Path(config.DATABASE_PATH).as_posix()}?mode=ro", uri=True)
    else:
        config.validate_config()
        conn = sqlite3.connect(str(config.DATABASE_PATH))

    try:
        holes = find_interior_holes(conn, since, until, countries, args.min_gap_hours)
    finally:
        conn.close()

    if not holes:
        logger_local.info("No interior holes found.")
        return 0

    logger_local.info("Found %d interior hole(s):", len(holes))
    for h in holes:
        logger_local.info("  %s: %s -> %s (%.1fh)", h.country, h.last_seen, h.next_seen, h.gap_hours)

    if args.dry_run:
        logger_local.info("[DRY RUN] not fetching")
        return 0

    client = ENTSOEClient()
    total_recovered = 0
    for h in holes:
        start_dt, end_dt = fetch_window(h)
        start_str, end_str = h.last_seen, h.next_seen

        with sqlite3.connect(str(config.DATABASE_PATH)) as count_conn:
            before = count_rows(count_conn, h.country, start_str, end_str)

        inserted, updated, failed = fetch_load.fetch_load_data(client, h.country, start_dt, end_dt)

        with sqlite3.connect(str(config.DATABASE_PATH)) as count_conn:
            after = count_rows(count_conn, h.country, start_str, end_str)

        total_recovered += after - before
        logger_local.info(
            "  %s %s..%s: %d -> %d rows (+%d) [inserted=%d updated=%d failed=%d]",
            h.country, start_str, end_str, before, after, after - before, inserted, updated, failed,
        )

    logger_local.info("Catch-up pass complete: %d row(s) recovered across %d hole(s)", total_recovered, len(holes))
    return 0


if __name__ == "__main__":
    sys.exit(main())
