#!/usr/bin/env python3
"""Report month-sized holes in the actuals tables. Read-only.

The ABL-29 audit found whole calendar months missing -- HU 2026-01 and 2026-05,
FR 2026-05, HR 2026-05 in `net_position`, each an exact boundary-to-boundary
hole. They predate the cron's 7-day lookback, so only a backfill re-run fills
them, and `backfill_crossborder_progress.json` had marked every one of those
months "completed" while carrying `total_errors: 1533`. A month that failed and
a month that succeeded looked identical from the checkpoint.

This is the check that tells them apart, and it is meant to be run AFTER a
backfill rather than trusted from the checkpoint. It exits non-zero when it
finds a hole, so it works as a gate.

Usage:
    python scripts/check_month_completeness.py
    python scripts/check_month_completeness.py --table crossborder_flows
    python scripts/check_month_completeness.py --db /path/to/replica.db --since 2026-01
    python scripts/check_month_completeness.py --countries HU FR HR --json

Read-only by construction: the database is opened with SQLite's `mode=ro` URI,
so this cannot write to the replica even by accident (see the workstation's
replica-purity rule -- only the sync script may write there).
"""
from __future__ import annotations

import argparse
import calendar
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Iterator, NamedTuple

sys.path.insert(0, str(Path(__file__).parent.parent))

# Tables this can check, and the columns that identify a series in each.
# `key_columns` is what a "series" means for that table: net_position is one
# series per country, crossborder_flows is one per directed border -- a country
# whose FR border is complete and whose CH border is empty is not "complete",
# and aggregating to the country would hide exactly that.
TABLES = {
    "net_position": {"key_columns": ["country_code"], "timestamp": "timestamp_utc"},
    "crossborder_flows": {
        "key_columns": ["country_from", "country_to"],
        "timestamp": "timestamp_utc",
    },
}

# Below this share of the month's hours, report it. A month is either published
# or it is not; real months land at or very near 1.0, and the holes this exists
# for are at exactly 0.0.
INCOMPLETE_THRESHOLD = 0.99


class Hole(NamedTuple):
    series: str
    month: str
    present: int
    expected: int

    @property
    def share(self) -> float:
        return self.present / self.expected if self.expected else 0.0

    @property
    def kind(self) -> str:
        return "MISSING" if self.present == 0 else "PARTIAL"


def hours_in_month(year: int, month: int) -> int:
    """UTC hours in a calendar month.

    Always 24 * days: the timestamps are UTC, which has no DST transitions, so
    a 23- or 25-hour local day never shows up here. (The audit checked the
    fetchers convert to UTC before resampling -- A1.8, clean.)
    """
    return 24 * calendar.monthrange(year, month)[1]


def months_between(start: str, end: str) -> Iterator[tuple[int, int]]:
    """Inclusive range of (year, month) between two 'YYYY-MM' strings."""
    sy, sm = (int(p) for p in start.split("-"))
    ey, em = (int(p) for p in end.split("-"))
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m == 13:
            y, m = y + 1, 1


def previous_month(today: date | None = None) -> str:
    """The last month that is actually over, as 'YYYY-MM'.

    The default upper bound. A month still in progress is 19% complete on the
    6th and that is not a hole; reporting it as one for all 21 countries buries
    the four real ones under noise.
    """
    d = today or date.today()
    return f"{d.year - 1:04d}-12" if d.month == 1 else f"{d.year:04d}-{d.month - 1:02d}"


def find_holes(
    conn: sqlite3.Connection,
    table: str,
    since: str | None = None,
    until: str | None = None,
    countries: list[str] | None = None,
    include_first_month: bool = False,
) -> list[Hole]:
    """Every month-sized hole in `table`, oldest first.

    A series is only checked over the span it actually covers: from its own
    first month to its own last. A country that started publishing in 2024 is
    not "missing" 2023, and one that stopped is not missing every month since
    -- that is a different fact (an outage, ABL-35 defect 1) and reporting it
    here as thousands of holes would bury the real ones.

    A series' own first month is skipped by default for the same reason: it is
    partial by construction whenever the series began mid-month (LU appeared on
    2026-07-18 and reads 43%, which is its birthday, not a gap). Pass
    `include_first_month=True` to check it anyway.
    """
    spec = TABLES[table]
    keys = spec["key_columns"]
    ts = spec["timestamp"]
    key_expr = " || '->' || ".join(keys)

    where, params = "", []
    if countries:
        placeholders = ",".join("?" for _ in countries)
        where = f"WHERE {keys[0]} IN ({placeholders})"
        params = list(countries)

    # substr(...,1,7) takes 'YYYY-MM' under either timestamp separator, which
    # this database really does mix within one column.
    rows = conn.execute(
        f"""SELECT {key_expr} AS series, substr({ts}, 1, 7) AS month,
                   COUNT(DISTINCT {ts}) AS present
              FROM {table} {where}
             GROUP BY series, month""",
        params,
    ).fetchall()

    by_series: dict[str, dict[str, int]] = {}
    for series, month, present in rows:
        by_series.setdefault(series, {})[month] = present

    holes: list[Hole] = []
    for series, months in by_series.items():
        first, last = min(months), max(months)
        floor = first
        if not include_first_month:
            # Step past the series' own first month. months_between handles the
            # rollover, so this is "the month after `first`".
            y0, m0 = (int(p) for p in first.split("-"))
            floor = f"{y0 + 1:04d}-01" if m0 == 12 else f"{y0:04d}-{m0 + 1:02d}"
        lo = max(floor, since) if since else floor
        hi = min(last, until) if until else last
        if lo > hi:
            continue
        for y, m in months_between(lo, hi):
            month = f"{y:04d}-{m:02d}"
            expected = hours_in_month(y, m)
            present = months.get(month, 0)
            if present / expected < INCOMPLETE_THRESHOLD:
                holes.append(Hole(series, month, present, expected))

    return sorted(holes, key=lambda h: (h.month, h.series))


def main() -> int:
    import config

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None, help="SQLite path (default: config.DATABASE_PATH)")
    parser.add_argument("--table", default="net_position", choices=sorted(TABLES))
    parser.add_argument("--since", default=None, help="First month to check, YYYY-MM")
    parser.add_argument(
        "--until",
        default=None,
        help="Last month to check, YYYY-MM (default: the last month that is over)",
    )
    parser.add_argument("--countries", nargs="*", default=None)
    parser.add_argument(
        "--include-first-month",
        action="store_true",
        help="Also check each series' first month, which is partial whenever the "
        "series began mid-month",
    )
    parser.add_argument("--json", action="store_true", help="Machine-readable output")
    args = parser.parse_args()

    until = args.until or previous_month()
    db_path = args.db or str(config.DATABASE_PATH)
    conn = sqlite3.connect(f"file:{Path(db_path).as_posix()}?mode=ro", uri=True)
    try:
        holes = find_holes(
            conn, args.table, args.since, until, args.countries, args.include_first_month
        )
    finally:
        conn.close()

    if args.json:
        print(json.dumps([h._asdict() | {"kind": h.kind} for h in holes], indent=2))
    else:
        print(f"{args.table} @ {db_path}  (through {until})")
        if not holes:
            print("  no month-sized holes")
        else:
            missing = [h for h in holes if h.kind == "MISSING"]
            print(f"  {len(holes)} hole(s): {len(missing)} whole month(s) absent, "
                  f"{len(holes) - len(missing)} partial\n")
            for h in holes:
                print(f"  {h.kind:8s} {h.series:12s} {h.month}  "
                      f"{h.present:4d}/{h.expected} hours ({h.share:5.1%})")

    return 1 if holes else 0


if __name__ == "__main__":
    raise SystemExit(main())
