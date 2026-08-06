"""The month-completeness check (ABL-35 defect 5).

The ABL-29 audit found whole calendar months missing from `net_position` -- HU
2026-01 and 2026-05, FR 2026-05, HR 2026-05 -- while
`backfill_crossborder_progress.json` recorded every one of them as the
"last_completed_month" it had walked past, alongside `total_errors: 1533`. A
month that failed and a month that succeeded were indistinguishable from the
checkpoint, which is why nobody noticed for months.

So the backfill's own record is not evidence. This is, and the issue asks for
it explicitly: re-run the backfill, then verify against the data.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.check_month_completeness import (  # noqa: E402
    find_holes,
    hours_in_month,
    months_between,
    previous_month,
)

SCHEMA = """
CREATE TABLE net_position (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code TEXT NOT NULL,
    timestamp_utc TEXT NOT NULL,
    net_position_mw REAL NOT NULL,
    UNIQUE(country_code, timestamp_utc)
);
CREATE TABLE crossborder_flows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_from TEXT NOT NULL,
    country_to TEXT NOT NULL,
    timestamp_utc TEXT NOT NULL,
    flow_mw REAL NOT NULL,
    UNIQUE(country_from, country_to, timestamp_utc)
);
"""


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.executescript(SCHEMA)
    yield c
    c.close()


def fill(conn, country: str, month: str, hours: int | None = None, sep: str = " "):
    """Write `hours` of a month for a country (default: the whole month)."""
    y, m = (int(p) for p in month.split("-"))
    total = hours if hours is not None else hours_in_month(y, m)
    rows = []
    for i in range(total):
        day, hour = divmod(i, 24)
        rows.append((country, f"{month}-{day + 1:02d}{sep}{hour:02d}:00:00", 1.0))
    conn.executemany(
        "INSERT INTO net_position (country_code, timestamp_utc, net_position_mw) "
        "VALUES (?, ?, ?)",
        rows,
    )


# ============================================================================
# hours_in_month / months_between / previous_month
# ============================================================================

def test_hours_in_month_counts_utc_hours_not_local_days():
    # UTC has no DST, so a month is always 24 * days -- including the months
    # that contain a European clock change. A 23- or 25-hour day here would
    # mean the fetchers had stored local time (audit A1.8 confirmed they do
    # not), and it would make every March and October look incomplete.
    assert hours_in_month(2026, 3) == 744   # contains the spring transition
    assert hours_in_month(2025, 10) == 744  # contains the autumn transition
    assert hours_in_month(2026, 2) == 672
    assert hours_in_month(2024, 2) == 696   # leap


def test_months_between_rolls_over_the_year():
    assert list(months_between("2025-11", "2026-02")) == [
        (2025, 11), (2025, 12), (2026, 1), (2026, 2)
    ]


def test_months_between_is_inclusive_of_a_single_month():
    assert list(months_between("2026-05", "2026-05")) == [(2026, 5)]


def test_previous_month_rolls_back_over_january():
    assert previous_month(date(2026, 8, 6)) == "2026-07"
    assert previous_month(date(2026, 1, 15)) == "2025-12"


# ============================================================================
# find_holes
# ============================================================================

def test_finds_a_whole_month_hole_between_two_full_months(conn):
    # The exact shape the audit found: an exact boundary-to-boundary hole with
    # healthy months either side, too old for the cron's 7-day lookback.
    fill(conn, "HU", "2026-04")
    fill(conn, "HU", "2026-06")

    holes = find_holes(conn, "net_position")

    assert [(h.series, h.month, h.kind) for h in holes] == [("HU", "2026-05", "MISSING")]
    assert holes[0].present == 0
    assert holes[0].expected == 744


def test_a_complete_series_has_no_holes(conn):
    for month in ("2026-04", "2026-05", "2026-06"):
        fill(conn, "BE", month)

    assert find_holes(conn, "net_position") == []


def test_does_not_invent_holes_before_a_series_started(conn):
    # A country that began publishing in 2026-05 is not "missing" 2023-2026.
    fill(conn, "LU", "2026-05")
    fill(conn, "LU", "2026-06")

    assert find_holes(conn, "net_position") == []


def test_does_not_invent_holes_after_a_series_stopped(conn):
    # GR/IE's outage is a different fact from a backfill gap, and reporting it
    # as ten holes per country would bury the four real ones.
    fill(conn, "GR", "2026-04")
    fill(conn, "GR", "2026-05")

    assert find_holes(conn, "net_position") == []


def test_skips_a_series_own_first_month_by_default(conn):
    # LU appeared on 2026-07-18 and reads 43% of that month. That is its
    # birthday, not a gap.
    fill(conn, "LU", "2026-05", hours=300)
    fill(conn, "LU", "2026-06")

    assert find_holes(conn, "net_position") == []
    forced = find_holes(conn, "net_position", include_first_month=True)
    assert [(h.series, h.month) for h in forced] == [("LU", "2026-05")]


def test_reports_a_materially_partial_month(conn):
    # GR 2026-02 really is 97 of 672 hours. Partial, not absent -- and the
    # distinction matters, because a partial month means the fetch ran and came
    # back nearly empty rather than never running.
    fill(conn, "GR", "2026-01")
    fill(conn, "GR", "2026-02", hours=97)
    fill(conn, "GR", "2026-03")

    holes = find_holes(conn, "net_position")

    assert [(h.series, h.month, h.kind) for h in holes] == [("GR", "2026-02", "PARTIAL")]
    assert holes[0].present == 97
    assert 0.14 < holes[0].share < 0.15


def test_a_month_short_by_a_single_hour_is_not_a_hole(conn):
    # The threshold exists so a one-off missing hour is not treated as a
    # month-sized failure. This check is for backfill holes, and a 99.9% month
    # is not one.
    fill(conn, "BE", "2026-04")
    fill(conn, "BE", "2026-05", hours=743)
    fill(conn, "BE", "2026-06")

    assert find_holes(conn, "net_position") == []


def test_respects_an_explicit_window(conn):
    fill(conn, "HU", "2026-01")
    fill(conn, "HU", "2026-04")

    assert len(find_holes(conn, "net_position")) == 2      # 02 and 03
    assert len(find_holes(conn, "net_position", since="2026-03")) == 1
    assert find_holes(conn, "net_position", until="2026-01") == []


def test_filters_to_named_countries(conn):
    fill(conn, "HU", "2026-04")
    fill(conn, "HU", "2026-06")
    fill(conn, "FR", "2026-04")
    fill(conn, "FR", "2026-06")

    assert {h.series for h in find_holes(conn, "net_position")} == {"HU", "FR"}
    assert {h.series for h in find_holes(conn, "net_position", countries=["HU"])} == {"HU"}


def test_counts_both_timestamp_separators_as_the_same_month(conn):
    # This database mixes '2026-05-01T00:00:00' and '2026-05-01 00:00:00'
    # inside one column. substr(...,1,7) has to see through that, or a month
    # written in the other form would read as entirely missing.
    fill(conn, "PL", "2026-04")
    fill(conn, "PL", "2026-05", hours=372, sep=" ")
    conn.executemany(
        "INSERT INTO net_position (country_code, timestamp_utc, net_position_mw) "
        "VALUES (?, ?, ?)",
        [
            ("PL", f"2026-05-{16 + (i // 24):02d}T{i % 24:02d}:00:00", 1.0)
            for i in range(372)
        ],
    )
    fill(conn, "PL", "2026-06")

    assert find_holes(conn, "net_position") == []


def test_crossborder_completeness_is_per_border_not_per_country(conn):
    # A country whose FR border is complete and whose CH border is empty is not
    # complete. Aggregating to the country would hide exactly the failure the
    # NaN-rollback bug produced.
    rows = []
    for month, hours in (("2026-04", 720), ("2026-05", 744), ("2026-06", 720)):
        for i in range(hours):
            ts = f"{month}-{i // 24 + 1:02d} {i % 24:02d}:00:00"
            rows.append(("DE", "FR", ts, 1.0))
            if month != "2026-05":
                rows.append(("DE", "CH", ts, 1.0))
    conn.executemany(
        "INSERT INTO crossborder_flows (country_from, country_to, timestamp_utc, flow_mw) "
        "VALUES (?, ?, ?, ?)",
        rows,
    )

    holes = find_holes(conn, "crossborder_flows")

    assert [(h.series, h.month) for h in holes] == [("DE->CH", "2026-05")]
