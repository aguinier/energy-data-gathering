"""The late-publisher catch-up scan (ABL-85).

ABL-84 found AL's `energy_load` frozen since 2026-08-06 21:45 UTC. ABL-85
generalised the root cause: the update cron only ever refetches a trailing
7-day window, so a zone that goes dark for longer than that and later
republishes is never re-requested once the outage slides out of view -- even
though ENTSO-E is holding the data. AL (twice) and CY do exactly this,
measured 2026-08-09 by probing ENTSO-E directly for windows prod has zero rows
in and finding hundreds to over a thousand points sitting there unclaimed.

This is the scan half: it has to find a bounded interior gap (recovered) and
say nothing about a trailing one (still down, or never published at all --
MK/MD's shape), or the ordinary update cron's own job and this pass's job
would collide.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.catchup import (  # noqa: E402
    Hole,
    _full_timestamp,
    count_rows,
    fetch_window,
    find_interior_holes,
)

SCHEMA = """
CREATE TABLE energy_load (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code TEXT NOT NULL,
    timestamp_utc TIMESTAMP NOT NULL,
    load_mw REAL NOT NULL,
    data_quality TEXT DEFAULT 'actual',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    publication_timestamp_utc TIMESTAMP,
    UNIQUE(country_code, timestamp_utc)
);
"""


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.executescript(SCHEMA)
    yield c
    c.close()


def fill(conn, country: str, timestamps: list[str], load_mw: float = 100.0):
    conn.executemany(
        "INSERT INTO energy_load (country_code, timestamp_utc, load_mw) VALUES (?, ?, ?)",
        [(country, ts, load_mw) for ts in timestamps],
    )


def hourly(start: str, count: int, sep: str = " ") -> list[str]:
    """`count` consecutive hourly timestamps starting at `start` ('YYYY-MM-DD HH:MM:SS')."""
    base = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    return [(base + timedelta(hours=i)).strftime(f"%Y-%m-%d{sep}%H:%M:%S") for i in range(count)]


# ============================================================================
# find_interior_holes
# ============================================================================

def test_finds_a_bounded_interior_hole(conn):
    # Real data before AND after a >6h silent stretch -- exactly a
    # late-then-recovered publisher.
    fill(conn, "AL", hourly("2026-06-28 00:00:00", 24))
    fill(conn, "AL", hourly("2026-07-08 00:00:00", 24))

    holes = find_interior_holes(conn, since="2026-06-01", until="2026-08-01")

    assert len(holes) == 1
    h = holes[0]
    assert h.country == "AL"
    assert h.last_seen == "2026-06-28 23:00:00"
    assert h.next_seen == "2026-07-08 00:00:00"
    assert 200 < h.gap_hours < 220


def test_ordinary_hourly_cadence_is_not_a_hole(conn):
    fill(conn, "DE", hourly("2026-06-28 00:00:00", 72))

    assert find_interior_holes(conn, since="2026-06-01", until="2026-07-01") == []


def test_a_gap_at_or_under_the_threshold_is_not_reported(conn):
    # Exactly 6h and a hair under both pass; only strictly-over counts.
    fill(conn, "SE", ["2026-06-01 00:00:00", "2026-06-01 06:00:00"])
    fill(conn, "FI", ["2026-06-01 00:00:00", "2026-06-01 05:59:59"])

    assert find_interior_holes(conn, since="2026-05-01", until="2026-07-01") == []


def test_a_gap_just_over_the_threshold_is_reported(conn):
    fill(conn, "SE", ["2026-06-01 00:00:00", "2026-06-01 06:00:01"])

    holes = find_interior_holes(conn, since="2026-05-01", until="2026-07-01")

    assert len(holes) == 1
    assert holes[0].country == "SE"


def test_an_ongoing_outage_has_no_after_boundary_and_is_not_reported(conn):
    # AL's actual live shape (ABL-84): continuous data up to 2026-08-06 21:00
    # and nothing since. There is no `next_seen` inside the window, so this is
    # invisible here on purpose -- it is the ordinary update cron / freshness
    # pill's signal, not this pass's.
    fill(conn, "AL", hourly("2026-08-01 00:00:00", 142))  # ends 2026-08-06 21:00

    assert find_interior_holes(conn, since="2026-07-01", until="2026-08-11") == []


def test_a_gap_that_never_recovered_within_the_fleets_history_is_not_reported(conn):
    # MK/MD's shape: silence, and upstream genuinely has nothing for that
    # window either. From this table's point of view that looks identical to
    # the ongoing-outage case above -- no `next_seen` -- so it is correctly
    # not flagged; the fetch attempt this script would make for a *bounded*
    # hole simply never gets scheduled, and there is nothing to special-case.
    fill(conn, "MK", hourly("2026-05-01 00:00:00", 24))

    assert find_interior_holes(conn, since="2026-04-01", until="2026-08-01") == []


def test_a_gap_starting_before_the_window_is_only_found_from_its_recovery_point(conn):
    # The front half of a gap that predates `since` is out of the query's
    # view; only pairs of rows both inside [since, until] are considered. This
    # is a deliberate, documented limitation (module docstring): the routine
    # weekly scan self-heals it as the window slides, and a wide `--since`
    # covers it in one pass.
    fill(conn, "CY", ["2026-05-01 00:00:00"])  # before the window
    fill(conn, "CY", hourly("2026-06-20 00:00:00", 2))  # recovery, inside window

    assert find_interior_holes(conn, since="2026-06-01", until="2026-08-01") == []


def test_filters_to_named_countries(conn):
    fill(conn, "AL", hourly("2026-06-28 00:00:00", 1))
    fill(conn, "AL", hourly("2026-07-08 00:00:00", 1))
    fill(conn, "CY", hourly("2026-05-21 00:00:00", 1))
    fill(conn, "CY", hourly("2026-06-18 00:00:00", 1))

    all_holes = find_interior_holes(conn, since="2026-04-01", until="2026-08-01")
    assert {h.country for h in all_holes} == {"AL", "CY"}

    al_only = find_interior_holes(conn, since="2026-04-01", until="2026-08-01", countries=["AL"])
    assert {h.country for h in al_only} == {"AL"}


def test_respects_a_custom_min_gap_hours(conn):
    fill(conn, "BE", ["2026-06-01 00:00:00", "2026-06-01 02:00:00"])

    assert find_interior_holes(conn, since="2026-05-01", until="2026-07-01") == []
    strict = find_interior_holes(conn, since="2026-05-01", until="2026-07-01", min_gap_hours=1.0)
    assert len(strict) == 1


def test_mixed_separators_do_not_hide_or_duplicate_rows(conn):
    # This column really does mix '2026-07-08T00:00:00' and
    # '2026-07-08 00:00:00' inside itself (ABL-21's defect class). A hole
    # spanning the two forms must still be found, and the boundary rows must
    # still be counted once each.
    fill(conn, "AL", ["2026-06-28T23:00:00"], )  # 'T' form, pre-cutover style
    fill(conn, "AL", ["2026-07-08 00:00:00"])     # space form

    holes = find_interior_holes(conn, since="2026-06-01", until="2026-08-01")

    assert len(holes) == 1
    assert holes[0].last_seen == "2026-06-28 23:00:00"
    assert holes[0].next_seen == "2026-07-08 00:00:00"


def test_multiple_countries_are_independent(conn):
    fill(conn, "AL", hourly("2026-06-28 00:00:00", 1))
    fill(conn, "AL", hourly("2026-07-08 00:00:00", 1))
    fill(conn, "DE", hourly("2026-06-01 00:00:00", 240))  # fully healthy, no holes

    holes = find_interior_holes(conn, since="2026-04-01", until="2026-08-01")

    assert [h.country for h in holes] == ["AL"]


def test_output_is_sorted_by_country_then_time(conn):
    fill(conn, "CY", hourly("2026-05-21 00:00:00", 1))
    fill(conn, "CY", hourly("2026-06-18 00:00:00", 1))
    fill(conn, "AL", hourly("2026-01-05 00:00:00", 1))
    fill(conn, "AL", hourly("2026-01-20 00:00:00", 1))
    fill(conn, "AL", hourly("2026-06-28 00:00:00", 1))
    fill(conn, "AL", hourly("2026-07-08 00:00:00", 1))

    holes = find_interior_holes(conn, since="2025-12-01", until="2026-08-01")

    # Each adjacent pair of present timestamps > min_gap_hours apart is its own
    # hole -- including AL's Jan20->Jun28 span, since both ends are real data
    # inside the window. This scan does not group by "block"; it is agnostic
    # to how many isolated points are involved and how large the gap is, which
    # is what makes it able to notice a case nobody anticipated in advance.
    assert [(h.country, h.last_seen) for h in holes] == [
        ("AL", "2026-01-05 00:00:00"),
        ("AL", "2026-01-20 00:00:00"),
        ("AL", "2026-06-28 00:00:00"),
        ("CY", "2026-05-21 00:00:00"),
    ]


# ============================================================================
# _full_timestamp
# ============================================================================

def test_full_timestamp_leaves_an_explicit_time_alone():
    assert _full_timestamp("2026-06-01 12:00:00", end_of_day=False) == "2026-06-01 12:00:00"


def test_full_timestamp_pads_a_bare_lower_bound_to_midnight():
    assert _full_timestamp("2026-06-01", end_of_day=False) == "2026-06-01 00:00:00"


def test_full_timestamp_pads_a_bare_upper_bound_to_end_of_day():
    # The failure this guards against: '2026-06-01' < '2026-06-01 00:15:00' as
    # plain strings, so an un-padded bare date used as an upper bound would
    # silently exclude the entire day.
    assert _full_timestamp("2026-06-01", end_of_day=True) == "2026-06-01 23:59:59"
    assert "2026-06-01 00:15:00" <= _full_timestamp("2026-06-01", end_of_day=True)


# ============================================================================
# fetch_window
# ============================================================================

def test_fetch_window_spans_the_known_good_boundaries_plus_a_pad_hour():
    hole = Hole(country="AL", last_seen="2026-06-28 23:00:00", next_seen="2026-07-08 00:00:00", gap_hours=217.0)

    start, end = fetch_window(hole)

    assert start.isoformat() == "2026-06-28T23:00:00+00:00"
    assert end.isoformat() == "2026-07-08T01:00:00+00:00"


# ============================================================================
# count_rows
# ============================================================================

def test_count_rows_uses_the_same_mixed_separator_safe_bound(conn):
    fill(conn, "AL", ["2026-06-28T23:00:00", "2026-07-08 00:00:00", "2026-07-09 00:00:00"])

    assert count_rows(conn, "AL", "2026-06-28 23:00:00", "2026-07-08 00:00:00") == 2
    assert count_rows(conn, "AL", "2026-06-01 00:00:00", "2026-08-01 00:00:00") == 3
    assert count_rows(conn, "CY", "2026-06-01 00:00:00", "2026-08-01 00:00:00") == 0
