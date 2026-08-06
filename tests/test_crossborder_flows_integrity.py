"""Cross-border flow ingest: the NaN-rollback batch loss, and DE's zone key.

Both defects come from the ABL-29 data audit and are tracked on ABL-35.

**The NaN rollback (audit finding A1.8.1).** `_normalize_wide_to_long` dropped
NaN flow values *before* `resample("h")`, and the resample then re-created NaN
for every hour with no source point inside it -- a gapped border, a suspended
interconnector, an upstream outage. That NaN reached `flow_mw REAL NOT NULL`,
the IntegrityError propagated out of `db.get_connection()`, and its
`except: rollback()` discarded **every row already written for that country in
that window**. One gapped border cost the country's whole batch, and the caller
saw a single "error fetching" line rather than "the write you thought landed did
not".

Two layers are tested here, because either one alone leaves the other's callers
exposed: the drop moved after the resample (prevention), and a per-row skip in
`upsert_crossborder_flows` (containment, for any other caller).

Neither layer fills the gap with `0.0`. An hour ENTSO-E did not publish is
unknown; a `0.0` would assert a measured "no flow across this border", which is
a different and false claim. The table expresses "not published" by the row's
absence.

**DE's neighbour key (audit finding A1.3).** The border fan-out used
`DE_AT_LU`, a bidding zone that ceased to exist in October 2018. The audit
attributed the missing DE-DK/SE/NO borders to that key; measured against
entsoe-py's own NEIGHBOURS map, that is only half right, and the tests below
pin what is actually true so the next reader does not re-derive it.
"""
from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: E402
from src import db  # noqa: E402
from src.entsoe_client import COUNTRY_TO_NEIGHBOURS_KEYS  # noqa: E402
from src.fetch_crossborder_flows import _normalize_wide_to_long  # noqa: E402


# ============================================================================
# _normalize_wide_to_long: the NaN must not survive the resample
# ============================================================================

def _wide(index, **columns):
    """The shape query_crossborder_all returns: index=timestamps, one column
    per neighbour zone."""
    return pd.DataFrame(columns, index=pd.DatetimeIndex(index, tz="UTC"))


def test_resample_reintroduced_nan_is_dropped():
    # A two-hour hole in the middle of the series. resample("h") reindexes onto
    # a continuous hourly range and fills 01:00 and 02:00 with NaN -- after the
    # original dropna had already run and found nothing to do.
    wide = _wide(
        ["2026-07-01T00:00:00Z", "2026-07-01T03:00:00Z"],
        FR=[100.0, 400.0],
    )

    out = _normalize_wide_to_long(wide, "BE")

    assert not out["flow_mw"].isna().any(), "a NaN here reaches flow_mw NOT NULL"
    assert sorted(out["flow_mw"]) == [100.0, 400.0]
    assert len(out) == 2, "the gapped hours are absent, not present as zeros"


def test_gapped_hours_are_absent_not_zero():
    # The distinction the whole ingest turns on: 0.0 is a measured "no flow",
    # absence is "not published". Filling the hole would be a fabricated
    # measurement on a border that may well have been carrying a gigawatt.
    wide = _wide(
        ["2026-07-01T00:00:00Z", "2026-07-01T02:00:00Z"],
        NL=[500.0, 700.0],
    )

    out = _normalize_wide_to_long(wide, "BE")

    assert 0.0 not in list(out["flow_mw"])
    assert list(out["timestamp_utc"].dt.strftime("%H:%M")) == ["00:00", "02:00"]


def test_one_gapped_border_does_not_cost_the_others():
    # The failure this file exists for. FR is gapped, NL is complete. Before the
    # fix FR's NaN aborted the write and NL went with it.
    wide = _wide(
        ["2026-07-01T00:00:00Z", "2026-07-01T01:00:00Z", "2026-07-01T02:00:00Z"],
        FR=[100.0, float("nan"), 300.0],
        NL=[10.0, 20.0, 30.0],
    )

    out = _normalize_wide_to_long(wide, "BE")

    assert not out["flow_mw"].isna().any()
    assert len(out[out["country_to"] == "NL"]) == 3, "NL is complete and must survive"
    assert len(out[out["country_to"] == "FR"]) == 2, "only FR's gapped hour is lost"


def test_warns_about_dropped_hours_rather_than_dropping_them_quietly(caplog):
    wide = _wide(
        ["2026-07-01T00:00:00Z", "2026-07-01T03:00:00Z"],
        FR=[100.0, 400.0],
    )

    with caplog.at_level(logging.WARNING):
        _normalize_wide_to_long(wide, "BE")

    assert any("BE" in r.message and "2" in r.message for r in caplog.records), (
        "silent data loss is what let this reach a data audit"
    )


def test_a_complete_series_logs_nothing_and_keeps_every_hour(caplog):
    wide = _wide(
        ["2026-07-01T00:00:00Z", "2026-07-01T01:00:00Z"],
        FR=[100.0, 200.0],
    )

    with caplog.at_level(logging.WARNING):
        out = _normalize_wide_to_long(wide, "BE")

    assert len(out) == 2
    assert not caplog.records, "the rule must cost nothing on a healthy border"


def test_multi_zone_neighbours_still_aggregate_to_one_country():
    # IT_NORD + IT_CSUD -> IT, summed before the hourly mean. Unchanged
    # behaviour; asserted so the dropna move cannot quietly break it.
    wide = _wide(
        ["2026-07-01T00:00:00Z", "2026-07-01T01:00:00Z"],
        IT_NORD=[100.0, 150.0],
        IT_CSUD=[10.0, 15.0],
    )

    out = _normalize_wide_to_long(wide, "AT")

    assert set(out["country_to"]) == {"IT"}
    assert sorted(out["flow_mw"]) == [110.0, 165.0]


# ============================================================================
# db.upsert_crossborder_flows: partial and loud, never all-or-nothing
# ============================================================================

CROSSBORDER_SCHEMA = """
CREATE TABLE crossborder_flows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_from TEXT NOT NULL,
    country_to TEXT NOT NULL,
    timestamp_utc TEXT NOT NULL,
    flow_mw REAL NOT NULL,
    data_quality TEXT DEFAULT 'actual',
    publication_timestamp_utc TEXT,
    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(country_from, country_to, timestamp_utc)
)
"""


@pytest.fixture
def scratch_crossborder_db(tmp_path, monkeypatch):
    """A throwaway SQLite file with the real crossborder_flows DDL -- copied
    verbatim from energy_dashboard.db, because `flow_mw REAL NOT NULL` is the
    thing under test. Never the replica, never prod."""
    scratch_path = tmp_path / "scratch_crossborder.db"
    monkeypatch.setattr(config, "DATABASE_PATH", scratch_path)
    conn = sqlite3.connect(scratch_path)
    conn.execute(CROSSBORDER_SCHEMA)
    conn.commit()
    conn.close()
    return scratch_path


def _rows(path):
    conn = sqlite3.connect(path)
    try:
        return conn.execute(
            "SELECT country_to, timestamp_utc, flow_mw FROM crossborder_flows "
            "ORDER BY country_to, timestamp_utc"
        ).fetchall()
    finally:
        conn.close()


def test_a_null_value_no_longer_discards_the_whole_batch(scratch_crossborder_db, caplog):
    # The regression in one test: 4 good rows and 1 NaN. Before the fix this
    # wrote ZERO rows -- the IntegrityError rolled the connection back.
    df = pd.DataFrame({
        "country_to": ["FR", "FR", "NL", "NL", "NL"],
        "timestamp_utc": pd.to_datetime([
            "2026-07-01T00:00:00Z", "2026-07-01T01:00:00Z",
            "2026-07-01T00:00:00Z", "2026-07-01T01:00:00Z", "2026-07-01T02:00:00Z",
        ]),
        "flow_mw": [100.0, float("nan"), 10.0, 20.0, 30.0],
    })

    with caplog.at_level(logging.WARNING):
        written, _ = db.upsert_crossborder_flows(df, "BE")

    assert written == 4
    stored = _rows(scratch_crossborder_db)
    assert len(stored) == 4
    assert ("FR", "2026-07-01 00:00:00", 100.0) in stored
    assert len([r for r in stored if r[0] == "NL"]) == 3
    assert any("Skipped 1" in r.message for r in caplog.records), "the loss must be loud"


def test_the_skipped_row_is_absent_not_stored_as_zero(scratch_crossborder_db):
    df = pd.DataFrame({
        "country_to": ["FR", "FR"],
        "timestamp_utc": pd.to_datetime(["2026-07-01T00:00:00Z", "2026-07-01T01:00:00Z"]),
        "flow_mw": [float("nan"), 250.0],
    })

    db.upsert_crossborder_flows(df, "BE")

    stored = _rows(scratch_crossborder_db)
    assert stored == [("FR", "2026-07-01 01:00:00", 250.0)]
    assert all(r[2] != 0.0 for r in stored)


def test_a_missing_timestamp_is_skipped_the_same_way(scratch_crossborder_db):
    # timestamp_utc is NOT NULL too, and used to be passed as None on the same
    # code path, with the same batch-wide consequence.
    df = pd.DataFrame({
        "country_to": ["FR", "FR"],
        "timestamp_utc": [pd.NaT, pd.Timestamp("2026-07-01T01:00:00Z")],
        "flow_mw": [100.0, 250.0],
    })

    written, _ = db.upsert_crossborder_flows(df, "BE")

    assert written == 1
    assert _rows(scratch_crossborder_db) == [("FR", "2026-07-01 01:00:00", 250.0)]


def test_a_clean_batch_writes_everything_and_says_nothing(scratch_crossborder_db, caplog):
    df = pd.DataFrame({
        "country_to": ["FR", "NL"],
        "timestamp_utc": pd.to_datetime(["2026-07-01T00:00:00Z", "2026-07-01T00:00:00Z"]),
        "flow_mw": [100.0, 10.0],
    })

    with caplog.at_level(logging.WARNING):
        written, _ = db.upsert_crossborder_flows(df, "BE")

    assert written == 2
    assert not [r for r in caplog.records if "Skipped" in r.message]


# ============================================================================
# DE's neighbour key
# ============================================================================

def test_de_uses_the_live_bidding_zone_not_the_2018_one():
    assert COUNTRY_TO_NEIGHBOURS_KEYS["DE"] == ["DE_LU"], (
        "DE_AT_LU ceased to exist in October 2018"
    )


def test_de_lu_covers_every_border_that_currently_returns_rows():
    # The no-regression property, measured against crossborder_flows on
    # 2026-08-06: DE lands rows for exactly BE, CH, CZ, FR, NL, PL. All six
    # must still be reachable under the new key or this "fix" loses data.
    from entsoe.mappings import NEIGHBOURS

    working = {"BE", "CH", "CZ", "FR", "NL", "PL"}
    assert working <= set(NEIGHBOURS["DE_LU"])


def test_de_lu_adds_at_and_no2_and_drops_austrias_borders():
    # Pins what the change actually does, because the ABL-29 audit described it
    # as "DE_LU is a subset" and as the cause of the missing DK/SE/NO borders,
    # and neither is right. DK_1/DK_2/SE_4 were ALREADY in DE_AT_LU and still
    # returned nothing -- see query_crossborder_all's note on the query domain.
    from entsoe.mappings import NEIGHBOURS

    old, new = set(NEIGHBOURS["DE_AT_LU"]), set(NEIGHBOURS["DE_LU"])

    assert new - old == {"AT", "NO_2"}
    # Italy and Slovenia are Austria's borders, inherited from the combined
    # zone. Germany does not touch either, so these were guaranteed-empty calls.
    assert old - new == {"IT_NORD", "IT_NORD_AT", "SI"}
    assert {"DK_1", "DK_2", "SE_4"} <= old & new
