"""net_position.publication_timestamp_utc was NULL across every one of the
~640,000 existing rows -- nothing recorded when ENTSO-E published each value.
That mattered less while the table only held realized data; it matters now
that the pipeline also fetches the D+1 day-ahead net position (net_position
is flagged `is_dayahead: True` in config.py), because the table now mixes
"published yesterday for today" with "published today for tomorrow" with no
way to tell them apart.

This file exercises the three pieces that close the gap:
  1. ENTSOEClient.query_net_position_data_with_metadata -- the raw-XML-then-
     parsed-frame pattern already used by query_load_with_metadata /
     query_day_ahead_prices_with_metadata / query_load_forecast_with_metadata /
     query_wind_solar_forecast_with_metadata / query_generation_per_type_with_metadata,
     applied to net position.
  2. db.upsert_net_position accepting and persisting publication_timestamp.
  3. fetch_net_position.fetch_net_position_data threading the metadata
     method's publication_time into the upsert.

The original query_net_position_data (no metadata) is left untouched --
tests/test_entsoe_client_tz.py's stub client only sets `c.client`, not
`c.raw_client`, so a change to the base method's request shape would have
broken that regression test. The new metadata variant sits alongside it,
following the same "keep the base, add a _with_metadata sibling" shape as
every other query method in this client.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: E402
from src import db  # noqa: E402
from src import fetch_net_position  # noqa: E402
from src.entsoe_client import ENTSOEClient  # noqa: E402


# ============================================================================
# ENTSOEClient.query_net_position_data_with_metadata
# ============================================================================

class _StubRawClient:
    """Stands in for entsoe.EntsoeRawClient -- only query_net_position is
    used, and only to source the publication timestamp."""

    def __init__(self, created="2026-07-25T10:00:00Z"):
        self.calls = []
        self._created = created

    def query_net_position(self, country_code, start, end, dayahead=True):
        self.calls.append({"country_code": country_code, "start": start, "end": end, "dayahead": dayahead})
        return (
            "<Publication_MarketDocument>"
            f"<createdDateTime>{self._created}</createdDateTime>"
            "</Publication_MarketDocument>"
        )


class _StubPandasClient:
    """Stands in for entsoe.EntsoePandasClient."""

    def __init__(self, series):
        self.calls = []
        self._series = series

    def query_net_position(self, country_code, start, end, dayahead=True):
        self.calls.append({"country_code": country_code, "start": start, "end": end, "dayahead": dayahead})
        return self._series


def _series(periods=2):
    idx = pd.date_range("2026-07-25T00:00:00Z", periods=periods, freq="h")
    return pd.Series([100.0, 200.0][:periods], index=idx)


@pytest.fixture
def client():
    c = ENTSOEClient.__new__(ENTSOEClient)  # bypass __init__ (needs an API key)
    c.raw_client = _StubRawClient()
    c.client = _StubPandasClient(_series())
    c._rate_limit = types.MethodType(lambda self: None, c)
    return c


def test_returns_series_and_publication_timestamp(client):
    series, pub_time = client.query_net_position_data_with_metadata(
        "BE", pd.Timestamp("2026-07-25T00:00:00Z"), pd.Timestamp("2026-07-25T02:00:00Z")
    )

    assert series is not None and not series.empty
    assert pub_time is not None
    assert pub_time.isoformat() == "2026-07-25T10:00:00+00:00"


def test_maps_de_to_de_lu_on_both_calls(client):
    """DE_LU rewrite must apply consistently to both the raw-XML call and the
    parsed-frame call -- if only one branch remapped the zone, the two
    requests would silently disagree about what they're asking ENTSO-E for."""
    client.query_net_position_data_with_metadata(
        "DE", pd.Timestamp("2026-07-25T00:00:00Z"), pd.Timestamp("2026-07-25T02:00:00Z")
    )

    assert client.raw_client.calls[0]["country_code"] == "DE_LU"
    assert client.client.calls[0]["country_code"] == "DE_LU"


def test_dayahead_flag_forwarded_to_both_calls(client):
    client.query_net_position_data_with_metadata(
        "BE", pd.Timestamp("2026-07-25T00:00:00Z"), pd.Timestamp("2026-07-25T02:00:00Z"),
        dayahead=True,
    )

    assert client.raw_client.calls[0]["dayahead"] is True
    assert client.client.calls[0]["dayahead"] is True


def test_no_data_returns_none_none():
    class _EmptyPandasClient:
        def query_net_position(self, country_code, start, end, dayahead=True):
            return pd.Series([], dtype=float)

    c = ENTSOEClient.__new__(ENTSOEClient)
    c.raw_client = _StubRawClient()
    c.client = _EmptyPandasClient()
    c._rate_limit = types.MethodType(lambda self: None, c)

    series, pub_time = c.query_net_position_data_with_metadata(
        "BE", pd.Timestamp("2026-07-25T00:00:00Z"), pd.Timestamp("2026-07-25T02:00:00Z")
    )
    assert series is None
    assert pub_time is None


def test_raw_client_no_matching_data_returns_none_none():
    """If ENTSO-E has nothing to publish, the raw-XML leg raises
    NoMatchingDataError (wrapped by _make_request into ENTSOENoDataError) --
    this must surface as (None, None), not propagate and crash the pipeline."""
    from entsoe.exceptions import NoMatchingDataError

    class _NoDataRawClient:
        def query_net_position(self, country_code, start, end, dayahead=True):
            raise NoMatchingDataError("no data")

    c = ENTSOEClient.__new__(ENTSOEClient)
    c.raw_client = _NoDataRawClient()
    c.client = _StubPandasClient(_series())
    c._rate_limit = types.MethodType(lambda self: None, c)

    series, pub_time = c.query_net_position_data_with_metadata(
        "BE", pd.Timestamp("2026-07-25T00:00:00Z"), pd.Timestamp("2026-07-25T02:00:00Z")
    )
    assert series is None
    assert pub_time is None


def test_accepts_naive_and_tz_aware_datetimes(client):
    """Regression guard matching test_entsoe_client_tz.py's coverage of the
    base method: the scheduled path passes tz-aware datetimes, the backfill
    path passes naive ones -- both must work."""
    import datetime as dt

    naive_start, naive_end = dt.datetime(2026, 7, 25), dt.datetime(2026, 7, 26)
    aware_start = dt.datetime(2026, 7, 25, tzinfo=dt.timezone.utc)
    aware_end = dt.datetime(2026, 7, 26, tzinfo=dt.timezone.utc)

    for start, end in [(naive_start, naive_end), (aware_start, aware_end)]:
        series, pub_time = client.query_net_position_data_with_metadata("BE", start, end)
        assert series is not None
        assert pub_time is not None


def test_exactly_two_requests_per_call(client):
    """The trade-off this feature accepts: query_net_position_data (the base
    method, untouched) issues exactly 1 HTTP request per call today, because
    EntsoePandasClient.query_net_position makes its own single raw request
    internally. query_net_position_data_with_metadata issues 2 -- one
    explicit raw_client call for createdDateTime, one pandas_client call for
    the parsed series -- exactly matching the established, already-accepted
    cost of every sibling *_with_metadata method (query_load_with_metadata,
    query_day_ahead_prices_with_metadata, query_load_forecast_with_metadata,
    query_wind_solar_forecast_with_metadata, query_generation_per_type_with_metadata).
    Nothing here should call raw_client or client more than once each."""
    calls = []
    real_make_request = client._make_request

    def _spy(method, *args, **kwargs):
        calls.append(getattr(method, "__self__", None))
        return real_make_request(method, *args, **kwargs)

    client._make_request = _spy

    series, pub_time = client.query_net_position_data_with_metadata(
        "BE", pd.Timestamp("2026-07-25T00:00:00Z"), pd.Timestamp("2026-07-25T02:00:00Z")
    )

    assert series is not None
    assert len(calls) == 2, f"expected exactly 2 _make_request calls, got {len(calls)}"
    assert calls[0] is client.raw_client
    assert calls[1] is client.client


# ============================================================================
# db.upsert_net_position persists publication_timestamp
# ============================================================================

@pytest.fixture
def scratch_net_position_db(tmp_path, monkeypatch):
    """A fresh, throwaway SQLite file with the net_position schema applied --
    never the read-only replica or prod. Points db.get_connection() at this
    file only for the duration of the test."""
    scratch_path = tmp_path / "scratch_net_position.db"
    monkeypatch.setattr(config, "DATABASE_PATH", scratch_path)
    db.create_net_position_table()
    return scratch_path


def test_upsert_net_position_persists_publication_timestamp(scratch_net_position_db):
    df = pd.DataFrame({
        "timestamp_utc": [pd.Timestamp("2026-07-25T00:00:00Z"), pd.Timestamp("2026-07-25T01:00:00Z")],
        "net_position_mw": [123.4, -56.7],
    })

    inserted, _ = db.upsert_net_position(
        df, "BE", publication_timestamp=pd.Timestamp("2026-07-25T10:00:00Z")
    )
    assert inserted == 2

    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT timestamp_utc, net_position_mw, publication_timestamp_utc "
            "FROM net_position WHERE country_code = 'BE' ORDER BY timestamp_utc"
        )
        rows = [dict(r) for r in cursor.fetchall()]

    assert len(rows) == 2
    for row in rows:
        assert row["publication_timestamp_utc"] is not None
        assert "2026-07-25" in row["publication_timestamp_utc"] and "10:00:00" in row["publication_timestamp_utc"]


def test_upsert_net_position_without_publication_timestamp_stays_null(scratch_net_position_db):
    """Backward compatibility: publication_timestamp is optional, and omitting
    it must not raise -- existing callers (if any survive) keep working."""
    df = pd.DataFrame({
        "timestamp_utc": [pd.Timestamp("2026-07-25T00:00:00Z")],
        "net_position_mw": [42.0],
    })

    inserted, _ = db.upsert_net_position(df, "BE")
    assert inserted == 1

    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT publication_timestamp_utc FROM net_position WHERE country_code = 'BE'"
        )
        row = cursor.fetchone()

    assert row["publication_timestamp_utc"] is None


def test_upsert_net_position_upserts_on_conflict_not_duplicates(scratch_net_position_db):
    ts = pd.Timestamp("2026-07-25T00:00:00Z")

    db.upsert_net_position(
        pd.DataFrame({"timestamp_utc": [ts], "net_position_mw": [100.0]}),
        "DE", publication_timestamp=pd.Timestamp("2026-07-24T10:00:00Z"),
    )
    db.upsert_net_position(
        pd.DataFrame({"timestamp_utc": [ts], "net_position_mw": [200.0]}),
        "DE", publication_timestamp=pd.Timestamp("2026-07-25T10:00:00Z"),
    )

    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) as n, MAX(net_position_mw) as mw, MAX(publication_timestamp_utc) as pub "
            "FROM net_position WHERE country_code = 'DE'"
        )
        row = cursor.fetchone()

    assert row["n"] == 1, "conflicting upsert must update in place, not duplicate"
    assert row["mw"] == 200.0
    assert "2026-07-25" in row["pub"] and "10:00:00" in row["pub"]


# ============================================================================
# fetch_net_position.fetch_net_position_data threads publication_time through
# ============================================================================

def test_fetch_net_position_data_passes_publication_time_to_upsert(monkeypatch):
    series = _series()
    publication_time = pd.Timestamp("2026-07-25T10:00:00Z")

    mock_client = MagicMock()
    mock_client.query_net_position_data_with_metadata.return_value = (series, publication_time)

    monkeypatch.setattr(db, "create_net_position_table", MagicMock())
    monkeypatch.setattr(db, "upsert_net_position", MagicMock(return_value=(2, 0)))

    inserted, updated, failed = fetch_net_position.fetch_net_position_data(
        mock_client, "BE",
        pd.Timestamp("2026-07-25T00:00:00Z"), pd.Timestamp("2026-07-25T02:00:00Z"),
    )

    assert failed == 0
    assert inserted == 2

    mock_client.query_net_position_data_with_metadata.assert_called_once()
    call_args = mock_client.query_net_position_data_with_metadata.call_args
    assert call_args[0][0] == "BE"
    assert call_args[1]["dayahead"] is True

    db.upsert_net_position.assert_called_once()
    upsert_args = db.upsert_net_position.call_args
    assert upsert_args[0][1] == "BE"
    assert upsert_args[1]["publication_timestamp"] == publication_time


def test_fetch_net_position_data_skips_lu_without_calling_client(monkeypatch):
    """ABL-35 defect 4: LU and DE both resolve to the DE_LU bidding zone
    (NET_POSITION_BIDDING_ZONES), so a separate LU fetch would write a
    byte-identical duplicate of whatever DE already wrote, double-counting DE
    in every per-country net_position aggregate. Board-approved fix
    (confirmation 820fa10c, accepted 2026-08-11): skip LU entirely, before any
    API call -- not merely dedupe after fetching."""
    from src.entsoe_client import ENTSOEClient

    mock_client = MagicMock()
    mock_client.NET_POSITION_DUPLICATE_ZONE_COUNTRIES = ENTSOEClient.NET_POSITION_DUPLICATE_ZONE_COUNTRIES
    mock_client.NET_POSITION_BIDDING_ZONES = ENTSOEClient.NET_POSITION_BIDDING_ZONES

    upsert_mock = MagicMock()
    monkeypatch.setattr(db, "create_net_position_table", MagicMock())
    monkeypatch.setattr(db, "upsert_net_position", upsert_mock)

    inserted, updated, failed = fetch_net_position.fetch_net_position_data(
        mock_client, "LU",
        pd.Timestamp("2026-07-25T00:00:00Z"), pd.Timestamp("2026-07-25T02:00:00Z"),
    )

    assert (inserted, updated, failed) == (0, 0, 0)
    mock_client.query_net_position_data_with_metadata.assert_not_called()
    assert not upsert_mock.called


def test_fetch_net_position_data_still_fetches_de(monkeypatch):
    """DE must be unaffected by the LU skip -- DE is the country whose fetch
    has to keep happening, since it's the one that actually writes the
    DE_LU zone's series."""
    series = _series()
    publication_time = pd.Timestamp("2026-07-25T10:00:00Z")

    mock_client = MagicMock()
    mock_client.NET_POSITION_DUPLICATE_ZONE_COUNTRIES = {"LU"}
    mock_client.query_net_position_data_with_metadata.return_value = (series, publication_time)

    monkeypatch.setattr(db, "create_net_position_table", MagicMock())
    monkeypatch.setattr(db, "upsert_net_position", MagicMock(return_value=(2, 0)))

    inserted, updated, failed = fetch_net_position.fetch_net_position_data(
        mock_client, "DE",
        pd.Timestamp("2026-07-25T00:00:00Z"), pd.Timestamp("2026-07-25T02:00:00Z"),
    )

    assert (inserted, updated, failed) == (2, 0, 0)
    mock_client.query_net_position_data_with_metadata.assert_called_once()
    db.upsert_net_position.assert_called_once()


def test_fetch_net_position_data_no_series_skips_upsert(monkeypatch):
    mock_client = MagicMock()
    mock_client.query_net_position_data_with_metadata.return_value = (None, None)

    monkeypatch.setattr(db, "create_net_position_table", MagicMock())
    upsert_mock = MagicMock()
    monkeypatch.setattr(db, "upsert_net_position", upsert_mock)

    inserted, updated, failed = fetch_net_position.fetch_net_position_data(
        mock_client, "BE",
        pd.Timestamp("2026-07-25T00:00:00Z"), pd.Timestamp("2026-07-25T02:00:00Z"),
    )

    assert (inserted, updated, failed) == (0, 0, 0)
    assert not upsert_mock.called


# ============================================================================
# scripts/backfill_publication_timestamps.py: net_position wiring, resumability,
# and no-data-vs-failed handling
# ============================================================================
#
# ENTSOEClient() requires a live API key (raises ValueError without one, see
# entsoe_client.py's __init__) -- this workstation has none, so every test
# below monkeypatches the ENTSOEClient name inside the backfill module to a
# stub factory rather than constructing a real client.

import scripts.backfill_publication_timestamps as backfill_pub  # noqa: E402


class _StubMetadataClient:
    """Stands in for ENTSOEClient inside backfill_table -- only the one
    method backfill_net_position_timestamps calls is implemented."""

    def __init__(self, responder):
        self._responder = responder
        self.calls = []

    def query_net_position_data_with_metadata(self, country_code, start, end, dayahead=True):
        self.calls.append((country_code, start, end, dayahead))
        return self._responder(country_code, start, end)


@pytest.fixture
def scratch_backfill_db(tmp_path, monkeypatch):
    scratch_path = tmp_path / "scratch_backfill.db"
    monkeypatch.setattr(config, "DATABASE_PATH", scratch_path)
    db.create_net_position_table()
    return scratch_path


def _seed_null_pub_time_rows(country_code, start, hours):
    """Insert net_position rows with NULL publication_timestamp_utc, the
    exact shape ~640,000 existing production rows are in today."""
    df = pd.DataFrame({
        "timestamp_utc": pd.date_range(start, periods=hours, freq="h"),
        "net_position_mw": [10.0 * i for i in range(hours)],
    })
    db.upsert_net_position(df, country_code)  # no publication_timestamp -> NULL


def test_backfill_net_position_fills_missing_timestamps(scratch_backfill_db, monkeypatch):
    _seed_null_pub_time_rows("BE", "2026-07-20T00:00:00Z", 24)

    pub_time = pd.Timestamp("2026-07-19T10:00:00Z")
    stub_client = _StubMetadataClient(
        lambda cc, s, e: (_series(periods=2), pub_time)
    )
    monkeypatch.setattr(backfill_pub, "ENTSOEClient", lambda: stub_client)

    stats = backfill_pub.backfill_table(
        table="net_position", country_code="BE",
        start_date="2026-07-20", end_date="2026-07-20",
    )

    assert stats["total_updated"] == 24

    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) as n FROM net_position "
            "WHERE country_code = 'BE' AND publication_timestamp_utc IS NULL"
        )
        assert cursor.fetchone()["n"] == 0


def test_backfill_net_position_is_resumable(scratch_backfill_db, monkeypatch):
    """Core resumability contract: once a range's rows are filled, a second
    run must not re-select that range (auto-detect finds nothing left with
    publication_timestamp_utc IS NULL), and must not call ENTSO-E again for
    it -- exactly what makes re-running the same command after an interrupted
    backfill safe."""
    _seed_null_pub_time_rows("BE", "2026-07-20T00:00:00Z", 24)

    pub_time = pd.Timestamp("2026-07-19T10:00:00Z")
    stub_client = _StubMetadataClient(
        lambda cc, s, e: (_series(periods=2), pub_time)
    )
    monkeypatch.setattr(backfill_pub, "ENTSOEClient", lambda: stub_client)

    # First run: auto-detect (no explicit start/end) picks up the seeded gap.
    first_stats = backfill_pub.backfill_table(table="net_position", country_code="BE")
    assert first_stats["total_updated"] == 24
    assert len(stub_client.calls) >= 1

    calls_after_first_run = len(stub_client.calls)

    # Second run: auto-detect must find nothing left to do.
    second_stats = backfill_pub.backfill_table(table="net_position", country_code="BE")
    assert second_stats["total_updated"] == 0
    assert second_stats["ranges_processed"] == 0
    assert len(stub_client.calls) == calls_after_first_run, (
        "resumable backfill must not re-query ENTSO-E for a range that is "
        "already fully backfilled"
    )


def test_backfill_net_position_no_data_is_not_counted_as_failure(scratch_backfill_db, monkeypatch, caplog):
    """Distinguish 'no data' from 'failed': when ENTSO-E legitimately has
    nothing (client returns (None, None), the same signal the metadata method
    uses for both no-XML-match and empty-series), the run must complete
    without raising, log a WARNING (not ERROR), and simply not update rows --
    it must not be indistinguishable from a real fetch/network failure."""
    _seed_null_pub_time_rows("BE", "2026-07-20T00:00:00Z", 24)

    stub_client = _StubMetadataClient(lambda cc, s, e: (None, None))
    monkeypatch.setattr(backfill_pub, "ENTSOEClient", lambda: stub_client)

    with caplog.at_level("WARNING"):
        stats = backfill_pub.backfill_table(
            table="net_position", country_code="BE",
            start_date="2026-07-20", end_date="2026-07-20",
        )

    assert stats["total_updated"] == 0
    assert any(
        "No data or publication time returned" in r.message and r.levelname == "WARNING"
        for r in caplog.records
    )
    assert not any(r.levelname == "ERROR" for r in caplog.records)

    # Rows are still NULL -- nothing was written for a no-data window, and a
    # future run (once ENTSO-E actually has data) can still pick them up.
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) as n FROM net_position "
            "WHERE country_code = 'BE' AND publication_timestamp_utc IS NULL"
        )
        assert cursor.fetchone()["n"] == 24


def test_backfill_net_position_failure_is_logged_at_error_not_swallowed_as_no_data(
    scratch_backfill_db, monkeypatch, caplog
):
    """The other half of the no-data/failed distinction: a genuine exception
    (network, auth, malformed response) must be visible at ERROR level, not
    silently treated the same as a legitimate no-data response."""
    _seed_null_pub_time_rows("BE", "2026-07-20T00:00:00Z", 24)

    class _RaisingClient:
        def query_net_position_data_with_metadata(self, country_code, start, end, dayahead=True):
            raise ConnectionError("simulated network failure")

    monkeypatch.setattr(backfill_pub, "ENTSOEClient", lambda: _RaisingClient())

    with caplog.at_level("WARNING"):
        stats = backfill_pub.backfill_table(
            table="net_position", country_code="BE",
            start_date="2026-07-20", end_date="2026-07-20",
        )

    assert stats["total_updated"] == 0
    assert any(
        "Error backfilling net position timestamps" in r.message and r.levelname == "ERROR"
        for r in caplog.records
    ), "a genuine failure must be logged at ERROR, not silently swallowed as no-data"


def test_net_position_included_in_table_choices_and_all():
    """Wiring check: net_position must be selectable on its own and included
    when --table all runs every table, matching load/price/renewable/load_forecast.

    The argparse choices list and the 'all' expansion are read directly out of
    main()'s source (main() itself parses sys.argv, so it isn't invoked here)
    -- a source-level assertion that the wiring wasn't missed."""
    import inspect
    source = inspect.getsource(backfill_pub)
    assert "choices=['load', 'price', 'renewable', 'load_forecast', 'net_position', 'all']" in source
    assert "tables = ['load', 'price', 'renewable', 'load_forecast', 'net_position']" in source
