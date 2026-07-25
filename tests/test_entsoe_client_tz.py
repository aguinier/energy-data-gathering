"""The crossborder / net-position queries must accept BOTH naive and tz-aware
datetimes.

Regression test for the 2026-07-25 production failure: the scheduled path
(`scripts/update.py` -> pipeline) passes tz-aware datetimes, which made
`pd.Timestamp(start, tz="UTC")` raise

    ValueError: Cannot pass a datetime or Timestamp with tzinfo with the tz
    parameter. Use tz_convert instead.

giving "crossborder_flows: 0 successful, 36 failed" on every cron run, while the
backfill (naive datetimes) worked. Every other query method in this client
already handles both; these two were the exceptions.
"""
from __future__ import annotations

import datetime as dt
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.entsoe_client import ENTSOEClient  # noqa: E402

UTC_AWARE_START = dt.datetime(2026, 7, 20, tzinfo=dt.timezone.utc)
UTC_AWARE_END = dt.datetime(2026, 7, 21, tzinfo=dt.timezone.utc)
NAIVE_START = dt.datetime(2026, 7, 20)
NAIVE_END = dt.datetime(2026, 7, 21)


class _StubInner:
    """Stands in for the entsoe-py client; records the timestamps it received."""

    def __init__(self):
        self.seen: dict[str, pd.Timestamp] = {}

    def query_net_position(self, country_code, start, end, dayahead=True):
        self.seen = {"start": start, "end": end, "country_code": country_code}
        idx = pd.date_range(start, periods=2, freq="h")
        return pd.Series([100.0, 200.0], index=idx)

    def query_physical_crossborder_allborders(self, country_code, start, end, export=True):
        self.seen = {"start": start, "end": end}
        idx = pd.date_range(start, periods=2, freq="h")
        return pd.DataFrame({"FR": [10.0, 20.0]}, index=idx)


@pytest.fixture
def client(monkeypatch):
    c = ENTSOEClient.__new__(ENTSOEClient)   # bypass __init__ (needs an API key)
    c.client = _StubInner()
    c._rate_limit = types.MethodType(lambda self: None, c)
    return c


@pytest.mark.parametrize("start,end", [
    pytest.param(UTC_AWARE_START, UTC_AWARE_END, id="tz-aware (scheduled path)"),
    pytest.param(NAIVE_START, NAIVE_END, id="naive (backfill path)"),
])
def test_net_position_accepts_naive_and_aware(client, start, end):
    series = client.query_net_position_data("BE", start, end)

    assert series is not None and not series.empty
    # Whatever came in, the inner client must receive UTC-aware timestamps.
    assert str(client.client.seen["start"].tz) == "UTC"


def test_net_position_maps_de_to_de_lu(client):
    """Germany's net position is published under the DE_LU bidding zone (which is
    also the Core CCR zone for DE+LU). Querying plain 'DE' returns
    NoMatchingDataError, which is why DE was the only Core country with no
    net_position data. Verified against the live API 2026-07-25:
    DE -> NoMatchingDataError, DE_LU -> 192 points.
    """
    client.query_net_position_data("DE", NAIVE_START, NAIVE_END)

    assert client.client.seen["country_code"] == "DE_LU"


def test_net_position_leaves_unmapped_zones_alone(client):
    """Only zones in the mapping are rewritten; everything else passes through."""
    client.query_net_position_data("BE", NAIVE_START, NAIVE_END)

    assert client.client.seen["country_code"] == "BE"


@pytest.mark.parametrize("start,end", [
    pytest.param(UTC_AWARE_START, UTC_AWARE_END, id="tz-aware (scheduled path)"),
    pytest.param(NAIVE_START, NAIVE_END, id="naive (backfill path)"),
])
def test_crossborder_accepts_naive_and_aware(client, start, end):
    # Only the timestamp coercion is under test; a neighbour-less code short-circuits
    # before the network call, so use BE which has neighbours in the mapping.
    result = client.query_crossborder_all("BE", start, end)

    # Must not raise. Data may legitimately be None, but the tz coercion must
    # have happened without error.
    assert result is None or isinstance(result, pd.DataFrame)
