"""ABL-55 -- the net-position ingest stored rows it manufactured itself.

Second occurrence of the ABL-50 defect, in a second table. An ENTSO-E `Period`
declares a resolution over a time interval, implying N positions, but may carry
fewer than N `Point` elements; entsoe-py 0.8.0 forward-fills the gap and
`query_net_position_data_with_metadata` stored the expansion verbatim.

Every XML fixture below is the shape of a document measured against the live
API on 2026-08-07, reduced to the elements the parser reads. All carry
`curveType=A03`.

  GR  2026-07-23T22:00Z..2026-07-24T22:00Z, PT60M, 24 declared positions,
      ONE Point: position 1, quantity 0.
      -> entsoe-py returns 24 rows, every one 0.0, and we stored all 24 as a
         realized day-ahead net position of 0 MW -- while GR's own
         `crossborder_flows` show a median net export of 1,142 MW over the same
         hours. 192 GR rows and 24 IE rows in `net_position` came from this.
         This is the acceptance case: it must yield 1 row, never 24.

  IE  2026-07-24, PT60M, 24 declared positions, 24 Points, first 610.2 MW --
      the ordinary case, on the same zone and the same day GR was fabricated.
      Nothing is filled and nothing may be dropped.

  PT  2026-02-18, PT15M, a Period declaring 18 positions and carrying TWO
      Points, the first 500 MW. This one is the regression pin. The first
      proposal for this fix was to keep only genuinely published Points, and
      this document is why that was rejected: PT's interconnector sits at
      exactly 500 MW for hours and the document encodes the hold as a single
      Point. Measured the same day, PT 2026-02-18 also had a Period declaring
      47 positions with 7 Points, and ES 2026-02-08 one declaring 112 with 51.
      Dropping every forward-filled position would have deleted more than half
      of PT's and ES's genuine rows.

The zero half of the rule is what separates GR's manufactured day from PT's
flat interconnector: both are forward-filled, only one is exactly 0.0.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import published_points  # noqa: E402
from src.entsoe_client import ENTSOEClient  # noqa: E402


# A25 is a Publication_MarketDocument -- a different root and namespace from
# the GL_MarketDocument the load path reads. The parser derives the namespace
# from the root tag, and this fixture is what proves it rather than assumes it.
NS = "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"

GR_START = "2026-07-23T22:00Z"
GR_END = "2026-07-24T22:00Z"


def _a25(resolution, points, start=GR_START, end=GR_END, curve_type="A03"):
    """One TimeSeries / one Period, shaped like a real A25 document."""
    body = "".join(
        f"<Point><position>{position}</position><quantity>{quantity}</quantity></Point>"
        for position, quantity in points
    )
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<Publication_MarketDocument xmlns="{NS}">'
        f"<mRID>b0cb4fdeb98d48f99064cb5be8be2fd4</mRID>"
        f"<revisionNumber>1</revisionNumber><type>A25</type>"
        f"<createdDateTime>2026-08-07T05:55:08Z</createdDateTime>"
        # The document-level interval is `period.timeInterval`, NOT `Period`.
        # A parser that matched it loosely would read the whole document as one
        # period and get every position wrong.
        f"<period.timeInterval><start>{start}</start><end>{end}</end>"
        f"</period.timeInterval>"
        f"<TimeSeries><mRID>1</mRID><auction.type>A01</auction.type>"
        f"<businessType>B09</businessType><curveType>{curve_type}</curveType>"
        f"<Period>"
        f"<timeInterval><start>{start}</start><end>{end}</end></timeInterval>"
        f"<resolution>{resolution}</resolution>{body}"
        f"</Period></TimeSeries></Publication_MarketDocument>"
    )


def _forward_filled(points, count):
    """Expand published (position, quantity) pairs across `count` positions."""
    by_position = dict(points)
    out, last = [], None
    for position in range(1, count + 1):
        last = by_position.get(position, last)
        out.append(float(last))
    return out


def _series(start, resolution_minutes, values, tz="UTC"):
    index = pd.date_range(
        start, periods=len(values), freq=f"{resolution_minutes}min", tz="UTC"
    ).tz_convert(tz)
    return pd.Series(values, index=index)


GR_POINTS = [(1, 0)]
GR_XML = _a25("PT60M", GR_POINTS)
GR_SERIES = _series(GR_START, 60, _forward_filled(GR_POINTS, 24))

IE_POINTS = [(position, 610.2 + position) for position in range(1, 25)]
IE_XML = _a25("PT60M", IE_POINTS)
IE_SERIES = _series(GR_START, 60, _forward_filled(IE_POINTS, 24))

PT_START = "2026-02-18T17:15Z"
PT_END = "2026-02-18T21:45Z"
PT_POINTS = [(1, 500), (12, 500)]
PT_XML = _a25("PT15M", PT_POINTS, start=PT_START, end=PT_END)
PT_SERIES = _series(PT_START, 15, _forward_filled(PT_POINTS, 18))


# ============================================================================
# The rule, in the Series shape the net-position path uses
# ============================================================================

def test_gr_single_point_day_yields_one_row_never_twentyfour():
    """The acceptance case. A document asserting 0 MW from one Point across 24
    declared hours must store the one position ENTSO-E published, not 24."""
    kept, dropped = published_points.drop_unpublished_zeros_series(
        GR_SERIES, GR_XML, label="GR net position"
    )

    assert dropped == 23
    assert len(kept) == 1
    assert kept.iloc[0] == 0.0
    assert kept.index[0] == pd.Timestamp(GR_START)


def test_ie_fully_published_day_is_untouched():
    """Same zone shape, same day, real data: nothing may be dropped."""
    kept, dropped = published_points.drop_unpublished_zeros_series(
        IE_SERIES, IE_XML, label="IE net position"
    )

    assert dropped == 0
    assert kept.equals(IE_SERIES)


def test_pt_flat_interconnector_is_untouched():
    """The regression pin for the rejected stricter rule. 16 of these 18 rows
    are forward-filled, and all 18 must survive -- PT really is holding at
    500 MW, and the document encodes the hold as a single Point."""
    kept, dropped = published_points.drop_unpublished_zeros_series(
        PT_SERIES, PT_XML, label="PT net position"
    )

    assert dropped == 0
    assert len(kept) == 18
    assert (kept == 500.0).all()


def test_a_published_zero_survives_a_fabricated_one():
    """A genuinely published 0.0 is the TSO's number and is stored; only the
    positions we forward-filled to 0.0 are refused. This is what separates the
    rule from a point-level `== 0` filter, which would punch holes in PL."""
    points = [(1, 0), (5, 0), (9, 250)]
    xml = _a25("PT60M", points)
    series = _series(GR_START, 60, _forward_filled(points, 12))

    kept, dropped = published_points.drop_unpublished_zeros_series(series, xml)

    # positions 2,3,4 and 6,7,8 are filled zeros; 1 and 5 are published zeros
    assert dropped == 6
    assert len(kept) == 6
    assert list(kept.values) == [0.0, 0.0, 250.0, 250.0, 250.0, 250.0]


def test_negative_net_position_is_never_touched():
    """A net importer is negative, legitimately, and `-0.0 == 0.0` is True in
    IEEE 754 -- so a filled -0.0 is a fabricated zero and drops, but a real
    negative value never does."""
    points = [(1, -1688.2)]
    xml = _a25("PT60M", points)
    series = _series(GR_START, 60, _forward_filled(points, 24))

    kept, dropped = published_points.drop_unpublished_zeros_series(series, xml)

    assert dropped == 0
    assert len(kept) == 24


def test_index_is_normalised_to_utc_not_assumed_to_be_utc():
    """entsoe-py hands back a tz-aware index in the zone's local time. The
    published-position grid is UTC, so a rule that compared them naively would
    match nothing and silently fail open on every Greek document."""
    local = GR_SERIES.tz_convert("Europe/Athens")

    kept, dropped = published_points.drop_unpublished_zeros_series(local, GR_XML)

    assert dropped == 23
    assert kept.index[0] == pd.Timestamp(GR_START)


def test_naive_index_is_treated_as_utc():
    naive = GR_SERIES.tz_localize(None)

    kept, dropped = published_points.drop_unpublished_zeros_series(naive, GR_XML)

    assert dropped == 23
    assert len(kept) == 1


def test_series_name_and_index_name_survive():
    named = GR_SERIES.rename("net_position_mw")
    named.index.name = "timestamp_utc"

    kept, _ = published_points.drop_unpublished_zeros_series(named, GR_XML)

    assert kept.name == "net_position_mw"
    assert kept.index.name == "timestamp_utc"


@pytest.mark.parametrize("xml", [None, "", "not xml at all"])
def test_unreadable_xml_keeps_every_row(xml):
    kept, dropped = published_points.drop_unpublished_zeros_series(GR_SERIES, xml)

    assert dropped == 0
    assert kept.equals(GR_SERIES)


def test_grid_mismatch_keeps_every_row():
    """If our position->timestamp arithmetic matches nothing in the series, the
    parse is wrong, not the data."""
    xml = _a25("PT60M", [(1, 0)], start="2020-01-01T00:00Z", end="2020-01-02T00:00Z")

    kept, dropped = published_points.drop_unpublished_zeros_series(GR_SERIES, xml)

    assert dropped == 0
    assert kept.equals(GR_SERIES)


def test_empty_and_none_series_are_returned_as_given():
    empty = GR_SERIES.iloc[0:0]

    assert published_points.drop_unpublished_zeros_series(empty, GR_XML)[1] == 0
    assert published_points.drop_unpublished_zeros_series(None, GR_XML) == (None, 0)


# ============================================================================
# End to end through ENTSOEClient.query_net_position_data_with_metadata
# ============================================================================

class _StubRawClient:
    def __init__(self, xml):
        self._xml = xml
        self.calls = []

    def query_net_position(self, zone, start, end, dayahead):
        self.calls.append({"zone": zone, "start": start, "end": end})
        return self._xml


class _StubPandasClient:
    def __init__(self, series):
        self._series = series
        self.calls = []

    def query_net_position(self, zone, start, end, dayahead):
        self.calls.append({"zone": zone, "start": start, "end": end})
        return self._series


def _client(xml, series):
    c = ENTSOEClient.__new__(ENTSOEClient)  # bypass __init__ (needs an API key)
    c.raw_client = _StubRawClient(xml)
    c.client = _StubPandasClient(series)
    c._rate_limit = types.MethodType(lambda self: None, c)
    return c


def test_query_net_position_with_metadata_returns_one_gr_row():
    """End to end on the real code path: GR's single-point document must not
    produce 24 stored rows of 0 MW."""
    client = _client(GR_XML, GR_SERIES)

    series, publication_time = client.query_net_position_data_with_metadata(
        "GR", pd.Timestamp(GR_START), pd.Timestamp(GR_END)
    )

    assert len(series) == 1
    assert series.iloc[0] == 0.0
    assert publication_time is not None


def test_query_net_position_with_metadata_leaves_ie_untouched():
    client = _client(IE_XML, IE_SERIES)

    series, _ = client.query_net_position_data_with_metadata(
        "IE", pd.Timestamp(GR_START), pd.Timestamp(GR_END)
    )

    assert len(series) == 24


def test_only_one_upstream_request_per_leg():
    """The raw XML this rule reads is the document already downloaded for the
    publication timestamp. No extra request may appear."""
    client = _client(GR_XML, GR_SERIES)

    client.query_net_position_data_with_metadata(
        "GR", pd.Timestamp(GR_START), pd.Timestamp(GR_END)
    )

    assert len(client.raw_client.calls) == 1
    assert len(client.client.calls) == 1
