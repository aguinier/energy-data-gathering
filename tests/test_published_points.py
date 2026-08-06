"""ABL-50 -- the ingest stored load rows it manufactured itself.

An ENTSO-E `Period` declares a resolution over a time interval, implying N
positions, but may carry fewer than N `Point` elements. entsoe-py 0.8.0
forward-fills the gap, and `query_load_with_metadata` stored the expansion
verbatim as `data_quality = 'actual'`.

Every XML fixture below is the shape of a document measured against the live
API on 2026-08-06, reduced to the elements the parser reads:

  MK  PT60M, 24 declared positions, ONE Point: position 1, quantity 0.0
      -> entsoe-py returns 24 rows, all 0.0. We stored 24 rows of a national
         demand of 0 MW, 23 of which were ours. This is the acceptance case:
         it must yield 1 stored row, never 24.

  AL  PT15M, 96 declared positions, hourly Points at 1, 5, 9, ...
      -> three of every four AL rows are forward-filled copies of a real
         reading. Under the rule approved on ABL-50 they all survive, because
         the fill value is not zero. This test PINS that, so the deferred
         follow-up (drop every forward-filled position; AL 96 -> 24 rows/day)
         lands as a deliberate diff rather than as an accident.

  DE  PT15M, 96 declared positions, 96 Points -- the ordinary case, where
      nothing is filled and nothing may be dropped.

All three documents carry `curveType=A03`, measured the same day: a Point's
value holds until the next Point, so the forward fill implements the document
rather than inventing a value at random. MK's document really does assert 0 MW
across the whole day. It is still not 24 measurements, and a grid never draws
0 MW -- so we keep the position MEPSO published (the dashboard's read-side
guard from ABL-35 withholds it downstream) and refuse the 23 we extrapolated.
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


NS = "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"

WINDOW_START = "2026-08-01T22:00Z"
WINDOW_END = "2026-08-02T22:00Z"


def _document(resolution, points, start=WINDOW_START, end=WINDOW_END, namespace=NS,
              curve_type="A03"):
    """One TimeSeries / one Period, shaped like a real GL_MarketDocument."""
    xmlns = f' xmlns="{namespace}"' if namespace else ""
    body = "".join(
        f"<Point><position>{position}</position><quantity>{quantity}</quantity></Point>"
        for position, quantity in points
    )
    return (
        f'<?xml version="1.0" encoding="utf-8"?>'
        f"<GL_MarketDocument{xmlns}>"
        f"<mRID>abc</mRID><type>A65</type>"
        f"<createdDateTime>2026-08-06T16:44:47Z</createdDateTime>"
        # The document-level interval is `time_Period.timeInterval`, not
        # `Period` -- the parser must not mistake it for a period.
        f"<time_Period.timeInterval><start>{start}</start><end>{end}</end>"
        f"</time_Period.timeInterval>"
        f"<TimeSeries><mRID>1</mRID><businessType>A04</businessType>"
        f"<curveType>{curve_type}</curveType>"
        f"<Period>"
        f"<timeInterval><start>{start}</start><end>{end}</end></timeInterval>"
        f"<resolution>{resolution}</resolution>{body}"
        f"</Period></TimeSeries></GL_MarketDocument>"
    )


MK_XML = _document("PT60M", [(1, 0)])
AL_XML = _document("PT15M", [(position, quantity) for position, quantity in zip(
    range(1, 96, 4),
    [960, 867, 808, 771, 755, 759, 807, 876, 931, 980, 1010, 1030,
     1041, 1035, 1020, 1004, 1002, 1039, 1104, 1150, 1247, 1247, 1083],
)])
DE_XML = _document("PT15M", [(position, 40000 + position) for position in range(1, 97)])


def _frame(xml, resolution_minutes, count, values):
    """The frame entsoe-py builds from `xml`: every declared position present,
    gaps filled forward from the last published value."""
    index = pd.date_range(WINDOW_START, periods=count, freq=f"{resolution_minutes}min", tz="UTC")
    return pd.DataFrame({"timestamp_utc": index, "load_mw": values})


def _forward_filled(points, count):
    """Expand published (position, quantity) pairs across `count` positions."""
    by_position = dict(points)
    out, last = [], None
    for position in range(1, count + 1):
        last = by_position.get(position, last)
        out.append(float(last))
    return out


MK_FRAME = _frame(MK_XML, 60, 24, _forward_filled([(1, 0)], 24))
AL_POINTS = [(position, quantity) for position, quantity in zip(
    range(1, 96, 4),
    [960, 867, 808, 771, 755, 759, 807, 876, 931, 980, 1010, 1030,
     1041, 1035, 1020, 1004, 1002, 1039, 1104, 1150, 1247, 1247, 1083],
)]
AL_FRAME = _frame(AL_XML, 15, 96, _forward_filled(AL_POINTS, 96))
DE_FRAME = _frame(DE_XML, 15, 96, _forward_filled([(p, 40000 + p) for p in range(1, 97)], 96))


# ============================================================================
# parse_resolution
# ============================================================================

@pytest.mark.parametrize("text,expected", [
    ("PT15M", pd.Timedelta(minutes=15)),
    ("PT30M", pd.Timedelta(minutes=30)),
    ("PT60M", pd.Timedelta(hours=1)),
    ("PT1M", pd.Timedelta(minutes=1)),
    ("PT24H", pd.Timedelta(days=1)),
    ("P1D", pd.Timedelta(days=1)),
    ("P7D", pd.Timedelta(days=7)),
    (" PT60M ", pd.Timedelta(hours=1)),
])
def test_parse_resolution_accepts_entsoe_durations(text, expected):
    assert published_points.parse_resolution(text) == expected


@pytest.mark.parametrize("text", [None, "", "PT0M", "P", "PT", "nonsense", "15M"])
def test_parse_resolution_rejects_the_rest(text):
    """Anything unrecognised or non-positive must read as 'cannot judge this
    period', which makes the caller keep every row."""
    assert published_points.parse_resolution(text) is None


# ============================================================================
# published_timestamps
# ============================================================================

def test_mk_publishes_exactly_one_position():
    stamps = published_points.published_timestamps(MK_XML)
    assert stamps == {pd.Timestamp(WINDOW_START)}


def test_al_publishes_the_hourly_grid_inside_a_quarter_hourly_envelope():
    stamps = published_points.published_timestamps(AL_XML)

    assert len(stamps) == 23
    # Positions 1, 5, 9, ... step by four quarter-hours -- i.e. every hour.
    assert sorted(stamps) == list(
        pd.date_range(WINDOW_START, periods=23, freq="h", tz="UTC")
    )


def test_de_publishes_every_declared_position():
    assert len(published_points.published_timestamps(DE_XML)) == 96


def test_positions_are_unioned_across_periods_and_timeseries():
    """A window spanning several days arrives as several Periods; each maps
    its own positions from its own start."""
    xml = (
        '<GL_MarketDocument xmlns="%s">'
        "<TimeSeries><Period>"
        "<timeInterval><start>2026-08-01T22:00Z</start><end>2026-08-02T22:00Z</end></timeInterval>"
        "<resolution>PT60M</resolution>"
        "<Point><position>1</position><quantity>100</quantity></Point>"
        "</Period></TimeSeries>"
        "<TimeSeries><Period>"
        "<timeInterval><start>2026-08-02T22:00Z</start><end>2026-08-03T22:00Z</end></timeInterval>"
        "<resolution>PT60M</resolution>"
        "<Point><position>3</position><quantity>200</quantity></Point>"
        "</Period></TimeSeries>"
        "</GL_MarketDocument>"
    ) % NS

    assert published_points.published_timestamps(xml) == {
        pd.Timestamp("2026-08-01T22:00Z"),
        pd.Timestamp("2026-08-03T00:00Z"),
    }


def test_document_without_a_namespace_is_read_too():
    assert published_points.published_timestamps(
        _document("PT60M", [(1, 0)], namespace="")
    ) == {pd.Timestamp(WINDOW_START)}


@pytest.mark.parametrize("xml", [
    None,
    "",
    "not xml at all",
    '<GL_MarketDocument xmlns="%s"><mRID>abc</mRID></GL_MarketDocument>' % NS,
])
def test_unreadable_documents_return_none_not_an_empty_set(xml):
    """None means 'we cannot tell', and the caller keeps every row. An empty
    set would mean 'nothing was published', which would drop every zero."""
    assert published_points.published_timestamps(xml) is None


def test_period_with_an_unparseable_resolution_is_skipped_not_guessed():
    assert published_points.published_timestamps(
        _document("PT0M", [(1, 0)])
    ) is None


# ============================================================================
# drop_unpublished_zeros -- the rule
# ============================================================================

def test_mk_shape_stores_one_row_never_twentyfour():
    """The ABL-50 acceptance case."""
    kept, dropped = published_points.drop_unpublished_zeros(MK_FRAME, MK_XML, "load_mw")

    assert dropped == 23
    assert len(kept) == 1
    assert kept["timestamp_utc"].iloc[0] == pd.Timestamp(WINDOW_START)
    # The survivor is MEPSO's own published 0.0 -- kept here on purpose, and
    # withheld from the dashboard by measuredLoadClause() instead.
    assert kept["load_mw"].iloc[0] == 0.0


def test_al_shape_is_untouched_by_this_rule():
    """PINNED: AL's 96 rows/day survive today. The follow-up that takes AL to
    its true hourly resolution must change this assertion deliberately."""
    kept, dropped = published_points.drop_unpublished_zeros(AL_FRAME, AL_XML, "load_mw")

    assert dropped == 0
    assert len(kept) == 96
    assert kept.equals(AL_FRAME)


def test_de_shape_is_untouched():
    kept, dropped = published_points.drop_unpublished_zeros(DE_FRAME, DE_XML, "load_mw")

    assert dropped == 0
    assert len(kept) == 96


def test_a_published_zero_is_kept_even_where_a_filled_zero_is_dropped():
    """Both halves of the rule in one document: position 1 is a published 0.0
    and stays; positions 2-4 are filled 0.0 and go."""
    xml = _document("PT60M", [(1, 0)], end="2026-08-02T02:00Z")
    frame = _frame(xml, 60, 4, [0.0, 0.0, 0.0, 0.0])

    kept, dropped = published_points.drop_unpublished_zeros(frame, xml, "load_mw")

    assert dropped == 3
    assert list(kept["timestamp_utc"]) == [pd.Timestamp(WINDOW_START)]


def test_unpublished_nonzero_fill_is_kept():
    """Unpublished is not sufficient -- that is what leaves AL alone."""
    xml = _document("PT60M", [(1, 500)], end="2026-08-02T02:00Z")
    frame = _frame(xml, 60, 4, [500.0, 500.0, 500.0, 500.0])

    kept, dropped = published_points.drop_unpublished_zeros(frame, xml, "load_mw")

    assert dropped == 0
    assert len(kept) == 4


def test_nan_is_not_zero():
    """NaN == 0 is False, so an unknown stays unknown rather than being
    swept up as a fabricated zero."""
    xml = _document("PT60M", [(1, 500)], end="2026-08-02T02:00Z")
    frame = _frame(xml, 60, 4, [500.0, float("nan"), float("nan"), 0.0])

    kept, dropped = published_points.drop_unpublished_zeros(frame, xml, "load_mw")

    assert dropped == 1
    assert len(kept) == 3
    assert kept["load_mw"].isna().sum() == 2


def test_negative_values_are_untouched():
    """The rule is exactly zero, not a magnitude floor -- same reasoning as
    ABL-35's read-side guard."""
    xml = _document("PT60M", [(1, -20)], end="2026-08-02T02:00Z")
    frame = _frame(xml, 60, 4, [-20.0, -20.0, -20.0, -20.0])

    kept, dropped = published_points.drop_unpublished_zeros(frame, xml, "load_mw")

    assert dropped == 0
    assert len(kept) == 4


# ============================================================================
# drop_unpublished_zeros -- failing open
# ============================================================================

@pytest.mark.parametrize("xml", [None, "", "not xml at all"])
def test_unreadable_xml_keeps_every_row(xml):
    kept, dropped = published_points.drop_unpublished_zeros(MK_FRAME, xml, "load_mw")

    assert dropped == 0
    assert kept.equals(MK_FRAME)


def test_grid_mismatch_keeps_every_row():
    """If our position->timestamp arithmetic matches nothing in the frame, the
    parse is wrong, not the data. Dropping every zero on that basis would be
    the confidently-wrong move this rule exists to prevent."""
    xml = _document("PT60M", [(1, 0)], start="2020-01-01T00:00Z", end="2020-01-02T00:00Z")

    kept, dropped = published_points.drop_unpublished_zeros(MK_FRAME, xml, "load_mw")

    assert dropped == 0
    assert kept.equals(MK_FRAME)


def test_empty_and_none_frames_are_returned_as_given():
    empty = MK_FRAME.iloc[0:0]

    assert published_points.drop_unpublished_zeros(empty, MK_XML, "load_mw")[1] == 0
    assert published_points.drop_unpublished_zeros(None, MK_XML, "load_mw") == (None, 0)


# ============================================================================
# End to end through ENTSOEClient.query_load_with_metadata
# ============================================================================

class _StubRawClient:
    def __init__(self, xml):
        self._xml = xml
        self.calls = []

    def query_load(self, country_code, start, end):
        self.calls.append({"country_code": country_code, "start": start, "end": end})
        return self._xml


class _StubPandasClient:
    def __init__(self, frame):
        self._frame = frame
        self.calls = []

    def query_load(self, country_code, start, end):
        self.calls.append({"country_code": country_code, "start": start, "end": end})
        series = self._frame.set_index("timestamp_utc")["load_mw"]
        series.index.name = None
        return series


def _client(xml, frame):
    c = ENTSOEClient.__new__(ENTSOEClient)  # bypass __init__ (needs an API key)
    c.raw_client = _StubRawClient(xml)
    c.client = _StubPandasClient(frame)
    c._rate_limit = types.MethodType(lambda self: None, c)
    c._get_country_domain = types.MethodType(
        lambda self, country_code: {"entsoe_domain": "10YMK-MEPSO----8"}, c
    )
    return c


def test_query_load_with_metadata_stores_one_mk_row():
    """End to end on the real code path: the MK document must not produce 24
    stored rows of 0 MW."""
    client = _client(MK_XML, MK_FRAME)

    df, publication_time = client.query_load_with_metadata(
        "MK", pd.Timestamp(WINDOW_START), pd.Timestamp(WINDOW_END)
    )

    assert len(df) == 1
    assert df["load_mw"].iloc[0] == 0.0
    assert publication_time is not None


def test_query_load_with_metadata_leaves_al_at_ninetysix_rows():
    client = _client(AL_XML, AL_FRAME)

    df, _ = client.query_load_with_metadata(
        "AL", pd.Timestamp(WINDOW_START), pd.Timestamp(WINDOW_END)
    )

    assert len(df) == 96


def test_query_load_with_metadata_leaves_de_untouched():
    client = _client(DE_XML, DE_FRAME)

    df, _ = client.query_load_with_metadata(
        "DE", pd.Timestamp(WINDOW_START), pd.Timestamp(WINDOW_END)
    )

    assert len(df) == 96


def test_only_one_upstream_request_per_leg():
    """The one-fetch-per-country-per-window invariant: the raw XML this rule
    reads is the document already downloaded for the publication timestamp.
    No extra request may appear."""
    client = _client(MK_XML, MK_FRAME)

    client.query_load_with_metadata(
        "MK", pd.Timestamp(WINDOW_START), pd.Timestamp(WINDOW_END)
    )

    assert len(client.raw_client.calls) == 1
    assert len(client.client.calls) == 1
