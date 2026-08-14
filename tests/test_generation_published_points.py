"""ABL-268 -- the A75 generation path stored zeros it manufactured itself.

Third occurrence of the ABL-50 / ABL-55 mechanism. An ENTSO-E `Period`
declares a resolution over an interval, implying N positions, but may carry
fewer than N `Point` elements; entsoe-py 0.8.0 forward-fills the gap, and
`query_generation_and_renewable_with_metadata` stored the expansion verbatim
as `data_quality = 'actual'`.

Every XML fixture below is the shape of a document measured against the live
API on 2026-08-14, for the market day 2026-08-12:

  BE  Nuclear                 PT60M, 24 declared positions, ONE Point at 0.00
      -> entsoe-py returns 24 hourly rows, all 0.0, and we stored every one.
         23 of them are ours rather than Elia's. This is the acceptance case:
         1 value must survive, never 24. Note the rule is about provenance and
         not plausibility -- BE's reactors really were off that day, and the
         23 are refused regardless, because the document cannot tell us that
         and ES's genuinely-running coal fleet has the identical shape (58
         fabricated zeros the same day, against a measured 208 MW peak).

  BE  Biomass                 PT60M, 24 declared positions, 24 Points
      -> published in full, on the SAME document. This is what makes the
         per-sub-series rule necessary rather than cosmetic: a document-wide
         union of published positions (`published_timestamps`) reports all 24
         positions as published and leaves Nuclear's 23 invented zeros in
         place.

  AT  Waste                   PT15M, 96 declared positions, ONE Point at 100.0
      -> the A03 step function, the generation-side twin of PT's flat
         interconnector. Forward-filled and NOT zero, so untouched.

  AT  Hydro Pumped Storage    PT15M, both sides present, Consumption sparse
      -> the sides are keyed separately, so a sparse Consumption series cannot
         be rescued by a complete Aggregated one or vice versa.

  DE  Solar                   PT15M, explicit Points carrying 0.00 overnight
      -> genuinely published zeros. ABL-265 verified 56 of these are served,
         and the rule must keep every one: this is not a strictly-positive
         check, which is why ABL-113 stays in backlog.

The ABL-210 property has its own section at the bottom. That issue found 48%
of an approved delete enumeration had self-repaired within four hours, so
`absent` is a statement about one document at one moment and never a durable
fact. The guard is pure, holds no state, and declines to write rather than
recording an absence -- so the next fetch of the same window writes the real
value with no intervention.
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

DAY_START = "2026-08-12T00:00Z"
DAY_END = "2026-08-13T00:00Z"

AGGREGATED = "Actual Aggregated"
CONSUMPTION = "Actual Consumption"

# psrType codes, as they appear in a real A75 document.
PSR = {
    "Biomass": "B01",
    "Fossil Gas": "B04",
    "Fossil Hard coal": "B05",
    "Hydro Pumped Storage": "B10",
    "Hydro Run-of-river and poundage": "B11",
    "Nuclear": "B14",
    "Solar": "B16",
    "Waste": "B17",
    "Wind Onshore": "B19",
}


def _timeseries(mrid, production_type, points, side=AGGREGATED,
                resolution="PT60M", start=DAY_START, end=DAY_END,
                curve_type="A03", psr_code=None):
    """One TimeSeries, shaped like a real A75 GL_MarketDocument entry.

    The side is expressed the way ENTSO-E expresses it and the way entsoe-py
    reads it: an `outBiddingZone_Domain.mRID` marks the Consumption series,
    its absence marks Aggregated.
    """
    body = "".join(
        f"<Point><position>{position}</position><quantity>{quantity}</quantity></Point>"
        for position, quantity in points
    )
    zone = (
        "<outBiddingZone_Domain.mRID>10YBE----------2</outBiddingZone_Domain.mRID>"
        if side == CONSUMPTION
        else "<inBiddingZone_Domain.mRID>10YBE----------2</inBiddingZone_Domain.mRID>"
    )
    code = psr_code if psr_code is not None else PSR[production_type]
    return (
        f"<TimeSeries><mRID>{mrid}</mRID><businessType>A01</businessType>"
        f"{zone}"
        f"<MktPSRType><psrType>{code}</psrType></MktPSRType>"
        f"<curveType>{curve_type}</curveType>"
        f"<Period>"
        f"<timeInterval><start>{start}</start><end>{end}</end></timeInterval>"
        f"<resolution>{resolution}</resolution>{body}"
        f"</Period></TimeSeries>"
    )


def _document(*timeseries, namespace=NS):
    xmlns = f' xmlns="{namespace}"' if namespace else ""
    return (
        f'<?xml version="1.0" encoding="utf-8"?>'
        f"<GL_MarketDocument{xmlns}>"
        f"<mRID>abc</mRID><type>A75</type>"
        f"<createdDateTime>2026-08-14T06:00:00Z</createdDateTime>"
        # The document-level interval is `time_Period.timeInterval`, not
        # `Period` -- the parser must not mistake it for a period.
        f"<time_Period.timeInterval><start>{DAY_START}</start><end>{DAY_END}</end>"
        f"</time_Period.timeInterval>"
        + "".join(timeseries)
        + "</GL_MarketDocument>"
    )


def _forward_filled(points, count):
    """Expand published (position, quantity) pairs the way entsoe-py does.

    `series_parsers.py` reindexes onto the full declared range and `.ffill()`s,
    so a gap BEFORE the first published position stays NaN rather than being
    back-filled. Reproduced here so the fixture frames are what the real
    parser would have produced, not an idealisation of them.
    """
    by_position = dict(points)
    out, last = [], float("nan")
    for position in range(1, count + 1):
        if position in by_position:
            last = float(by_position[position])
        out.append(last)
    return out


def _frame(columns, resolution_minutes, count):
    """The MultiIndex frame entsoe-py builds for A75.

    Args:
        columns: {(production_type, side): [(position, quantity), ...]}
    """
    index = pd.date_range(DAY_START, periods=count, freq=f"{resolution_minutes}min", tz="UTC")
    return pd.DataFrame(
        {key: _forward_filled(points, count) for key, points in columns.items()},
        index=index,
        columns=pd.MultiIndex.from_tuples(list(columns)),
    )


# The BE 2026-08-12 shape: Nuclear one zero Point, Biomass published in full.
BE_XML = _document(
    _timeseries(1, "Nuclear", [(1, 0.0)]),
    _timeseries(2, "Biomass", [(p, 50.0 + p) for p in range(1, 25)]),
    _timeseries(3, "Fossil Gas", [(p, 3000.0 + p) for p in range(1, 25)]),
)
BE_FRAME = _frame(
    {
        ("Nuclear", AGGREGATED): [(1, 0.0)],
        ("Biomass", AGGREGATED): [(p, 50.0 + p) for p in range(1, 25)],
        ("Fossil Gas", AGGREGATED): [(p, 3000.0 + p) for p in range(1, 25)],
    },
    60, 24,
)


# ============================================================================
# published_timestamps_by_series
# ============================================================================

def test_each_production_type_gets_its_own_published_set():
    by_series = published_points.published_timestamps_by_series(BE_XML)

    assert set(by_series) == {
        ("Nuclear", AGGREGATED),
        ("Biomass", AGGREGATED),
        ("Fossil Gas", AGGREGATED),
    }
    assert by_series[("Nuclear", AGGREGATED)] == {pd.Timestamp(DAY_START)}
    assert len(by_series[("Biomass", AGGREGATED)]) == 24


def test_document_wide_union_would_have_missed_this():
    """The reason the per-series map exists, asserted rather than described.

    `published_timestamps` reports all 24 positions as published for the BE
    document, because Biomass published them. Judging Nuclear against that set
    leaves its 23 invented zeros in place.
    """
    assert len(published_points.published_timestamps(BE_XML)) == 24
    assert len(
        published_points.published_timestamps_by_series(BE_XML)[("Nuclear", AGGREGATED)]
    ) == 1


def test_the_two_sides_of_one_type_are_keyed_separately():
    xml = _document(
        _timeseries(1, "Hydro Pumped Storage", [(p, 100.0 + p) for p in range(1, 25)]),
        _timeseries(2, "Hydro Pumped Storage", [(1, 0.0)], side=CONSUMPTION),
    )
    by_series = published_points.published_timestamps_by_series(xml)

    assert len(by_series[("Hydro Pumped Storage", AGGREGATED)]) == 24
    assert by_series[("Hydro Pumped Storage", CONSUMPTION)] == {pd.Timestamp(DAY_START)}


def test_positions_are_unioned_across_timeseries_sharing_a_key():
    """entsoe-py concatenates same-named series into one column, so the
    column's published set is the union of its TimeSeries -- IT and GR return
    several periods per type this way."""
    xml = _document(
        _timeseries(1, "Solar", [(1, 0.0)], start=DAY_START, end="2026-08-12T12:00Z"),
        _timeseries(2, "Solar", [(1, 400.0)], start="2026-08-12T12:00Z", end=DAY_END),
    )
    assert published_points.published_timestamps_by_series(xml)[("Solar", AGGREGATED)] == {
        pd.Timestamp(DAY_START),
        pd.Timestamp("2026-08-12T12:00Z"),
    }


def test_an_unmapped_psr_code_falls_back_to_the_raw_code():
    """It then matches no frame column, so that series fails open rather than
    being judged against somebody else's published positions."""
    xml = _document(_timeseries(1, "Nuclear", [(1, 0.0)], psr_code="B99"))
    assert set(published_points.published_timestamps_by_series(xml)) == {("B99", AGGREGATED)}


@pytest.mark.parametrize("xml", [
    None,
    "",
    "not xml at all",
    '<GL_MarketDocument xmlns="%s"><mRID>abc</mRID></GL_MarketDocument>' % NS,
])
def test_unreadable_documents_return_none_not_an_empty_map(xml):
    """None means 'we cannot tell', and the caller keeps every value. An empty
    map would mean 'nothing was published anywhere', which would blank every
    zero in the document."""
    assert published_points.published_timestamps_by_series(xml) is None


def test_document_without_a_namespace_is_read_too():
    xml = _document(_timeseries(1, "Nuclear", [(1, 0.0)]), namespace="")
    assert published_points.published_timestamps_by_series(xml) == {
        ("Nuclear", AGGREGATED): {pd.Timestamp(DAY_START)}
    }


# ============================================================================
# blank_unpublished_zeros_by_series -- the rule
# ============================================================================

def test_be_nuclear_keeps_one_value_never_twentyfour():
    """The ABL-268 acceptance case."""
    kept, blanked = published_points.blank_unpublished_zeros_by_series(BE_FRAME, BE_XML)

    nuclear = kept[("Nuclear", AGGREGATED)]
    assert blanked == 23
    assert nuclear.notna().sum() == 1
    # The survivor is Elia's own published 0.0 -- kept here on purpose,
    # exactly as MK's published zero is kept on the load side.
    assert nuclear.iloc[0] == 0.0
    assert nuclear.iloc[1:].isna().all()


def test_the_rest_of_the_row_is_untouched():
    """The whole reason the unit is a cell: BE's genuine 3.3 GW of gas sits on
    the same 24 rows as its manufactured nuclear zero."""
    kept, _ = published_points.blank_unpublished_zeros_by_series(BE_FRAME, BE_XML)

    assert len(kept) == 24
    assert kept[("Fossil Gas", AGGREGATED)].notna().all()
    assert kept[("Biomass", AGGREGATED)].notna().all()
    pd.testing.assert_series_equal(
        kept[("Fossil Gas", AGGREGATED)], BE_FRAME[("Fossil Gas", AGGREGATED)]
    )


def test_a_forward_filled_nonzero_hold_is_untouched():
    """AT's Waste: one Point at 100 MW held across 96 positions. That is the
    document's own step function, the generation-side twin of PT's flat
    interconnector -- dropping it would delete a real reading."""
    xml = _document(_timeseries(1, "Waste", [(1, 100.0)], resolution="PT15M"))
    frame = _frame({("Waste", AGGREGATED): [(1, 100.0)]}, 15, 96)

    kept, blanked = published_points.blank_unpublished_zeros_by_series(frame, xml)

    assert blanked == 0
    assert (kept[("Waste", AGGREGATED)] == 100.0).all()


def test_published_zeros_survive():
    """NOT a strictly-positive check. DE publishes explicit 0.00 Points for
    solar overnight; ABL-265 verified 56 of them are served, and every one
    must survive. This is why ABL-113 stays in backlog."""
    overnight = [(p, 0.0) for p in range(1, 25)]
    daylight = [(p, float(p * 10)) for p in range(25, 97)]
    xml = _document(_timeseries(1, "Solar", overnight + daylight, resolution="PT15M"))
    frame = _frame({("Solar", AGGREGATED): overnight + daylight}, 15, 96)

    kept, blanked = published_points.blank_unpublished_zeros_by_series(frame, xml)

    assert blanked == 0
    assert (kept[("Solar", AGGREGATED)].iloc[:24] == 0.0).all()
    assert kept[("Solar", AGGREGATED)].notna().all()


def test_a_published_zero_next_to_an_invented_one_is_told_apart():
    """The distinction the whole issue turns on, in one series: position 1 is
    published as 0.0 and survives; positions 2-24 are ours and do not."""
    xml = _document(_timeseries(1, "Solar", [(1, 0.0), (24, 0.0)]))
    frame = _frame({("Solar", AGGREGATED): [(1, 0.0), (24, 0.0)]}, 60, 24)

    kept, blanked = published_points.blank_unpublished_zeros_by_series(frame, xml)

    solar = kept[("Solar", AGGREGATED)]
    assert blanked == 22
    assert solar.iloc[0] == 0.0
    assert solar.iloc[23] == 0.0
    assert solar.iloc[1:23].isna().all()


def test_a_sparse_consumption_side_is_judged_on_its_own_positions():
    """AT's pumped storage: the Aggregated side is published in full and the
    Consumption side is one zero Point. Keying the sides together would let
    the complete side vouch for the sparse one."""
    aggregated = [(p, 100.0 + p) for p in range(1, 25)]
    xml = _document(
        _timeseries(1, "Hydro Pumped Storage", aggregated),
        _timeseries(2, "Hydro Pumped Storage", [(1, 0.0)], side=CONSUMPTION),
    )
    frame = _frame(
        {
            ("Hydro Pumped Storage", AGGREGATED): aggregated,
            ("Hydro Pumped Storage", CONSUMPTION): [(1, 0.0)],
        },
        60, 24,
    )

    kept, blanked = published_points.blank_unpublished_zeros_by_series(frame, xml)

    assert blanked == 23
    assert kept[("Hydro Pumped Storage", AGGREGATED)].notna().all()
    assert kept[("Hydro Pumped Storage", CONSUMPTION)].notna().sum() == 1


def test_nan_stays_nan():
    """A leading gap is already unknown -- `NaN == 0` is False, so it is
    neither counted nor 'blanked' a second time."""
    xml = _document(_timeseries(1, "Solar", [(5, 0.0)]))
    frame = _frame({("Solar", AGGREGATED): [(5, 0.0)]}, 60, 24)
    assert frame[("Solar", AGGREGATED)].iloc[:4].isna().all()

    kept, blanked = published_points.blank_unpublished_zeros_by_series(frame, xml)

    assert blanked == 19  # positions 6..24, not the four leading NaNs
    assert kept[("Solar", AGGREGATED)].iloc[4] == 0.0


def test_the_input_frame_is_never_mutated():
    before = BE_FRAME.copy(deep=True)
    published_points.blank_unpublished_zeros_by_series(BE_FRAME, BE_XML)
    pd.testing.assert_frame_equal(BE_FRAME, before)


def test_a_single_series_document_with_a_flat_column_is_handled():
    """entsoe-py drops the redundant level when a document held one
    sub-series, so the column name no longer says which one. Recover it from
    the document -- but only when the document is unambiguous."""
    xml = _document(_timeseries(1, "Nuclear", [(1, 0.0)]))
    index = pd.date_range(DAY_START, periods=24, freq="60min", tz="UTC")
    frame = pd.DataFrame({"Nuclear": _forward_filled([(1, 0.0)], 24)}, index=index)

    kept, blanked = published_points.blank_unpublished_zeros_by_series(frame, xml)

    assert blanked == 23
    assert kept["Nuclear"].notna().sum() == 1


# ============================================================================
# blank_unpublished_zeros_by_series -- failing open
# ============================================================================

def test_an_unreadable_document_keeps_every_value(caplog):
    kept, blanked = published_points.blank_unpublished_zeros_by_series(
        BE_FRAME, "not xml at all", label="BE generation"
    )

    assert blanked == 0
    pd.testing.assert_frame_equal(kept, BE_FRAME)
    assert any(record.levelname == "WARNING" for record in caplog.records)


def test_a_column_the_document_does_not_describe_keeps_every_value(caplog):
    """A parsing disagreement is not evidence of fabrication."""
    frame = BE_FRAME.copy()
    frame[("Marine", AGGREGATED)] = 0.0

    kept, blanked = published_points.blank_unpublished_zeros_by_series(
        frame, BE_XML, label="BE generation"
    )

    assert (kept[("Marine", AGGREGATED)] == 0.0).all()
    assert blanked == 23  # Nuclear only
    assert any("Marine" in record.message for record in caplog.records)


def test_a_grid_that_matches_nothing_keeps_that_column(caplog):
    """If our position->timestamp grid disagrees with the frame entsoe-py
    built, that is a parsing fault, not 100% fabrication. Dropping on it would
    be exactly the confidently-wrong move this module exists to prevent."""
    shifted = BE_FRAME.copy()
    shifted.index = shifted.index + pd.Timedelta(minutes=7)

    kept, blanked = published_points.blank_unpublished_zeros_by_series(
        shifted, BE_XML, label="BE generation"
    )

    assert blanked == 0
    pd.testing.assert_frame_equal(kept, shifted)
    assert any("matches a published position" in record.message for record in caplog.records)


def test_a_non_numeric_column_is_left_alone_not_coerced_to_nan():
    """The guard coerces to numeric to compare against 0.0, and assigns the
    coerced Series back. A non-numeric column must never reach that
    assignment, or guarding the frame would destroy it."""
    frame = BE_FRAME.copy()
    frame[("Nuclear", "note")] = "reported late"

    kept, blanked = published_points.blank_unpublished_zeros_by_series(frame, BE_XML)

    assert blanked == 23
    assert (kept[("Nuclear", "note")] == "reported late").all()


@pytest.mark.parametrize("frame", [None, pd.DataFrame()])
def test_empty_input_is_returned_unchanged(frame):
    kept, blanked = published_points.blank_unpublished_zeros_by_series(frame, BE_XML)
    assert blanked == 0
    if frame is None:
        assert kept is None


def test_nothing_to_blank_returns_the_original_object():
    xml = _document(_timeseries(1, "Biomass", [(p, 50.0 + p) for p in range(1, 25)]))
    frame = _frame({("Biomass", AGGREGATED): [(p, 50.0 + p) for p in range(1, 25)]}, 60, 24)

    kept, blanked = published_points.blank_unpublished_zeros_by_series(frame, xml)

    assert blanked == 0
    assert kept is frame


# ============================================================================
# ABL-210: absent now is not absent forever
# ============================================================================
#
# ABL-182 enumerated 392 forward-filled zero-load rows for deletion on the
# rule "no <Point> at this position in ENTSO-E's current documents => we
# manufactured this row". A pre-flight re-enumeration four minutes after the
# Board approved the delete found 189 of them -- 48%, all of LU -- already
# rewritten with real values of 393-567 MW by an ordinary ingest pass. They
# were not garbage; they were placeholders, and the pipeline healed them by
# itself.
#
# So `absent` is a statement about one document at one moment. The guard is
# built so that it can never harden into a durable one.

def test_absent_now_is_not_absent_forever():
    """The same window, refetched after ENTSO-E filled the positions in, must
    store the real values. Nothing carries the earlier refusal forward."""
    sparse_xml = _document(_timeseries(1, "Nuclear", [(1, 0.0)]))
    sparse_frame = _frame({("Nuclear", AGGREGATED): [(1, 0.0)]}, 60, 24)

    first, blanked = published_points.blank_unpublished_zeros_by_series(
        sparse_frame, sparse_xml
    )
    assert blanked == 23
    assert first[("Nuclear", AGGREGATED)].notna().sum() == 1

    # Four hours later the TSO has published the day. Identical call site,
    # identical window, no state to reset, no flag to clear.
    complete = [(p, 4000.0 + p) for p in range(1, 25)]
    complete_xml = _document(_timeseries(1, "Nuclear", complete))
    complete_frame = _frame({("Nuclear", AGGREGATED): complete}, 60, 24)

    second, blanked_again = published_points.blank_unpublished_zeros_by_series(
        complete_frame, complete_xml
    )
    assert blanked_again == 0
    assert second[("Nuclear", AGGREGATED)].notna().all()
    assert second[("Nuclear", AGGREGATED)].iloc[23] == 4024.0


def test_a_repaired_position_is_repaired_even_when_the_repair_is_a_zero():
    """The healing case that a strictly-positive check could not express: the
    refetch publishes an explicit 0.0, which is now data and must be stored."""
    xml = _document(_timeseries(1, "Nuclear", [(p, 0.0) for p in range(1, 25)]))
    frame = _frame({("Nuclear", AGGREGATED): [(p, 0.0) for p in range(1, 25)]}, 60, 24)

    kept, blanked = published_points.blank_unpublished_zeros_by_series(frame, xml)

    assert blanked == 0
    assert (kept[("Nuclear", AGGREGATED)] == 0.0).all()


def test_the_guard_is_pure_and_repeatable():
    """No memo, no cache, no 'we already refused this position' record --
    which is what a durable `absent` verdict would need somewhere to live."""
    first, first_count = published_points.blank_unpublished_zeros_by_series(BE_FRAME, BE_XML)
    second, second_count = published_points.blank_unpublished_zeros_by_series(BE_FRAME, BE_XML)

    assert first_count == second_count == 23
    pd.testing.assert_frame_equal(first, second)


# ============================================================================
# The wired path: one A75 fetch, two frames, one guarded document
# ============================================================================

class _StubRawClient:
    def __init__(self, xml):
        self._xml = xml

    def query_generation(self, entsoe_domain, start, end, psr_type=None):
        return self._xml


class _StubPandasClient:
    def __init__(self, df):
        self._df = df

    def query_generation(self, entsoe_domain, start, end, psr_type=None):
        return self._df


@pytest.fixture
def be_client(monkeypatch):
    """The real `query_generation_and_renewable_with_metadata` path, with the
    network and the DB stubbed -- not a hand-built frame handed straight to
    the guard."""
    client = ENTSOEClient.__new__(ENTSOEClient)
    client.raw_client = _StubRawClient(BE_SOLAR_XML)
    client.client = _StubPandasClient(BE_SOLAR_FRAME)
    client._rate_limit = types.MethodType(lambda self: None, client)
    monkeypatch.setattr(
        client, "_get_country_domain", lambda country_code: {"entsoe_domain": "BE"}
    )
    return client


# Nuclear invented, Solar published in full (including a genuine overnight
# zero), so the two output frames can be compared on both kinds of value.
_SOLAR_POINTS = [(p, 0.0) for p in range(1, 7)] + [(p, float(p) * 100) for p in range(7, 25)]
BE_SOLAR_XML = _document(
    _timeseries(1, "Nuclear", [(1, 0.0)]),
    _timeseries(2, "Solar", _SOLAR_POINTS),
    _timeseries(3, "Wind Onshore", [(p, 200.0 + p) for p in range(1, 25)]),
)
BE_SOLAR_FRAME = _frame(
    {
        ("Nuclear", AGGREGATED): [(1, 0.0)],
        ("Solar", AGGREGATED): _SOLAR_POINTS,
        ("Wind Onshore", AGGREGATED): [(p, 200.0 + p) for p in range(1, 25)],
    },
    60, 24,
)


def test_generation_frame_stores_null_where_the_value_was_invented(be_client):
    generation, _renewable, _published = be_client.query_generation_and_renewable_with_metadata(
        "BE", pd.Timestamp(DAY_START), pd.Timestamp(DAY_END)
    )

    nuclear = generation.set_index("timestamp_utc")["nuclear_mw"]
    assert nuclear.notna().sum() == 1
    assert nuclear.iloc[0] == 0.0
    assert nuclear.iloc[1:].isna().all()

    # The genuine overnight solar zeros are still zeros, not NULLs.
    solar = generation.set_index("timestamp_utc")["solar_mw"]
    assert (solar.iloc[:6] == 0.0).all()
    assert generation["wind_onshore_mw"].notna().all()


def test_energy_renewable_output_is_byte_identical(be_client, monkeypatch):
    """The frozen table is deliberately unchanged by this guard.

    `_map_renewable_columns` initialises to 0.0 and `fillna(0)`s what it maps,
    so a blanked cell reaches `energy_renewable` as the same 0.0 it held
    before. Pinned here because the alternative -- teaching a table with
    `DEFAULT 0` on every column to emit NULL mid-life -- would leave one
    condition encoded two ways for a table whose remaining consumer trains
    models on it. Changing that is its owner's decision, and this assertion is
    where it will announce itself.
    """
    _, guarded_renewable, _ = be_client.query_generation_and_renewable_with_metadata(
        "BE", pd.Timestamp(DAY_START), pd.Timestamp(DAY_END)
    )

    monkeypatch.setattr(
        published_points,
        "blank_unpublished_zeros_by_series",
        lambda df, xml, label="": (df, 0),
    )
    _, unguarded_renewable, _ = be_client.query_generation_and_renewable_with_metadata(
        "BE", pd.Timestamp(DAY_START), pd.Timestamp(DAY_END)
    )

    pd.testing.assert_frame_equal(guarded_renewable, unguarded_renewable)
