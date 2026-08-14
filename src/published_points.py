"""
Which positions did ENTSO-E actually publish?

An ENTSO-E `Period` declares a `resolution` and a `timeInterval`, which
together imply N positions, but it is free to carry fewer than N `Point`
elements. entsoe-py 0.8.0 expands such a period by forward-filling the last
published value across every position it did not receive. We used to store
that expansion verbatim into `energy_load` with `data_quality = 'actual'`.

Measured against the live API on 2026-08-06, MK's document for
`2026-08-01T22:00Z .. 2026-08-02T22:00Z` declares `resolution=PT60M` over 24
hourly positions and carries **one** Point -- `position 1, quantity 0.0`.
entsoe-py returns 24 rows, every one `0.0`, and we stored all 24 as a measured
national demand of 0 MW. A national grid never draws 0 MW; 23 of those rows
were ours, not MEPSO's. That is the upstream source of the impossible-zero
load rows the dashboard withholds at read time (ABL-35) -- this module is the
writer-side half (ABL-50).

**On curveType (measured, and it matters for the follow-up).** Every load
document sampled on 2026-08-06 -- MK, AL and DE alike -- carries
`curveType=A03`, "variable sized block", whose contract is that a Point's
value holds until the next Point. So entsoe-py's forward fill is *implementing
the document*, not inventing a value at random: MK's document does assert 0 MW
across the whole day. It is still not 24 measurements, and it is still
impossible, which is why we keep the one position MEPSO published and drop the
23 we extrapolated -- the read-side guard then withholds the survivor
downstream. But do not carry the phrase "positions ENTSO-E never published"
into the general case without re-reading this paragraph: under A03 an absent
position is an encoded repeat, not an unknown. That distinction is the open
question on the deferred follow-up (drop *all* forward-filled positions, which
would take AL from 96 rows/day to its true 24), and it is why this module
deliberately does not branch on curveType today.

The rule implemented here is the narrow one approved on ABL-50: drop a row
only when it is **both** a position the document carried no Point for **and**
exactly `0.0`. Both halves are load-bearing.

- **Unpublished alone is not enough.** AL publishes hourly values inside a
  `PT15M` envelope -- raw positions 1, 5, 9, 13, ... -- so three of every four
  AL rows are forward-filled copies of a real reading. Those are the
  document's own step function and are left exactly as they were.
- **Zero alone is not enough.** MK's `position 1` really was published as
  `0.0`. However implausible, that is MEPSO's number and not ours, so it is
  stored; `measuredLoadClause()` in the dashboard is what keeps it off a
  chart. Only the rows we manufactured are refused here.

Everything fails open. If the XML cannot be parsed, or declares no usable
period, or lands on a grid that matches none of the returned rows, every row
is kept and a warning is logged -- a parser that silently deleted real
readings would be a worse defect than the one it fixes.

**Second occurrence: net position (ABL-55).** The same mechanism ran in the
A25 day-ahead net-position path. Measured against the live API on 2026-08-07,
GR's document for `2026-07-23T22:00Z .. 2026-07-24T22:00Z` declares
`resolution=PT60M` over 24 hourly positions and carries **one** Point --
`position 1, quantity 0`. We stored a full day of measured-looking 0.0 MW,
while GR's own crossborder flows show a median net export of 1,142 MW over
those hours. 192 GR rows and 24 IE rows in `net_position` were manufactured
this way. `drop_unpublished_zeros_series` is the same rule in the shape that
path uses, so both tables are now governed by this one module rather than by
two reimplementations.

**Why net position is NOT filtered down to "only the published Points".**
That stricter rule was the first proposal, and the live API refutes it. A25
documents are routinely and legitimately sparse under A03 -- measured
2026-08-07, all `curveType=A03`:

    PT 2026-02-18  Period declaring 47 positions, carrying  7 Points
    PT 2026-02-18  Period declaring 18 positions, carrying  2 Points
    ES 2026-02-08  Period declaring 112 positions, carrying 51 Points
    FI 2026-02-01  Period declaring 112 positions, carrying 109 Points
    BE 2026-02-01  Period declaring 112 positions, carrying 112 Points

PT's interconnector sits at exactly 500 MW or 1500 MW for hours at a time and
the document encodes that as one Point plus a hold. Dropping every
forward-filled position would delete more than half of PT's and ES's genuine
rows -- real readings, silently removed, which is the failure this module
exists to prevent. The zero half of the rule is what separates GR's
manufactured day from PT's flat interconnector: both are forward-filled, only
one is exactly 0.0.

**Third occurrence: generation (ABL-268).** The A75 actual-generation path ran
the same mechanism, unguarded, on every country every day. Measured against the
live API on 2026-08-14 for the market day 2026-08-12:

    BE  Nuclear                declaring 24 positions, carrying  1 Point at 0.00
    BE  Hydro Run-of-river     declaring 24 positions, carrying  1 Point at 0.00
    AT  Fossil Hard coal       declaring 96 positions, carrying  1 Point at 0.00
    AT  Fossil Oil             declaring 96 positions, carrying  1 Point at 0.00
    ES  Fossil Hard coal       declaring 96 positions, carrying 26 Points

**The Spanish coal row is the one to read.** ES burns hard coal -- 126,379 of
its 160,198 stored readings are positive, and it reached 208 MW on that very
day. Its document published 26 of 96 positions, so 58 quarter-hours of a
running coal fleet were stored as a measured `0.0` that Red Electrica never
published. That is a wrong number on a live chart, not a bookkeeping nicety.

**The Belgian nuclear row is the one to reason from.** We stored 24 hourly rows
of `nuclear_mw = 0.0`, and 23 of them are ours rather than Elia's -- but BE's
nuclear fleet really did shut down on 2026-04-04 (2,078 MW through January,
last non-trivial reading 2026-04-04 01:00, exactly 0.0 at every hour since), so
the underlying reality here is very likely zero. The rule refuses those 23
anyway, because it is about provenance and not plausibility. We cannot tell
from the document which of the two cases we are in; we could not have told in
April either; and a pipeline that decides which zeros to believe by how
plausible they look is the one that wrote GR a year of measured-looking 0.0 MW
net position. Refuse every position nobody published, keep the one they did,
whatever its value, and let the read side judge what survives.

**Why the A75 unit is a cell, not a row.** `energy_load` and `net_position`
each have one value column, so the row *is* the value and refusing the value
means refusing the row. An A75 row carries up to 21 production types measured
independently, and Belgium's genuine 3.3 GW of gas sits on the same row as its
manufactured nuclear zero. Dropping that row to refuse the nuclear zero would
delete twenty real readings to suppress one invented one -- the exact trade
this module exists to refuse. So `blank_unpublished_zeros_by_series` writes
NaN into the individual cell, which `_map_generation_columns` already carries
through to SQL NULL. "Write no row" and "write no value" are the same rule
resolved at the granularity the table actually has.

**Why the published set must be per sub-series and not per document.**
`published_timestamps` unions every Point in the document, which is right for a
single-series document and wrong for A75: Belgium's Biomass publishes all 24
positions, so a document-wide union reports every position as published and
Nuclear's 23 invented zeros survive untouched. `published_timestamps_by_series`
keys the set on `(production type, 'Actual Aggregated' | 'Actual Consumption')`
-- entsoe-py's own column identity, taken from the same two elements its parser
reads (`series_parsers.py`'s position arithmetic and `parsers.py`'s
`CONSUMPTION_ELEMENT` test) -- and unions across every TimeSeries sharing a
key, matching the concat-and-deduplicate entsoe-py performs on same-named
series.

**`energy_renewable` is deliberately unchanged by this.**
`_map_renewable_columns` initialises every column to 0.0 and `fillna(0)`s what
it maps, so a cell blanked here reaches that frozen table as the same 0.0 it
held before -- byte-identical output, pinned by a test. That is not an
oversight. The frozen table has `DEFAULT 0` on every value column and has never
been able to express "not reported"; teaching it mid-life would leave one
condition encoded two ways, with 485,069 older `wind_offshore_mw` rows saying
0.0 and newer ones saying NULL, for a table whose remaining consumer trains
models on it. That is a contract change for its owner to make deliberately, not
a side effect of a guard.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from typing import Dict, Optional, Set, Tuple

import pandas as pd

try:  # entsoe-py is a hard dependency of every caller; degrade, never raise.
    from entsoe.mappings import PSRTYPE_MAPPINGS
except Exception:  # pragma: no cover - only reachable on a broken install
    PSRTYPE_MAPPINGS = {}


logger = logging.getLogger('entsoe_pipeline')


# ISO 8601 durations as they appear in ENTSO-E `resolution` elements:
# PT15M, PT30M, PT60M, PT1M, PT24H, P1D, P7D.
_DURATION_RE = re.compile(
    r'^P(?:(?P<days>\d+)D)?'
    r'(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$'
)


def parse_resolution(text: Optional[str]) -> Optional[pd.Timedelta]:
    """
    Parse an ENTSO-E `<resolution>` into a Timedelta.

    Returns None for anything unrecognised or non-positive, which the callers
    treat as "cannot judge this period" rather than as an error.
    """
    if not text:
        return None

    match = _DURATION_RE.match(text.strip())
    if not match:
        return None

    parts = {key: int(value) for key, value in match.groupdict().items() if value}
    if not parts:
        return None

    step = pd.Timedelta(**parts)
    return step if step > pd.Timedelta(0) else None


def _parse_document(xml_response: Optional[str]) -> Optional[Tuple[ET.Element, str]]:
    """The document root and its namespace prefix, or None if unreadable."""
    if not xml_response:
        return None
    try:
        root = ET.fromstring(xml_response)
    except ET.ParseError as exc:
        logger.warning(f"Could not parse ENTSO-E XML for published positions: {exc}")
        return None
    return root, (root.tag.split('}')[0] + '}' if '}' in root.tag else '')


def _period_stamps(period: ET.Element, namespace: str) -> Optional[Set[pd.Timestamp]]:
    """
    The UTC timestamps one `<Period>` carries an explicit `<Point>` for.

    Position n maps to `period_start + (n - 1) * resolution` -- the same
    arithmetic entsoe-py's own parser uses to build the frame's index
    (`series_parsers.py`: `data[start + (position-1)*delta] = value`), so the
    two grids agree by construction rather than by coincidence.

    Returns None when the period carried nothing readable, which is how
    callers count how many periods they actually managed to read.
    """
    start_elem = period.find(f'{namespace}timeInterval/{namespace}start')
    resolution_elem = period.find(f'{namespace}resolution')
    if start_elem is None or resolution_elem is None:
        return None

    step = parse_resolution(resolution_elem.text)
    if step is None:
        logger.warning(
            f"Unrecognised ENTSO-E resolution {resolution_elem.text!r}; "
            "skipping this period's published positions"
        )
        return None

    try:
        period_start = pd.Timestamp(start_elem.text)
    except (ValueError, TypeError):
        return None
    period_start = (
        period_start.tz_localize('UTC') if period_start.tzinfo is None
        else period_start.tz_convert('UTC')
    )

    stamps: Set[pd.Timestamp] = set()
    for point in period.iter(f'{namespace}Point'):
        position_elem = point.find(f'{namespace}position')
        if position_elem is None:
            continue
        try:
            position = int(position_elem.text)
        except (ValueError, TypeError):
            continue
        if position < 1:
            continue
        stamps.add(period_start + (position - 1) * step)

    return stamps or None


def published_timestamps(xml_response: Optional[str]) -> Optional[Set[pd.Timestamp]]:
    """
    The UTC timestamps an ENTSO-E document carries an explicit `<Point>` for.

    Position n of a period maps to `period_start + (n - 1) * resolution`.
    Timestamps are unioned across every TimeSeries and Period in the document.

    Correct for a document that carries one measured quantity. For A75, where
    each production type is its own TimeSeries and one type's published
    positions say nothing about another's, use
    `published_timestamps_by_series` instead -- see this module's header.

    Returns None when no period could be read at all -- callers must keep
    every row in that case, not drop every row.
    """
    parsed = _parse_document(xml_response)
    if parsed is None:
        return None
    root, namespace = parsed

    stamps: Set[pd.Timestamp] = set()
    periods_read = 0

    for period in root.iter(f'{namespace}Period'):
        period_stamps = _period_stamps(period, namespace)
        if period_stamps is None:
            continue
        stamps |= period_stamps
        periods_read += 1

    if periods_read == 0:
        logger.warning("No readable Period/Point found in ENTSO-E XML response")
        return None

    return stamps


def _series_key(timeseries: ET.Element, namespace: str) -> Tuple[str, str]:
    """
    The column name entsoe-py will give this TimeSeries in an A75 frame.

    Both halves are read from the same two elements entsoe-py's parser reads,
    so the key matches the frame's column identity rather than approximating
    it:

    - the production type, `MktPSRType/psrType` through `PSRTYPE_MAPPINGS`
      (`parsers.py`, `_parse_generation_timeseries`). An unmapped code falls
      back to the raw code, which then matches no column and leaves that
      series untouched -- the fail-open direction.
    - the side. entsoe-py labels a series 'Actual Consumption' when the
      TimeSeries carries `outBiddingZone_Domain.mRID` and 'Actual Aggregated'
      otherwise (`parsers.py`, `CONSUMPTION_ELEMENT`).
    """
    psr_elem = timeseries.find(f'.//{namespace}MktPSRType/{namespace}psrType')
    psr_code = psr_elem.text.strip() if psr_elem is not None and psr_elem.text else ''
    production_type = PSRTYPE_MAPPINGS.get(psr_code, psr_code)

    consumption = timeseries.find(f'.//{namespace}outBiddingZone_Domain.mRID')
    side = 'Actual Consumption' if consumption is not None else 'Actual Aggregated'

    return production_type, side


def published_timestamps_by_series(
    xml_response: Optional[str],
) -> Optional[Dict[Tuple[str, str], Set[pd.Timestamp]]]:
    """
    `published_timestamps`, resolved per (production type, side) sub-series.

    A75 carries one TimeSeries per production type per side, and entsoe-py
    concatenates every TimeSeries sharing a name into one column, dropping
    duplicate timestamps. So a column's published set is the *union* across
    its TimeSeries, which is what this returns.

    Args:
        xml_response: The raw A75 XML for the request the frame came from

    Returns:
        {(production_type, 'Actual Aggregated' | 'Actual Consumption'):
         {published timestamps}}, or None when no period could be read at
        all -- callers must keep every value in that case.
    """
    parsed = _parse_document(xml_response)
    if parsed is None:
        return None
    root, namespace = parsed

    by_series: Dict[Tuple[str, str], Set[pd.Timestamp]] = {}
    periods_read = 0

    for timeseries in root.iter(f'{namespace}TimeSeries'):
        key = _series_key(timeseries, namespace)
        for period in timeseries.iter(f'{namespace}Period'):
            period_stamps = _period_stamps(period, namespace)
            if period_stamps is None:
                continue
            by_series.setdefault(key, set()).update(period_stamps)
            periods_read += 1

    if periods_read == 0:
        logger.warning(
            "No readable TimeSeries/Period/Point found in ENTSO-E XML response"
        )
        return None

    return by_series


def drop_unpublished_zeros(
    df: Optional[pd.DataFrame],
    xml_response: Optional[str],
    value_column: str,
    timestamp_column: str = 'timestamp_utc',
    label: str = '',
) -> Tuple[Optional[pd.DataFrame], int]:
    """
    Drop rows that are forward-filled *and* exactly zero.

    A row survives if the document published a Point at its position, or if
    its value is anything other than exactly `0.0` (NaN included -- `NaN == 0`
    is False, so an unknown stays unknown).

    Args:
        df: Frame as returned by entsoe-py, one row per position
        xml_response: The raw XML for the same request
        value_column: Column holding the measured value, e.g. 'load_mw'
        timestamp_column: Column holding tz-aware UTC timestamps
        label: Free-text context for log lines, e.g. a country code

    Returns:
        (frame, number_of_rows_dropped). The frame is returned unchanged
        whenever the document cannot be read confidently.
    """
    if df is None or df.empty:
        return df, 0

    published = published_timestamps(xml_response)
    if not published:
        logger.warning(
            f"{label}: could not determine which positions were published; "
            "keeping every row"
        )
        return df, 0

    is_published = df[timestamp_column].isin(pd.DatetimeIndex(sorted(published)))

    # If nothing lines up, our position->timestamp grid disagrees with the
    # frame entsoe-py built. That is a parsing fault, not 100% fabrication,
    # and dropping rows on it would be exactly the confidently-wrong move this
    # module exists to prevent.
    if not is_published.any():
        logger.warning(
            f"{label}: no returned row matches a published position "
            f"({len(published)} published, {len(df)} rows); keeping every row"
        )
        return df, 0

    invented_zero = (~is_published) & (df[value_column] == 0.0)
    dropped = int(invented_zero.sum())
    if dropped == 0:
        return df, 0

    logger.warning(
        f"{label}: dropping {dropped} of {len(df)} {value_column} rows that "
        "ENTSO-E published no Point for and that forward-filled to exactly 0"
    )
    return df[~invented_zero].reset_index(drop=True), dropped


def drop_unpublished_zeros_series(
    series: Optional[pd.Series],
    xml_response: Optional[str],
    label: str = '',
) -> Tuple[Optional[pd.Series], int]:
    """
    `drop_unpublished_zeros` for a timestamp-indexed Series.

    entsoe-py returns net position as a Series rather than a frame, so this
    exists to keep the rule itself in one place instead of reimplementing it
    per call site. Same rule, same fail-open behaviour; the only additions are
    normalising the index to UTC and putting it back afterwards.

    Args:
        series: Series as returned by entsoe-py, indexed by timestamp
        xml_response: The raw XML for the same request
        label: Free-text context for log lines, e.g. a country code

    Returns:
        (series, number_of_rows_dropped), the index converted to UTC. The
        series is returned unchanged whenever nothing is dropped.
    """
    if series is None or series.empty:
        return series, 0

    index = pd.DatetimeIndex(series.index)
    index = index.tz_localize('UTC') if index.tz is None else index.tz_convert('UTC')

    # The frame form needs a column name; the Series may not have one.
    column = 'value'
    frame = pd.DataFrame({'timestamp_utc': index, column: series.to_numpy()})

    frame, dropped = drop_unpublished_zeros(frame, xml_response, column, label=label)
    if dropped == 0:
        return series, 0

    kept = pd.Series(
        frame[column].to_numpy(),
        index=pd.DatetimeIndex(frame['timestamp_utc']),
        name=series.name,
    )
    kept.index.name = series.index.name
    return kept, dropped


def blank_unpublished_zeros_by_series(
    df: Optional[pd.DataFrame],
    xml_response: Optional[str],
    label: str = '',
) -> Tuple[Optional[pd.DataFrame], int]:
    """
    Blank the cells of an A75 frame that are forward-filled *and* exactly zero.

    Same rule as `drop_unpublished_zeros`, at the granularity a wide document
    has: the unit is one production type's value at one timestamp, not the
    whole row, because an A75 row carries up to 21 independently measured
    types and refusing one invented zero must not delete the twenty real
    readings beside it. A blanked cell becomes NaN, which
    `_map_generation_columns` already carries through to SQL NULL.

    A cell survives if its sub-series published a Point at that position, or
    if its value is anything other than exactly `0.0` (NaN included -- an
    unknown stays unknown). A production type whose whole day is one Point
    holding a real value -- AT's Waste at a flat 100 MW -- is untouched: that
    is the document's own A03 step function, the same case PT's flat
    interconnector makes for net position.

    Fails open in three separate places, each independently:
      - an unreadable document keeps every cell
      - a column whose sub-series is absent from the document keeps every cell
      - a column whose timestamps match none of its published positions keeps
        every cell, since a grid disagreement is a parsing fault rather than
        100% fabrication

    Args:
        df: The frame entsoe-py returns for A75 -- timestamp index, columns
            either a (production_type, data_type) MultiIndex or, when the
            document held a single sub-series, a flat Index
        xml_response: The raw XML for the same request
        label: Free-text context for log lines, e.g. a country code

    Returns:
        (frame, number_of_cells_blanked). The frame is returned unchanged
        whenever the document cannot be read confidently. The input frame is
        never mutated.
    """
    if df is None or df.empty:
        return df, 0

    by_series = published_timestamps_by_series(xml_response)
    if not by_series:
        logger.warning(
            f"{label}: could not determine which positions were published; "
            "keeping every value"
        )
        return df, 0

    index = pd.DatetimeIndex(df.index)
    index = index.tz_localize('UTC') if index.tz is None else index.tz_convert('UTC')

    # A flat Index means entsoe-py returned a single sub-series and dropped
    # the redundant level, so the frame's column name no longer identifies
    # which one. Recover it from the document, and only when the document is
    # unambiguous about it.
    single_key = next(iter(by_series)) if len(by_series) == 1 else None

    blanked_by_column: Dict[str, int] = {}
    result = df.copy()

    for column in df.columns:
        if isinstance(column, tuple):
            key = tuple(column)
        elif single_key is not None:
            key = single_key
        else:
            continue

        published = by_series.get(key)
        if not published:
            # Not an error: the frame can only hold a column the document
            # produced, so this is an unmapped psrType or a shape we do not
            # recognise. Keep it rather than guess.
            logger.warning(
                f"{label}: no published positions found for {column!r}; "
                "keeping every value in that column"
            )
            continue

        is_published = index.isin(pd.DatetimeIndex(sorted(published)))
        if not is_published.any():
            logger.warning(
                f"{label}: no {column!r} row matches a published position "
                f"({len(published)} published, {len(df)} rows); keeping every "
                "value in that column"
            )
            continue

        values = pd.to_numeric(df[column], errors='coerce')
        invented_zero = (~is_published) & (values == 0.0).to_numpy()
        count = int(invented_zero.sum())
        if count:
            # `.mask` rather than `.loc[...] =`: a tuple column label on a
            # MultiIndex frame is ambiguous to `.loc`, and this keeps the
            # assignment a whole-column replacement either way.
            result[column] = values.mask(invented_zero)
            blanked_by_column[str(column)] = count

    blanked = sum(blanked_by_column.values())
    if blanked == 0:
        return df, 0

    detail = ', '.join(
        f"{name} x{count}" for name, count in sorted(blanked_by_column.items())
    )
    logger.warning(
        f"{label}: blanking {blanked} generation values that ENTSO-E published "
        f"no Point for and that forward-filled to exactly 0 ({detail})"
    )
    return result, blanked
