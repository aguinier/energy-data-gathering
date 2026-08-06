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
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from typing import Optional, Set, Tuple

import pandas as pd


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


def published_timestamps(xml_response: Optional[str]) -> Optional[Set[pd.Timestamp]]:
    """
    The UTC timestamps an ENTSO-E document carries an explicit `<Point>` for.

    Position n of a period maps to `period_start + (n - 1) * resolution`.
    Timestamps are unioned across every TimeSeries and Period in the document.

    Returns None when no period could be read at all -- callers must keep
    every row in that case, not drop every row.
    """
    if not xml_response:
        return None

    try:
        root = ET.fromstring(xml_response)
    except ET.ParseError as exc:
        logger.warning(f"Could not parse ENTSO-E XML for published positions: {exc}")
        return None

    namespace = root.tag.split('}')[0] + '}' if '}' in root.tag else ''

    stamps: Set[pd.Timestamp] = set()
    periods_read = 0

    for period in root.iter(f'{namespace}Period'):
        start_elem = period.find(f'{namespace}timeInterval/{namespace}start')
        resolution_elem = period.find(f'{namespace}resolution')
        if start_elem is None or resolution_elem is None:
            continue

        step = parse_resolution(resolution_elem.text)
        if step is None:
            logger.warning(
                f"Unrecognised ENTSO-E resolution {resolution_elem.text!r}; "
                "skipping this period's published positions"
            )
            continue

        try:
            period_start = pd.Timestamp(start_elem.text)
        except (ValueError, TypeError):
            continue
        period_start = (
            period_start.tz_localize('UTC') if period_start.tzinfo is None
            else period_start.tz_convert('UTC')
        )

        read_a_point = False
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
            read_a_point = True

        if read_a_point:
            periods_read += 1

    if periods_read == 0:
        logger.warning("No readable Period/Point found in ENTSO-E XML response")
        return None

    return stamps


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
