"""_map_generation_columns must keep every A75 production type ENTSO-E
returns -- nuclear and fossil included -- instead of narrowing to the 8
renewable columns the way _map_renewable_columns does.

The distinction that matters most: a production type a country does not
report must land as NULL (NaN in the frame), not 0. `fillna(0)`, which
_map_renewable_columns uses, would destroy that distinction -- a country
not reporting coal is not the same claim as a country generating 0 coal.

_map_renewable_columns itself is untouched by this file; energy_renewable's
behaviour is frozen (see docs/superpowers/plans/2026-07-29-a75-full-generation.md).
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: E402
from src.entsoe_client import ENTSOEClient  # noqa: E402


@pytest.fixture
def client():
    # Bypass __init__ (it needs a live API key) -- _map_generation_columns
    # is pure and touches no instance state besides config.
    return ENTSOEClient.__new__(ENTSOEClient)


def test_keeps_nuclear_and_fossil(client):
    df = pd.DataFrame({
        'timestamp_utc': [pd.Timestamp('2026-07-29T00:00:00Z')],
        'Nuclear': [42000.0],
        'Fossil Gas': [3000.0],
        'Solar': [0.0],
    })
    out = client._map_generation_columns(df)
    assert out['nuclear_mw'].iloc[0] == 42000.0
    assert out['fossil_gas_mw'].iloc[0] == 3000.0


def test_absent_type_is_null_not_zero(client):
    """A country that does not report coal must read NULL, not 0 - the two are
    different claims and the dashboard renders them differently."""
    df = pd.DataFrame({'timestamp_utc': [pd.Timestamp('2026-07-29T00:00:00Z')], 'Nuclear': [1.0]})
    out = client._map_generation_columns(df)
    assert pd.isna(out['fossil_hard_coal_mw'].iloc[0])


def test_reported_zero_stays_zero(client):
    """Solar at night is a measured 0, not missing data."""
    df = pd.DataFrame({'timestamp_utc': [pd.Timestamp('2026-07-29T00:00:00Z')], 'Solar': [0.0]})
    out = client._map_generation_columns(df)
    assert out['solar_mw'].iloc[0] == 0.0
    assert not pd.isna(out['solar_mw'].iloc[0])


def test_all_21_measured_columns_are_mapped(client):
    """Every ENTSO-E column measured live (DE/PL/ES, 2026-07-29) must map to
    its own energy_generation column, with no folding -- unlike the renewable
    mapping, which combines several ENTSO-E names into one column."""
    measured_names = [
        'Biomass', 'Energy storage', 'Fossil Brown coal/Lignite',
        'Fossil Coal-derived gas', 'Fossil Gas', 'Fossil Hard coal',
        'Fossil Oil', 'Fossil Oil shale', 'Fossil Peat', 'Geothermal',
        'Hydro Pumped Storage', 'Hydro Run-of-river and poundage',
        'Hydro Water Reservoir', 'Marine', 'Nuclear', 'Other',
        'Other renewable', 'Solar', 'Waste', 'Wind Offshore', 'Wind Onshore',
    ]
    assert set(measured_names) == set(config.GENERATION_COLUMN_MAP.keys())

    data = {name: [float(i)] for i, name in enumerate(measured_names, start=1)}
    data['timestamp_utc'] = [pd.Timestamp('2026-07-29T00:00:00Z')]
    df = pd.DataFrame(data)

    out = client._map_generation_columns(df)

    for i, name in enumerate(measured_names, start=1):
        our_col = config.GENERATION_COLUMN_MAP[name]
        assert out[our_col].iloc[0] == float(i), f"{name} -> {our_col} did not round-trip"

    # No folding: 21 distinct ENTSO-E names must produce 21 distinct columns.
    assert len(set(config.GENERATION_COLUMN_MAP.values())) == 21


def test_hydro_pumped_storage_is_not_folded_into_reservoir(client):
    """Unlike _map_renewable_columns (which combines Hydro Pumped Storage into
    hydro_reservoir_mw), the full mapping keeps them as separate columns --
    pumped storage is a store, can be negative, and needs its own column."""
    df = pd.DataFrame({
        'timestamp_utc': [pd.Timestamp('2026-07-29T00:00:00Z')],
        'Hydro Pumped Storage': [-500.0],
        'Hydro Water Reservoir': [1200.0],
    })
    out = client._map_generation_columns(df)
    assert out['hydro_pumped_mw'].iloc[0] == -500.0
    assert out['hydro_reservoir_mw'].iloc[0] == 1200.0


def test_unmapped_column_is_logged_at_warning_not_dropped_silently(client, caplog):
    """A new upstream production type must be visible, not silently discarded --
    that is exactly how nuclear was lost the first time."""
    df = pd.DataFrame({
        'timestamp_utc': [pd.Timestamp('2026-07-29T00:00:00Z')],
        'Some New PSR Type': [17.0],
        'Nuclear': [1.0],
    })
    with caplog.at_level('WARNING'):
        out = client._map_generation_columns(df)

    assert any(
        'Some New PSR Type' in record.message and record.levelname == 'WARNING'
        for record in caplog.records
    ), "unmapped ENTSO-E column must be logged at WARNING"
    # And it must not have been silently written under some column either.
    assert 'Some New PSR Type' not in out.columns


class _StubRawClient:
    """Stands in for entsoe.EntsoeRawClient -- only query_generation is used
    by query_generation_all_types_with_metadata, and only to source the
    publication timestamp."""

    def query_generation(self, entsoe_domain, start, end, psr_type=None):
        return (
            '<GL_MarketDocument>'
            '<createdDateTime>2026-07-29T10:00:00Z</createdDateTime>'
            '</GL_MarketDocument>'
        )


class _StubPandasClient:
    """Stands in for entsoe.EntsoePandasClient -- returns a MultiIndex frame
    shaped exactly like the real ENTSO-E A75 response: (production_type,
    data_type) columns, some types with both 'Actual Aggregated' and
    'Actual Consumption', some with only one or the other."""

    def __init__(self, df):
        self._df = df

    def query_generation(self, entsoe_domain, start, end, psr_type=None):
        return self._df


def _france_shaped_multiindex_frame():
    """Mirrors the live-measured FR evidence: Hydro Pumped Storage reports
    both Aggregated (generating) and Consumption (pumping) at every
    timestamp; Solar reports Aggregated only; Fossil Hard coal reports
    Consumption only."""
    idx = pd.date_range('2026-07-29T00:00:00Z', periods=3, freq='15min')
    columns = pd.MultiIndex.from_tuples([
        ('Hydro Pumped Storage', 'Actual Aggregated'),
        ('Hydro Pumped Storage', 'Actual Consumption'),
        ('Solar', 'Actual Aggregated'),
        ('Fossil Hard coal', 'Actual Consumption'),
    ])
    data = [
        [26.56, 284.99, 100.0, 50.0],
        [26.50, 349.00, 110.0, 60.0],
        [26.48, 348.63, 120.0, 70.0],
    ]
    return pd.DataFrame(data, index=idx, columns=columns)


@pytest.fixture
def flatten_client(monkeypatch):
    """A client wired to query_generation_all_types_with_metadata's full
    path (fetch -> flatten/net -> _map_generation_columns), with the
    network and DB dependencies mocked out rather than calling either."""
    c = ENTSOEClient.__new__(ENTSOEClient)
    c.raw_client = _StubRawClient()
    c.client = _StubPandasClient(_france_shaped_multiindex_frame())
    c._rate_limit = types.MethodType(lambda self: None, c)
    # _get_country_domain hits the DB; the domain value itself is unused by
    # the stub query_generation methods above, so any dict with the key
    # they don't even read is fine -- what matters is not touching a DB.
    monkeypatch.setattr(
        c, '_get_country_domain', lambda country_code: {'entsoe_domain': 'FR'}
    )
    return c


def test_flatten_path_nets_pumped_storage_and_keeps_consumption_only_type(flatten_client):
    """Regression test for the review finding: query_generation_all_types_with_metadata
    used to flatten the MultiIndex by dropping every 'Actual Consumption'
    sub-series and preferring 'Actual Aggregated' -- copied verbatim from
    query_generation_per_type_with_metadata, where it's fine because
    energy_renewable never claimed signed values. That silently turned FR's
    net *consumption* of ~320 MW on pumped storage into a positive +26.5 MW
    generation reading, and made Fossil Hard coal (Consumption-only here)
    disappear entirely. This exercises the real flatten path -- through
    query_generation_all_types_with_metadata, not a hand-built single-level
    frame handed straight to _map_generation_columns -- which is exactly
    what let the bug hide behind the existing unit tests.
    """
    df, publication_time = flatten_client.query_generation_all_types_with_metadata(
        'FR', pd.Timestamp('2026-07-29T00:00:00Z'), pd.Timestamp('2026-07-29T01:00:00Z')
    )

    assert df is not None
    assert publication_time is not None

    # Both series present -> Aggregated - Consumption. FR is net *consuming*
    # (pumping) ~258-322 MW; the netted value must be negative, not the
    # bare +26.5 the old "prefer Aggregated" logic produced.
    pumped = df.set_index('timestamp_utc')['hydro_pumped_mw']
    assert (pumped < 0).all(), "FR pumped storage nets to consumption and must be negative"
    assert pumped.iloc[0] == pytest.approx(26.56 - 284.99)
    assert pumped.iloc[1] == pytest.approx(26.50 - 349.00)
    assert pumped.iloc[2] == pytest.approx(26.48 - 348.63)

    # Aggregated-only type passes through unchanged.
    solar = df.set_index('timestamp_utc')['solar_mw']
    assert list(solar) == [100.0, 110.0, 120.0]

    # Consumption-only type must survive the flatten -- not vanish -- and
    # be recorded as a negative net contribution to the grid.
    coal = df.set_index('timestamp_utc')['fossil_hard_coal_mw']
    assert list(coal) == [-50.0, -60.0, -70.0]


def test_net_generation_consumption_neither_present_stays_null(client):
    """Unit-level check on the netting helper directly: a timestamp where
    neither Aggregated nor Consumption was reported for a type that
    otherwise has both sub-series must stay NaN, never be coerced to 0 by
    a fillna(0)-style substitution."""
    idx = pd.date_range('2026-07-29T00:00:00Z', periods=3, freq='15min')
    columns = pd.MultiIndex.from_tuples([
        ('Energy storage', 'Actual Aggregated'),
        ('Energy storage', 'Actual Consumption'),
    ])
    df = pd.DataFrame(
        [
            [float('nan'), float('nan')],  # neither present -> NaN
            [5.0, float('nan')],           # only Aggregated -> 5.0
            [float('nan'), 3.0],           # only Consumption -> -3.0
        ],
        index=idx,
        columns=columns,
    )

    out = client._net_generation_consumption(df)

    assert pd.isna(out['Energy storage'].iloc[0])
    assert out['Energy storage'].iloc[1] == 5.0
    assert out['Energy storage'].iloc[2] == -3.0


def test_output_has_no_default_zero_fill(client):
    """Regression guard: the renewable mapping initialises every column to 0.0
    and fillna(0)s incoming values. The generation mapping must not -- any
    column not present in the source frame must stay NaN all the way through,
    including after the groupby/aggregation step."""
    df = pd.DataFrame({
        'timestamp_utc': [pd.Timestamp('2026-07-29T00:00:00Z')],
        'Nuclear': [1.0],
    })
    out = client._map_generation_columns(df)

    all_columns = set(config.GENERATION_COLUMN_MAP.values())
    present_columns = {'nuclear_mw'}
    absent_columns = all_columns - present_columns

    for col in absent_columns:
        assert pd.isna(out[col].iloc[0]), f"{col} should be NaN (absent), not filled"
