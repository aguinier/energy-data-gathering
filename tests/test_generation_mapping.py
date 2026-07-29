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
