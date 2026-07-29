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
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: E402
from src import db  # noqa: E402
from src import fetch_renewable  # noqa: E402
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


# ============================================================================
# Task 3: one A75 fetch, two writes -- query_generation_and_renewable_with_metadata,
# db.upsert_generation_data, fetch_renewable.fetch_renewable_data
# ============================================================================
#
# The central risk in this task: query_generation_all_types_with_metadata
# (Task 2) nets 'Actual Aggregated' - 'Actual Consumption' per production
# type, but query_generation_per_type_with_metadata (the method
# energy_renewable has always been written from) only ever keeps
# 'Actual Aggregated' and silently drops 'Actual Consumption'. Deriving
# energy_renewable from the ALREADY-netted energy_generation frame would
# therefore change hydro_reservoir_mw (it folds in Hydro Pumped Storage) --
# and possibly other renewable columns -- for any country/window where a
# renewable-mapped type reports both sub-series. The tests below prove the
# derived renewable frame matches the old, frozen path across every shape a
# production type can take.


def _all_21_types_mixed_shapes_multiindex_frame():
    """Covers all 21 GENERATION_COLUMN_MAP production types, split across
    the three shapes a type can take in a real A75 response: both
    'Actual Aggregated' and 'Actual Consumption', 'Actual Aggregated' only,
    or 'Actual Consumption' only.

    The renewable-mapped ENTSO-E names (the 11 that feed
    _map_renewable_columns's column_mapping, including the two pairs it
    folds together -- Hydro Pumped Storage + Hydro Water Reservoir into
    hydro_reservoir_mw, and Energy storage + Other renewable + Marine into
    other_renewable_mw) are deliberately spread across all three shapes, so
    the equivalence test exercises every combination that folding can hit:
      - both-series:        Hydro Pumped Storage, Wind Onshore, Marine
      - aggregated-only:     Solar, Wind Offshore, Hydro Run-of-river and
                              poundage, Hydro Water Reservoir, Biomass
      - consumption-only:    Geothermal, Energy storage, Other renewable

    The remaining 10 (non-renewable-mapped: nuclear, fossil, waste, other)
    are also spread across all three shapes for full 21-type coverage, even
    though they cannot affect the renewable frame -- they matter for the
    energy_generation side of the equivalence, and for catching a
    regression where the old-style flatten accidentally starts keeping a
    fossil/nuclear column it never used to.
    """
    both = [
        'Hydro Pumped Storage', 'Wind Onshore', 'Marine',
        'Nuclear', 'Fossil Hard coal', 'Fossil Gas',
    ]
    aggregated_only = [
        'Solar', 'Wind Offshore', 'Hydro Run-of-river and poundage',
        'Hydro Water Reservoir', 'Biomass', 'Waste', 'Other', 'Fossil Oil',
    ]
    consumption_only = [
        'Geothermal', 'Energy storage', 'Other renewable',
        'Fossil Brown coal/Lignite', 'Fossil Oil shale',
        'Fossil Peat', 'Fossil Coal-derived gas',
    ]

    assert set(both) | set(aggregated_only) | set(consumption_only) == set(
        config.GENERATION_COLUMN_MAP.keys()
    )
    assert len(both) + len(aggregated_only) + len(consumption_only) == 21

    idx = pd.date_range('2026-07-29T00:00:00Z', periods=2, freq='15min')

    columns_data = {}
    value = 10.0
    for prod_type in both:
        columns_data[(prod_type, 'Actual Aggregated')] = [value, value + 1]
        value += 100
        columns_data[(prod_type, 'Actual Consumption')] = [value, value + 1]
        value += 100
    for prod_type in aggregated_only:
        columns_data[(prod_type, 'Actual Aggregated')] = [value, value + 1]
        value += 100
    for prod_type in consumption_only:
        columns_data[(prod_type, 'Actual Consumption')] = [value, value + 1]
        value += 100

    return pd.DataFrame(columns_data, index=idx)


def _stub_client(df, monkeypatch, country_code='FR'):
    """Build an ENTSOEClient wired to a fixed stub response, network and DB
    dependencies mocked out -- same pattern as the `flatten_client` fixture
    above, generalised to an arbitrary source frame so it can be reused for
    both the old and the new fetch path from the same underlying document."""
    c = ENTSOEClient.__new__(ENTSOEClient)
    c.raw_client = _StubRawClient()
    c.client = _StubPandasClient(df)
    c._rate_limit = types.MethodType(lambda self: None, c)
    monkeypatch.setattr(
        c, '_get_country_domain', lambda cc: {'entsoe_domain': country_code}
    )
    return c


def test_derived_renewable_frame_matches_old_path_across_all_shapes(monkeypatch):
    """The equivalence proof this task exists to deliver: energy_renewable
    derived via query_generation_and_renewable_with_metadata must be
    byte-identical to what query_generation_per_type_with_metadata (the
    method energy_renewable has always been written from) produces from the
    exact same underlying A75 document -- across all 21 production types and
    all three sub-series shapes, not just the France-shaped smoke case.

    Both calls read from the same stub document (the same DataFrame object,
    read-only on every call, never mutated by either flatten), so any
    difference in the result can only come from the flatten/mapping logic
    itself -- exactly the class of bug this test is meant to catch.
    """
    df = _all_21_types_mixed_shapes_multiindex_frame()
    client = _stub_client(df, monkeypatch)

    start = pd.Timestamp('2026-07-29T00:00:00Z')
    end = pd.Timestamp('2026-07-29T01:00:00Z')

    old_renewable_df, old_publication_time = client.query_generation_per_type_with_metadata(
        'FR', start, end
    )
    generation_df, new_renewable_df, new_publication_time = (
        client.query_generation_and_renewable_with_metadata('FR', start, end)
    )

    assert old_renewable_df is not None
    assert new_renewable_df is not None
    assert generation_df is not None
    assert old_publication_time == new_publication_time

    # Column set, row count and every value must match exactly -- sort
    # columns so an incidental ordering difference isn't mistaken for a
    # real one.
    old_sorted = old_renewable_df.sort_index(axis=1).reset_index(drop=True)
    new_sorted = new_renewable_df.sort_index(axis=1).reset_index(drop=True)
    pd.testing.assert_frame_equal(old_sorted, new_sorted)

    # Spot-check the exact case that would break if energy_renewable were
    # derived from the NETTED frame instead of the pre-netting flatten:
    # Hydro Pumped Storage is both-series here, and folds into
    # hydro_reservoir_mw alongside Hydro Water Reservoir (aggregated-only).
    # The old/frozen path keeps only the Aggregated side of Hydro Pumped
    # Storage (its Consumption side is silently dropped) -- if the new path
    # instead netted Aggregated-Consumption before folding, this column
    # would come out lower (or negative) instead of matching.
    assert (new_renewable_df['hydro_reservoir_mw'] == old_renewable_df['hydro_reservoir_mw']).all()
    assert not old_renewable_df['hydro_reservoir_mw'].isna().any()


def test_query_generation_and_renewable_makes_exactly_two_requests(monkeypatch):
    """One A75 fetch, two writes: query_generation_and_renewable_with_metadata
    must not issue more _make_request calls than
    query_generation_all_types_with_metadata already does on its own (one
    raw-XML call for the publication timestamp, one pandas-client call for
    the structured frame) -- it must NOT call both
    query_generation_per_type_with_metadata and
    query_generation_all_types_with_metadata internally, which would double
    the request count and defeat the entire point of this task."""
    df = _all_21_types_mixed_shapes_multiindex_frame()
    client = _stub_client(df, monkeypatch)

    calls = []
    real_make_request = client._make_request

    def _spy(method, *args, **kwargs):
        calls.append(getattr(method, '__self__', None))
        return real_make_request(method, *args, **kwargs)

    monkeypatch.setattr(client, '_make_request', _spy)

    start = pd.Timestamp('2026-07-29T00:00:00Z')
    end = pd.Timestamp('2026-07-29T01:00:00Z')
    generation_df, renewable_df, publication_time = (
        client.query_generation_and_renewable_with_metadata('FR', start, end)
    )

    assert generation_df is not None
    assert renewable_df is not None
    assert len(calls) == 2, f"expected exactly 2 _make_request calls, got {len(calls)}"
    assert calls[0] is client.raw_client
    assert calls[1] is client.client


def test_fetch_renewable_data_writes_both_tables_from_one_client_call(monkeypatch):
    """fetch_renewable.fetch_renewable_data must call the client's combined
    method exactly once and upsert both tables from its output -- never call
    a second query method to fill energy_generation, and never leave either
    table unwritten."""
    generation_df = pd.DataFrame({
        'timestamp_utc': [pd.Timestamp('2026-07-29T00:00:00Z')],
        'nuclear_mw': [42000.0],
    })
    renewable_df = pd.DataFrame({
        'timestamp_utc': [pd.Timestamp('2026-07-29T00:00:00Z')],
        'solar_mw': [100.0],
    })
    publication_time = pd.Timestamp('2026-07-29T10:00:00Z')

    mock_client = MagicMock()
    mock_client.query_generation_and_renewable_with_metadata.return_value = (
        generation_df, renewable_df, publication_time
    )

    monkeypatch.setattr(
        db, 'upsert_generation_data',
        MagicMock(return_value=(1, 0)),
    )
    monkeypatch.setattr(
        db, 'upsert_renewable_data',
        MagicMock(return_value=(1, 0)),
    )

    start = pd.Timestamp('2026-07-29T00:00:00Z')
    end = pd.Timestamp('2026-07-29T01:00:00Z')
    inserted, updated, failed = fetch_renewable.fetch_renewable_data(
        mock_client, 'FR', start, end
    )

    assert failed == 0
    assert inserted == 2  # 1 generation + 1 renewable
    assert updated == 0

    mock_client.query_generation_and_renewable_with_metadata.assert_called_once_with(
        'FR', start, end
    )
    # query_generation_per_type_with_metadata / query_generation_all_types_with_metadata
    # must NOT be called by the ingest path -- that would be a second A75 fetch.
    assert not mock_client.query_generation_per_type_with_metadata.called
    assert not mock_client.query_generation_all_types_with_metadata.called

    db.upsert_generation_data.assert_called_once()
    gen_call_args = db.upsert_generation_data.call_args
    assert gen_call_args[0][0] is generation_df
    assert gen_call_args[0][1] == 'FR'
    assert gen_call_args[1]['publication_timestamp'] == publication_time

    db.upsert_renewable_data.assert_called_once()
    ren_call_args = db.upsert_renewable_data.call_args
    assert ren_call_args[0][0] is renewable_df
    assert ren_call_args[0][1] == 'FR'
    assert ren_call_args[1]['publication_timestamp'] == publication_time


@pytest.fixture
def scratch_generation_db(tmp_path, monkeypatch):
    """A fresh, throwaway SQLite file with the energy_generation schema
    applied -- never the read-only replica or prod. Points db.get_connection()
    at this file only for the duration of the test."""
    scratch_path = tmp_path / "scratch_generation.db"
    monkeypatch.setattr(config, 'DATABASE_PATH', scratch_path)
    db.create_generation_table()
    return scratch_path


def test_upsert_generation_data_writes_null_for_absent_types_not_zero(scratch_generation_db):
    """The upsert's core contract: a column absent from the source frame (NaN
    in pandas) must land as SQL NULL in energy_generation, never 0 -- 0 is a
    measurement claim (e.g. solar at night), NULL is "not reported"."""
    generation_cols = config.get_generation_columns()
    df = pd.DataFrame({
        'timestamp_utc': [pd.Timestamp('2026-07-29T00:00:00Z')],
        'nuclear_mw': [42000.0],
        'solar_mw': [0.0],  # measured zero -- must stay 0, not become NULL
    })

    inserted, updated = db.upsert_generation_data(
        df, 'FR', publication_timestamp=pd.Timestamp('2026-07-29T10:00:00Z')
    )
    assert inserted == 1

    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT {', '.join(generation_cols)}, publication_timestamp_utc "
            "FROM energy_generation WHERE country_code = 'FR'"
        )
        row = cursor.fetchone()

    assert row is not None
    row_dict = dict(row)

    assert row_dict['nuclear_mw'] == 42000.0
    assert row_dict['solar_mw'] == 0.0  # measured zero survives, not NULL

    absent_cols = set(generation_cols) - {'nuclear_mw', 'solar_mw'}
    for col in absent_cols:
        assert row_dict[col] is None, f"{col} should be SQL NULL, got {row_dict[col]!r}"

    assert row_dict['publication_timestamp_utc'] is not None


def test_upsert_generation_data_upserts_on_conflict_not_duplicates(scratch_generation_db):
    """Re-upserting the same (country_code, timestamp_utc) must update the
    existing row in place -- the unique index this table depends on for
    resumable backfills (Task 4) -- not insert a second row."""
    ts = pd.Timestamp('2026-07-29T00:00:00Z')

    df_v1 = pd.DataFrame({'timestamp_utc': [ts], 'nuclear_mw': [1000.0]})
    db.upsert_generation_data(df_v1, 'DE')

    df_v2 = pd.DataFrame({'timestamp_utc': [ts], 'nuclear_mw': [1234.0]})
    db.upsert_generation_data(df_v2, 'DE')

    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) as n, MAX(nuclear_mw) as nuclear_mw "
            "FROM energy_generation WHERE country_code = 'DE'"
        )
        row = cursor.fetchone()

    assert row['n'] == 1, "conflicting upsert must update in place, not duplicate"
    assert row['nuclear_mw'] == 1234.0
