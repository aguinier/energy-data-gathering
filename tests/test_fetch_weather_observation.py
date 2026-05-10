"""Tests for src/fetch_weather_observation.py helpers (Phase 3a).

Scope: pure helper functions that don't hit Open-Meteo. The real-API
functions (fetch_realtime_forecast / fetch_previous_runs / fetch_archive_era5)
are tested indirectly via the backfill scripts' dry-run mode.
"""

from __future__ import annotations

import pytest

from src.fetch_weather_observation import (
    MAX_LOCATIONS_PER_CALL,
    REGIONAL_MODEL_COVERAGE,
    _batch_locations,
    _filter_locations_by_model,
    _get_all_locations,
)


# ---------------------------------------------------------------------------
# _batch_locations — pure list chunker
# ---------------------------------------------------------------------------


def _mk_locs(n: int) -> list[dict]:
    return [{"location_id": i, "lat": float(i), "lon": float(i)} for i in range(n)]


def test_batch_locations_single_batch_under_cap() -> None:
    locs = _mk_locs(10)
    batches = _batch_locations(locs, batch_size=50)
    assert len(batches) == 1
    assert len(batches[0]) == 10


def test_batch_locations_exactly_at_cap() -> None:
    locs = _mk_locs(50)
    batches = _batch_locations(locs, batch_size=50)
    assert len(batches) == 1
    assert len(batches[0]) == 50


def test_batch_locations_just_over_cap() -> None:
    locs = _mk_locs(51)
    batches = _batch_locations(locs, batch_size=50)
    assert len(batches) == 2
    assert len(batches[0]) == 50
    assert len(batches[1]) == 1


def test_batch_locations_empty_input() -> None:
    assert _batch_locations([], batch_size=50) == []


def test_batch_locations_337_at_default_cap() -> None:
    # Realistic Phase 3 shape: 337 LOCATIONS, default cap = 50.
    locs = _mk_locs(337)
    batches = _batch_locations(locs, batch_size=MAX_LOCATIONS_PER_CALL)
    assert len(batches) == 7
    # Batches preserve ordering — first batch starts with location_id 0.
    assert batches[0][0]["location_id"] == 0
    assert batches[-1][-1]["location_id"] == 336


def test_batch_locations_preserves_order() -> None:
    locs = _mk_locs(120)
    batches = _batch_locations(locs, batch_size=50)
    flat = [loc for batch in batches for loc in batch]
    assert [loc["location_id"] for loc in flat] == list(range(120))


# ---------------------------------------------------------------------------
# _filter_locations_by_model — per-model regional bbox filter
# ---------------------------------------------------------------------------


def test_filter_locations_by_model_global_models_pass_through() -> None:
    """best_match + ECMWF/ICON-seamless/GFS are global, no filter applied."""
    locs = [
        {"location_id": 1, "lat": 50.5, "lon": 4.45},   # Belgium
        {"location_id": 2, "lat": 36.581, "lon": -5.5},  # Southern Spain
        {"location_id": 3, "lat": 65.0, "lon": 25.0},    # Northern Finland
    ]
    for model_id in ("best_match", "ecmwf_ifs025", "icon_seamless", "gfs_seamless"):
        assert _filter_locations_by_model(locs, model_id) == locs


def test_filter_locations_by_model_knmi_drops_southern_iberia() -> None:
    """The actual failure mode discovered 2026-04-24."""
    in_domain = {"location_id": 1, "lat": 50.5, "lon": 4.45}    # Belgium
    out_domain = {"location_id": 2, "lat": 36.581, "lon": -5.5}  # Southern Spain
    result = _filter_locations_by_model(
        [in_domain, out_domain], "knmi_harmonie_arome_europe"
    )
    assert result == [in_domain]


def test_filter_locations_by_model_meteofrance_drops_far_east_and_far_south() -> None:
    """MF AROME's served domain is wider than mainland France — Belgium and
    Berlin both work. The actual boundaries are around 38-56°N, -10-13°E."""
    in_france = {"location_id": 1, "lat": 47.0, "lon": 2.0}    # Central France
    in_belgium = {"location_id": 2, "lat": 50.5, "lon": 4.45}  # Brussels — works
    out_east = {"location_id": 3, "lat": 50.0, "lon": 15.0}    # Czech Rep — fails
    out_south = {"location_id": 4, "lat": 36.0, "lon": -5.0}   # S Spain — fails
    out_north = {"location_id": 5, "lat": 58.0, "lon": 5.0}    # Norway — fails
    result = _filter_locations_by_model(
        [in_france, in_belgium, out_east, out_south, out_north],
        "meteofrance_arome_france",
    )
    assert result == [in_france, in_belgium]


def test_filter_locations_by_model_icon_d2_pass_through() -> None:
    """ICON-D2 serves a wide domain in practice and is intentionally not
    filtered — only ~1/7 batches fail, not worth a tight bbox that risks
    dropping legitimate locations."""
    locs = [
        {"location_id": 1, "lat": 52.5, "lon": 13.4},  # Berlin
        {"location_id": 2, "lat": 40.4, "lon": -3.7},  # Madrid
        {"location_id": 3, "lat": 60.0, "lon": 10.7},  # Oslo
    ]
    assert _filter_locations_by_model(locs, "icon_d2") == locs


def test_filter_locations_by_model_empty_when_all_out_of_domain() -> None:
    """If filter excludes all locations, return empty list (caller will skip the model)."""
    locs = [{"location_id": 1, "lat": 36.0, "lon": -5.0}]  # Southern Spain
    assert _filter_locations_by_model(locs, "knmi_harmonie_arome_europe") == []


def test_filter_locations_by_model_does_not_mutate_input() -> None:
    locs = [{"location_id": 1, "lat": 50.0, "lon": 4.0}]
    original = list(locs)
    _filter_locations_by_model(locs, "knmi_harmonie_arome_europe")
    assert locs == original


def test_filter_locations_by_model_unknown_model_pass_through() -> None:
    """Models not in REGIONAL_MODEL_COVERAGE get no filter (safe default)."""
    locs = [{"location_id": 1, "lat": 36.0, "lon": -5.0}]
    assert _filter_locations_by_model(locs, "some_future_model") == locs


def test_regional_model_coverage_has_expected_models() -> None:
    """Guard against accidental removal of known regional models."""
    assert "knmi_harmonie_arome_europe" in REGIONAL_MODEL_COVERAGE
    assert "meteofrance_arome_france" in REGIONAL_MODEL_COVERAGE
    # ICON-D2 deliberately omitted — see comment in REGIONAL_MODEL_COVERAGE


# ---------------------------------------------------------------------------
# _get_all_locations — SQL generation with filters (tested end-to-end via
# a real in-memory DB seeded with a couple of zones)
# ---------------------------------------------------------------------------


@pytest.fixture
def db_with_mixed_locations(monkeypatch):
    """Seed an in-memory DB with 3 countries × 2 zone_types, patch db.get_connection."""
    import sqlite3
    from src import db as db_module

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE weather_location (
            location_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT NOT NULL,
            zone_id TEXT NOT NULL,
            zone_type TEXT,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            weight REAL,
            capacity_mw REAL,
            description TEXT,
            UNIQUE(country_code, zone_id)
        )
        """
    )
    rows = [
        ("BE", "centroid", "centroid", 50.5, 4.45, 1.0, None, "BE centroid"),
        ("BE", "solar_1", "solar", 50.8, 4.3, 0.6, 500.0, "BE solar"),
        ("FR", "centroid", "centroid", 46.2, 2.25, 1.0, None, "FR centroid"),
        ("FR", "solar_1", "solar", 48.0, 2.5, 0.5, 1000.0, "FR solar"),
        ("DE", "centroid", "centroid", 51.2, 10.45, 1.0, None, "DE centroid"),
    ]
    conn.executemany(
        "INSERT INTO weather_location "
        "(country_code, zone_id, zone_type, lat, lon, weight, capacity_mw, description) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows,
    )
    conn.commit()

    class _Ctx:
        def __enter__(self): return conn
        def __exit__(self, *a): return False

    monkeypatch.setattr(db_module, "get_connection", lambda: _Ctx())
    return conn


def test_get_all_locations_no_filter(db_with_mixed_locations) -> None:
    locs = _get_all_locations()
    assert len(locs) == 5
    # Ordered by location_id (insertion order here).
    assert [l["zone_id"] for l in locs][:2] == ["centroid", "solar_1"]


def test_get_all_locations_by_zone_type(db_with_mixed_locations) -> None:
    locs = _get_all_locations(zone_type_filter="centroid")
    assert len(locs) == 3
    assert all(l["zone_type"] == "centroid" for l in locs)


def test_get_all_locations_by_country(db_with_mixed_locations) -> None:
    locs = _get_all_locations(country_filter="BE")
    assert len(locs) == 2
    assert all(l["country_code"] == "BE" for l in locs)


def test_get_all_locations_both_filters(db_with_mixed_locations) -> None:
    locs = _get_all_locations(zone_type_filter="centroid", country_filter="FR")
    assert len(locs) == 1
    assert locs[0]["country_code"] == "FR"
    assert locs[0]["zone_type"] == "centroid"


def test_get_all_locations_no_match(db_with_mixed_locations) -> None:
    locs = _get_all_locations(country_filter="XX")
    assert locs == []
