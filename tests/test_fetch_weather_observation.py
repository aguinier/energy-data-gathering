"""Tests for src/fetch_weather_observation.py helpers (Phase 3a).

Scope: pure helper functions that don't hit Open-Meteo. The real-API
functions (fetch_realtime_forecast / fetch_previous_runs / fetch_archive_era5)
are tested indirectly via the backfill scripts' dry-run mode.
"""

from __future__ import annotations

import pytest

from src.fetch_weather_observation import (
    MAX_LOCATIONS_PER_CALL,
    _batch_locations,
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
