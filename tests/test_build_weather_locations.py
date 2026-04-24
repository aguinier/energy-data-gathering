"""Tests for scripts/build_weather_locations.py.

Tests the pure logic on synthetic plant DataFrames (no real GEM xlsx
required). The two end-to-end tests that DO need the real GEM files
are gated on file existence so CI without them still passes.
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
import pytest

from scripts.build_weather_locations import (
    GEM_SOLAR_FILE,
    GEM_WIND_FILE,
    RANDOM_STATE,
    build_locations,
    classify_wind_tech,
    cluster_zones,
    haversine_km,
    pick_adaptive_k,
)


# ---------------------------------------------------------------------------
# haversine_km — sanity check on a well-known great-circle distance
# ---------------------------------------------------------------------------


def test_haversine_brussels_to_paris_about_260km() -> None:
    # Brussels (50.85, 4.35) -> Paris (48.86, 2.35)
    d = haversine_km(50.85, 4.35, 48.86, 2.35)
    assert 250 <= d <= 270, f"expected ~260 km, got {d:.1f}"


def test_haversine_zero_distance() -> None:
    assert haversine_km(50.0, 4.0, 50.0, 4.0) == pytest.approx(0.0, abs=1e-6)


# ---------------------------------------------------------------------------
# classify_wind_tech — GEM Installation Type → our tech_type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "installation_type, expected",
    [
        ("Onshore", "wind_onshore"),
        ("Offshore hard mount", "wind_offshore"),
        ("Offshore mount unknown", "wind_offshore"),
        ("Offshore floating", "wind_offshore"),
        ("Unknown", None),
        ("", None),
    ],
)
def test_classify_wind_tech(installation_type, expected) -> None:
    assert classify_wind_tech(installation_type) == expected


# ---------------------------------------------------------------------------
# pick_adaptive_k — spec's tie-breaking rules
# ---------------------------------------------------------------------------


def _make_coords(coords: list[tuple[float, float]]) -> list[tuple[float, float]]:
    return coords


# Tuning knobs for tests — realistic shapes, smaller MW thresholds so small
# synthetic fixtures can still trigger cluster decisions.
_SOLAR_TEST_TUNING = {"max_radius_km": 150, "min_cluster_mw": 50, "max_k": 10}


def test_pick_adaptive_k_tight_cluster_picks_k1() -> None:
    # All plants within ~10 km of each other — k=1 satisfies radius + MW constraints.
    coords = [(50.0, 4.0), (50.05, 4.05), (49.95, 3.95), (50.02, 4.03)]
    mws = [100.0, 100.0, 100.0, 100.0]  # total 400 MW > min 50
    k = pick_adaptive_k(coords, mws, _SOLAR_TEST_TUNING)
    assert k == 1


def test_pick_adaptive_k_wide_spread_picks_k_gt_1() -> None:
    # Two clusters ~500 km apart — k=1 violates max_radius_km=150, k=2 should fit.
    coords = [
        (50.0, 4.0), (50.1, 4.1), (49.9, 3.9),   # cluster A near Brussels
        (43.0, 12.0), (43.1, 12.1), (42.9, 11.9), # cluster B near Rome
    ]
    mws = [200.0] * 6  # plenty of MW per cluster
    k = pick_adaptive_k(coords, mws, _SOLAR_TEST_TUNING)
    assert k == 2


def test_pick_adaptive_k_insufficient_total_mw_returns_1() -> None:
    # Total MW < min_cluster_mw — spec says fall back to k=1 (country-wide single zone).
    coords = [(50.0, 4.0), (43.0, 12.0)]  # wide spread
    mws = [10.0, 10.0]  # total 20 MW < min 50 — can't split
    k = pick_adaptive_k(coords, mws, _SOLAR_TEST_TUNING)
    assert k == 1


def test_pick_adaptive_k_deterministic_across_runs() -> None:
    # Same input, same random_state=42 → identical k.
    coords = [(50.0, 4.0), (50.05, 4.05), (43.0, 12.0), (43.05, 12.05)]
    mws = [100.0] * 4
    k1 = pick_adaptive_k(coords, mws, _SOLAR_TEST_TUNING)
    k2 = pick_adaptive_k(coords, mws, _SOLAR_TEST_TUNING)
    assert k1 == k2


# ---------------------------------------------------------------------------
# cluster_zones — end-to-end for one (country, tech)
# ---------------------------------------------------------------------------


def _mk_plants_df(rows: list[dict]) -> pd.DataFrame:
    """Helper: build the normalized plants DataFrame the clustering consumes."""
    return pd.DataFrame(
        rows, columns=["country_iso", "tech_type", "capacity_mw", "lat", "lon"]
    )


def test_cluster_zones_single_tight_cluster() -> None:
    plants = _mk_plants_df([
        {"country_iso": "BE", "tech_type": "solar", "capacity_mw": 200, "lat": 50.8, "lon": 4.3},
        {"country_iso": "BE", "tech_type": "solar", "capacity_mw": 150, "lat": 50.85, "lon": 4.35},
        {"country_iso": "BE", "tech_type": "solar", "capacity_mw": 100, "lat": 50.9, "lon": 4.4},
    ])
    zones = cluster_zones(plants, tech_type="solar", tuning=_SOLAR_TEST_TUNING)
    assert len(zones) == 1
    assert zones[0]["weight"] == pytest.approx(1.0)
    assert zones[0]["capacity_mw"] == pytest.approx(450.0)


def test_cluster_zones_weights_sum_to_one() -> None:
    # Two separate clusters — weights should normalize to 1.0.
    plants = _mk_plants_df([
        {"country_iso": "FR", "tech_type": "solar", "capacity_mw": 600, "lat": 50.0, "lon": 4.0},
        {"country_iso": "FR", "tech_type": "solar", "capacity_mw": 300, "lat": 50.1, "lon": 4.1},
        {"country_iso": "FR", "tech_type": "solar", "capacity_mw": 100, "lat": 43.0, "lon": 12.0},
    ])
    zones = cluster_zones(plants, tech_type="solar", tuning=_SOLAR_TEST_TUNING)
    total_weight = sum(z["weight"] for z in zones)
    assert total_weight == pytest.approx(1.0, abs=1e-9)


def test_cluster_zones_empty_plants_returns_empty_list() -> None:
    plants = _mk_plants_df([])
    zones = cluster_zones(plants, tech_type="solar", tuning=_SOLAR_TEST_TUNING)
    assert zones == []


def test_cluster_zones_deterministic() -> None:
    plants = _mk_plants_df([
        {"country_iso": "DE", "tech_type": "solar", "capacity_mw": 100, "lat": 51.0, "lon": 10.0},
        {"country_iso": "DE", "tech_type": "solar", "capacity_mw": 100, "lat": 51.5, "lon": 11.0},
        {"country_iso": "DE", "tech_type": "solar", "capacity_mw": 100, "lat": 48.0, "lon": 9.0},
        {"country_iso": "DE", "tech_type": "solar", "capacity_mw": 100, "lat": 54.0, "lon": 10.0},
    ])
    zones1 = cluster_zones(plants, tech_type="solar", tuning=_SOLAR_TEST_TUNING)
    zones2 = cluster_zones(plants, tech_type="solar", tuning=_SOLAR_TEST_TUNING)
    # Same zone_ids, same lat/lon (rounded), same weights.
    assert [(z["lat"], z["lon"], z["weight"]) for z in zones1] == \
           [(z["lat"], z["lon"], z["weight"]) for z in zones2]


# ---------------------------------------------------------------------------
# build_locations — full pipeline, synthesizes centroid rows + zone rows
# ---------------------------------------------------------------------------


def test_build_locations_emits_centroid_per_country_even_without_plants() -> None:
    """Per the spec: every country always gets a centroid row."""
    # No plants at all — we still expect centroids for all countries.
    plants = _mk_plants_df([])
    country_coords = {
        "BE": (50.5, 4.45, "Belgium"),
        "DE": (51.2, 10.45, "Germany"),
    }
    locations = build_locations(plants, country_coords)
    centroids = [loc for loc in locations if loc[2] == "centroid"]
    assert len(centroids) == 2
    be_centroid = next(loc for loc in centroids if loc[0] == "BE")
    assert be_centroid[1] == "centroid"   # zone_id
    assert be_centroid[3] == 50.5         # lat
    assert be_centroid[4] == 4.45         # lon
    assert be_centroid[5] == 1.0          # weight


def test_build_locations_emits_tech_zones_for_covered_country() -> None:
    plants = _mk_plants_df([
        {"country_iso": "BE", "tech_type": "solar", "capacity_mw": 200, "lat": 50.8, "lon": 4.3},
        {"country_iso": "BE", "tech_type": "solar", "capacity_mw": 100, "lat": 50.9, "lon": 4.4},
    ])
    country_coords = {"BE": (50.5, 4.45, "Belgium")}
    locations = build_locations(plants, country_coords)
    # Exactly one centroid + at least one solar zone for BE.
    be_rows = [loc for loc in locations if loc[0] == "BE"]
    assert any(loc[2] == "centroid" for loc in be_rows)
    assert any(loc[2] == "solar" for loc in be_rows)


def test_build_locations_skips_country_missing_from_coords() -> None:
    # A country present in plants but absent from country_coords is silently
    # skipped — we can't place a centroid without coords.
    plants = _mk_plants_df([
        {"country_iso": "XX", "tech_type": "solar", "capacity_mw": 100, "lat": 0, "lon": 0},
    ])
    country_coords = {"BE": (50.5, 4.45, "Belgium")}
    locations = build_locations(plants, country_coords)
    assert not any(loc[0] == "XX" for loc in locations)


def test_build_locations_zone_weights_sum_to_one_per_country_tech() -> None:
    # DE has 3 solar plants in two far-apart clusters; weights per tech_type
    # must normalize to 1.0 within the country.
    plants = _mk_plants_df([
        {"country_iso": "DE", "tech_type": "solar", "capacity_mw": 400, "lat": 51.0, "lon": 10.0},
        {"country_iso": "DE", "tech_type": "solar", "capacity_mw": 300, "lat": 51.1, "lon": 10.1},
        {"country_iso": "DE", "tech_type": "solar", "capacity_mw": 300, "lat": 48.0, "lon": 9.0},
    ])
    country_coords = {"DE": (51.2, 10.45, "Germany")}
    locations = build_locations(plants, country_coords)
    solar_rows = [loc for loc in locations if loc[0] == "DE" and loc[2] == "solar"]
    assert sum(loc[5] for loc in solar_rows) == pytest.approx(1.0, abs=1e-9)


def test_build_locations_offshore_requires_operating_data() -> None:
    """Per design: offshore zones are only emitted for countries where GEM has
    at least one plant flagged 'wind_offshore' (fed from operating data upstream
    by the loader). If plants for a country only have wind_onshore rows, no
    wind_offshore zones appear."""
    plants = _mk_plants_df([
        {"country_iso": "PL", "tech_type": "wind_onshore", "capacity_mw": 500,
         "lat": 52.0, "lon": 19.0},
    ])
    country_coords = {"PL": (51.9, 19.15, "Poland")}
    locations = build_locations(plants, country_coords)
    assert not any(loc[0] == "PL" and loc[2] == "wind_offshore" for loc in locations)


# ---------------------------------------------------------------------------
# End-to-end smoke test — real GEM xlsx if available
# ---------------------------------------------------------------------------

_GEM_WIND_PATH = Path("C:/Code/able/data") / GEM_WIND_FILE
_GEM_SOLAR_PATH = Path("C:/Code/able/data") / GEM_SOLAR_FILE


@pytest.mark.skipif(
    not _GEM_WIND_PATH.exists() or not _GEM_SOLAR_PATH.exists(),
    reason="GEM xlsx files not present — end-to-end test skipped",
)
def test_load_gem_plants_smoke() -> None:
    """Sanity-check the real loader — returns BE + DE rows with expected tech types."""
    from scripts.build_weather_locations import load_gem_plants

    plants = load_gem_plants(_GEM_WIND_PATH, _GEM_SOLAR_PATH)
    # Schema contract.
    assert set(plants.columns) >= {"country_iso", "tech_type", "capacity_mw", "lat", "lon"}
    # BE and DE both exist in GEM.
    assert not plants[plants["country_iso"] == "BE"].empty
    assert not plants[plants["country_iso"] == "DE"].empty
    # Only operating + valid-coord + EU39 rows survive.
    assert plants["lat"].notna().all()
    assert plants["lon"].notna().all()
    assert plants["capacity_mw"].gt(0).all()
    # All three tech_types appear somewhere (solar, wind_onshore, wind_offshore).
    assert set(plants["tech_type"]) == {"solar", "wind_onshore", "wind_offshore"}
