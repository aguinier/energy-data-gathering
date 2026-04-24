"""Tests for scripts/check_weather_coherence.py."""

from __future__ import annotations

import pytest

from scripts.check_weather_coherence import check_sources_dimension


def test_check_sources_dimension_passes_when_db_matches_schema(
    seeded_observation_db,
) -> None:
    drift = check_sources_dimension(seeded_observation_db)
    assert drift == {"only_in_schema": set(), "only_in_db": set()}


def test_check_sources_dimension_detects_extra_db_row(
    seeded_observation_db,
) -> None:
    cursor = seeded_observation_db.cursor()
    cursor.execute(
        "INSERT INTO weather_source (provider, model_id, lead_time_hours, description) "
        "VALUES ('rogue_provider', 'rogue_model', 99, 'should-not-be-here')"
    )
    seeded_observation_db.commit()

    drift = check_sources_dimension(seeded_observation_db)
    assert drift["only_in_db"] == {("rogue_provider", "rogue_model", 99)}
    assert drift["only_in_schema"] == set()


def test_check_sources_dimension_detects_missing_db_row(
    seeded_observation_db,
) -> None:
    cursor = seeded_observation_db.cursor()
    # Delete the era5 archive source.
    cursor.execute(
        "DELETE FROM weather_source WHERE provider = 'open_meteo_archive'"
    )
    seeded_observation_db.commit()

    drift = check_sources_dimension(seeded_observation_db)
    assert drift["only_in_schema"] == {("open_meteo_archive", "era5", 0)}
    assert drift["only_in_db"] == set()


from scripts.check_weather_coherence import check_columns_dimension


def test_check_columns_dimension_passes_when_db_matches_schema(
    seeded_observation_db,
) -> None:
    drift = check_columns_dimension(seeded_observation_db)
    assert drift == {"only_in_schema": set(), "only_in_db": set()}


def test_check_columns_dimension_detects_extra_db_column(
    seeded_observation_db,
) -> None:
    cursor = seeded_observation_db.cursor()
    cursor.execute(
        "ALTER TABLE weather_observation ADD COLUMN rogue_column REAL"
    )
    seeded_observation_db.commit()

    drift = check_columns_dimension(seeded_observation_db)
    assert drift["only_in_db"] == {"rogue_column"}
    assert drift["only_in_schema"] == set()


from scripts.check_weather_coherence import run_all_checks


def test_run_all_checks_returns_zero_on_clean_db(seeded_observation_db, capsys) -> None:
    exit_code = run_all_checks(seeded_observation_db)
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "PASS" in captured.out


def test_run_all_checks_returns_one_on_drift(seeded_observation_db, capsys) -> None:
    cursor = seeded_observation_db.cursor()
    cursor.execute(
        "INSERT INTO weather_source (provider, model_id, lead_time_hours, description) "
        "VALUES ('rogue', 'rogue', 1, 'drift')"
    )
    seeded_observation_db.commit()

    exit_code = run_all_checks(seeded_observation_db)
    captured = capsys.readouterr()
    assert exit_code == 1
    assert "DRIFT" in captured.out
    assert "rogue" in captured.out


from scripts.check_weather_coherence import check_locations_dimension


def test_check_locations_dimension_passes_when_db_matches_schema(
    seeded_observation_db,
) -> None:
    drift = check_locations_dimension(seeded_observation_db)
    assert drift == {"only_in_schema": set(), "only_in_db": set()}


def test_check_locations_dimension_detects_extra_db_row(
    seeded_observation_db,
) -> None:
    cursor = seeded_observation_db.cursor()
    cursor.execute(
        "INSERT INTO weather_location "
        "(country_code, zone_id, zone_type, lat, lon, weight, capacity_mw, description) "
        "VALUES ('ZZ', 'rogue', 'solar', 0, 0, 1.0, 100, 'drift')"
    )
    seeded_observation_db.commit()

    drift = check_locations_dimension(seeded_observation_db)
    assert drift["only_in_db"] == {("ZZ", "rogue", "solar")}
    assert drift["only_in_schema"] == set()


def test_check_locations_dimension_detects_missing_db_row(
    seeded_observation_db,
) -> None:
    cursor = seeded_observation_db.cursor()
    # Delete the BE centroid row — centroid exists for every country in LOCATIONS
    # so this test stays stable as the autogen block regenerates.
    cursor.execute(
        "DELETE FROM weather_location WHERE country_code = 'BE' AND zone_id = 'centroid'"
    )
    seeded_observation_db.commit()

    drift = check_locations_dimension(seeded_observation_db)
    assert drift["only_in_schema"] == {("BE", "centroid", "centroid")}
    assert drift["only_in_db"] == set()
