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
