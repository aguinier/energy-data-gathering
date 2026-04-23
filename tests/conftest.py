"""Shared pytest fixtures for the energy-data-gathering test suite."""

from __future__ import annotations

import sqlite3
from typing import Iterator

import pytest


@pytest.fixture
def in_memory_db() -> Iterator[sqlite3.Connection]:
    """Empty in-memory SQLite DB. Tests are responsible for creating their own schema."""
    conn = sqlite3.connect(":memory:")
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def seeded_observation_db(in_memory_db: sqlite3.Connection) -> sqlite3.Connection:
    """In-memory DB with the current production weather_observation schema applied + BE seed rows.

    Use this when testing migration scripts or coherence checks against the
    pre-Phase-1 schema state. Post-Phase-1 schema is the responsibility of the
    test that exercises the migration.
    """
    from src.weather_schema import (
        LOCATIONS,
        OPEN_METEO_SOURCES,
        ALL_SCHEMA_SQL,
    )

    cursor = in_memory_db.cursor()
    for stmt in ALL_SCHEMA_SQL:
        cursor.execute(stmt)
    cursor.executemany(
        "INSERT OR IGNORE INTO weather_location "
        "(country_code, zone_id, zone_type, lat, lon, weight, capacity_mw, description) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        LOCATIONS,
    )
    cursor.executemany(
        "INSERT OR IGNORE INTO weather_source "
        "(provider, model_id, lead_time_hours, description) "
        "VALUES (?, ?, ?, ?)",
        OPEN_METEO_SOURCES,
    )
    in_memory_db.commit()
    return in_memory_db
