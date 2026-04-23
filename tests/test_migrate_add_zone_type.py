"""Tests for scripts/migrate_add_zone_type.py."""

from __future__ import annotations

import sqlite3

import pytest


# Pre-Phase-1 fixture: schema as it lived before this migration (no zone_type/capacity_mw).
@pytest.fixture
def pre_migration_db() -> sqlite3.Connection:
    """In-memory DB with the OLD weather_location schema (no zone_type / capacity_mw)."""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE weather_location (
            location_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code  TEXT    NOT NULL,
            zone_id       TEXT    NOT NULL,
            lat           REAL    NOT NULL,
            lon           REAL    NOT NULL,
            weight        REAL,
            description   TEXT,
            created_at    TEXT    DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(country_code, zone_id)
        )
        """
    )
    # Seed the 5 pre-Phase-1 BE rows (matching what prod has today).
    pre_phase1_rows = [
        ("BE", "centroid", 50.5, 4.45, None, "Able centroid"),
        ("BE", "central",  50.8, 4.3,  0.40, "Central Belgium"),
        ("BE", "north",    51.1, 4.8,  0.30, "Northern Belgium"),
        ("BE", "south",    50.4, 4.0,  0.20, "Southern Belgium"),
        ("BE", "east",     50.2, 5.5,  0.10, "Eastern Belgium"),
    ]
    cursor.executemany(
        "INSERT INTO weather_location "
        "(country_code, zone_id, lat, lon, weight, description) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        pre_phase1_rows,
    )
    conn.commit()
    return conn


def test_migration_adds_zone_type_and_capacity_mw_columns(pre_migration_db) -> None:
    from scripts.migrate_add_zone_type import migrate

    migrate(pre_migration_db)

    cursor = pre_migration_db.cursor()
    cursor.execute("PRAGMA table_info(weather_location)")
    col_names = {row[1] for row in cursor.fetchall()}
    assert "zone_type" in col_names
    assert "capacity_mw" in col_names


def test_migration_backfills_existing_be_rows(pre_migration_db) -> None:
    from scripts.migrate_add_zone_type import migrate

    migrate(pre_migration_db)

    cursor = pre_migration_db.cursor()
    cursor.execute(
        "SELECT zone_id, zone_type FROM weather_location WHERE country_code = 'BE' "
        "ORDER BY zone_id"
    )
    rows = cursor.fetchall()
    assert ("centroid", "centroid") in rows
    assert ("central", "solar") in rows
    assert ("north", "solar") in rows
    assert ("south", "solar") in rows
    assert ("east", "solar") in rows


def test_migration_creates_zone_type_index(pre_migration_db) -> None:
    from scripts.migrate_add_zone_type import migrate

    migrate(pre_migration_db)

    cursor = pre_migration_db.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='weather_location'"
    )
    index_names = {row[0] for row in cursor.fetchall()}
    assert "idx_weather_location_zone_type" in index_names


def test_migration_is_idempotent(pre_migration_db) -> None:
    from scripts.migrate_add_zone_type import migrate

    migrate(pre_migration_db)
    # Second run must not raise (e.g., "duplicate column name") and must not corrupt data.
    migrate(pre_migration_db)

    cursor = pre_migration_db.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather_location")
    n = cursor.fetchone()[0]
    assert n == 5  # No duplicates introduced.
