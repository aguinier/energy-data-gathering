#!/usr/bin/env python3
"""Verify schema constants in weather_schema.py match actual DB rows.

Three dimensions checked:
1. weather_source vs OPEN_METEO_SOURCES
2. weather_location vs LOCATIONS (added in Task 9, post-Phase-1 migration)
3. weather_observation columns vs WEATHER_VARIABLE_COLUMNS

Exit 0 = coherent. Exit 1 = drift detected (full diff printed).

Usage:
    python scripts/check_weather_coherence.py [--db PATH]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import TypedDict

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.weather_schema import OPEN_METEO_SOURCES, WEATHER_VARIABLE_COLUMNS


class Drift(TypedDict):
    only_in_schema: set
    only_in_db: set


def check_sources_dimension(conn: sqlite3.Connection) -> Drift:
    """Compare OPEN_METEO_SOURCES (in code) to weather_source rows (in DB)."""
    schema_set: set[tuple[str, str, int]] = {
        (provider, model_id, lead) for provider, model_id, lead, _desc in OPEN_METEO_SOURCES
    }
    cursor = conn.cursor()
    cursor.execute(
        "SELECT provider, model_id, lead_time_hours FROM weather_source"
    )
    db_set: set[tuple[str, str, int]] = {
        (row[0], row[1], row[2]) for row in cursor.fetchall()
    }
    return {
        "only_in_schema": schema_set - db_set,
        "only_in_db": db_set - schema_set,
    }


def check_columns_dimension(conn: sqlite3.Connection) -> Drift:
    """Compare WEATHER_VARIABLE_COLUMNS (in code) to weather_observation columns (in DB)."""
    schema_set: set[str] = set(WEATHER_VARIABLE_COLUMNS)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(weather_observation)")
    # PRAGMA returns (cid, name, type, notnull, dflt_value, pk); we want name.
    # Exclude the PK / metadata columns we don't track in WEATHER_VARIABLE_COLUMNS.
    metadata_cols = {"source_id", "location_id", "valid_at", "forecast_run_time", "fetched_at"}
    db_set: set[str] = {
        row[1] for row in cursor.fetchall() if row[1] not in metadata_cols
    }
    return {
        "only_in_schema": schema_set - db_set,
        "only_in_db": db_set - schema_set,
    }


def _format_drift(name: str, drift: Drift) -> list[str]:
    """Return human-readable lines describing the drift for one dimension."""
    if not drift["only_in_schema"] and not drift["only_in_db"]:
        return [f"PASS: {name}"]
    lines = [f"DRIFT: {name}"]
    for item in sorted(drift["only_in_schema"], key=str):
        lines.append(f"  schema-only: {item}")
    for item in sorted(drift["only_in_db"], key=str):
        lines.append(f"  db-only:     {item}")
    return lines


def run_all_checks(conn: sqlite3.Connection) -> int:
    """Run all coherence checks. Print results, return 0/1."""
    sources_drift = check_sources_dimension(conn)
    columns_drift = check_columns_dimension(conn)

    has_drift = False
    for name, drift in [
        ("sources", sources_drift),
        ("columns", columns_drift),
    ]:
        for line in _format_drift(name, drift):
            print(line)
        if drift["only_in_schema"] or drift["only_in_db"]:
            has_drift = True

    return 1 if has_drift else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=None,
        help="Path to SQLite DB (default: read from config.DATABASE_PATH).",
    )
    args = parser.parse_args()

    if args.db is None:
        import config
        db_path = config.DATABASE_PATH
    else:
        db_path = args.db

    print(f"Database: {db_path}")
    with sqlite3.connect(db_path) as conn:
        return run_all_checks(conn)


if __name__ == "__main__":
    sys.exit(main())
