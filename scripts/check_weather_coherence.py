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

from src.weather_schema import OPEN_METEO_SOURCES


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
