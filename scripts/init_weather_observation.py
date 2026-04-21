#!/usr/bin/env python3
"""Idempotent bootstrap for the versioned weather observation tables.

Creates `weather_location`, `weather_source`, and `weather_observation`
(+ indexes) and seeds the Belgian location + Open-Meteo source dimensions.

Safe to run on prod or locally any number of times — `CREATE TABLE IF NOT
EXISTS` + `INSERT OR IGNORE` make it a no-op after the first success.

Usage:
    python scripts/init_weather_observation.py            # run + verify
    python scripts/init_weather_observation.py --verify   # verify only

Intended to be invoked once per deployment (e.g. by the entrypoint or
a one-off `docker compose run` on prod).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
import utils
from src import db
from src.weather_schema import BE_LOCATIONS, OPEN_METEO_SOURCES


def _verify() -> int:
    """Return 0 on pass, non-zero on fail. Prints diagnostics either way."""
    with db.get_connection() as conn:
        cursor = conn.cursor()

        # Check tables exist.
        expected = {"weather_location", "weather_source", "weather_observation"}
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        present = {row[0] for row in cursor.fetchall()}
        missing = expected - present
        if missing:
            print(f"FAIL: missing tables: {sorted(missing)}")
            return 1

        # Check dimension row counts.
        cursor.execute(
            "SELECT COUNT(*) FROM weather_location WHERE country_code = 'BE'"
        )
        n_loc = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM weather_source")
        n_src = cursor.fetchone()[0]

        print("Tables present: " + ", ".join(sorted(expected)))
        print(f"weather_location (BE): {n_loc} rows (expected {len(BE_LOCATIONS)})")
        print(f"weather_source:        {n_src} rows (expected {len(OPEN_METEO_SOURCES)})")

        if n_loc < len(BE_LOCATIONS):
            print("WARN: fewer BE locations seeded than expected")
        if n_src < len(OPEN_METEO_SOURCES):
            print("WARN: fewer sources seeded than expected")

        # Verify the replay index exists.
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name='weather_observation'"
        )
        idx_names = [row[0] for row in cursor.fetchall()]
        print(f"weather_observation indexes: {idx_names}")

        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Skip CREATE/INSERT; only verify schema + seed counts.",
    )
    args = parser.parse_args()

    utils.setup_logging()

    print(f"Database: {config.DATABASE_PATH}")

    if not args.verify:
        db.create_weather_observation_tables()
        print("OK: weather_observation schema + seeds applied")

    return _verify()


if __name__ == "__main__":
    sys.exit(main())
