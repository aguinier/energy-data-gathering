#!/usr/bin/env python3
"""Phase 1 migration: add zone_type + capacity_mw columns to weather_location.

Idempotent — safe to rerun. Uses ALTER TABLE ADD COLUMN (online, O(1) per
SQLite docs) plus an UPDATE backfill for the 5 pre-existing BE rows.

Usage:
    python scripts/migrate_add_zone_type.py [--db PATH]

Per CLAUDE.md `EXTENDING.md` rules:
  1. Backup prod DB before running.
  2. Test on a /tmp/scratch.db copy first.
  3. Verify with `init_weather_observation.py --verify` after.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def _index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?", (index_name,)
    )
    return cursor.fetchone() is not None


def migrate(conn: sqlite3.Connection) -> None:
    """Apply the Phase 1 migration. Idempotent."""
    cursor = conn.cursor()

    # Step 1: ALTER TABLE ADD COLUMN (skip if already added).
    if not _column_exists(conn, "weather_location", "zone_type"):
        cursor.execute("ALTER TABLE weather_location ADD COLUMN zone_type TEXT")
    if not _column_exists(conn, "weather_location", "capacity_mw"):
        cursor.execute("ALTER TABLE weather_location ADD COLUMN capacity_mw REAL")

    # NOTE — non-atomic DDL+DML: Python's sqlite3 module issues an implicit COMMIT
    # before each DDL statement (ALTER TABLE), so the two ADD COLUMN statements above
    # each commit immediately and are NOT part of the transaction closed by conn.commit()
    # below. If the process dies between the ALTERs and the UPDATEs the columns exist
    # but zone_type is NULL; on rerun _column_exists() skips the ALTERs and the UPDATE
    # backfill re-applies the same values idempotently, leaving the DB in a clean state.

    # Step 2: Backfill BE rows. Idempotent — reapplies same value if already set.
    cursor.execute(
        "UPDATE weather_location SET zone_type = 'centroid' "
        "WHERE country_code = 'BE' AND zone_id = 'centroid'"
    )
    cursor.execute(
        "UPDATE weather_location SET zone_type = 'solar' "
        "WHERE country_code = 'BE' AND zone_id IN ('central', 'north', 'south', 'east')"
    )

    # Step 3: Create the zone_type index (skip if exists).
    if not _index_exists(conn, "idx_weather_location_zone_type"):
        cursor.execute(
            "CREATE INDEX idx_weather_location_zone_type "
            "ON weather_location(zone_type)"
        )

    conn.commit()


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
    print("Applying migration: add zone_type + capacity_mw to weather_location")
    with sqlite3.connect(db_path) as conn:
        migrate(conn)
    print("OK: migration complete (idempotent — safe to rerun)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
