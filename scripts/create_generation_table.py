#!/usr/bin/env python3
"""Idempotent bootstrap for the `energy_generation` table.

Creates `energy_generation` (+ indexes) to hold the *complete* ENTSO-E A75
"generation per production type" document -- nuclear and fossil types
included -- instead of the 8-column renewable-only subset that
`energy_renewable` has always held.

Safe to run on prod or locally any number of times -- `CREATE TABLE IF NOT
EXISTS` / `CREATE INDEX IF NOT EXISTS` make it a no-op after the first
success. Never touches `energy_renewable`: its schema, mapping and values
are frozen by this plan.

Usage:
    python scripts/create_generation_table.py            # create + verify
    python scripts/create_generation_table.py --verify    # verify only

Point ENERGY_DB_PATH at a scratch copy to test before touching the replica
or prod:
    ENERGY_DB_PATH=/path/to/scratch.db python scripts/create_generation_table.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
import utils
from src import db

EXPECTED_COLUMNS = {
    "id",
    "country_code",
    "timestamp_utc",
    # renewables (mirrors energy_renewable's semantics, plus a standalone
    # pumped-storage column -- see create_generation_table()'s docstring)
    "solar_mw",
    "wind_onshore_mw",
    "wind_offshore_mw",
    "hydro_run_mw",
    "hydro_reservoir_mw",
    "hydro_pumped_mw",
    "biomass_mw",
    "geothermal_mw",
    "marine_mw",
    "other_renewable_mw",
    "energy_storage_mw",
    # everything the renewable-only mapping discards
    "nuclear_mw",
    "fossil_gas_mw",
    "fossil_hard_coal_mw",
    "fossil_brown_coal_mw",
    "fossil_oil_mw",
    "fossil_oil_shale_mw",
    "fossil_peat_mw",
    "fossil_coal_derived_gas_mw",
    "waste_mw",
    "other_mw",
    # metadata
    "data_quality",
    "fetched_at",
    "publication_timestamp_utc",
}

EXPECTED_INDEXES = {"idx_generation_country_time", "idx_generation_time"}


def _verify() -> int:
    """Return 0 on pass, non-zero on fail. Prints diagnostics either way."""
    with db.get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        present_tables = {row[0] for row in cursor.fetchall()}
        if "energy_generation" not in present_tables:
            print("FAIL: energy_generation table is missing")
            return 1

        cursor.execute("PRAGMA table_info(energy_generation)")
        columns = cursor.fetchall()
        col_names = {c[1] for c in columns}
        missing_cols = EXPECTED_COLUMNS - col_names
        if missing_cols:
            print(f"FAIL: missing columns: {sorted(missing_cols)}")
            return 1

        # The whole point of this table: no column may default to 0. A
        # country that doesn't report a type must read NULL, not 0.
        zero_defaults = [
            c[1] for c in columns
            if c[4] is not None and str(c[4]).strip() in ("0", "0.0")
        ]
        if zero_defaults:
            print(f"FAIL: columns with a DEFAULT 0 (must be NULL-default): {zero_defaults}")
            return 1

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name='energy_generation'"
        )
        idx_names = {row[0] for row in cursor.fetchall()}
        missing_idx = EXPECTED_INDEXES - idx_names
        if missing_idx:
            print(f"FAIL: missing indexes: {sorted(missing_idx)}")
            return 1

        # Confirm the unique index is actually unique (not just present).
        cursor.execute("PRAGMA index_info(idx_generation_country_time)")
        idx_cols = [row[2] for row in cursor.fetchall()]
        if idx_cols != ["country_code", "timestamp_utc"]:
            print(f"FAIL: idx_generation_country_time covers {idx_cols}, expected ['country_code', 'timestamp_utc']")
            return 1

        # energy_renewable must be untouched by this script.
        if "energy_renewable" in present_tables:
            cursor.execute("PRAGMA table_info(energy_renewable)")
            renewable_cols = {c[1] for c in cursor.fetchall()}
            print(f"energy_renewable: present, {len(renewable_cols)} columns (unmodified)")
        else:
            print("energy_renewable: not present in this DB (expected for a fresh scratch copy)")

        print(f"energy_generation: present, {len(col_names)} columns")
        print(f"energy_generation indexes: {sorted(idx_names)}")

        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Skip CREATE; only verify schema.",
    )
    args = parser.parse_args()

    utils.setup_logging()

    print(f"Database: {config.DATABASE_PATH}")

    if not args.verify:
        db.create_generation_table()
        print("OK: energy_generation schema applied")

    return _verify()


if __name__ == "__main__":
    sys.exit(main())
