#!/usr/bin/env python3
"""One-shot backfill of weather_observation from helio's per-zone CSVs.

Reads ``weather_nwp_{model}_day{1,3}_zones_{start}_{end}.csv`` from
``helioforge/tools/`` and inserts one row per (zone, hourly target) into
``weather_observation``. `fetched_at` is set to the CSV's mtime — a best
approximation for historical data since we don't know when the original
fetch happened. `forecast_run_time` is NULL (Open-Meteo doesn't expose
NWP init times).

Scope per the plan: backfill **2024-01-01 onwards only** (when per-model
NWP archives start). Earlier ERA5 history is kept in helio's flat CSVs.

**Known partial coverage — day3 zones files:** Helio's day3 zones CSVs
only contain the `south` zone due to a pre-existing helio-side pipeline
issue (the day1 equivalents have all 4 zones). This backfill therefore
brings in full day1 history (4 zones × 4 models × 2.3 years) but only
south-zone day3 history. Going forward, Phase 2's 3×/day cron populates
all zones × all models × {day1, day3} from fresh Open-Meteo calls, so
the gap narrows with every tick.

Usage:
    # Default — reads files from default path, 2024-01-01 onwards
    python scripts/backfill_weather_observation.py

    # Custom helio-tools path
    python scripts/backfill_weather_observation.py --helio-tools /path/to/tools

    # Dry run — count rows, don't insert
    python scripts/backfill_weather_observation.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: F401
import utils
from src import db
from src.weather_schema import OPENMETEO_TO_DB, WEATHER_VARIABLE_COLUMNS


NWP_MODELS = ("best_match", "ecmwf_ifs025", "gfs_seamless", "icon_seamless")
LEAD_DAYS = (1, 3)

BATCH_SIZE = 5000


log = logging.getLogger("entsoe_pipeline")


def _convert_for_backfill(api_var: str, value):
    """Mirror fetch_weather_observation._convert_value."""
    if value is None or pd.isna(value):
        return None
    if api_var == "relative_humidity_2m":
        return value / 100.0
    if api_var.startswith("cloud_cover"):
        return value / 100.0
    return value


def _find_zones_csv(helio_tools: Path, model: str, lead: int) -> Path | None:
    """Locate the widest-range zones CSV for (model, lead) in helio_tools/.

    Picks the file with the largest date span encoded in its name. Returns
    None if no match.

    Legacy naming quirk: best_match's CSVs omit the model prefix (since it
    was Open-Meteo's default when those were first fetched), so the file
    is `weather_nwp_day1_zones_*.csv` rather than `weather_nwp_best_match_day1_zones_*.csv`.
    """
    patterns = [f"weather_nwp_{model}_day{lead}_zones_*.csv"]
    if model == "best_match":
        patterns.append(f"weather_nwp_day{lead}_zones_*.csv")
    candidates: list[Path] = []
    for p in patterns:
        candidates.extend(helio_tools.glob(p))
    # De-duplicate (in case a file matches both patterns — shouldn't happen for best_match
    # since its explicit-prefix file doesn't exist, but defense-in-depth).
    candidates = sorted(set(candidates))
    if not candidates:
        return None

    def _span(p: Path) -> int:
        import re
        m = re.search(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})", p.name)
        if not m:
            return 0
        try:
            return (pd.Timestamp(m.group(2)) - pd.Timestamp(m.group(1))).days
        except ValueError:
            return 0

    return max(candidates, key=_span)


def _load_location_map() -> dict[str, int]:
    """zone_id → location_id for BE (excludes centroid)."""
    with db.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT zone_id, location_id FROM weather_location
            WHERE country_code = 'BE' AND zone_id != 'centroid'
            """
        ).fetchall()
    return {r["zone_id"]: r["location_id"] for r in rows}


def _lookup_source_id(model: str, lead_hours: int) -> int:
    with db.get_connection() as conn:
        row = conn.execute(
            """
            SELECT source_id FROM weather_source
            WHERE provider = 'open_meteo_previous_runs'
              AND model_id = ? AND lead_time_hours = ?
            """,
            (model, lead_hours),
        ).fetchone()
    if not row:
        raise RuntimeError(
            f"No source row for previous_runs/{model}/{lead_hours}h — run init_weather_observation.py first."
        )
    return row["source_id"]


def _backfill_one(
    csv_path: Path,
    model: str,
    lead_hours: int,
    location_map: dict[str, int],
    min_date: pd.Timestamp | None,
    dry_run: bool,
) -> int:
    """Load one CSV, decimate to hourly, upsert into weather_observation."""
    source_id = _lookup_source_id(model, lead_hours)
    fetched_at_dt = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)
    fetched_at_str = fetched_at_dt.isoformat(timespec="seconds")

    log.info(
        "Loading %s (source_id=%d, fetched_at=%s from mtime)",
        csv_path.name, source_id, fetched_at_str,
    )

    df = pd.read_csv(csv_path, parse_dates=["timestamp_utc"])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    # Decimate to hourly (keep only minute==0 rows)
    df = df[df["timestamp_utc"].dt.minute == 0]
    if min_date is not None:
        df = df[df["timestamp_utc"] >= min_date]
    log.info("  %d hourly rows after filter", len(df))

    if df.empty:
        return 0

    # Build insert tuples.
    cols = [
        "source_id", "location_id", "valid_at", "forecast_run_time", "fetched_at",
    ] + WEATHER_VARIABLE_COLUMNS
    placeholders = ",".join("?" * len(cols))
    sql = (
        f"INSERT OR IGNORE INTO weather_observation ({','.join(cols)}) "
        f"VALUES ({placeholders})"
    )

    # Pre-compute the api_var -> db_col -> csv_col mapping.
    api_vars_present = [c for c in OPENMETEO_TO_DB if c in df.columns]

    rows_to_insert: list[tuple] = []
    for _, r in df.iterrows():
        zone_id = r.get("zone_id")
        loc_id = location_map.get(zone_id)
        if loc_id is None:
            continue  # zone we don't track
        valid_at = r["timestamp_utc"].isoformat(timespec="seconds").replace("+00:00", "Z")
        row_values = [source_id, loc_id, valid_at, None, fetched_at_str]
        for db_col in WEATHER_VARIABLE_COLUMNS:
            # Reverse-map db_col to api_var
            api_var = next(
                (av for av, dc in OPENMETEO_TO_DB.items() if dc == db_col), None
            )
            if api_var is None or api_var not in api_vars_present:
                row_values.append(None)
                continue
            val = r.get(api_var)
            row_values.append(_convert_for_backfill(api_var, val))
        rows_to_insert.append(tuple(row_values))

    if dry_run:
        log.info("  DRY RUN: would insert %d rows", len(rows_to_insert))
        return len(rows_to_insert)

    inserted = 0
    with db.get_connection() as conn:
        cursor = conn.cursor()
        for i in range(0, len(rows_to_insert), BATCH_SIZE):
            chunk = rows_to_insert[i : i + BATCH_SIZE]
            cursor.executemany(sql, chunk)
            inserted += cursor.rowcount
            if (i // BATCH_SIZE) % 10 == 0:
                log.info("  ...inserted %d/%d", i + len(chunk), len(rows_to_insert))
    log.info("  Inserted %d rows (duplicates skipped by PK)", inserted)
    return inserted


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--helio-tools",
        default=os.environ.get(
            "HELIO_TOOLS_DIR",
            r"C:\Code\helio\helioforge\tools",
        ),
        help="Path to helioforge/tools/ directory with CSV source files.",
    )
    p.add_argument(
        "--min-date",
        default="2024-01-01",
        help="Only backfill rows with valid_at >= this date (default: 2024-01-01).",
    )
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    utils.setup_logging()

    helio_tools = Path(args.helio_tools)
    if not helio_tools.is_dir():
        print(f"ERROR: helio tools dir not found: {helio_tools}")
        return 2

    db.create_weather_observation_tables()
    location_map = _load_location_map()
    if not location_map:
        print("ERROR: no BE zones in weather_location. Run init_weather_observation.py.")
        return 2
    log.info("Location map (zone_id -> location_id): %s", location_map)

    min_date = pd.Timestamp(args.min_date, tz="UTC") if args.min_date else None

    total = 0
    for model in NWP_MODELS:
        for lead_days in LEAD_DAYS:
            csv_path = _find_zones_csv(helio_tools, model, lead_days)
            if csv_path is None:
                log.warning(
                    "No CSV found for %s day%d in %s — skipping",
                    model, lead_days, helio_tools,
                )
                continue
            n = _backfill_one(
                csv_path, model, lead_days * 24,
                location_map, min_date, args.dry_run,
            )
            total += n

    print()
    print(f"{'Dry run — ' if args.dry_run else ''}Backfill total: {total} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
