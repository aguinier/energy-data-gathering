#!/usr/bin/env python3
"""ERA5 historical backfill for `weather_observation` (Phase 3).

Pulls the Open-Meteo Archive API for each `weather_location` row over a
configurable date range (default: 2024-01-01 → today). Chunks the date
range into 90-day windows, batches locations at
``MAX_LOCATIONS_PER_CALL``, re-uses a single ``fetched_at`` timestamp so
re-runs are idempotent.

**Resumability** — `--resume` skips locations whose
``MAX(valid_at)`` in `weather_observation` for the ERA5 source is
already within ``RESUME_TAIL_DAYS`` of ``--end``. Useful if the backfill
gets interrupted mid-run.

Usage:
    # Full backfill since 2024-01-01 for all locations
    python scripts/backfill_weather_observation_era5.py

    # Centroid-first rollout: just country centroids
    python scripts/backfill_weather_observation_era5.py --zone-type centroid

    # One country (e.g. FR) — useful for smoke-testing on a small subset
    python scripts/backfill_weather_observation_era5.py --countries FR

    # Resume after interruption (skip already-loaded locations)
    python scripts/backfill_weather_observation_era5.py --resume \\
        --fetched-at 2026-04-24T09:00:00+00:00

    # Dry run — print the plan + projected API call count
    python scripts/backfill_weather_observation_era5.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: F401 — loads env
import utils
from src import db
from src.fetch_weather_observation import (
    MAX_LOCATIONS_PER_CALL,
    _get_all_locations,
    _get_source_id,
    fetch_archive_era5,
)

log = logging.getLogger("entsoe_pipeline")

DEFAULT_CHUNK_DAYS = 90
INTER_CALL_SLEEP_SECS = 2.0
RESUME_TAIL_DAYS = 7


def _chunk_range(start: datetime, end: datetime, days: int):
    """Yield ``(chunk_start, chunk_end)`` date strings, max ``days`` apart."""
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        yield cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")
        cur = nxt + timedelta(days=1)


def _filter_resumable(
    locations: list[dict], target_end_iso: str, era5_source_id: int,
) -> list[dict]:
    """Drop locations whose latest ERA5 valid_at is within RESUME_TAIL_DAYS of target_end."""
    target_end = datetime.strptime(target_end_iso, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    resume_cutoff = (target_end - timedelta(days=RESUME_TAIL_DAYS)).isoformat()
    remaining: list[dict] = []
    skipped = 0
    with db.get_connection() as conn:
        for loc in locations:
            row = conn.execute(
                """
                SELECT MAX(valid_at) FROM weather_observation
                WHERE source_id = ? AND location_id = ?
                """,
                (era5_source_id, loc["location_id"]),
            ).fetchone()
            latest = row[0] if row else None
            if latest is not None and latest >= resume_cutoff:
                skipped += 1
            else:
                remaining.append(loc)
    log.info("Resume filter: %d locations already at ERA5 MAX(valid_at) >= %s; "
             "%d remain to backfill.", skipped, resume_cutoff, len(remaining))
    return remaining


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--start", default="2024-01-01",
                   help="Backfill window start (default: 2024-01-01).")
    p.add_argument("--end", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                   help="Backfill window end (default: today UTC).")
    p.add_argument("--fetched-at", default=None,
                   help="Fixed fetched_at ISO timestamp (default: script start). "
                        "Re-use on resume for idempotency.")
    p.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS,
                   help=f"Days per API call (default: {DEFAULT_CHUNK_DAYS}).")
    p.add_argument("--countries", default=None,
                   help="Comma-separated country_code filter (e.g. 'BE,FR').")
    p.add_argument("--zone-type", default=None,
                   help="Filter to one zone_type (e.g. 'centroid' for "
                        "centroid-first staged rollout).")
    p.add_argument("--resume", action="store_true",
                   help="Skip locations already at MAX(valid_at) >= end - "
                        f"{RESUME_TAIL_DAYS}d for the ERA5 source.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the plan without calling the API.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    utils.setup_logging()

    db.create_weather_observation_tables()

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    if args.fetched_at:
        fetched_at_dt = datetime.fromisoformat(args.fetched_at)
    else:
        fetched_at_dt = datetime.now(timezone.utc)

    # Resolve locations + optional filters.
    countries = (tuple(args.countries.split(","))
                 if args.countries else (None,))
    locations: list[dict] = []
    for c in countries:
        locations.extend(_get_all_locations(
            zone_type_filter=args.zone_type, country_filter=c,
        ))

    if args.resume and locations:
        era5_source_id = _get_source_id("open_meteo_archive", "era5", 0)
        locations = _filter_resumable(locations, args.end, era5_source_id)

    if not locations:
        log.error("No locations match the filters — nothing to backfill.")
        return 1

    chunks = list(_chunk_range(start_dt, end_dt, args.chunk_days))
    batches_per_chunk = max(
        1, (len(locations) + MAX_LOCATIONS_PER_CALL - 1) // MAX_LOCATIONS_PER_CALL
    )
    total_calls = len(chunks) * batches_per_chunk

    log.info("Backfill window: %s .. %s (chunk=%dd, %d chunks)",
             args.start, args.end, args.chunk_days, len(chunks))
    log.info("Locations: %d (zone_type=%s, countries=%s), %d batch(es)/chunk",
             len(locations), args.zone_type or "all",
             args.countries or "all", batches_per_chunk)
    log.info("fetched_at: %s", fetched_at_dt.isoformat(timespec="seconds"))
    log.info("Plan: %d API calls", total_calls)

    if args.dry_run:
        log.info("Dry run — exiting.")
        return 0

    t0 = time.time()
    rows_total = 0
    calls_ok = 0
    calls_fail = 0
    chunk_n = 0
    for chunk_start, chunk_end in chunks:
        chunk_n += 1
        elapsed = time.time() - t0
        log.info("[chunk %d/%d t=%.0fs] %s..%s",
                 chunk_n, len(chunks), elapsed, chunk_start, chunk_end)
        try:
            n = fetch_archive_era5(
                start_date=chunk_start,
                end_date=chunk_end,
                fetched_at=fetched_at_dt,
                locations=locations,
            )
            rows_total += n
            calls_ok += 1
        except Exception as e:  # noqa: BLE001
            log.exception("  FAILED: ERA5 %s..%s: %s", chunk_start, chunk_end, e)
            calls_fail += 1
        if chunk_n < len(chunks):
            time.sleep(INTER_CALL_SLEEP_SECS)

    runtime = time.time() - t0
    print()
    print(f"ERA5 backfill complete ({runtime:.0f}s = {runtime/60:.1f}min)")
    print(f"Rows inserted: {rows_total:,}")
    print(f"Chunks OK / fail: {calls_ok} / {calls_fail}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
