#!/usr/bin/env python3
"""API-based backfill of weather_observation for Previous Runs archive.

Iterates the configured NWP models × lead times, chunks the date range
into windows, and calls ``fetch_previous_runs`` for each chunk. Uses a
single ``fetched_at`` timestamp for the whole run so re-running with the
same ``--fetched-at`` is idempotent (PK includes ``fetched_at``).

Complements the CSV-based ``backfill_weather_observation.py`` by covering
(a) day2 (48h) lead for the legacy 4 models and (b) all 3 leads for
sources added in the 2026-04-22 extension (knmi / meteofrance / icon_d2).
Can also be used to do a full 7-model API re-fetch if desired.

Usage:
    # Full backfill since 2024-01-01, all 7 models × {day1, day2, day3}
    python scripts/backfill_previous_runs_full.py

    # Only the newly-added sources
    python scripts/backfill_previous_runs_full.py \
        --models knmi_harmonie_arome_europe,meteofrance_arome_france,icon_d2

    # Only day2 (for legacy models that were previously missing it)
    python scripts/backfill_previous_runs_full.py \
        --models best_match,ecmwf_ifs025,gfs_seamless,icon_seamless --leads 48

    # Resume with the same fetched_at to skip already-inserted rows
    python scripts/backfill_previous_runs_full.py \
        --fetched-at 2026-04-22T19:30:00+00:00
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: F401  — loads env
import utils
from src import db
from src.fetch_weather_observation import (
    LEAD_TIMES_HOURS,
    NWP_MODELS,
    fetch_previous_runs,
)

log = logging.getLogger("entsoe_pipeline")

DEFAULT_CHUNK_DAYS = 90
INTER_CALL_SLEEP_SECS = 2.0


def _chunk_range(start: datetime, end: datetime, days: int):
    """Yield ``(chunk_start, chunk_end)`` date strings, max ``days`` apart."""
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        yield cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")
        cur = nxt + timedelta(days=1)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", default="2024-01-01",
                   help="Backfill window start (default: 2024-01-01).")
    p.add_argument("--end", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                   help="Backfill window end (default: today).")
    p.add_argument("--fetched-at", default=None,
                   help="Fixed fetched_at ISO timestamp (default: script start). "
                        "Re-use on resume for idempotency.")
    p.add_argument("--models", default=None,
                   help=f"Comma-separated NWP models (default: {','.join(NWP_MODELS)}).")
    p.add_argument("--leads", default=None,
                   help=f"Comma-separated lead hours (default: {','.join(str(x) for x in LEAD_TIMES_HOURS)}).")
    p.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS,
                   help=f"Days per API call (default: {DEFAULT_CHUNK_DAYS}).")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the plan without calling the API.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    utils.setup_logging()

    db.create_weather_observation_tables()

    models = tuple(args.models.split(",")) if args.models else NWP_MODELS
    leads = (tuple(int(x) for x in args.leads.split(","))
             if args.leads else LEAD_TIMES_HOURS)

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    if args.fetched_at:
        fetched_at_dt = datetime.fromisoformat(args.fetched_at)
    else:
        fetched_at_dt = datetime.now(timezone.utc)

    chunks = list(_chunk_range(start_dt, end_dt, args.chunk_days))
    total_calls = len(models) * len(leads) * len(chunks)

    log.info("Backfill window: %s .. %s (chunk=%dd, %d chunks)",
             args.start, args.end, args.chunk_days, len(chunks))
    log.info("Models (%d): %s", len(models), ", ".join(models))
    log.info("Leads (%d): %s", len(leads), leads)
    log.info("fetched_at: %s", fetched_at_dt.isoformat(timespec="seconds"))
    log.info("Plan: %d API calls", total_calls)

    if args.dry_run:
        log.info("Dry run — exiting.")
        return 0

    summary: dict[str, int] = {}
    t0 = time.time()
    call_n = 0
    for model in models:
        for lead in leads:
            key = f"{model}_day{lead // 24}"
            rows_total = 0
            calls_ok = 0
            calls_fail = 0
            for chunk_start, chunk_end in chunks:
                call_n += 1
                elapsed = time.time() - t0
                log.info("[%d/%d t=%.0fs] %-30s lead=%dh  %s..%s",
                         call_n, total_calls, elapsed, model, lead,
                         chunk_start, chunk_end)
                try:
                    n = fetch_previous_runs(
                        model_id=model,
                        lead_time_hours=lead,
                        start_date=chunk_start,
                        end_date=chunk_end,
                        fetched_at=fetched_at_dt,
                    )
                    rows_total += n
                    calls_ok += 1
                except Exception as e:  # noqa: BLE001
                    log.exception("  FAILED: %s %dh %s..%s: %s",
                                  model, lead, chunk_start, chunk_end, e)
                    calls_fail += 1
                if call_n < total_calls:
                    time.sleep(INTER_CALL_SLEEP_SECS)
            summary[key] = rows_total
            log.info("  %s: %d rows (%d/%d chunks OK)",
                     key, rows_total, calls_ok, calls_ok + calls_fail)

    runtime = time.time() - t0
    print()
    print(f"Backfill complete ({runtime:.0f}s = {runtime/60:.1f}min)")
    print("Rows inserted per model/lead:")
    for k, v in summary.items():
        print(f"  {k:<40} {v:>10,}")
    print(f"Total rows: {sum(summary.values()):,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
