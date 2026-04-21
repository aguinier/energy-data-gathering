#!/usr/bin/env python3
"""Cron entry point for the versioned weather_observation ingest.

Called 3× per day on prod (07:00, 13:30, 19:30 UTC) per
`docker/crontab`. Each run:

1. Ensures the schema exists (idempotent; no-op after first run).
2. Pulls Open-Meteo real-time forecast for the next 7 days + past 1 day.
3. Pulls Open-Meteo Previous Runs API for day1 + day3 lead times across
   all 4 NWP models, for the past `--window` days.

All writes carry `fetched_at = now()` so replay queries (`WHERE
fetched_at <= :t`) return the snapshot known at any past timestamp.

Usage:
    python scripts/update_weather_observation.py
    python scripts/update_weather_observation.py --window 3     # tighter
    python scripts/update_weather_observation.py --skip-realtime
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: F401  — ensures env vars load
import utils
from src import db
from src.fetch_weather_observation import (
    fetch_previous_runs,
    fetch_realtime_forecast,
    run_ingest,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--window",
        type=int,
        default=7,
        help="Trailing days to re-fetch for Previous Runs (default: 7).",
    )
    p.add_argument(
        "--forecast-days",
        type=int,
        default=7,
        help="Days ahead to fetch for the real-time forecast (default: 7).",
    )
    p.add_argument(
        "--skip-realtime",
        action="store_true",
        help="Skip the real-time forecast pull (just do Previous Runs).",
    )
    p.add_argument(
        "--skip-previous-runs",
        action="store_true",
        help="Skip Previous Runs pulls (just do real-time forecast).",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    utils.setup_logging()

    # Always ensure schema exists first — cheap idempotent call.
    db.create_weather_observation_tables()

    if args.skip_realtime and args.skip_previous_runs:
        print("Nothing to do (both paths skipped). Exit.")
        return 0

    if args.skip_realtime:
        # Previous Runs only.
        from src.fetch_weather_observation import NWP_MODELS, LEAD_TIMES_HOURS
        from datetime import datetime, timedelta, timezone
        today = datetime.now(timezone.utc).date()
        start = (today - timedelta(days=args.window)).isoformat()
        end = today.isoformat()
        for model in NWP_MODELS:
            for lead in LEAD_TIMES_HOURS:
                fetch_previous_runs(model, lead, start, end)
        return 0

    if args.skip_previous_runs:
        fetch_realtime_forecast(forecast_days=args.forecast_days, past_days=1)
        return 0

    # Default: full ingest.
    summary = run_ingest(
        previous_runs_window_days=args.window,
        forecast_days_ahead=args.forecast_days,
        forecast_past_days=1,
    )
    print("Ingest summary (rows inserted per source):")
    for k, v in summary.items():
        tag = "OK" if v >= 0 else "FAIL"
        print(f"  [{tag:<4}] {k:<40} {v}")
    return 0 if all(v >= 0 for v in summary.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
