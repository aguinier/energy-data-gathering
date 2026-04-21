#!/usr/bin/env python3
"""Hourly real-time weather ingest for heliocast + dashboards.

Runs at XX:30 UTC every hour via `docker/crontab`. Pulls the
`/v1/forecast` endpoint for all 4 NWP models (best_match + ecmwf +
icon + gfs) × 5 BE locations, inserts into `weather_observation`.

Designed so heliocast's :45 UTC runner finds fresh rows ≤ 15 min
old when it queries `GET /api/weather/latest` from the frontend API.

This is the light cadence. The heavier `update_weather_observation.py`
(3× daily) additionally pulls the Previous Runs API archive for
replay/backtesting — that data is stable once ≥ 24h old, so it
doesn't need hourly updates.

Usage:
    python scripts/update_weather_observation_hourly.py
    python scripts/update_weather_observation_hourly.py --forecast-days 3
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: F401
import utils
from src import db
from src.fetch_weather_observation import run_hourly_realtime_ingest


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--forecast-days",
        type=int,
        default=3,
        help="Days ahead for the real-time forecast (default: 3).",
    )
    p.add_argument(
        "--past-days",
        type=int,
        default=1,
        help="Past days included in the response (default: 1).",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    utils.setup_logging()

    db.create_weather_observation_tables()
    summary = run_hourly_realtime_ingest(
        forecast_days_ahead=args.forecast_days,
        forecast_past_days=args.past_days,
    )

    print("Hourly ingest summary:")
    for k, v in summary.items():
        tag = "OK" if v >= 0 else "FAIL"
        print(f"  [{tag:<4}] {k:<30} {v}")

    return 0 if all(v >= 0 for v in summary.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
