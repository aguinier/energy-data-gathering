"""Query helpers for the versioned `weather_observation` table.

Two core reads:

* :func:`latest_weather` — the freshest snapshot available *right now*,
  for a given (location, source, valid_at range). Latest = max
  `fetched_at` per target hour.
* :func:`weather_as_of` — the snapshot that was available at a past
  time `t`. Used for replay / backtesting ("what did the weather look
  like when heliocast called the API at 07:45 UTC?").

Both helpers return a pandas DataFrame keyed by `valid_at`, with one
column per weather variable plus provenance columns (`source_id`,
`location_id`, `fetched_at`, `forecast_run_time`).

Usage in an external consumer:

>>> from src.weather_read import weather_as_of, resolve_location, resolve_source
>>> loc = resolve_location("BE", "central")
>>> src = resolve_source("open_meteo_forecast", "best_match", -1)
>>> df = weather_as_of(loc, src, "2026-04-19T22:00:00Z", "2026-04-20T21:45:00Z",
...                    at="2026-04-19T07:45:00Z")
"""
from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd

from . import db
from .weather_schema import WEATHER_VARIABLE_COLUMNS


def resolve_location(country_code: str, zone_id: str) -> int:
    """Return location_id or raise KeyError if not seeded."""
    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT location_id FROM weather_location "
            "WHERE country_code = ? AND zone_id = ?",
            (country_code, zone_id),
        ).fetchone()
    if row is None:
        raise KeyError(f"No weather_location for ({country_code}, {zone_id})")
    return row["location_id"]


def resolve_source(provider: str, model_id: str, lead_time_hours: int) -> int:
    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT source_id FROM weather_source "
            "WHERE provider = ? AND model_id = ? AND lead_time_hours = ?",
            (provider, model_id, lead_time_hours),
        ).fetchone()
    if row is None:
        raise KeyError(
            f"No weather_source for ({provider}, {model_id}, {lead_time_hours}h)"
        )
    return row["source_id"]


def _query_window(
    location_id: int,
    source_id: int,
    valid_from: str,
    valid_to: str,
    fetched_at_cutoff: Optional[str],
) -> pd.DataFrame:
    """Shared query helper — picks freshest row per valid_at up to cutoff."""
    var_cols = ", ".join(WEATHER_VARIABLE_COLUMNS)
    cutoff_clause = "AND fetched_at <= :cutoff" if fetched_at_cutoff else ""
    sql = f"""
        SELECT
            valid_at,
            source_id,
            location_id,
            fetched_at,
            forecast_run_time,
            {var_cols}
        FROM weather_observation
        WHERE location_id = :location_id
          AND source_id = :source_id
          AND valid_at BETWEEN :valid_from AND :valid_to
          {cutoff_clause}
        GROUP BY valid_at
        HAVING MAX(fetched_at)
        ORDER BY valid_at
    """
    params = {
        "location_id": location_id,
        "source_id": source_id,
        "valid_from": valid_from,
        "valid_to": valid_to,
    }
    if fetched_at_cutoff:
        params["cutoff"] = fetched_at_cutoff
    with db.get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


def latest_weather(
    location_id: int,
    source_id: int,
    valid_from: str,
    valid_to: str,
) -> pd.DataFrame:
    """Freshest snapshot currently available for each target hour."""
    return _query_window(location_id, source_id, valid_from, valid_to, None)


def weather_as_of(
    location_id: int,
    source_id: int,
    valid_from: str,
    valid_to: str,
    at: str,
) -> pd.DataFrame:
    """Snapshot that was available at time ``at`` (replay).

    ``at`` is an ISO-8601 UTC timestamp. Rows with ``fetched_at > at``
    are excluded, simulating the information state at that moment.
    """
    return _query_window(location_id, source_id, valid_from, valid_to, at)


__all__ = ["resolve_location", "resolve_source", "latest_weather", "weather_as_of"]
