"""Fetch versioned per-NWP-model weather into `weather_observation`.

Three data paths covered:

* **Real-time forecast** — Open-Meteo `/v1/forecast` with `models=best_match`
  for the `open_meteo_forecast` provider, `lead_time_hours=-1`. Captures
  whatever the API is currently serving for the next ~7 days.
* **Previous Runs day1** — `/v1/forecast` on the `previous-runs-api`
  host with the `_previous_day1` variable suffix for each NWP model.
  Retrospective archive of the 24–48 h-ahead run.
* **Previous Runs day3** — same but with `_previous_day3` suffix.

All writes target the `weather_observation` table. The PK
`(source_id, location_id, valid_at, fetched_at)` means successive
cadence pulls accumulate snapshots rather than overwriting — that's the
whole point of the new table.

Designed to be called from `scripts/update_weather_observation.py` via
a cron schedule (see `docker/crontab`). Runs are idempotent: re-running
for the same `fetched_at` second does nothing (PK conflict → OR IGNORE).
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

from . import db
from .weather_schema import (
    OPENMETEO_TO_DB,
    WEATHER_VARIABLE_COLUMNS,
)

logger = logging.getLogger("entsoe_pipeline")


# ---------------------------------------------------------------------------
# Open-Meteo endpoints
# ---------------------------------------------------------------------------

COMMERCIAL_FORECAST_URL = "https://customer-api.open-meteo.com/v1/forecast"
COMMERCIAL_PREVIOUS_RUNS_URL = "https://customer-previous-runs-api.open-meteo.com/v1/forecast"
FREE_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
FREE_PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"

# Open-Meteo variable names (API-side) — subset of helio's requirements that
# is available on Previous Runs. The 3 cloud-layer variants and the GTI
# variants aren't in Previous Runs and will be NULL for those sources.
OPENMETEO_VARIABLES_PREVIOUS_RUNS = [
    "shortwave_radiation",
    "direct_radiation",
    "direct_normal_irradiance",
    "diffuse_radiation",
    "terrestrial_radiation",
    "shortwave_radiation_instant",
    "direct_radiation_instant",
    "direct_normal_irradiance_instant",
    "diffuse_radiation_instant",
    "terrestrial_radiation_instant",
    "cloud_cover",
    "sunshine_duration",
    "temperature_2m",
    "dew_point_2m",
    "relative_humidity_2m",
    "pressure_msl",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "wind_speed_100m",
    "precipitation",
    "rain",
    "snowfall",
]

# Real-time /v1/forecast supports the cloud-layer breakdown + GTI that
# Previous Runs doesn't. We can ask for the fuller set here.
OPENMETEO_VARIABLES_FORECAST = OPENMETEO_VARIABLES_PREVIOUS_RUNS + [
    "global_tilted_irradiance",
    "global_tilted_irradiance_instant",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
]

API_KEY = os.getenv("api_key_openmeteo", "")

PANEL_TILT = 35
PANEL_AZIMUTH = 180


# ---------------------------------------------------------------------------
# Unit conversion — API value → DB column value
# ---------------------------------------------------------------------------

def _convert_value(api_var: str, value):
    """Scale Open-Meteo's raw value to match the DB column's unit convention.

    Most variables are pass-through (radiation in W/m², temp in °C, pressure
    in hPa, wind in m/s — all match our column suffixes). Humidity and
    cloud cover come as percentages 0–100 from the API; we store as
    fractions 0–1.
    """
    if value is None:
        return None
    if api_var == "relative_humidity_2m":
        return value / 100.0
    if api_var.startswith("cloud_cover"):
        return value / 100.0
    return value


# ---------------------------------------------------------------------------
# Multi-point API calls
# ---------------------------------------------------------------------------

def _call_openmeteo(
    url: str,
    params: dict,
    retries: int = 3,
    timeout: int = 120,
) -> Optional[list]:
    """Single API call with retry + free-tier fallback.

    Returns the list of per-location response dicts, or None on final failure.
    Open-Meteo returns a list when multiple lat/lon pairs are passed, or a
    single dict for one point — we normalize to always-a-list.
    """
    use_free = params.pop("_use_free", False)
    url_to_use = url
    if (not use_free) and API_KEY:
        params = {**params, "apikey": API_KEY}
    if use_free:
        url_to_use = url.replace("customer-api", "api").replace(
            "customer-previous-runs-api", "previous-runs-api"
        )

    for attempt in range(retries):
        try:
            resp = requests.get(url_to_use, params=params, timeout=timeout)
            if resp.status_code in (401, 403) and not use_free and API_KEY:
                logger.warning(
                    "Open-Meteo %s returned %d — falling back to free tier",
                    url_to_use, resp.status_code,
                )
                params.pop("apikey", None)
                params["_use_free"] = True
                return _call_openmeteo(url, params, retries=retries, timeout=timeout)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = 2 ** (attempt + 1)
                logger.warning(
                    "Open-Meteo HTTP %d, retrying in %ds (attempt %d/%d)",
                    resp.status_code, wait, attempt + 1, retries,
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else [data]
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                logger.warning("Open-Meteo error %s, retrying in %ds", e, wait)
                time.sleep(wait)
            else:
                logger.error("Open-Meteo request failed after %d attempts: %s", retries, e)
                return None
    return None


def _get_be_locations() -> list[dict]:
    """Load the 5 BE rows from weather_location, ordered consistently."""
    with db.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT location_id, zone_id, lat, lon
            FROM weather_location
            WHERE country_code = 'BE'
            ORDER BY location_id
            """
        ).fetchall()
    return [dict(r) for r in rows]


def _get_source_id(provider: str, model_id: str, lead_time_hours: int) -> int:
    """Look up the source_id for a (provider, model, lead) triple."""
    with db.get_connection() as conn:
        row = conn.execute(
            """
            SELECT source_id FROM weather_source
            WHERE provider = ? AND model_id = ? AND lead_time_hours = ?
            """,
            (provider, model_id, lead_time_hours),
        ).fetchone()
    if row is None:
        raise RuntimeError(
            f"No weather_source for ({provider}, {model_id}, {lead_time_hours}). "
            "Run `python scripts/init_weather_observation.py` on this DB."
        )
    return row["source_id"]


# ---------------------------------------------------------------------------
# Insertion
# ---------------------------------------------------------------------------

def _upsert_observations(
    rows: list[dict],
    source_id: int,
    fetched_at: str,
) -> int:
    """Insert a batch of weather_observation rows.

    ``rows`` items are dicts with keys:
      - location_id (int)
      - valid_at (ISO-8601 UTC string)
      - forecast_run_time (ISO-8601 UTC string or None)
      - one entry per column in WEATHER_VARIABLE_COLUMNS (value or None)
    """
    if not rows:
        return 0
    cols = [
        "source_id", "location_id", "valid_at", "forecast_run_time", "fetched_at",
    ] + WEATHER_VARIABLE_COLUMNS
    placeholders = ",".join("?" * len(cols))
    sql = (
        f"INSERT OR IGNORE INTO weather_observation ({','.join(cols)}) "
        f"VALUES ({placeholders})"
    )
    inserted = 0
    with db.get_connection() as conn:
        cursor = conn.cursor()
        for r in rows:
            values = [
                source_id,
                r["location_id"],
                r["valid_at"],
                r.get("forecast_run_time"),
                fetched_at,
            ] + [r.get(c) for c in WEATHER_VARIABLE_COLUMNS]
            cursor.execute(sql, values)
            inserted += cursor.rowcount
    return inserted


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _parse_hourly_response(
    loc_responses: list,
    locations: list[dict],
    api_variables: list[str],
    column_suffix: str = "",
    forecast_run_time: Optional[str] = None,
) -> list[dict]:
    """Parse Open-Meteo's multi-point hourly response into row dicts.

    ``column_suffix`` is the trailing ``_previous_dayN`` segment for
    Previous Runs API responses; empty string for the real-time forecast.
    """
    rows: list[dict] = []
    for i, loc_data in enumerate(loc_responses):
        if i >= len(locations):
            break
        loc = locations[i]
        hourly = loc_data.get("hourly", {}) or {}
        times = hourly.get("time", []) or []
        if not times:
            continue
        # Build a dict of api_var -> list of values (same length as times)
        var_arrays: dict[str, list] = {}
        for api_var in api_variables:
            api_col = f"{api_var}{column_suffix}"
            var_arrays[api_var] = hourly.get(api_col, [None] * len(times))
        for t_idx, t_str in enumerate(times):
            row = {
                "location_id": loc["location_id"],
                "valid_at": t_str if "Z" in t_str else f"{t_str}Z",
                "forecast_run_time": forecast_run_time,
            }
            for api_var, values in var_arrays.items():
                db_col = OPENMETEO_TO_DB.get(api_var)
                if db_col is None:
                    continue
                raw = values[t_idx] if t_idx < len(values) else None
                row[db_col] = _convert_value(api_var, raw)
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_realtime_forecast(
    model_id: str = "best_match",
    forecast_days: int = 7,
    past_days: int = 1,
    fetched_at: Optional[datetime] = None,
) -> int:
    """Pull the real-time `/v1/forecast` endpoint for all 5 BE locations.

    ``model_id`` selects the NWP source:
      - ``"best_match"``  — Open-Meteo's default (no ``models=`` param)
      - ``"ecmwf_ifs025"``, ``"icon_seamless"``, ``"gfs_seamless"`` — specific

    Returns the number of rows inserted.
    """
    locations = _get_be_locations()
    if not locations:
        logger.warning("No BE locations in weather_location — did you init the schema?")
        return 0

    source_id = _get_source_id("open_meteo_forecast", model_id, -1)
    fetched_at_dt = fetched_at or datetime.now(timezone.utc)
    fetched_at_str = fetched_at_dt.isoformat(timespec="seconds")

    params = {
        "latitude": ",".join(str(loc["lat"]) for loc in locations),
        "longitude": ",".join(str(loc["lon"]) for loc in locations),
        "hourly": ",".join(OPENMETEO_VARIABLES_FORECAST),
        "forecast_days": forecast_days,
        "past_days": past_days,
        "timezone": "UTC",
        "wind_speed_unit": "ms",
        "tilt": PANEL_TILT,
        "azimuth": PANEL_AZIMUTH,
    }
    if model_id != "best_match":
        params["models"] = model_id

    data = _call_openmeteo(COMMERCIAL_FORECAST_URL, params)
    if not data:
        logger.error("fetch_realtime_forecast(%s): no data returned", model_id)
        return 0

    rows = _parse_hourly_response(
        data, locations, OPENMETEO_VARIABLES_FORECAST,
        column_suffix="",
        forecast_run_time=None,  # real-time API doesn't expose run time
    )
    inserted = _upsert_observations(rows, source_id, fetched_at_str)
    logger.info(
        "fetch_realtime_forecast(%s): %d rows (fetched_at=%s)",
        model_id, inserted, fetched_at_str,
    )
    return inserted


# NWP models we fetch at real-time (every hour) — matches helio/heliocast
# NWP_INFERENCE_MODELS. best_match is a no-models merged view, plus the
# individual NWP models so heliocast can compute cross-model disagreement.
# KNMI HARMONIE-AROME added 2026-04-22: live hindcast on Predico sessions
# 515-517 showed it cuts q50 RMSE by ~36% on cloudy-NWP-but-clear-reality
# days. See heliocast/docs/2026-04-22_knmi_addition.md.
# Meteofrance AROME + ICON-D2 added 2026-04-22 as part of helioforge's
# multi-NWP bakeoff — hourly realtime captures their fetched_at timing
# diversity going forward.
REALTIME_NWP_MODELS = (
    "best_match", "ecmwf_ifs025", "icon_seamless", "gfs_seamless",
    "knmi_harmonie_arome_europe", "meteofrance_arome_france", "icon_d2",
)


def fetch_previous_runs(
    model_id: str,
    lead_time_hours: int,
    start_date: str,
    end_date: str,
    fetched_at: Optional[datetime] = None,
) -> int:
    """Pull Previous Runs API for one NWP model × one lead time.

    ``lead_time_hours`` must be 24 or 72 (the two supported by the API
    suffix convention `_previous_day1` / `_previous_day3`).
    """
    if lead_time_hours == 24:
        suffix = "_previous_day1"
        provider_lead = 24
    elif lead_time_hours == 48:
        suffix = "_previous_day2"
        provider_lead = 48
    elif lead_time_hours == 72:
        suffix = "_previous_day3"
        provider_lead = 72
    else:
        raise ValueError(f"Unsupported lead_time_hours: {lead_time_hours}")

    locations = _get_be_locations()
    if not locations:
        return 0

    source_id = _get_source_id("open_meteo_previous_runs", model_id, provider_lead)
    fetched_at_dt = fetched_at or datetime.now(timezone.utc)
    fetched_at_str = fetched_at_dt.isoformat(timespec="seconds")

    suffixed_vars = [f"{v}{suffix}" for v in OPENMETEO_VARIABLES_PREVIOUS_RUNS]
    params = {
        "latitude": ",".join(str(loc["lat"]) for loc in locations),
        "longitude": ",".join(str(loc["lon"]) for loc in locations),
        "hourly": ",".join(suffixed_vars),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "UTC",
        "wind_speed_unit": "ms",
        "tilt": PANEL_TILT,
        "azimuth": PANEL_AZIMUTH,
    }
    if model_id != "best_match":
        params["models"] = model_id

    data = _call_openmeteo(COMMERCIAL_PREVIOUS_RUNS_URL, params)
    if not data:
        logger.error(
            "fetch_previous_runs: no data returned for model=%s lead=%d %s..%s",
            model_id, lead_time_hours, start_date, end_date,
        )
        return 0

    rows = _parse_hourly_response(
        data, locations, OPENMETEO_VARIABLES_PREVIOUS_RUNS,
        column_suffix=suffix,
        # forecast_run_time is approximated as valid_at - lead_time on insert
        # since Open-Meteo doesn't expose the NWP init time; set None here
        # and leave the column NULL.
        forecast_run_time=None,
    )
    inserted = _upsert_observations(rows, source_id, fetched_at_str)
    logger.info(
        "fetch_previous_runs(%s, %dh): %d rows inserted for %s..%s (fetched_at=%s)",
        model_id, lead_time_hours, inserted, start_date, end_date, fetched_at_str,
    )
    return inserted


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------

NWP_MODELS = (
    "best_match", "ecmwf_ifs025", "gfs_seamless", "icon_seamless",
    "knmi_harmonie_arome_europe", "meteofrance_arome_france", "icon_d2",
)
LEAD_TIMES_HOURS = (24, 48, 72)


def run_hourly_realtime_ingest(
    forecast_days_ahead: int = 3,
    forecast_past_days: int = 1,
    fetched_at: Optional[datetime] = None,
) -> dict:
    """Hourly cadence: pull the real-time forecast for **all 4 NWP models**.

    Light: only the real-time `/v1/forecast` endpoint, no Previous Runs.
    Designed to run at :30 UTC each hour so that downstream readers
    (heliocast at :45 UTC) see ≤ 15 min of staleness.

    Returns per-source row-insert summary.
    """
    fetched_at_dt = fetched_at or datetime.now(timezone.utc)
    summary: dict[str, int] = {}

    for model in REALTIME_NWP_MODELS:
        key = f"realtime_{model}"
        try:
            summary[key] = fetch_realtime_forecast(
                model_id=model,
                forecast_days=forecast_days_ahead,
                past_days=forecast_past_days,
                fetched_at=fetched_at_dt,
            )
        except Exception as e:  # noqa: BLE001
            logger.exception("realtime_forecast(%s) failed: %s", model, e)
            summary[key] = -1

    total = sum(v for v in summary.values() if v >= 0)
    logger.info("hourly realtime ingest complete: %d rows (fetched_at=%s)",
                total, fetched_at_dt.isoformat(timespec="seconds"))
    return summary


def run_ingest(
    previous_runs_window_days: int = 7,
    forecast_days_ahead: int = 7,
    forecast_past_days: int = 1,
    fetched_at: Optional[datetime] = None,
) -> dict:
    """Full ingest cycle: real-time forecast (4 NWP) + Previous Runs (4 × 2).

    Used by the 3×/day slots — pulls the full lot. The lightweight
    :meth:`run_hourly_realtime_ingest` is the other cadence tick.

    Returns a summary dict of rows inserted per source.
    """
    fetched_at_dt = fetched_at or datetime.now(timezone.utc)
    today = fetched_at_dt.date()
    start_date = (today - timedelta(days=previous_runs_window_days)).isoformat()
    end_date = today.isoformat()

    logger.info(
        "weather_observation ingest starting (fetched_at=%s, PR window=%s..%s)",
        fetched_at_dt.isoformat(timespec="seconds"), start_date, end_date,
    )

    summary: dict[str, int] = {}

    # Real-time forecast — all 4 NWP models (same as hourly ingest).
    summary.update(run_hourly_realtime_ingest(
        forecast_days_ahead=forecast_days_ahead,
        forecast_past_days=forecast_past_days,
        fetched_at=fetched_at_dt,
    ))

    # Previous Runs × 4 models × 2 leads.
    for model in NWP_MODELS:
        for lead in LEAD_TIMES_HOURS:
            key = f"prev_{model}_day{lead // 24}"
            try:
                summary[key] = fetch_previous_runs(
                    model_id=model,
                    lead_time_hours=lead,
                    start_date=start_date,
                    end_date=end_date,
                    fetched_at=fetched_at_dt,
                )
            except Exception as e:  # noqa: BLE001
                logger.exception("previous_runs %s/%dh ingest failed: %s", model, lead, e)
                summary[key] = -1

    total = sum(v for v in summary.values() if v >= 0)
    logger.info("weather_observation ingest complete: %d rows inserted total", total)
    return summary
