"""Canonical schema for the versioned weather observation tables.

These tables (added 2026-04-21) hold multi-NWP, multi-zone, multi-lead
weather data with snapshot versioning via `fetched_at`. They live next
to the existing `weather_data` table (which stays as-is for dashboards).

Schema owner: `able` repo. External consumers (e.g. heliocast) should
import column names and dimension constants from here to stay in sync.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Weather variable column list (canonical order for INSERT statements)
# ---------------------------------------------------------------------------

# Units in column names mirror `weather_data` (able's existing convention):
# _wm2 = W/m², _k = Kelvin, _c = Celsius, _frac = 0–1, _hpa = hPa,
# _ms = m/s, _deg = degrees, _mm = millimetres, _s = seconds.
#
# Open-Meteo returns most radiation + temp + wind values already in these
# units (or degrees C for temperature). Ingestors convert where needed.

WEATHER_VARIABLE_COLUMNS = [
    # Radiation — accumulated (period-averaged, W/m²)
    "shortwave_radiation_wm2",
    "direct_radiation_wm2",
    "direct_normal_irradiance_wm2",
    "diffuse_radiation_wm2",
    "global_tilted_irradiance_wm2",
    "terrestrial_radiation_wm2",
    # Radiation — instantaneous
    "shortwave_radiation_instant_wm2",
    "direct_radiation_instant_wm2",
    "direct_normal_irradiance_instant_wm2",
    "diffuse_radiation_instant_wm2",
    "global_tilted_irradiance_instant_wm2",
    "terrestrial_radiation_instant_wm2",
    # Cloud
    "cloud_cover_frac",
    "cloud_cover_low_frac",
    "cloud_cover_mid_frac",
    "cloud_cover_high_frac",
    "sunshine_duration_s",
    # Thermodynamic
    "temperature_2m_c",
    "dew_point_2m_c",
    "relative_humidity_2m_frac",
    "pressure_msl_hpa",
    # Wind
    "wind_speed_10m_ms",
    "wind_direction_10m_deg",
    "wind_gusts_10m_ms",
    "wind_speed_100m_ms",
    # Precipitation
    "precip_mm",
    "rain_mm",
    "snowfall_mm",
]


# ---------------------------------------------------------------------------
# Mapping from Open-Meteo API field names → our suffixed column names
# ---------------------------------------------------------------------------
#
# Ingestors use this to translate raw API responses into DB-ready rows.
# Keys are Open-Meteo hourly variable names; values are our DB columns.

OPENMETEO_TO_DB = {
    "shortwave_radiation": "shortwave_radiation_wm2",
    "direct_radiation": "direct_radiation_wm2",
    "direct_normal_irradiance": "direct_normal_irradiance_wm2",
    "diffuse_radiation": "diffuse_radiation_wm2",
    "global_tilted_irradiance": "global_tilted_irradiance_wm2",
    "terrestrial_radiation": "terrestrial_radiation_wm2",
    "shortwave_radiation_instant": "shortwave_radiation_instant_wm2",
    "direct_radiation_instant": "direct_radiation_instant_wm2",
    "direct_normal_irradiance_instant": "direct_normal_irradiance_instant_wm2",
    "diffuse_radiation_instant": "diffuse_radiation_instant_wm2",
    "global_tilted_irradiance_instant": "global_tilted_irradiance_instant_wm2",
    "terrestrial_radiation_instant": "terrestrial_radiation_instant_wm2",
    "cloud_cover": "cloud_cover_frac",
    "cloud_cover_low": "cloud_cover_low_frac",
    "cloud_cover_mid": "cloud_cover_mid_frac",
    "cloud_cover_high": "cloud_cover_high_frac",
    "sunshine_duration": "sunshine_duration_s",
    "temperature_2m": "temperature_2m_c",
    "dew_point_2m": "dew_point_2m_c",
    "relative_humidity_2m": "relative_humidity_2m_frac",
    "pressure_msl": "pressure_msl_hpa",
    "wind_speed_10m": "wind_speed_10m_ms",
    "wind_direction_10m": "wind_direction_10m_deg",
    "wind_gusts_10m": "wind_gusts_10m_ms",
    "wind_speed_100m": "wind_speed_100m_ms",
    "precipitation": "precip_mm",
    "rain": "rain_mm",
    "snowfall": "snowfall_mm",
}


# ---------------------------------------------------------------------------
# Dimension seeds — Belgium first
# ---------------------------------------------------------------------------
#
# BE_LOCATIONS mirrors helio's capacity-weighted 4-zone split of Belgian
# PV + a single country centroid used by able's existing weather_data
# single-point ingest.

BE_LOCATIONS = [
    # country_code, zone_id,     lat,  lon,  weight, description
    ("BE", "centroid", 50.5,  4.45, None, "Able centroid (legacy weather_data point)"),
    ("BE", "central",  50.8,  4.3,  0.40, "Central Belgium (40% of PV capacity)"),
    ("BE", "north",    51.1,  4.8,  0.30, "Northern Belgium (30%)"),
    ("BE", "south",    50.4,  4.0,  0.20, "Southern Belgium (20%)"),
    ("BE", "east",     50.2,  5.5,  0.10, "Eastern Belgium (10%)"),
]


# ---------------------------------------------------------------------------
# Source dimension — provider × model × lead-time combinations we'll use
# ---------------------------------------------------------------------------

OPEN_METEO_SOURCES = [
    # provider,                     model_id,         lead_time_hours, description
    ("open_meteo_archive",          "era5",                        0, "ERA5 reanalysis (historical truth)"),
    # Real-time forecast API (hourly ingest — serves heliocast's inference).
    ("open_meteo_forecast",         "best_match",                 -1, "Real-time forecast API, Open-Meteo best_match"),
    ("open_meteo_forecast",         "ecmwf_ifs025",               -1, "Real-time forecast API, ECMWF IFS 0.25°"),
    ("open_meteo_forecast",         "icon_seamless",              -1, "Real-time forecast API, DWD ICON-EU 11 km"),
    ("open_meteo_forecast",         "gfs_seamless",               -1, "Real-time forecast API, NOAA GFS 0.11°"),
    # Previous Runs archive (3×/day ingest — replay + backtest substrate).
    ("open_meteo_previous_runs",    "best_match",                 24, "Previous Runs API, day1 lead"),
    ("open_meteo_previous_runs",    "best_match",                 72, "Previous Runs API, day3 lead"),
    ("open_meteo_previous_runs",    "ecmwf_ifs025",               24, "ECMWF IFS 0.25°, day1 lead"),
    ("open_meteo_previous_runs",    "ecmwf_ifs025",               72, "ECMWF IFS 0.25°, day3 lead"),
    ("open_meteo_previous_runs",    "gfs_seamless",               24, "NOAA GFS, day1 lead"),
    ("open_meteo_previous_runs",    "gfs_seamless",               72, "NOAA GFS, day3 lead"),
    ("open_meteo_previous_runs",    "icon_seamless",              24, "DWD ICON-EU, day1 lead"),
    ("open_meteo_previous_runs",    "icon_seamless",              72, "DWD ICON-EU, day3 lead"),
]


# ---------------------------------------------------------------------------
# DDL — CREATE TABLE IF NOT EXISTS statements
# ---------------------------------------------------------------------------

SCHEMA_LOCATION = """
CREATE TABLE IF NOT EXISTS weather_location (
    location_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code  TEXT    NOT NULL,
    zone_id       TEXT    NOT NULL,
    lat           REAL    NOT NULL,
    lon           REAL    NOT NULL,
    weight        REAL,
    description   TEXT,
    created_at    TEXT    DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(country_code, zone_id)
)
"""

SCHEMA_SOURCE = """
CREATE TABLE IF NOT EXISTS weather_source (
    source_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    provider         TEXT    NOT NULL,
    model_id         TEXT    NOT NULL,
    lead_time_hours  INTEGER NOT NULL,
    description      TEXT,
    created_at       TEXT    DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(provider, model_id, lead_time_hours)
)
"""

# weather_observation columns are generated from WEATHER_VARIABLE_COLUMNS.
_OBSERVATION_VAR_DDL = ",\n    ".join(f"{c} REAL" for c in WEATHER_VARIABLE_COLUMNS)

SCHEMA_OBSERVATION = f"""
CREATE TABLE IF NOT EXISTS weather_observation (
    source_id          INTEGER NOT NULL REFERENCES weather_source(source_id),
    location_id        INTEGER NOT NULL REFERENCES weather_location(location_id),
    valid_at           TEXT    NOT NULL,
    forecast_run_time  TEXT,
    fetched_at         TEXT    NOT NULL,
    {_OBSERVATION_VAR_DDL},
    PRIMARY KEY (source_id, location_id, valid_at, fetched_at)
)
"""

SCHEMA_INDEXES = [
    """
    CREATE INDEX IF NOT EXISTS idx_wx_replay
    ON weather_observation(location_id, valid_at, source_id, fetched_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_wx_source_latest
    ON weather_observation(source_id, location_id, fetched_at DESC, valid_at)
    """,
]


ALL_SCHEMA_SQL = [SCHEMA_LOCATION, SCHEMA_SOURCE, SCHEMA_OBSERVATION, *SCHEMA_INDEXES]


__all__ = [
    "WEATHER_VARIABLE_COLUMNS",
    "OPENMETEO_TO_DB",
    "BE_LOCATIONS",
    "OPEN_METEO_SOURCES",
    "SCHEMA_LOCATION",
    "SCHEMA_SOURCE",
    "SCHEMA_OBSERVATION",
    "SCHEMA_INDEXES",
    "ALL_SCHEMA_SQL",
]
