#!/usr/bin/env python3
"""Phase 2 — generate the unified `LOCATIONS` block from GEM plant data.

Reads the Global Energy Monitor Wind + Solar tracker xlsx files, filters
to EU39 operating plants, clusters them per `(country, tech_type)` with
adaptive k-means, and writes the result as a `LOCATIONS = [...]` block
into `src/weather_schema.py` between AUTOGEN markers. Also writes a
coverage report to `data/external/build_locations_report.json`.

Usage:
    python scripts/build_weather_locations.py
    python scripts/build_weather_locations.py --dry-run   # preview only, no file writes

Per the design spec (docs/superpowers/specs/2026-04-23-unified-weather-locations-design.md):
  - Inputs: GEM Wind + Solar xlsx (CC-BY 4.0, stored at C:/Code/able/data/)
  - Filter: EU39 × operating × non-null lat/lon × capacity > 0
  - Offshore classification is data-driven via GEM's `Installation Type`
    column (operating-only — no speculative pre-construction farms).
  - Tuning per tech type: solar (150 km, 500 MW), wind_onshore
    (100 km, 300 MW), wind_offshore (80 km, 200 MW).
  - `random_state=42` pinned so rebuilds are reproducible.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent

# Shared data dir (outside the repo; the xlsx files live alongside the prod
# DB replica). If you move them, update this constant.
GEM_DATA_DIR = Path("C:/Code/able/data")
GEM_WIND_FILE = "Global-Wind-Power-Tracker-February-2026.xlsx"
GEM_SOLAR_FILE = "Global-Solar-Power-Tracker-February-2026.xlsx"

SCHEMA_PATH = REPO_ROOT / "src" / "weather_schema.py"
REPORT_PATH = REPO_ROOT / "data" / "external" / "build_locations_report.json"

LOCATIONS_START_MARKER = "# === LOCATIONS-AUTOGEN-START ==="
LOCATIONS_END_MARKER = "# === LOCATIONS-AUTOGEN-END ==="

RANDOM_STATE = 42

# Tuning per tech type. Deviations from the 2026-04-23 design spec:
#   - max_k raised from 10 to 15 so large countries (DE, ES, FR, GB, PL)
#     get finer clusters instead of maxing out.
#   - min_cluster_mw harmonized to 300 across all techs (spec had
#     500/300/200). 300 symmetric makes small-country tuning uniform and
#     lets BE solar split into 2 zones (was k=1 under the 500 floor).
TECH_TUNING = {
    "solar":         {"max_radius_km": 150, "min_cluster_mw": 300, "max_k": 15},
    "wind_onshore":  {"max_radius_km": 100, "min_cluster_mw": 300, "max_k": 15},
    "wind_offshore": {"max_radius_km":  80, "min_cluster_mw": 300, "max_k": 15},
}

# Country lat/lon + ISO codes. Mirrors the legacy COUNTRY_COORDINATES dict in
# src/fetch_weather.py (Phase 4 will delete that duplicate). DK1/DK2 bidding
# zones intentionally excluded — they share a country centroid.
COUNTRY_COORDS: dict[str, tuple[float, float, str]] = {
    "AT": (47.7,  13.35, "Austria"),
    "BE": (50.5,   4.45, "Belgium"),
    "BG": (42.7,  25.5,  "Bulgaria"),
    "CH": (46.8,   8.2,  "Switzerland"),
    "CZ": (49.85, 15.5,  "Czechia"),
    "DE": (51.2,  10.45, "Germany"),
    "DK": (56.2,  10.0,  "Denmark"),
    "EE": (58.6,  25.0,  "Estonia"),
    "ES": (39.85, -2.5,  "Spain"),
    "FI": (64.95, 25.35, "Finland"),
    "FR": (46.2,   2.25, "France"),
    "GB": (54.0,  -2.0,  "United Kingdom"),
    "GR": (38.25, 24.5,  "Greece"),
    "HR": (44.45, 16.45, "Croatia"),
    "HU": (47.15, 19.5,  "Hungary"),
    "IE": (53.4,  -8.25, "Ireland"),
    "IT": (41.85, 12.55, "Italy"),
    "LT": (55.2,  23.85, "Lithuania"),
    "LU": (49.8,   6.1,  "Luxembourg"),
    "LV": (56.9,  24.55, "Latvia"),
    "NL": (52.2,   5.3,  "Netherlands"),
    "NO": (64.55, 17.95, "Norway"),
    "PL": (51.9,  19.15, "Poland"),
    "PT": (39.5,  -7.85, "Portugal"),
    "RO": (45.95, 25.0,  "Romania"),
    "SE": (62.2,  17.6,  "Sweden"),
    "SI": (46.15, 15.0,  "Slovenia"),
    "SK": (48.65, 19.7,  "Slovakia"),
    "AL": (41.0,  20.0,  "Albania"),
    "BA": (43.9,  17.7,  "Bosnia and Herzegovina"),
    "CY": (35.0,  33.0,  "Cyprus"),
    "MD": (47.0,  28.5,  "Moldova"),
    "ME": (42.5,  19.3,  "Montenegro"),
    "MK": (41.5,  21.5,  "North Macedonia"),
    "RS": (44.0,  21.0,  "Serbia"),
    "UA": (49.0,  32.0,  "Ukraine"),
}

# GEM's `Country/Area` column uses full country names. This maps those
# exact strings (as they appear in the xlsx) to our ISO codes.
#
# Aliases cover names where GEM uses a spelling different from ISO 3166:
#   - 'Czech Republic' is GEM's form; 'Czechia' is ISO.
# Iceland and Malta are intentionally NOT mapped — they have no ENTSO-E data
# (per CLAUDE.md) so they're excluded from `weather_observation` coverage.
GEM_NAME_TO_ISO: dict[str, str] = {
    name: iso for iso, (_lat, _lon, name) in COUNTRY_COORDS.items()
}
GEM_NAME_TO_ISO["Czech Republic"] = "CZ"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two lat/lon points in km."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def classify_wind_tech(installation_type: str | None) -> str | None:
    """Map GEM `Installation Type` column value → our tech_type, or None to skip."""
    if not installation_type:
        return None
    s = str(installation_type).strip().lower()
    if s == "onshore":
        return "wind_onshore"
    if s.startswith("offshore"):
        return "wind_offshore"
    return None  # 'Unknown' or anything else — skip


# ---------------------------------------------------------------------------
# GEM loader
# ---------------------------------------------------------------------------


def _clean_coords(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with missing/invalid lat/lon or non-positive capacity."""
    df = df.dropna(subset=["lat", "lon", "capacity_mw"]).copy()
    df = df[df["capacity_mw"] > 0]
    df = df[(df["lat"].between(-90, 90)) & (df["lon"].between(-180, 180))]
    return df


def load_gem_plants(wind_path: Path, solar_path: Path) -> pd.DataFrame:
    """Load both GEM trackers, return unified `plants` DataFrame.

    Schema: country_iso, tech_type, capacity_mw, lat, lon, name
    Filter: EU39 (via GEM_NAME_TO_ISO) × operating × non-null coords × mw > 0.
    """
    logger.info("Loading GEM wind tracker: %s", wind_path.name)
    wind = pd.read_excel(wind_path, sheet_name="Data")
    wind = wind[wind["Status"].str.lower() == "operating"].copy()
    wind["country_iso"] = wind["Country/Area"].map(GEM_NAME_TO_ISO)
    wind = wind.dropna(subset=["country_iso"])
    wind["tech_type"] = wind["Installation Type"].map(classify_wind_tech)
    wind = wind.dropna(subset=["tech_type"])
    wind = wind.rename(
        columns={"Capacity (MW)": "capacity_mw", "Latitude": "lat", "Longitude": "lon",
                 "Project Name": "name"}
    )[["country_iso", "tech_type", "capacity_mw", "lat", "lon", "name"]]
    wind = _clean_coords(wind)
    logger.info("  → %d operating EU39 wind plants (%d onshore, %d offshore)",
                len(wind),
                (wind["tech_type"] == "wind_onshore").sum(),
                (wind["tech_type"] == "wind_offshore").sum())

    logger.info("Loading GEM solar tracker: %s", solar_path.name)
    solar = pd.read_excel(solar_path, sheet_name="Utility-Scale (1 MW+)")
    solar = solar[solar["Status"].str.lower() == "operating"].copy()
    solar["country_iso"] = solar["Country/Area"].map(GEM_NAME_TO_ISO)
    solar = solar.dropna(subset=["country_iso"])
    solar["tech_type"] = "solar"
    solar = solar.rename(
        columns={"Capacity (MW)": "capacity_mw", "Latitude": "lat", "Longitude": "lon",
                 "Project Name": "name"}
    )[["country_iso", "tech_type", "capacity_mw", "lat", "lon", "name"]]
    solar = _clean_coords(solar)
    logger.info("  → %d operating EU39 solar plants", len(solar))

    return pd.concat([wind, solar], ignore_index=True)


# ---------------------------------------------------------------------------
# Adaptive k-means
# ---------------------------------------------------------------------------


def _kmeans_fit(coords: np.ndarray, weights: np.ndarray, k: int) -> KMeans:
    """Run weighted k-means on (lat, lon) coords. Deterministic via RANDOM_STATE."""
    km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
    km.fit(coords, sample_weight=weights)
    return km


def _cluster_stats(
    coords: np.ndarray, weights: np.ndarray, labels: np.ndarray, k: int
) -> tuple[list[tuple[float, float]], list[float], list[float]]:
    """For each cluster i ∈ [0..k): return (centroid_lat_lon, total_mw, max_radius_km)."""
    centroids: list[tuple[float, float]] = []
    totals: list[float] = []
    radii: list[float] = []
    for i in range(k):
        mask = labels == i
        if not mask.any():
            centroids.append((float("nan"), float("nan")))
            totals.append(0.0)
            radii.append(0.0)
            continue
        cluster_coords = coords[mask]
        cluster_weights = weights[mask]
        # Capacity-weighted centroid — the geographic centre-of-mass for the
        # cluster, which matches how the weather fetcher will interpret it.
        c_lat = float(np.average(cluster_coords[:, 0], weights=cluster_weights))
        c_lon = float(np.average(cluster_coords[:, 1], weights=cluster_weights))
        centroids.append((c_lat, c_lon))
        totals.append(float(cluster_weights.sum()))
        radii.append(
            max(haversine_km(c_lat, c_lon, p[0], p[1]) for p in cluster_coords)
        )
    return centroids, totals, radii


def pick_adaptive_k(
    coords: Iterable[tuple[float, float]],
    mws: Iterable[float],
    tuning: dict,
) -> int:
    """Choose smallest k ∈ [1..max_k] satisfying radius + MW constraints.

    Tie-breaking per the spec:
      1. If MIN_CLUSTER_MW can't be satisfied at any k (total_mw < min), return k=1.
      2. Else (MAX_RADIUS binding), pick the largest k ≤ max_k that still
         satisfies MIN_CLUSTER_MW.
    """
    coords_arr = np.asarray(list(coords), dtype=float)
    weights_arr = np.asarray(list(mws), dtype=float)
    n = len(coords_arr)
    if n == 0:
        return 0
    total_mw = float(weights_arr.sum())
    min_mw = tuning["min_cluster_mw"]
    max_k = min(tuning["max_k"], n)

    # Shortcut: if total MW is below the threshold, splitting always violates
    # the constraint — fall back to k=1 (single country-wide zone).
    if total_mw < min_mw:
        return 1

    max_radius = tuning["max_radius_km"]

    # Try k = 1, 2, ..., max_k. Track the largest k that still satisfies
    # the MW floor so we can fall back per rule 2 if no k meets the radius.
    largest_mw_compliant_k = 1
    for k in range(1, max_k + 1):
        if k > n:
            break
        km = _kmeans_fit(coords_arr, weights_arr, k)
        _, totals, radii = _cluster_stats(coords_arr, weights_arr, km.labels_, k)
        mw_ok = min(totals) >= min_mw if totals else False
        rad_ok = max(radii) <= max_radius if radii else False
        if mw_ok:
            largest_mw_compliant_k = k
        if mw_ok and rad_ok:
            return k

    # No k satisfied both constraints — radius is binding. Return the largest
    # k that still honours the MW floor (rule 2).
    return largest_mw_compliant_k


# ---------------------------------------------------------------------------
# Cluster a single (country, tech) → zones
# ---------------------------------------------------------------------------


def cluster_zones(
    plants: pd.DataFrame, tech_type: str, tuning: dict,
) -> list[dict]:
    """Cluster the given plants for one tech_type and emit zone dicts.

    Returned dict shape:
      {zone_id, lat, lon, weight, capacity_mw, radius_km, n_plants}

    Plants is already filtered to a single (country, tech_type) upstream,
    but we accept the full DataFrame and filter here for test ergonomics.
    """
    sub = plants[plants["tech_type"] == tech_type]
    if sub.empty:
        return []

    coords = sub[["lat", "lon"]].to_numpy(dtype=float)
    mws = sub["capacity_mw"].to_numpy(dtype=float)
    k = pick_adaptive_k(coords, mws, tuning)
    if k == 0:
        return []

    km = _kmeans_fit(coords, mws, k)
    centroids, totals, radii = _cluster_stats(coords, mws, km.labels_, k)
    grand_total = sum(totals)

    # Rank clusters by capacity_mw desc so zone_ids are stable + readable.
    order = sorted(range(k), key=lambda i: -totals[i])
    zones: list[dict] = []
    for rank, i in enumerate(order, start=1):
        c_lat, c_lon = centroids[i]
        mw = totals[i]
        radius = radii[i]
        weight = mw / grand_total if grand_total > 0 else 0.0
        if k == 1:
            zone_id = f"{tech_type}_country"
        else:
            zone_id = f"{tech_type}_{rank}"
        zones.append({
            "zone_id": zone_id,
            "lat": round(c_lat, 4),
            "lon": round(c_lon, 4),
            "weight": round(weight, 4),
            "capacity_mw": round(mw, 1),
            "radius_km": round(radius, 1),
            "n_plants": int((km.labels_ == i).sum()),
        })
    return zones


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------


def build_locations(
    plants: pd.DataFrame,
    country_coords: dict[str, tuple[float, float, str]],
) -> list[tuple]:
    """Generate the full LOCATIONS list of 8-tuples.

    For each country in country_coords:
      1. Emit a centroid row (always).
      2. For each tech in (solar, wind_onshore, wind_offshore): emit cluster zones.

    Plants for countries absent from country_coords are silently skipped.
    """
    rows: list[tuple] = []
    for iso_code in sorted(country_coords):
        lat, lon, name = country_coords[iso_code]
        # Centroid row — always present, always weight=1.0.
        rows.append((iso_code, "centroid", "centroid", lat, lon, 1.0, None,
                     f"{name} centroid"))

        country_plants = plants[plants["country_iso"] == iso_code]
        for tech in ("solar", "wind_onshore", "wind_offshore"):
            tech_plants = country_plants[country_plants["tech_type"] == tech]
            if tech_plants.empty:
                continue
            zones = cluster_zones(tech_plants, tech_type=tech, tuning=TECH_TUNING[tech])
            for z in zones:
                desc = (
                    f"{name} {tech} cluster, {z['capacity_mw']:.0f} MW across "
                    f"{z['n_plants']} plants (radius {z['radius_km']:.0f} km)"
                )
                rows.append((
                    iso_code, z["zone_id"], tech, z["lat"], z["lon"],
                    z["weight"], z["capacity_mw"], desc,
                ))
    return rows


# ---------------------------------------------------------------------------
# Coverage report
# ---------------------------------------------------------------------------


def build_coverage_report(
    plants: pd.DataFrame,
    country_coords: dict[str, tuple[float, float, str]],
    locations: list[tuple],
) -> dict:
    """Per-country-per-tech: plant count, total MW, chosen k, max radius."""
    by_country: dict[str, dict] = {}
    for iso_code in sorted(country_coords):
        name = country_coords[iso_code][2]
        country_plants = plants[plants["country_iso"] == iso_code]
        tech_breakdown: dict[str, dict] = {}
        for tech in ("solar", "wind_onshore", "wind_offshore"):
            tech_plants = country_plants[country_plants["tech_type"] == tech]
            zones_for_tech = [
                loc for loc in locations if loc[0] == iso_code and loc[2] == tech
            ]
            tech_breakdown[tech] = {
                "n_plants": int(len(tech_plants)),
                "total_capacity_mw": round(float(tech_plants["capacity_mw"].sum()), 1),
                "chosen_k": len(zones_for_tech),
                "fell_back_to_country": (
                    len(zones_for_tech) == 1
                    and zones_for_tech[0][1] == f"{tech}_country"
                ),
            }
        by_country[iso_code] = {"name": name, **tech_breakdown}
    return {
        "generated_at_utc": pd.Timestamp.utcnow().isoformat(),
        "total_locations": len(locations),
        "gem_wind_file": GEM_WIND_FILE,
        "gem_solar_file": GEM_SOLAR_FILE,
        "by_country": by_country,
    }


# ---------------------------------------------------------------------------
# File writers
# ---------------------------------------------------------------------------


def _format_locations_block(rows: list[tuple]) -> str:
    """Emit a pretty Python list literal for the AUTOGEN block."""
    lines = [
        "LOCATIONS = [",
        "    # country, zone_id, zone_type, lat, lon, weight, capacity_mw, description",
    ]
    for row in rows:
        country, zone_id, zone_type, lat, lon, weight, cap, desc = row
        cap_lit = "None" if cap is None else f"{cap}"
        lines.append(
            f'    ({country!r:<5}, {zone_id!r:<24}, {zone_type!r:<14}, '
            f'{lat:>7.3f}, {lon:>7.3f}, {weight:>6.3f}, {cap_lit:>7}, {desc!r}),'
        )
    lines.append("]")
    return "\n".join(lines)


def write_locations_block(schema_path: Path, rows: list[tuple]) -> None:
    """Rewrite the AUTOGEN block in src/weather_schema.py in place."""
    text = schema_path.read_text(encoding="utf-8")
    pattern = re.compile(
        rf"{re.escape(LOCATIONS_START_MARKER)}.*?{re.escape(LOCATIONS_END_MARKER)}",
        flags=re.DOTALL,
    )
    new_block = (
        f"{LOCATIONS_START_MARKER} (generated by scripts/build_weather_locations.py"
        f" — do not edit by hand) ===\n"
        f"{_format_locations_block(rows)}\n"
        f"{LOCATIONS_END_MARKER}"
    )
    if not pattern.search(text):
        raise RuntimeError(
            f"AUTOGEN markers not found in {schema_path}. "
            "Add them manually before running the build."
        )
    schema_path.write_text(pattern.sub(new_block, text), encoding="utf-8")


def write_coverage_report(report_path: Path, report: dict) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--gem-dir", default=str(GEM_DATA_DIR),
        help=f"Directory with the GEM xlsx files (default: {GEM_DATA_DIR}).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print a summary + first 20 generated rows, don't write files.",
    )
    args = parser.parse_args()

    gem_dir = Path(args.gem_dir)
    wind_path = gem_dir / GEM_WIND_FILE
    solar_path = gem_dir / GEM_SOLAR_FILE
    for p in (wind_path, solar_path):
        if not p.exists():
            logger.error("Missing GEM input: %s", p)
            return 1

    plants = load_gem_plants(wind_path, solar_path)
    logger.info("Total operating EU39 plants: %d", len(plants))

    locations = build_locations(plants, COUNTRY_COORDS)
    logger.info("Generated %d LOCATIONS rows across %d countries",
                len(locations), len(COUNTRY_COORDS))

    report = build_coverage_report(plants, COUNTRY_COORDS, locations)

    if args.dry_run:
        print("\n=== DRY RUN — no files written ===")
        print(f"Total rows: {len(locations)}")
        print("\nFirst 20 rows:")
        for row in locations[:20]:
            print(" ", row)
        print("\nCountries with zero non-centroid coverage:")
        by_c = report["by_country"]
        for iso in sorted(by_c):
            if all(by_c[iso][t]["chosen_k"] == 0 for t in ("solar", "wind_onshore", "wind_offshore")):
                print(f"  {iso} ({by_c[iso]['name']})")
        return 0

    write_locations_block(SCHEMA_PATH, locations)
    logger.info("Wrote LOCATIONS block → %s", SCHEMA_PATH)
    write_coverage_report(REPORT_PATH, report)
    logger.info("Wrote coverage report → %s", REPORT_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
