# Unified Weather Locations + Coherence Tooling — Design Spec

**Date:** 2026-04-23
**Status:** Draft, pending user review
**Related:** `WEATHER_DB.md`, `EXTENDING.md`, `weather-db-extend` skill, `weather-db-deploy` skill

## Goal

Collapse the weather sampling stack to one source of truth and one fetcher, while expanding `weather_observation` from one country (BE, 5 zones) to **all 39 European countries × 3 generation tech types × 1–10 capacity-weighted zones**.

End state at a glance:

| Today | After |
|---|---|
| 4 weather tables (`weather_data`, `weather_data_multipoint`, `weather_point_data`, `weather_observation`) | 1 (`weather_observation`) |
| 3 coordinate sources of truth (`COUNTRY_COORDINATES`, `BE_LOCATIONS`, `WEATHER_ZONES`) | 1 (`LOCATIONS` in `weather_schema.py`) |
| 3 weather fetchers (`fetch_weather.py`, `fetch_weather_multipoint.py`, `fetch_weather_observation.py`) | 1 (`fetch_weather_observation.py`) |
| 5 sampling locations (BE only) | ~470 locations: 39 centroids + ~430 capacity-weighted tech zones |

## Motivating audit findings

From the 2026-04-23 weather-pipeline audit:

1. **Canonical replica broken** — `/c/Code/able/data/energy_dashboard.db` (3.9 GB) has an all-zero header, helio research can't read.
2. **Three coordinate sources duplicate the BE centroid** identically; FR/DE generation-zone definitions live in a third file.
3. **Three legacy weather tables are dead or partial** — `weather_point_data` (5 mo stale), `weather_data_multipoint` (3 countries only), `weather_data | best_match | actual` (frozen Nov 2025).
4. **Crontab + skill docs claim "4 NWP models"** — reality is 7 since recent commits.
5. **No drift-detection** between schema constants and DB rows; the gap above went unnoticed for weeks.

## Scope (4 phases + Phase 0 quick wins)

This spec covers all phases. Implementation lands incrementally with verification gates between phases.

---

### Phase 0 — Quick wins (this session, ~1 hr)

#### 0.1 Investigate broken `/c/Code/able/data/` replica
- Diagnose: file is sparse-allocated (header is `0x00 × 16`) but 3.9 GB on disk, suggesting interrupted rsync or pre-allocation that never received data.
- Confirm working source-of-truth on prod (`192.168.86.36:/home/clavain/energy-dashboard/data/energy_dashboard.db`) is intact via `ssh ... sqlite3 ... "PRAGMA quick_check"`.
- Repair via re-running the daily sync; document the recovery command in `WEATHER_DB.md` runbook.

#### 0.2 Coherence-check script
**File:** `scripts/check_weather_coherence.py`

**Behavior:** compares schema-declared constants in code vs. actual DB rows for three dimensions:
1. `OPEN_METEO_SOURCES` (from `weather_schema.py`) ↔ `SELECT provider, model_id, lead_time_hours FROM weather_source`
2. `LOCATIONS` (from `weather_schema.py`, post-Phase-1) ↔ `SELECT country_code, zone_id, zone_type FROM weather_location`
3. `WEATHER_VARIABLE_COLUMNS` (from `weather_schema.py`) ↔ `PRAGMA table_info(weather_observation)`

**CLI:**
```bash
python scripts/check_weather_coherence.py [--db PATH]
# Exit 0 = coherent; exit 1 = drift; prints set-diff in both directions per dimension.
```

**Integration:** invoked by `init_weather_observation.py --verify` after the existing seed/check logic.

#### 0.3 Doc refresh
- `docker/crontab:11` comment: "all 4 NWP models" → "all 7 NWP models" (matches `REALTIME_NWP_MODELS` reality).
- `weather-db-query` skill (located at user-machine `~/.claude/skills/weather-db-query/SKILL.md`, or wherever the operator's Claude config lives): source-id reference table updated to enumerate the full current source list (1 + 7 + 21 = 29 sources, with day1/day2/day3 leads). Note: the skill file is outside this repo — operator commits separately or via dotfiles repo.

---

### Phase 1 — Schema migration

**Migration script:** `scripts/migrate_add_zone_type.py`

```sql
ALTER TABLE weather_location ADD COLUMN zone_type TEXT;
ALTER TABLE weather_location ADD COLUMN capacity_mw REAL;
CREATE INDEX IF NOT EXISTS idx_weather_location_zone_type
    ON weather_location(zone_type);

-- Backfill: existing 5 BE rows get zone_type assigned.
-- 'centroid' row is the legacy single-point Able centroid.
-- 'central'/'north'/'south'/'east' were originally PV-capacity-weighted
-- zones, so they map to zone_type='solar' under the new taxonomy.
UPDATE weather_location SET zone_type = 'centroid'
    WHERE zone_id = 'centroid';
UPDATE weather_location SET zone_type = 'solar'
    WHERE zone_id IN ('central', 'north', 'south', 'east');
```

**Per CLAUDE.md `EXTENDING.md` rules:**
- Backup prod DB before running (`.backup` to dated file).
- Test on `/tmp/scratch.db` copy first.
- Verify with `init_weather_observation.py --verify` after.

**Coherence script extension:** `LOCATIONS` dimension check is added in this phase (depends on the new columns existing).

---

### Phase 2 — Sourcing pipeline

**File:** `scripts/build_weather_locations.py`

**Inputs (committed under `data/external/`):**
- OPSD `renewable_power_plants_EU.csv` (best coverage for DE/FR/CH/NL/BE/DK; lat/lon + capacity_mw + technology)
- Global Energy Monitor Wind Power Tracker (≥10 MW projects, global, 2024 vintage; CSV under research license)
- Global Energy Monitor Solar Power Tracker (≥10 MW projects, global, 2024 vintage)
- ENTSO-E installed capacity per country per tech (country totals, used for sanity-checking + when normalizing missing capacity weights)

**Algorithm per `(country_code, tech_type)` ∈ {39 countries} × {solar, wind_onshore, wind_offshore}:**

1. Filter inputs to country + tech, dedupe by name + coords.
2. Cap outliers: drop plants with `capacity_mw < 1` or missing lat/lon.
3. Adaptive k-means clustering:
   - Try `k = 1, 2, ..., MAX_N=10`.
   - For each `k`: run k-means with `random_state=42` (reproducibility), compute cluster radii (max distance from centroid in km, haversine) and cluster MW totals.
   - Pick smallest `k` such that **all cluster radii ≤ MAX_RADIUS_KM** AND **smallest cluster MW ≥ MIN_CLUSTER_MW**.
   - **Tie-breaking when no `k ≤ MAX_N` satisfies both constraints:**
     1. If MIN_CLUSTER_MW is the binding constraint (every k violates it), the country lacks enough installed capacity to support multi-zone modeling: emit a single zone (k=1) with `zone_id='<tech>_country'`, weight=1.0, capacity_mw = country total.
     2. Else (MAX_RADIUS is binding), pick the largest `k ≤ MAX_N` that still satisfies MIN_CLUSTER_MW. Log a warning to the coverage report so a human can review whether MAX_N should be raised for that country.
4. Tech-specific tuning:
   | Tech | MAX_RADIUS_KM | MIN_CLUSTER_MW | Notes |
   |---|---|---|---|
   | `solar` | 150 | 500 | Cloud-field decorrelation length |
   | `wind_onshore` | 100 | 300 | Shorter scale; wind decorrelates faster |
   | `wind_offshore` | 80 | 200 | Tightest; only run for coastal allowlist |
5. Cluster centroid → `(lat, lon)` (rounded to 4 decimals); `weight = cluster_mw / country_tech_total_mw`.
6. Per `(country, tech_type)`, weights sum to 1.0 (normalize).

**Coastal allowlist for `wind_offshore`:** Hardcoded as `OFFSHORE_WIND_COUNTRIES = {"BE","DE","DK","ES","FI","FR","GB","IE","IT","NL","NO","PL","PT","SE"}` constant in `build_weather_locations.py` (14 countries with ≥1 operating offshore farm as of 2025). When a new country commissions its first offshore farm, add to this set and rerun the build script.

**Fallback:** countries with zero plant data for a tech → no row of that `zone_type`. Every country always gets one `zone_type='centroid'` row regardless.

**Output:**
1. Generated `LOCATIONS = [...]` block written into `src/weather_schema.py` between marker comments:
   ```python
   # === LOCATIONS-AUTOGEN-START === (do not edit by hand; rebuild with build_weather_locations.py) ===
   LOCATIONS = [...]
   # === LOCATIONS-AUTOGEN-END ===
   ```
2. Coverage report `data/external/build_locations_report.json` — per (country, tech_type): plant count, total MW, chosen N, max radius, min cluster MW, fell-back-to-centroid flag.

**Run cadence:** annual manual rebuild (capacity grows but plant locations are sticky). Trigger: when a new OPSD or GEM tracker release ships (typically Q1), the operator pulls the new CSVs into `data/external/`, runs the script, reviews the diff in the generated `LOCATIONS` block, and commits the updated `weather_schema.py`. Not cron-scheduled — review-gated.

---

### Phase 3 — Fetcher updates + backfill

#### 3.1 Fetcher changes
- `src/fetch_weather_observation.py` reads full `LOCATIONS` instead of hardcoded BE-only iteration.
- `scripts/init_weather_observation.py` seeds all `LOCATIONS` rows into `weather_location` (idempotent — `INSERT OR IGNORE` on `(country_code, zone_id)` natural key).

#### 3.2 ERA5 historical backfill
- New script: `scripts/backfill_weather_observation_era5.py`.
- Per new location (i.e., everything except the 5 BE rows already in `weather_observation`): pull Open-Meteo Archive API for 2023-01-01 → present.
- Estimated runtime: ~2-3 days (rate-limited at 0.5 s/request × ~470 locations × ~1100 days / chunk size).
- Resumable: skip locations already at `MAX(valid_at) >= today - 7d`.

#### 3.3 Forecasts (no backfill)
Realtime + previous_runs forecasts start accumulating from the next cron tick. No historical forecast backfill — accepted as the new-table value prop is forward-looking accumulation.

#### 3.4 Quota probe
Before kicking off backfill, `build_weather_locations.py` reports projected daily call count:
```
Projected hourly realtime calls:    <N_locations × 7 models>
Projected daily previous_runs calls: <N_locations × 7 models × 3 leads × 3 runs/day>
Total projected:                     <X> calls/day
```
Fail-loud if projected > Open-Meteo Professional tier limit (operator confirms tier limit at runtime via `--quota-limit N` flag; defaults to a conservative 200k/day).

---

### Phase 4 — Deprecate legacy

#### 4.0 Preflight: consumer audit (before any DROP)
- Grep `/c/Code/able/energy-dashboard-frontend/` for `weather_data`, `weather_point_data`, `weather_data_multipoint` references.
- Grep `/c/Code/able/helioforge/` for same.
- For any hits on `weather_data_multipoint` with `zone_type IN ('hydro','biomass')`: the consumer must either drop the dependency or accept loss of those weather points. Decision logged before proceeding.

#### 4.1 Frontend migration
Audit `/c/Code/able/energy-dashboard-frontend/` for reads of `weather_data`. Migrate each to `weather_observation` filtered by `zone_type='centroid'` (semantically equivalent for current single-point dashboards). Separate PR in the frontend repo.

#### 4.2 Legacy table drops
After 1 quarter of `weather_observation` running stable, in this order:
1. DROP TABLE `weather_point_data` — already dead since Nov 2025.
2. DROP TABLE `weather_data_multipoint` — solar/wind_onshore/wind_offshore data for BE/DE/FR now lives as the corresponding `zone_type` in `weather_observation`. **Note:** legacy table also held `hydro` and `biomass` zone_type for those 3 countries; those are NOT migrated (out of scope) and any consumer relying on hydro/biomass weather points must be migrated or rebuilt before this DROP. Audit consumers in Phase 4.0 (preflight).
3. Mark `weather_data` read-only via prod permissions; archive to backup; DROP after one more quarter.

#### 4.3 Code cleanup
- DELETE `src/weather_zones_real.py` (content lives in `LOCATIONS`).
- DELETE `src/fetch_weather.py` (legacy single-point fetcher).
- DELETE `src/fetch_weather_multipoint.py`.
- DELETE `scripts/update_weather.py`, `scripts/backfill_weather.py`, `scripts/backfill_multipoint.py`.
- Remove the daily 15:00 UTC cron entry for `update_weather.py` from `docker/crontab`.

## Architecture

### Single source of truth: `weather_schema.py::LOCATIONS`

Flat list of tuples, auto-generated by `build_weather_locations.py`:

```python
LOCATIONS = [
    # country_code, zone_id, zone_type, lat, lon, weight, capacity_mw, description
    ("BE", "centroid",       "centroid",      50.5,   4.45, 1.00,  None,    "Belgium centroid"),
    ("BE", "solar_central",  "solar",         50.78,  4.41, 0.42,  2310.0,  "BE solar cluster, 2310 MW"),
    ("BE", "solar_north",    "solar",         51.10,  4.83, 0.31,  1705.0,  "BE solar cluster, 1705 MW"),
    ("BE", "wind_on_west",   "wind_onshore",  50.95,  3.20, 0.45,  1620.0,  "BE wind onshore, 1620 MW"),
    ("BE", "wind_off_north", "wind_offshore", 51.65,  2.85, 1.00,  2260.0,  "BE wind offshore (single)"),
    ("DE", "centroid",       "centroid",      51.2,  10.45, 1.00,  None,    "Germany centroid"),
    # ... ~470 rows total
]
```

**Invariants enforced by build script + checked by coherence script:**
- Every country has exactly one `zone_type='centroid'` row with `weight=1.0`.
- Per `(country, tech_type)` group, weights sum to 1.0 (within float tolerance).
- `zone_id` is unique within a country.
- `lat ∈ [-90, 90]`, `lon ∈ [-180, 180]`, `weight ∈ [0, 1]`.

### Coherence script architecture

```
scripts/check_weather_coherence.py
├── load_schema_dimensions() → returns 3 sets
│   ├── set of (provider, model_id, lead_time_hours) from OPEN_METEO_SOURCES
│   ├── set of (country_code, zone_id, zone_type) from LOCATIONS
│   └── set of column names from WEATHER_VARIABLE_COLUMNS
├── load_db_dimensions(db_path) → returns 3 sets via SQL
└── diff(schema_set, db_set) → returns (only_in_schema, only_in_db); fail on either non-empty
```

### Build pipeline data flow

```
data/external/
├── opsd_renewable_power_plants_EU.csv  ─┐
├── gem_wind_power_tracker.csv          ─┼→ build_weather_locations.py
├── gem_solar_power_tracker.csv         ─┤      ├── filter per (country, tech)
└── entsoe_installed_capacity.csv       ─┘      ├── adaptive k-means
                                                ├── normalize weights
                                                ├── write LOCATIONS into weather_schema.py
                                                └── emit coverage report
                                                            ↓
                                          init_weather_observation.py
                                          (seeds weather_location table)
                                                            ↓
                                          fetch_weather_observation.py
                                          (hourly/3×daily fetch per location × source)
                                                            ↓
                                          weather_observation table
                                                            ↓
                                          consumers (frontend, helio, dashboards)
```

## Testing strategy

### Unit tests
- `tests/test_build_weather_locations.py`:
  - Given fake plant CSVs (deterministic), verify k-means picks expected `N` per country/tech.
  - Verify weights sum to 1.0 per `(country, tech_type)`.
  - Verify random seed produces identical output across runs.
  - Verify fallback: country with zero plants → no rows of that tech_type, only centroid.
- `tests/test_check_weather_coherence.py`:
  - Given mocked schema + DB cursor, verify drift in each of the 3 dimensions is detected.
  - Verify exit code 0 on perfect match.

### Integration tests
- Schema migration: run on `/tmp/scratch.db` copy of prod, verify `PRAGMA table_info(weather_location)` shows new columns and `weather_observation`-querying scripts still pass.
- Coherence script: against a real DB, verify exit 0 in coherent state and exit 1 with intentional drift (insert a fake `weather_source` row, verify detection).

### Smoke checks
- After Phase 3 backfill of any one new country: spot-check ERA5 row count matches `(today - 2023-01-01) × 24` hours per location.
- After Phase 3 cron tick: spot-check forecast rows appear for new countries within 1 hour.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Plant data coverage varies (Eastern Europe / small countries may be sparse) | Build script reports coverage; uncovered (country, tech) → no rows of that tech_type; centroid always present |
| K-means non-determinism | `random_state=42` pinned in build script |
| Forecast accumulation gap (no historical forecast for new countries) | Accepted; documented in spec; future helio backtests for non-BE countries start from deploy date |
| API quota surprise on Professional tier | Quota probe in build script; fail-loud if projected exceeds operator-configured limit |
| Frontend break during Phase 4 read migration | Frontend PR is separate workstream; legacy `weather_data` kept read-only during transition quarter |
| Helio research breaks if `weather_data` schema changes | Spec only DROPs `weather_data` after frontend migration confirmed; helio reads via `weather_db_loader.py` and can switch to `weather_observation` independently |
| Hydro/biomass weather points (BE/DE/FR only, in `weather_data_multipoint`) lost on DROP | Phase 4.0 preflight audit confirms no consumer depends on them. If a consumer does, either migrate that consumer first or expand spec scope to include hydro/biomass tech_types |

## Out of scope

- Hydro / biomass capacity-weighted zones (not requested; legacy `weather_zones_real.py` carries this for FR/DE only — leave behind on file deletion).
- Resource-potential zoning via Global Wind Atlas / PV-GIS (alternative method considered in brainstorm, deferred).
- Helio research backtest revalidation against new zone definitions (owned by `helioforge` repo).
- Frontend repo PR for read migration (separate workstream; this spec only describes the requirement).

## Build sequence

Implementation lands in this order, with a verification gate between each phase:

1. **Phase 0** — investigate replica + coherence script + doc refresh (parallel-safe, all this session)
2. **Phase 1** — schema migration on scratch → on prod (with backup)
3. **Phase 2** — build pipeline + first run + manual review of generated `LOCATIONS`
4. **Phase 3** — fetcher updates + ERA5 backfill (multi-day runtime)
5. **Phase 4** — frontend migration + legacy code/table drops (separate PR cycle, post-stability)

## Open questions for review

None at draft time. Update this section if review surfaces any.
