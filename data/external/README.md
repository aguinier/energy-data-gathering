# `data/external/` — third-party inputs for `build_weather_locations.py`

This directory is used by `scripts/build_weather_locations.py` (Phase 2
of the unified weather locations roadmap). The raw input files are
**not committed** — they're large, licensed, and change over time.

## Inputs used by the build script

The build script reads two Excel files from the **shared data
directory** at `C:/Code/able/data/` (alongside the prod DB replica):

| File | Source | License | Refresh |
|---|---|---|---|
| `Global-Wind-Power-Tracker-February-2026.xlsx` | [Global Energy Monitor — Global Wind Power Tracker](https://globalenergymonitor.org/projects/global-wind-power-tracker/download-data/) | CC-BY 4.0 | Quarterly |
| `Global-Solar-Power-Tracker-February-2026.xlsx` | [Global Energy Monitor — Global Solar Power Tracker](https://globalenergymonitor.org/projects/global-solar-power-tracker/download-data/) | CC-BY 4.0 | Quarterly |

The path is hardcoded in the build script as `GEM_DATA_DIR =
C:/Code/able/data/`. If you move the files, update that constant.

## How to refresh

When GEM publishes a new release (typically quarterly):

1. Request a fresh download from both GEM tracker pages (simple form — name, email, affiliation).
2. Replace the two xlsx files in `C:/Code/able/data/` with the new release.
3. Re-run `python scripts/build_weather_locations.py` from inside this repo.
4. Review the diff in `src/weather_schema.py` (specifically the
   `LOCATIONS-AUTOGEN` block) and the freshly-written
   `data/external/build_locations_report.json`.
5. If the diff is sensible, commit both files and open a PR.

## Outputs written here

| File | Committed? | Purpose |
|---|---|---|
| `build_locations_report.json` | ✅ yes | Per-country-per-tech: plant count, total MW, chosen k, max radius km, fell-back-to-centroid flag. Snapshot of the last successful build. |

The generated `LOCATIONS = [...]` block is written into
`src/weather_schema.py` between `# === LOCATIONS-AUTOGEN-START ===`
and `# === LOCATIONS-AUTOGEN-END ===` markers — not in this directory.

## What's NOT used (vs the original design spec)

The 2026-04-23 design spec also listed OPSD and ENTSO-E installed
capacity as inputs. They're deliberately omitted from the MVP build:

- **OPSD** `renewable_power_plants_EU.csv` — last release 2020-08-25
  (5.5 years stale) and only 8 countries (DE/DK/FR/PL/UK/CH/SE/CZ). GEM
  February 2026 covers all 39 with fresher data. If OPSD-specific
  granular data becomes useful later (e.g., for DE rooftop), add it
  then.
- **ENTSO-E installed capacity (document A68)** — originally planned
  as a country-total sanity check. GEM's plant-sum capacity per
  `(country, tech)` gives us the same numbers. Revisit if a country's
  GEM sum looks implausible.
