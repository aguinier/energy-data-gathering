# Weather Sources — Catalog, Quality, and Caveats

**Last verified:** 2026-04-22 (post nwp-bakeoff investigation)
**Audience:** anyone consuming `weather_observation` for inference,
training, backtesting, or disagreement features.

This is the **per-source data quality** companion to
[`WEATHER_DB.md`](WEATHER_DB.md). `WEATHER_DB.md` describes the
*architecture* (schema, ingest cadence, replay queries). This doc
describes **what's in each source, what's trustworthy, and what
silently isn't**.

If you're about to write code that reads `weather_observation` for a
particular `(provider, model_id, lead_time_hours)`, **read this
document first**. There is at least one silent-fallback bug in
Open-Meteo's API that will bite you otherwise.

## Source catalog (29 rows in `weather_source` as of 2026-04-22)

```
provider                 | model_id                     | lead_h | description
-------------------------+------------------------------+--------+-----------------------------
open_meteo_archive       | era5                         |      0 | ERA5 reanalysis (truth)

open_meteo_forecast      | best_match                   |     -1 | Realtime, Open-Meteo blend
open_meteo_forecast      | ecmwf_ifs025                 |     -1 | Realtime, ECMWF IFS 0.25°
open_meteo_forecast      | icon_seamless                |     -1 | Realtime, DWD ICON-EU 11 km
open_meteo_forecast      | gfs_seamless                 |     -1 | Realtime, NOAA GFS 0.11°
open_meteo_forecast      | knmi_harmonie_arome_europe   |     -1 | Realtime, KNMI HARMONIE-AROME 5.5 km
open_meteo_forecast      | meteofrance_arome_france     |     -1 | Realtime, Météo-France AROME 1.3 km
open_meteo_forecast      | icon_d2                      |     -1 | Realtime, DWD ICON-D2 2.2 km

open_meteo_previous_runs | <each of the 7 above>        | 24/48/72 | Historical archive, day1/2/3 lead
```

7 NWP models × 3 leads (day1=24h, day2=48h, day3=72h) on Previous Runs
= 21 rows; plus 7 realtime + 1 ERA5 = **29 sources total**.

The full list is generated from `OPEN_METEO_SOURCES` in
[`src/weather_schema.py`](src/weather_schema.py) and seeded by
`scripts/init_weather_observation.py`.

## THE Open-Meteo Previous Runs surrogate fallback (CRITICAL CAVEAT)

When you query the Previous Runs API with `models=<short_range_model>`
for a lead time the model can't actually serve at that location, the
API silently returns the corresponding **`icon_seamless`** values
instead — without any flag in the response indicating it did so.

Verified by direct shortwave-column diff against `icon_seamless` over
80,253 hourly rows of 2024-01 → 2026-04 day1 data:

| `models=` requested | Distinct from icon_seamless | Identical to icon_seamless | Distinct % |
|---|---:|---:|---:|
| `icon_d2` | 1,811 | 78,442 | **2.3 %** |
| `meteofrance_arome_france` | 60,622 | 19,631 | **75.5 %** |
| `knmi_harmonie_arome_europe` | 61,576 | 18,677 | **76.7 %** |
| (`ecmwf_ifs025` — control) | 80,253 | 0 | 100 % |

**Read this carefully:**

* **`icon_d2` Previous Runs is essentially `icon_seamless` in disguise**
  (97.7 % bit-identical on shortwave). Don't use it for anything
  expecting independent signal — it's a duplicate column. It was
  caught when two distinct single-model strategies in the helioforge
  bakeoff produced bit-identical PV-MAE results
  (`single_icon_d2` MAE = `single_icon_seamless` MAE = 347.7442…
  to 6 decimals).
* **KNMI and Meteofrance Previous Runs are mostly genuine** (75–77 %
  distinct). The 23–25 % surrogate rate is concentrated at hours past
  each model's native horizon (KNMI maxes at ~48 h, Meteofrance ~48 h).
  Treat day1 as legitimate; day2/day3 we haven't yet verified, but
  expect higher surrogate rates as the horizon is exceeded.
* **The realtime `/v1/forecast` API does NOT have this problem.**
  KNMI / AROME / ICON-D2 via the realtime path return genuinely
  distinct data (with NULLs past their horizon, not surrogates).
  This is the path heliocast uses for inference.
* **All 4 legacy models (best_match, ecmwf_ifs025, icon_seamless,
  gfs_seamless) are genuinely distinct from each other on day1 + day3.**
  No surrogate concerns.

**How to detect surrogate contamination on a new source:** pull the
source's day1 shortwave for the same window as `icon_seamless`'s day1,
diff column-wise. If > 50 % of rows are bit-identical (`abs(diff) <
0.001`), it's surrogate-contaminated. See *Reproduction* below for the
exact one-liner.

## Per-source quality profiles

Day1 metrics computed against ERA5 ground truth on 9,785 daytime
hours (`shortwave_radiation > 10 W/m²`), 2024-01-01 → 2026-04-15
window. From `helioforge/evaluation/results/nwp_weather_skill.json`.

| Source | Day1 MAE (W/m²) | Day1 RMSE | Day1 bias | Verdict |
|---|---:|---:|---:|---|
| **ECMWF IFS025** | **33.2** | 54.9 | +3.4 | Best single-model day1. Backbone of any ensemble. |
| icon_d2 ⚠ | 43.6 | 69.9 | -14.4 | **Surrogate-contaminated** — actually icon_seamless 97.7 % of the time. Real ICON-D2 only available realtime. |
| icon_seamless | 44.8 | 72.4 | -12.6 | Solid; best for `cloud_cover` (separate from shortwave). |
| Meteofrance AROME | 49.0 | 82.5 | +4.6 | 75.5 % genuine signal; mild positive bias. Good high-res complement to ECMWF. |
| best_match | 51.0 | 81.0 | -11.3 | Open-Meteo's auto-blend — opaque. Day1 weaker than ECMWF; day3 surprisingly best (47.7) — best_match's blend logic favors longer leads. |
| gfs_seamless | 59.1 | 93.4 | +8.3 | Weakest of the legacy 4. Candidate to retire. |
| **KNMI HARMONIE-AROME** | **97.6** | 168.3 | **-70.7** | **Genuine but heavily biased low.** Mean shortwave 93.7 W/m² vs ERA5's 128.1 (-27 %). Episodic value on bad-NWP-but-clear-reality days (Apr 21 hindcast: -208 RMSE). Long-term: worst of the 7. |

Day3 has data only for the legacy 4 (short-range models return all
NULL beyond ~48 h). Day3 best is `best_match` at 47.7 MAE — same
ranking otherwise.

### Per-cloud-regime — shortwave_radiation MAE day1

ECMWF wins every regime. KNMI is worst in every regime.

| Regime | ECMWF | icon_seamless | gfs | best_match | KNMI |
|---|---:|---:|---:|---:|---:|
| clear (<30 %) | **14.8** | 22.9 | 21.5 | 24.5 | 52.9 |
| partly (30-70 %) | **31.8** | 51.2 | 61.6 | 58.0 | 115.4 |
| overcast (>70 %) | **40.0** | 49.9 | 71.1 | 57.6 | 106.5 |

## Trust matrix — what to use for what

| Use case | Trustworthy sources | Notes |
|---|---|---|
| **Ground truth** for evaluation | `era5` (open_meteo_archive) | Reanalysis; gold standard. 1 day lag. |
| **Production inference** (heliocast realtime forecast) | `open_meteo_forecast` × {best_match, ecmwf_ifs025, icon_seamless, gfs_seamless, knmi_harmonie_arome_europe, meteofrance_arome_france, icon_d2} | All 7 are genuine via the realtime API. icon_d2 + meteofrance accumulating since 2026-04-22. |
| **Training data** for new models | Previous Runs day1/day3 of `best_match`, `ecmwf_ifs025`, `icon_seamless`, `gfs_seamless`, plus `knmi_harmonie_arome_europe` and `meteofrance_arome_france` (with caveat that KNMI carries a -71 W/m² bias the model will learn around). | **DO NOT** use `icon_d2` Previous Runs for training — it's surrogate-contaminated. |
| **Backtesting** (helioforge) | Previous Runs day1 for legacy 4 + KNMI + Meteofrance. | Stage 1 + Stage 2 of the helioforge bakeoff use this path. |
| **Multi-NWP disagreement features** | The 4 legacy models — what v018/v020/v021 were trained on. | Adding KNMI/Meteofrance-day1 to disagreement is reasonable; adding `icon_d2`-day1 would inject correlated noise (it == icon_seamless). |
| **Replay** (`weather_as_of(at=T)`) | All sources, but interpret carefully. | Previous Runs `fetched_at` accumulates per-cron-tick (3×/day). Realtime accumulates hourly. Real "what production saw" replay is realtime-only. |

## Per-source ingest cadence

| Source family | Ingest cadence | Coverage range |
|---|---|---|
| `open_meteo_forecast` (lead=-1) | hourly (XX:30 UTC, `update_weather_observation_hourly.py`) | Past 1 day + next 7 days, refreshed every hour |
| `open_meteo_previous_runs` (lead 24/48/72) | 3×/day (07:00, 13:30, 19:30 UTC, `update_weather_observation.py`) | Past 7 days, refreshed each tick |
| `open_meteo_archive` (era5) | daily | Up to ~24h ago |

Backfill of all 21 Previous Runs combos (7 models × 3 leads) over
2024-01-01 → 2026-04-22 was performed once on 2026-04-22 via
`scripts/backfill_previous_runs_full.py`; result: 102,120 rows per
combo, all with `fetched_at = 2026-04-22T19:21:33Z`. Every subsequent
3×/day cron tick adds a new `fetched_at` snapshot for the trailing
7-day window.

## Reproduction — verify these claims yourself

### Re-check the surrogate-fallback rate for any source

```python
import pandas as pd
from pathlib import Path

REF = "icon_seamless"   # the one that gets substituted
TEST = "icon_d2"        # the suspect

ref = max(Path("tools").glob(f"weather_nwp_{REF}_day1_agg_*.csv"),
          key=lambda p: p.stat().st_size)
tst = max(Path("tools").glob(f"weather_nwp_{TEST}_day1_agg_*.csv"),
          key=lambda p: p.stat().st_size)
ref_df = pd.read_csv(ref, parse_dates=["timestamp_utc"]).set_index("timestamp_utc")
tst_df = pd.read_csv(tst, parse_dates=["timestamp_utc"]).set_index("timestamp_utc")

common = ref_df.index.intersection(tst_df.index)
diff = (ref_df.loc[common, "shortwave_radiation"]
        - tst_df.loc[common, "shortwave_radiation"]).abs()
print(f"{TEST}: identical to {REF} in {(diff < 0.001).sum()}/{len(common)} rows "
      f"({100 * (diff < 0.001).mean():.1f}%)")
# > 50% identical → surrogate contaminated.
```

### Re-run Stage 1 weather skill (helioforge)

```bash
cd helioforge
python scripts/analyze_nwp_weather_skill.py
# writes evaluation/results/nwp_weather_skill.json + console ranking
```

### Spot-check raw realtime ingest is truly distinct (e.g. for KNMI)

```sql
-- Connect to a synced read-only copy of energy_dashboard.db
SELECT
  ws.model_id,
  AVG(wo.shortwave_radiation_wm2) as mean_ghi,
  COUNT(*) as n_rows
FROM weather_observation wo
JOIN weather_source ws ON wo.source_id = ws.source_id
WHERE ws.provider = 'open_meteo_forecast'
  AND ws.lead_time_hours = -1
  AND wo.fetched_at > datetime('now', '-2 hours')
  AND wo.shortwave_radiation_wm2 IS NOT NULL
GROUP BY ws.model_id
ORDER BY mean_ghi;
-- KNMI's mean_ghi should be visibly different from icon_seamless,
-- because realtime has no surrogate fallback.
```

## Open questions

* **day2 surrogate rate for short-range models** — not yet measured;
  we registered day2 (lead=48) but haven't audited surrogate
  contamination there. Expected: KNMI/Meteofrance day2 still mostly
  genuine (their horizons reach ~48 h); icon_d2 day2 likely worse than
  day1 (probably ~99 % surrogate).
* **`ecmwf_aifs025`** (the AI-driven ECMWF model) was deliberately
  excluded — Previous Runs returns NaN for `shortwave_radiation`
  on it. Worth re-checking periodically; if it starts returning real
  shortwave, it's a candidate to add.
* **Per-zone surrogate contamination** — the diff above was on the
  4-zone capacity-weighted aggregate. Whether one specific zone
  (e.g. east) has a different surrogate rate than central is
  unverified.

## Change log

* **2026-04-22** — initial doc. 7 sources audited at day1. Surrogate
  fallback discovered for icon_d2 Previous Runs (97.7 %). KNMI's -71
  W/m² shortwave bias confirmed. helioforge bakeoff (`nwp-bakeoff`
  branch) produced the source data: see
  `helioforge/evaluation/nwp_bakeoff_report.md`.

## Related

* [`WEATHER_DB.md`](WEATHER_DB.md) — schema + architecture (read this first)
* [`EXTENDING.md`](EXTENDING.md) — how to add a new source / variable / country
* `helioforge/evaluation/nwp_bakeoff_report.md` — full bakeoff write-up
  including PV-skill (Stage 2) and timing-replay (Stage 3) findings
* `helioforge/scripts/analyze_nwp_weather_skill.py` — re-run Stage 1
