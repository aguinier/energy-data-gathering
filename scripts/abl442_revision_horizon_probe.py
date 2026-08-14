#!/usr/bin/env python3
"""Read-only probe: is a stored actual's LEVEL a function of how old it was when we fetched it?

ABL-442, routed from ABL-439. Background, in one paragraph.

`config.UPDATE_DAYS_BACK` is 7, and `scripts/update.py` re-fetches exactly that
trailing window on the routine cron. Its own docstring gives one of the three
purposes as "Data revisions". ABL-439 measured, on NL `wind_onshore`, that
ENTSO-E's revision horizon is roughly **four weeks**, not one -- so the routine
job re-fetches inside a window where the upstream value has not settled, and
then never looks again. Every row therefore freezes on whichever vintage was
current about 7 days after delivery, unless an ad-hoc backfill happens to touch
it later. Which vintage any given row holds is decided by when someone last ran
a backfill, not by policy.

This script measures that, read-only. It changes nothing and fetches nothing.

WHY `fetched_at` / `created_at` IS THE RIGHT CLOCK
--------------------------------------------------
Both are written as `CURRENT_TIMESTAMP` inside an `INSERT OR REPLACE`
(`src/db.py`, e.g. the generation upsert and the load upsert). `INSERT OR
REPLACE` deletes the conflicting row and inserts a new one, so the column is
rewritten on every pass -- it dates the LAST write, not the first. That makes
`age_at_fetch = fetched_at - timestamp_utc` exactly "how old was this instant
when we last asked upstream about it", which is the quantity the revision
horizon is defined against. Note the naming is not uniform and is not a
semantic distinction: `energy_generation` and `energy_renewable` call it
`fetched_at`, `energy_load` and `energy_price` call it `created_at`. `--exposure`
prints the column it used for each table.

THE MEASUREMENT, AND WHY IT IS NOT CONFOUNDED BY WEATHER
--------------------------------------------------------
Comparing "old rows" against "new rows" directly would compare January against
July, and a wind series really is different in January. The `sweep` mode avoids
that entirely by using a SAME-INSTANT control:

`energy_generation` and `energy_renewable` are written from ONE A75 fetch
(`src/fetch_renewable.py` -> `query_generation_and_renewable_with_metadata`),
so for the columns they share they are two recordings of one quantity. Their
ratio at the same (country, timestamp) has no weather in it. What it does have
is vintage: the routine job writes both tables, but the ad-hoc
`scripts/backfill_generation.py` writes only `energy_generation`, so
`energy_renewable` has stayed on the unrevised vintage while `energy_generation`
was selectively moved onto the revised one. Measured on this replica,
`energy_renewable` is 548,339 rows at age < 7d against 32,977 at 28-90d.

So the reported statistic is a ratio OF ratios:

    vintage_effect = (sum_gen/sum_ren | age >= H) / (sum_gen/sum_ren | age < H)

That is self-normalising twice over. The inner ratio cancels weather because
both terms are the same instant. The outer ratio cancels any CONSTANT
difference between the two tables -- a mapping difference, a column that means
something slightly different -- because such a difference is present in both
bands. What survives is only the part that depends on age at fetch, which is
the thing under test. A pair with no revision effect reads 1.00 whether or not
its two tables agree in absolute terms.

`hydro_reservoir_mw` is deliberately NOT compared: `energy_renewable` folds
pumped storage into it and `energy_generation` splits `hydro_pumped_mw` out, so
that pair differs for a known structural reason and the outer ratio would be
measuring the wrong thing if the folding were itself age-dependent.

`energy_load` and `energy_price` have no paired table, so `spot` uses the other
available design: a single large backfill session that wrote targets on BOTH
sides of the candidate horizon. Same session means same code and same mapper,
which is what ABL-439 used to prove the mapper is not the author. Load gets a
real control (its own TSO day-ahead forecast, a separate fetch that is not
rewritten by the session); price does not, so `spot` reports price's detectable
effect size instead of implying it measured a null.

USAGE
-----
    python scripts/abl442_revision_horizon_probe.py --exposure
    python scripts/abl442_revision_horizon_probe.py --sweep
    python scripts/abl442_revision_horizon_probe.py --boundary --country NL \
        --column wind_onshore_mw
    python scripts/abl442_revision_horizon_probe.py --spot
    python scripts/abl442_revision_horizon_probe.py --all --json out.json

The database is taken from --db, else $ENERGY_DB_PATH. It is opened
`mode=ro` with `PRAGMA query_only=ON`, so it is safe to point at a replica or
at production. It is NOT defaulted to `config.DATABASE_PATH`: that resolves to
a stale in-repo `energy_dashboard.db` on a workstation checkout, and a probe
that silently reads a 2024 snapshot reports "no problem found".
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Candidate revision horizon, in days. ABL-439 measured the boundary on NL
# wind_onshore as lying between 28.03 and 28.05 days with no exceptions in
# 19,968 rows. This is the default the bands are cut at; `--boundary` re-derives
# it from the data rather than assuming it.
DEFAULT_HORIZON_DAYS = 28.0

# Columns `energy_generation` and `energy_renewable` both carry AND both mean
# the same thing. hydro_reservoir_mw is excluded on purpose -- see the module
# docstring.
PAIRED_COLUMNS = [
    "solar_mw",
    "wind_onshore_mw",
    "wind_offshore_mw",
    "hydro_run_mw",
    "biomass_mw",
    "geothermal_mw",
    "other_renewable_mw",
]

# Default target window for the sweep. Both tables are in their modern regime
# from 2026-01-01: past the ~2025-11-26 timestamp-separator cutover, and inside
# energy_renewable's continuous coverage.
DEFAULT_SWEEP_START = "2026-01-01"

# Open-ended upper bound. It MUST NOT be all digits, and that is not a style
# preference -- it is a correctness requirement discovered while writing this
# script. Every timestamp column here is DECLARED `TIMESTAMP`, and "TIMESTAMP"
# contains none of INT/CHAR/CLOB/TEXT/BLOB/REAL/FLOA/DOUB, so under SQLite's
# affinity rules the column takes NUMERIC affinity. A comparison against a
# literal that looks like a number therefore converts the literal to an integer,
# and an integer always sorts BELOW every text value. So:
#
#     timestamp_utc < '9999'        -> 0  (matches nothing, silently)
#     timestamp_utc < '9999-12-31'  -> 1  (matches, as intended)
#
# verified on this replica against a row whose typeof(timestamp_utc) is 'text'.
# The first form returns an empty result set with no error, which is exactly the
# "confidently wrong, quietly" failure this codebase keeps paying for -- the
# first run of this sweep reported "0 pairs affected" because of it. Any bound
# written against these columns needs at least one non-digit character.
OPEN_ENDED_UPPER_BOUND = "9999-12-31"

# A pair is reported as carrying a vintage effect when the ratio-of-ratios is
# outside this band. ABL-439 measured unaffected pairs at gen/ren 0.99-1.07 and
# the three affected ones at 2.46 / 1.63 / 0.79, so 10% sits well clear of the
# noise floor without needing to be tuned to catch the known cases.
VINTAGE_EFFECT_TOLERANCE = 0.10

# Minimum paired rows in EACH band before a pair is judged at all. Below this a
# ratio is arithmetic, not evidence.
MIN_ROWS_PER_BAND = 200

# Tables, and the column each one dates its last write with.
WRITE_CLOCK = {
    "energy_generation": "fetched_at",
    "energy_renewable": "fetched_at",
    "energy_load": "created_at",
    "energy_price": "created_at",
}


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


def resolve_db_path(explicit: Optional[str]) -> Path:
    """Resolve the database path, refusing to guess."""
    raw = explicit or os.getenv("ENERGY_DB_PATH")
    if not raw:
        sys.exit(
            "No database given. Pass --db PATH or set ENERGY_DB_PATH.\n"
            "This probe deliberately does not fall back to config.DATABASE_PATH: "
            "on a workstation checkout that resolves to a stale in-repo copy, and "
            "a probe that reads a stale copy reports a clean bill of health."
        )
    path = Path(raw)
    if not path.exists():
        sys.exit(f"Database not found: {path}")
    return path


def connect(path: Path) -> sqlite3.Connection:
    """Open strictly read-only. Two independent locks, because this may be prod."""
    uri = f"file:{path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")
    return conn


def describe_db(conn: sqlite3.Connection, path: Path) -> Dict[str, Any]:
    """Print what we actually opened, and how fresh it is.

    A probe that reports "no revision effect found" against a snapshot nobody
    has written to since 2024 is worse than no probe. Print the newest stored
    target so a stale file is obvious in the first ten lines of output rather
    than in the conclusion.
    """
    newest = conn.execute(
        "SELECT MAX(timestamp_utc) FROM energy_generation"
    ).fetchone()[0]
    size_gib = path.stat().st_size / (1024**3)
    info = {
        "path": str(path),
        "size_gib": round(size_gib, 2),
        "newest_energy_generation_target": newest,
    }
    print(f"database : {path}")
    print(f"size     : {size_gib:.2f} GiB")
    print(f"newest energy_generation target : {newest}")
    if newest and newest < "2026":
        print(
            "  !! WARNING: newest target predates 2026. This looks like a stale "
            "snapshot, not a live replica. Findings below are not trustworthy."
        )
    print()
    return info


# ---------------------------------------------------------------------------
# Mode: exposure
# ---------------------------------------------------------------------------

AGE_BAND_SQL = """
    CASE
      WHEN age < 0  THEN '0 future target'
      WHEN age < 7  THEN '1 <7d'
      WHEN age < 14 THEN '2 7-14d'
      WHEN age < 21 THEN '3 14-21d'
      WHEN age < 28 THEN '4 21-28d'
      WHEN age < 35 THEN '5 28-35d'
      WHEN age < 90 THEN '6 35-90d'
      ELSE               '7 >90d'
    END
"""


def run_exposure(conn: sqlite3.Connection, horizon: float) -> Dict[str, Any]:
    """How many stored rows are sitting on the unrevised side of the horizon?

    This is the operational headline and it needs no model: a row whose age at
    last write is under the revision horizon has never been asked about since
    the upstream value settled, whatever that value turned out to be.
    """
    print("=" * 78)
    print(f"EXPOSURE -- rows by age at last write (horizon = {horizon:g} days)")
    print("=" * 78)
    out: Dict[str, Any] = {"horizon_days": horizon, "tables": {}}

    for table, clock in WRITE_CLOCK.items():
        band_rows = list(
            conn.execute(
                f"""
                SELECT {AGE_BAND_SQL} band, COUNT(*) n,
                       MIN(timestamp_utc), MAX(timestamp_utc)
                FROM (SELECT timestamp_utc,
                             julianday({clock}) - julianday(timestamp_utc) AS age
                      FROM {table} WHERE {clock} IS NOT NULL)
                GROUP BY band ORDER BY band
                """
            )
        )
        total = sum(r[1] for r in band_rows)
        unrevised = conn.execute(
            f"""
            SELECT COUNT(*), MIN(timestamp_utc), MAX(timestamp_utc)
            FROM {table}
            WHERE {clock} IS NOT NULL
              AND julianday({clock}) - julianday(timestamp_utc) < ?
            """,
            (horizon,),
        ).fetchone()

        print(f"\n{table}  (clock column: {clock})")
        for band, n, tmin, tmax in band_rows:
            print(
                f"   {band:<16} {n:>10,} ({100 * n / total:5.2f}%)  "
                f"targets {tmin} .. {tmax}"
            )
        pct = 100 * unrevised[0] / total if total else 0.0
        print(f"   {'-' * 66}")
        print(
            f"   UNREVISED (age < {horizon:g}d): {unrevised[0]:,} of {total:,} "
            f"({pct:.2f}%), targets {unrevised[1]} .. {unrevised[2]}"
        )

        out["tables"][table] = {
            "clock_column": clock,
            "total_rows": total,
            "bands": [
                {"band": b, "rows": n, "target_min": lo, "target_max": hi}
                for b, n, lo, hi in band_rows
            ],
            "unrevised_rows": unrevised[0],
            "unrevised_pct": round(pct, 4),
            "unrevised_target_min": unrevised[1],
            "unrevised_target_max": unrevised[2],
        }

    return out


def run_exposure_by_country(
    conn: sqlite3.Connection, horizon: float, table: str = "energy_generation"
) -> Dict[str, Any]:
    """Per country: where does the unrevised block begin, and how big is it?

    `settled_through` is the newest target this country holds that HAS been
    re-fetched past the horizon. Everything after it is provisional.
    """
    clock = WRITE_CLOCK[table]
    print("\n" + "=" * 78)
    print(f"EXPOSURE BY COUNTRY -- {table} (horizon = {horizon:g} days)")
    print("=" * 78)
    print(f"{'cc':<4} {'unrevised':>10} {'total':>10} {'%':>7}  "
          f"{'settled through':<21} {'unrevised from':<21}")

    rows = list(
        conn.execute(
            f"""
            SELECT country_code,
                   SUM(CASE WHEN age <  ? THEN 1 ELSE 0 END) unrev,
                   COUNT(*) n,
                   MAX(CASE WHEN age >= ? THEN timestamp_utc END) settled_through,
                   MIN(CASE WHEN age <  ? THEN timestamp_utc END) unrev_from
            FROM (SELECT country_code, timestamp_utc,
                         julianday({clock}) - julianday(timestamp_utc) AS age
                  FROM {table} WHERE {clock} IS NOT NULL)
            GROUP BY country_code ORDER BY country_code
            """,
            (horizon, horizon, horizon),
        )
    )
    result = []
    for cc, unrev, n, settled, unrev_from in rows:
        pct = 100 * unrev / n if n else 0.0
        print(
            f"{cc:<4} {unrev:>10,} {n:>10,} {pct:>6.2f}%  "
            f"{str(settled):<21} {str(unrev_from):<21}"
        )
        result.append(
            {
                "country_code": cc,
                "unrevised_rows": unrev,
                "total_rows": n,
                "unrevised_pct": round(pct, 4),
                "settled_through": settled,
                "unrevised_from": unrev_from,
            }
        )
    return {"table": table, "horizon_days": horizon, "countries": result}


# ---------------------------------------------------------------------------
# Mode: sweep
# ---------------------------------------------------------------------------


def run_sweep(
    conn: sqlite3.Connection,
    horizon: float,
    start: str,
    end: Optional[str],
    countries: Optional[List[str]],
    columns: Optional[List[str]],
) -> Dict[str, Any]:
    """Ratio-of-ratios test over every (country, paired column).

    See the module docstring for why this is not confounded by weather and not
    confounded by a constant difference between the two tables.
    """
    cols = columns or PAIRED_COLUMNS
    end = end or OPEN_ENDED_UPPER_BOUND
    print("\n" + "=" * 78)
    print(f"SWEEP -- energy_generation vs energy_renewable, same instant")
    print(f"targets {start} .. {end}   horizon {horizon:g}d   "
          f"flag |effect-1| > {VINTAGE_EFFECT_TOLERANCE:.0%}")
    print("=" * 78)

    cc_filter = ""
    params_head: Tuple[Any, ...] = ()
    if countries:
        cc_filter = f"AND g.country_code IN ({','.join('?' * len(countries))})"
        params_head = tuple(countries)

    findings: List[Dict[str, Any]] = []
    for col in cols:
        sql = f"""
            SELECT country_code,
                   SUM(CASE WHEN age <  ? THEN 1 ELSE 0 END) n_unrev,
                   SUM(CASE WHEN age <  ? THEN gv ELSE 0 END) g_unrev,
                   SUM(CASE WHEN age <  ? THEN rv ELSE 0 END) r_unrev,
                   SUM(CASE WHEN age >= ? THEN 1 ELSE 0 END) n_rev,
                   SUM(CASE WHEN age >= ? THEN gv ELSE 0 END) g_rev,
                   SUM(CASE WHEN age >= ? THEN rv ELSE 0 END) r_rev
            FROM (
              SELECT g.country_code, g.{col} AS gv, r.{col} AS rv,
                     julianday(g.fetched_at) - julianday(g.timestamp_utc) AS age
              FROM energy_generation g
              JOIN energy_renewable r
                ON r.country_code = g.country_code
               AND r.timestamp_utc = g.timestamp_utc
              WHERE g.timestamp_utc >= ? AND g.timestamp_utc < ?
                {cc_filter}
                AND g.{col} IS NOT NULL AND r.{col} IS NOT NULL
            )
            GROUP BY country_code ORDER BY country_code
        """
        params = (horizon,) * 6 + (start, end) + params_head
        for row in conn.execute(sql, params):
            cc, n_unrev, g_unrev, r_unrev, n_rev, g_rev, r_rev = row
            if n_unrev < MIN_ROWS_PER_BAND or n_rev < MIN_ROWS_PER_BAND:
                continue
            ratio_unrev = (g_unrev / r_unrev) if r_unrev else None
            ratio_rev = (g_rev / r_rev) if r_rev else None
            if not ratio_unrev or not ratio_rev:
                continue
            effect = ratio_rev / ratio_unrev
            # Mean stored MW in the revised band. Reported, not thresholded:
            # a ratio taken over a near-zero denominator is arithmetic rather
            # than evidence (the ABL-19 near-zero-denominator defect, one
            # measure over), and the honest fix is to put the magnitude on
            # screen next to the ratio rather than to invent a cutoff nobody
            # has calibrated. EE/FI other_renewable_mw are the live examples.
            level_rev = g_rev / n_rev if n_rev else 0.0
            findings.append(
                {
                    "country_code": cc,
                    "column": col,
                    "n_unrevised": n_unrev,
                    "n_revised": n_rev,
                    "ratio_unrevised": round(ratio_unrev, 4),
                    "ratio_revised": round(ratio_rev, 4),
                    "vintage_effect": round(effect, 4),
                    "mean_mw_revised": round(level_rev, 3),
                    "affected": abs(effect - 1.0) > VINTAGE_EFFECT_TOLERANCE,
                }
            )

    affected = [f for f in findings if f["affected"]]
    affected.sort(key=lambda f: -abs(f["vintage_effect"] - 1.0))

    print(f"\n{len(findings)} (country, column) pairs had >= {MIN_ROWS_PER_BAND} "
          f"paired rows in both bands.")
    print(f"{len(affected)} carry a vintage effect.\n")
    if affected:
        print(f"{'cc':<4} {'column':<20} {'n<H':>7} {'n>=H':>7} "
              f"{'g/r <H':>9} {'g/r >=H':>9} {'effect':>9} {'mean MW':>9}")
        for f in affected:
            print(
                f"{f['country_code']:<4} {f['column']:<20} {f['n_unrevised']:>7,} "
                f"{f['n_revised']:>7,} {f['ratio_unrevised']:>9.4f} "
                f"{f['ratio_revised']:>9.4f} {f['vintage_effect']:>9.4f} "
                f"{f['mean_mw_revised']:>9.2f}"
            )
        print("\n  'mean MW' is the mean stored value in the revised band. Read it "
              "before the\n  effect: a ratio over a near-zero denominator is "
              "arithmetic, not evidence.")
    return {
        "horizon_days": horizon,
        "target_start": start,
        "target_end": end,
        "pairs_tested": len(findings),
        "pairs_affected": len(affected),
        "findings": findings,
    }


# ---------------------------------------------------------------------------
# Mode: confirm -- independent third series
# ---------------------------------------------------------------------------

# `energy_generation_forecast` carries only these three. It is the TSO's own
# day-ahead forecast, fetched separately into a separate table, so it is
# independent of BOTH tables in the sweep.
TSO_FORECAST_COLUMNS = ["solar_mw", "wind_onshore_mw", "wind_offshore_mw"]


def run_confirm(
    conn: sqlite3.Connection,
    horizon: float,
    start: str,
    sweep: Dict[str, Any],
) -> Dict[str, Any]:
    """Re-test every screened pair against a THIRD, independent series.

    Why this is not optional. `sweep` compares `energy_generation` against
    `energy_renewable`, and it is a SCREEN, not a confirmation. Its ratio moves
    if the generation series was revised -- but it also moves if the two tables
    simply mean different things and that difference is not constant in time
    (a mapper change on either side would do it). The stored values say plainly
    that some screened pairs are of the second kind: EE `other_renewable_mw`
    reads max 0.9 MW in `energy_generation` against max 155.6 MW in
    `energy_renewable`, which is not one quantity recorded twice.

    So each screened pair is re-tested as `generation / TSO day-ahead forecast`.
    That reference is a separate fetch into a separate table, is not rewritten by
    the generation backfill, and is nobody's revision of anything. Dividing by it
    removes the weather the same way the paired-table ratio does, and it removes
    `energy_renewable` from the argument entirely -- which is what upgrades a
    screen into a finding. It is the design ABL-439 used on NL.

    Only solar / wind_onshore / wind_offshore have such a forecast. Pairs on any
    other column are reported as SCREENED-ONLY rather than quietly dropped or
    quietly promoted: no independent reference for them exists in this database,
    and that is a statement about our evidence, not about those pairs.
    """
    print("\n" + "=" * 78)
    print("CONFIRM -- screened pairs re-tested against the TSO day-ahead forecast")
    print("=" * 78)

    screened = [f for f in sweep.get("findings", []) if f["affected"]]
    out: List[Dict[str, Any]] = []
    confirmable = [f for f in screened if f["column"] in TSO_FORECAST_COLUMNS]
    unconfirmable = [f for f in screened if f["column"] not in TSO_FORECAST_COLUMNS]

    if confirmable:
        print(f"\n{'cc':<4} {'column':<20} {'gen/TSO <H':>11} {'gen/TSO >=H':>12} "
              f"{'effect':>9} {'verdict':<12}")
    for f in confirmable:
        col = f["column"]
        row = conn.execute(
            f"""
            SELECT SUM(CASE WHEN age <  ? THEN gv ELSE 0 END),
                   SUM(CASE WHEN age <  ? THEN fv ELSE 0 END),
                   SUM(CASE WHEN age >= ? THEN gv ELSE 0 END),
                   SUM(CASE WHEN age >= ? THEN fv ELSE 0 END)
            FROM (
              SELECT g.{col} AS gv, t.{col} AS fv,
                     julianday(g.fetched_at)-julianday(g.timestamp_utc) AS age
              FROM energy_generation g
              JOIN energy_generation_forecast t
                ON t.country_code = g.country_code
               AND t.target_timestamp_utc = g.timestamp_utc
              WHERE g.country_code = ? AND g.timestamp_utc >= ?
                AND t.forecast_type = 'day_ahead'
                AND g.{col} IS NOT NULL AND t.{col} IS NOT NULL AND t.{col} > 0
            )
            """,
            (horizon, horizon, horizon, horizon, f["country_code"], start),
        ).fetchone()
        g_u, f_u, g_r, f_r = row
        if not (f_u and f_r):
            verdict, eff, ru, rr = "no TSO cover", None, None, None
        else:
            ru, rr = g_u / f_u, g_r / f_r
            eff = (rr / ru) if ru else None
            verdict = (
                "CONFIRMED" if eff and abs(eff - 1.0) > VINTAGE_EFFECT_TOLERANCE
                else "NOT CONFIRMED"
            )
        print(
            f"{f['country_code']:<4} {col:<20} "
            f"{(f'{ru:.4f}' if ru else '--'):>11} "
            f"{(f'{rr:.4f}' if rr else '--'):>12} "
            f"{(f'{eff:.4f}' if eff else '--'):>9} {verdict:<12}"
        )
        out.append(
            {
                **{k: f[k] for k in ("country_code", "column", "vintage_effect")},
                "gen_over_tso_unrevised": round(ru, 4) if ru else None,
                "gen_over_tso_revised": round(rr, 4) if rr else None,
                "confirm_effect": round(eff, 4) if eff else None,
                "verdict": verdict,
            }
        )

    if unconfirmable:
        print(f"\nSCREENED ONLY -- no independent reference exists for these "
              f"columns in this\ndatabase, so they are neither confirmed nor "
              f"dismissed:")
        for f in unconfirmable:
            print(f"   {f['country_code']:<4} {f['column']:<20} "
                  f"screen effect {f['vintage_effect']:>8.4f}  "
                  f"(mean {f['mean_mw_revised']:.2f} MW)")
            out.append(
                {
                    **{k: f[k] for k in ("country_code", "column", "vintage_effect")},
                    "verdict": "SCREENED_ONLY_NO_REFERENCE",
                }
            )

    n_conf = sum(1 for o in out if o["verdict"] == "CONFIRMED")
    print(f"\n{n_conf} confirmed against an independent series; "
          f"{sum(1 for o in out if o['verdict'] == 'NOT CONFIRMED')} not confirmed; "
          f"{len(unconfirmable)} unconfirmable.")
    return {"results": out, "confirmed": n_conf}


# ---------------------------------------------------------------------------
# Mode: boundary
# ---------------------------------------------------------------------------


def run_boundary(
    conn: sqlite3.Connection, country: str, column: str, start: str
) -> Dict[str, Any]:
    """Locate the horizon from the data instead of assuming it.

    NOT a per-row equality test, and the reason matters. `energy_renewable` is
    built from the PRE-netting flatten while `energy_generation` stores the
    netted `aggregated - consumption` (ABL-412), so the two tables carry a
    standing offset wherever a country meters a type's auxiliary consumption --
    NL wind_onshore sits at gen/ren 0.9949 in the unrevised band, not 1.0000.
    An "are these two numbers equal" detector reads that offset as a permanent
    disagreement and never finds a boundary. What is being located here is a
    STEP IN THE RATIO, which the offset does not move.

    Two passes: find the day carrying the largest jump in the daily aggregate
    ratio, then re-scan that day at the stored sampling interval to pin the
    instant. Reports the age at fetch either side, which brackets the horizon.
    """
    print("\n" + "=" * 78)
    print(f"BOUNDARY -- {country} {column}, targets from {start}")
    print("=" * 78)

    # Profile the ratio against AGE directly, in half-day bins, aggregating
    # SUM(gen)/SUM(ren) within each bin.
    #
    # Aggregating sums rather than averaging per-row ratios is what makes this
    # robust: a mean-of-ratios is dominated by whichever quarter-hour had the
    # smallest denominator, and an earlier cut of this function was fooled
    # exactly that way -- it reported a "step" of 7.1x on 2026-03-10, which was
    # a calm hour with near-zero wind in the denominator and no vintage change
    # at all. Magnitude-weighting removes that failure mode, and it is the same
    # reason this repo reports WAPE rather than MAPE.
    raw = list(
        conn.execute(
            f"""
            SELECT julianday(g.fetched_at)-julianday(g.timestamp_utc) AS age,
                   g.{column} AS gv, r.{column} AS rv
            FROM energy_generation g
            JOIN energy_renewable r
              ON r.country_code = g.country_code
             AND r.timestamp_utc = g.timestamp_utc
            WHERE g.country_code = ? AND g.timestamp_utc >= ?
              AND g.{column} IS NOT NULL AND r.{column} IS NOT NULL
            """,
            (country, start),
        )
    )
    raw = [(a, gv, rv) for a, gv, rv in raw if a is not None and a <= 70]
    if len(raw) < 4 * MIN_ROWS_PER_BAND:
        print("  too few paired rows to locate a boundary")
        return {"country_code": country, "column": column, "boundary_found": False}

    def agg(lo: float, hi: float) -> Tuple[int, Optional[float]]:
        sg = sr = 0.0
        n = 0
        for a, gv, rv in raw:
            if lo <= a < hi:
                sg += gv
                sr += rv
                n += 1
        return n, (sg / sr if sr else None)

    # Display: 2-day bins, coarse enough to be readable.
    print(f"  gen/ren by age at fetch (2-day bins, magnitude-weighted):\n")
    print(f"    {'age (d)':>9} {'n':>7} {'gen/ren':>9}")
    profile = []
    for lo in range(0, 46, 2):
        n, ratio = agg(lo, lo + 2)
        if n >= 50 and ratio:
            print(f"    {lo:>4}-{lo + 2:<4} {n:>7,} {ratio:>9.4f}")
            profile.append({"age_lo": lo, "age_hi": lo + 2, "n": n,
                            "gen_over_ren": round(ratio, 4)})

    # Detection: compare a 7-day aggregate either side of each candidate split.
    # Seven days is chosen so a whole weather week sits on each side -- a
    # half-day bin is one slice of one afternoon, and an earlier cut of this
    # function reported a spurious 24x "step" from exactly that.
    HALF = 7.0
    best_h, best_jump, best_pair = None, 1.0, None
    h = 3.0
    while h <= 45.0:
        n_lo, r_lo = agg(h - HALF, h)
        n_hi, r_hi = agg(h, h + HALF)
        if (n_lo >= MIN_ROWS_PER_BAND and n_hi >= MIN_ROWS_PER_BAND
                and r_lo and r_hi and r_lo > 0 and r_hi > 0):
            jump = max(r_hi / r_lo, r_lo / r_hi)
            if jump > best_jump:
                best_jump, best_h, best_pair = jump, h, (r_lo, r_hi)
        h += 0.25

    result: Dict[str, Any] = {
        "country_code": country, "column": column,
        "profile": profile, "detector_half_window_days": HALF,
        "largest_jump": round(best_jump, 4),
    }
    if best_h is None or best_jump < 1.10:
        print(f"\n  largest 7-day-either-side change is {best_jump:.4f} -- no step."
              f"\n  This pair's stored level does NOT depend on age at fetch.")
        result["boundary_found"] = False
        return result

    r_lo, r_hi = best_pair
    print(f"\n  => step at age {best_h:.2f} days: gen/ren {r_lo:.4f} (younger) "
          f"-> {r_hi:.4f} (older),\n     a {best_jump:.2f}x change across the "
          f"boundary")
    print(f"  => rows fetched YOUNGER than {best_h:.2f} days hold the unrevised "
          f"vintage;\n     rows fetched older hold the revised one.")
    result.update(
        {
            "boundary_found": True,
            "horizon_days": round(best_h, 4),
            "ratio_younger": round(r_lo, 4),
            "ratio_older": round(r_hi, 4),
        }
    )
    return result


# ---------------------------------------------------------------------------
# Mode: spot -- load and price, via a single backfill session
# ---------------------------------------------------------------------------


def find_session(
    conn: sqlite3.Connection, table: str, horizon: float, min_rows: int = 100_000
) -> Optional[Dict[str, Any]]:
    """Find the most recent large single-day write session that spans the horizon.

    "Spans" means it wrote targets both older and newer than
    `session_day - horizon`, so within one run of one version of the code we
    have rows on both sides. That is what makes the mapper a non-explanation.
    """
    clock = WRITE_CLOCK[table]
    for day, n, tmin, tmax in conn.execute(
        f"""
        SELECT substr({clock},1,10) d, COUNT(*) n,
               MIN(timestamp_utc), MAX(timestamp_utc)
        FROM {table} WHERE {clock} IS NOT NULL
        GROUP BY d HAVING n >= ? ORDER BY d DESC
        """,
        (min_rows,),
    ):
        boundary = (
            datetime.strptime(day, "%Y-%m-%d") - timedelta(days=horizon)
        ).strftime("%Y-%m-%d")
        if tmin[:10] < boundary < tmax[:10]:
            return {
                "session_day": day,
                "rows": n,
                "target_min": tmin,
                "target_max": tmax,
                "boundary_date": boundary,
            }
    return None


def run_spot(conn: sqlite3.Connection, horizon: float, window_days: int = 20) -> Dict[str, Any]:
    """Spot-check energy_load and energy_price for the same age dependence."""
    print("\n" + "=" * 78)
    print(f"SPOT CHECK -- energy_load / energy_price, single-session design")
    print("=" * 78)
    out: Dict[str, Any] = {"horizon_days": horizon, "window_days": window_days}

    # --- energy_load: controlled by its own TSO day-ahead forecast ----------
    sess = find_session(conn, "energy_load", horizon)
    out["energy_load"] = {"session": sess}
    if not sess:
        print("\nenergy_load: no single session spans the horizon -- NOT MEASURABLE")
    else:
        b = sess["boundary_date"]
        lo = (datetime.strptime(b, "%Y-%m-%d") - timedelta(days=window_days)).strftime("%Y-%m-%d")
        hi = (datetime.strptime(b, "%Y-%m-%d") + timedelta(days=window_days)).strftime("%Y-%m-%d")
        print(f"\nenergy_load")
        print(f"  session {sess['session_day']} wrote {sess['rows']:,} rows, "
              f"targets {sess['target_min'][:10]} .. {sess['target_max'][:10]}")
        print(f"  boundary = session - {horizon:g}d = {b};  comparing {lo} .. {hi}")
        print(f"  control  = energy_load_forecast day_ahead (separate fetch, "
              f"not rewritten by this session)")
        # Date-only bounds are format-safe across the 'T'/space separator and
        # still use the index. Normalisation happens AFTER the range filter --
        # normalising inside the WHERE would defeat the index and full-scan the
        # replica (ABL-439's warning).
        # Joined on the raw columns, NOT on a normalised expression.
        #
        # An earlier cut derived `REPLACE(substr(ts,1,19),'T',' ')` on both sides
        # and joined on that. It is correct and it is unusably slow: a derived
        # key cannot use either table's index, so SQLite materialises both sides
        # and builds a transient b-tree (observed: an 86 MB `etilqs_` temp file,
        # and the pass had not finished after several minutes). Joining the raw
        # columns uses `idx_load_forecast_country_time_type` and returns in
        # milliseconds.
        #
        # That is exact HERE, and the two facts that make it exact are measured
        # rather than assumed: `energy_load_forecast` holds 0 rows whose
        # target_timestamp_utc is not 19 characters, and the `length()=19` guard
        # below drops any load row that is not in the same form. The guard is
        # what keeps a 'T'-separated or trailing-offset row from joining to the
        # wrong hour -- substr(...,1,19) of '...T00:00:00+02:00' is LOCAL time,
        # and those rows sit in 2025-11-13..28, straddling this very boundary.
        # `excluded` is reported so a window where that is NOT zero announces
        # itself instead of quietly comparing a subset.
        def band_ratio(split: str, w_lo: str, w_hi: str) -> Dict[str, Any]:
            rows = list(
                conn.execute(
                    """
                    SELECT CASE WHEN substr(a.timestamp_utc,1,10) < ?
                                THEN 'older' ELSE 'newer' END side,
                           COUNT(*) n, SUM(a.load_mw) sa, SUM(f.forecast_value_mw) sf
                    FROM energy_load a
                    JOIN energy_load_forecast f
                      ON f.country_code = a.country_code
                     AND f.target_timestamp_utc = a.timestamp_utc
                     AND f.forecast_type = 'day_ahead'
                    WHERE a.timestamp_utc >= ? AND a.timestamp_utc < ?
                      AND a.created_at LIKE ?
                      AND a.load_mw > 0 AND length(a.timestamp_utc) = 19
                      AND f.forecast_value_mw > 0
                    GROUP BY side ORDER BY side
                    """,
                    (split, w_lo, w_hi, sess["session_day"] + "%"),
                )
            )
            got = {
                side: {"n": n, "ratio": (sa / sf) if sf else None}
                for side, n, sa, sf in rows
            }
            if len(got) == 2 and got["older"]["ratio"] and got["newer"]["ratio"]:
                got["effect"] = got["older"]["ratio"] / got["newer"]["ratio"]
            return got

        excluded = conn.execute(
            """
            SELECT COUNT(*) FROM energy_load
            WHERE timestamp_utc >= ? AND timestamp_utc < ?
              AND created_at LIKE ? AND length(timestamp_utc) <> 19
            """,
            (lo, hi, sess["session_day"] + "%"),
        ).fetchone()[0]
        out["energy_load"]["rows_excluded_variant_timestamp_form"] = excluded
        print(f"  rows dropped for a variant timestamp form: {excluded}"
              + ("" if excluded == 0 else "   <-- comparison is on a SUBSET"))

        real = band_ratio(b, lo, hi)
        print(f"    UNREVISED (age<H)    n={real['newer']['n']:>7,}  "
              f"actual/D+1 = {real['newer']['ratio']:.4f}")
        print(f"    revised   (age>=H)   n={real['older']['n']:>7,}  "
              f"actual/D+1 = {real['older']['ratio']:.4f}")
        eff = real.get("effect")
        out["energy_load"]["bands"] = {
            "unrevised": real["newer"], "revised": real["older"]
        }
        out["energy_load"]["vintage_effect"] = round(eff, 4) if eff else None
        print(f"    => vintage_effect = {eff:.4f}")

        # PLACEBO. The actual/D+1 ratio is not constant in calendar time --
        # TSO forecast bias drifts with season and with load level -- so a small
        # reading at the real boundary is only meaningful against how much the
        # same statistic moves at boundaries where NO vintage change can exist.
        # Both placebo splits sit wholly inside one band, so any movement they
        # show is by construction not a revision effect. Without this the 2%-ish
        # reading at the real boundary would get written down as a null result
        # on no evidence, or as a finding on no evidence -- both wrong.
        # Every placebo must sit WHOLLY inside one band, or it re-measures the
        # real effect. The unrevised side only spans from the boundary to the
        # session day (~25 days here), which cannot hold a +/-20d window, so all
        # placebos are placed in the revised band, which runs back years.
        placebos = []
        for offset in (-30, -50, -70, -90, -110):
            pb_d = datetime.strptime(b, "%Y-%m-%d") + timedelta(days=offset)
            pb = pb_d.strftime("%Y-%m-%d")
            p_lo = (pb_d - timedelta(days=window_days)).strftime("%Y-%m-%d")
            p_hi = (pb_d + timedelta(days=window_days)).strftime("%Y-%m-%d")
            if not (p_hi <= b or p_lo >= b):
                continue
            r = band_ratio(pb, p_lo, p_hi)
            if r.get("effect") and r["older"]["n"] > 1000 and r["newer"]["n"] > 1000:
                placebos.append({"split": pb, "effect": round(r["effect"], 4),
                                 "n_older": r["older"]["n"], "n_newer": r["newer"]["n"]})
        out["energy_load"]["placebos"] = placebos
        if placebos and eff:
            print(f"    placebo splits (wholly inside one band, so no vintage "
                  f"change is possible there):")
            for p in placebos:
                print(f"      {p['split']}  effect = {p['effect']:.4f}  "
                      f"({abs(p['effect'] - 1):.2%} movement)")
            worst = max(abs(p["effect"] - 1.0) for p in placebos)
            real = abs(eff - 1.0)
            print(f"    => real boundary moves {real:.2%}; "
                  f"largest placebo moves {worst:.2%}")
            # The 3x factor is a judgement call and is labelled as one -- it is
            # not calibrated against anything. What carries the argument is the
            # ratio printed above, which is why both numbers are on screen.
            if worst > 0 and real > 3 * worst:
                verdict = (f"AGE-DEPENDENT -- {real / worst:.0f}x the largest "
                           f"placebo movement")
            else:
                verdict = "NOT DISTINGUISHABLE from ordinary drift in this statistic"
            print(f"    => {verdict}")
            out["energy_load"]["placebo_max_abs_effect"] = round(worst, 4)
            out["energy_load"]["real_abs_effect"] = round(real, 4)
            out["energy_load"]["verdict"] = verdict

    # --- energy_price: no same-quantity control ----------------------------
    sess = find_session(conn, "energy_price", horizon)
    out["energy_price"] = {"session": sess}
    if not sess:
        print("\nenergy_price: no single session spans the horizon -- NOT MEASURABLE")
    else:
        b = sess["boundary_date"]
        lo = (datetime.strptime(b, "%Y-%m-%d") - timedelta(days=window_days)).strftime("%Y-%m-%d")
        hi = (datetime.strptime(b, "%Y-%m-%d") + timedelta(days=window_days)).strftime("%Y-%m-%d")
        print(f"\nenergy_price")
        print(f"  session {sess['session_day']} wrote {sess['rows']:,} rows, "
              f"targets {sess['target_min'][:10]} .. {sess['target_max'][:10]}")
        print(f"  boundary = session - {horizon:g}d = {b};  comparing {lo} .. {hi}")
        print(f"  NOTE: no same-quantity control exists for price. A day-ahead "
              f"price is a\n        settled auction outcome, not a metered "
              f"quantity, so there is no\n        settlement mechanism that "
              f"would revise it. The figures below can\n        only exclude a "
              f"LARGE effect; read the spread, not just the means.")
        rows = list(
            conn.execute(
                """
                SELECT CASE WHEN substr(timestamp_utc,1,10) < ?
                            THEN 'revised (age>=H)' ELSE 'UNREVISED (age<H)' END side,
                       COUNT(*) n, AVG(price_eur_mwh) mean,
                       AVG(price_eur_mwh*price_eur_mwh) meansq
                FROM energy_price
                WHERE timestamp_utc >= ? AND timestamp_utc < ?
                  AND created_at LIKE ? AND price_eur_mwh IS NOT NULL
                GROUP BY side ORDER BY side
                """,
                (b, lo, hi, sess["session_day"] + "%"),
            )
        )
        res = {}
        for side, n, mean, meansq in rows:
            var = max(meansq - mean * mean, 0.0)
            sd = var**0.5
            se = sd / (n**0.5) if n else 0.0
            res[side] = {"n": n, "mean_eur_mwh": round(mean, 3),
                         "sd": round(sd, 3), "stderr": round(se, 4)}
            print(f"    {side:<20} n={n:>7,}  mean={mean:>8.3f}  "
                  f"sd={sd:>7.3f}  se={se:.4f}")
        out["energy_price"]["bands"] = res
        if len(res) == 2:
            u = res["UNREVISED (age<H)"]
            r = res["revised (age>=H)"]
            # Smallest level shift this comparison could resolve at ~2 se.
            pooled_se = (u["stderr"] ** 2 + r["stderr"] ** 2) ** 0.5
            mdd = 2 * pooled_se / u["mean_eur_mwh"] if u["mean_eur_mwh"] else None
            out["energy_price"]["min_detectable_effect"] = (
                round(mdd, 5) if mdd else None
            )
            print(f"    => smallest level shift this window could resolve "
                  f"(~2 s.e.): {mdd:.2%}")
            print(f"    => price levels are seasonal, so the raw difference here "
                  f"is NOT a\n       vintage estimate. This bounds the effect; "
                  f"it does not measure it.")

    # --- energy_renewable: answered by construction ------------------------
    row = conn.execute(
        """
        SELECT SUM(CASE WHEN age < ? THEN 1 ELSE 0 END), COUNT(*)
        FROM (SELECT julianday(fetched_at)-julianday(timestamp_utc) age
              FROM energy_renewable WHERE timestamp_utc >= '2026-01-01')
        """,
        (horizon,),
    ).fetchone()
    pct = 100 * row[0] / row[1] if row[1] else 0.0
    print(f"\nenergy_renewable (targets from 2026-01-01)")
    print(f"  {row[0]:,} of {row[1]:,} rows ({pct:.2f}%) are on the unrevised side.")
    print(f"  No ad-hoc backfill writes this table, so it has stayed on the "
          f"vintage the\n  routine 7-day job captured. That is what makes it a "
          f"usable control above --\n  and it means the table is uniformly "
          f"unrevised rather than mixed.")
    out["energy_renewable"] = {
        "unrevised_rows": row[0], "total_rows": row[1],
        "unrevised_pct": round(pct, 4),
    }
    return out


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ABL-442: measure whether a stored actual's level depends on "
                    "how old it was when we last fetched it. Read-only.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--db", help="Path to energy_dashboard.db (else $ENERGY_DB_PATH)")
    p.add_argument("--horizon", type=float, default=DEFAULT_HORIZON_DAYS,
                   help=f"Revision horizon in days (default {DEFAULT_HORIZON_DAYS:g})")
    p.add_argument("--exposure", action="store_true",
                   help="Rows by age at last write, overall and per country")
    p.add_argument("--sweep", action="store_true",
                   help="gen/ren ratio-of-ratios over every (country, column)")
    p.add_argument("--boundary", action="store_true",
                   help="Locate the horizon from the data (needs --country/--column)")
    p.add_argument("--spot", action="store_true",
                   help="Spot-check energy_load / energy_price / energy_renewable")
    p.add_argument("--all", action="store_true", help="Run every mode")
    p.add_argument("--country", default="NL")
    p.add_argument("--column", default="wind_onshore_mw")
    p.add_argument("--countries", help="Comma-separated filter for --sweep")
    p.add_argument("--start", default=DEFAULT_SWEEP_START, help="Target window start")
    p.add_argument("--end", help="Target window end (exclusive)")
    p.add_argument("--json", help="Write the machine record here")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not (args.exposure or args.sweep or args.boundary or args.spot or args.all):
        args.all = True

    path = resolve_db_path(args.db)
    conn = connect(path)
    record: Dict[str, Any] = {
        "issue": "ABL-442",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "horizon_days": args.horizon,
    }
    record["database"] = describe_db(conn, path)

    countries = (
        [c.strip().upper() for c in args.countries.split(",")]
        if args.countries else None
    )

    if args.exposure or args.all:
        record["exposure"] = run_exposure(conn, args.horizon)
        record["exposure_by_country"] = run_exposure_by_country(conn, args.horizon)
    if args.sweep or args.all:
        record["sweep"] = run_sweep(
            conn, args.horizon, args.start, args.end, countries, None
        )
        record["confirm"] = run_confirm(
            conn, args.horizon, args.start, record["sweep"]
        )
    if args.boundary or args.all:
        record["boundary"] = run_boundary(conn, args.country, args.column, args.start)
    if args.spot or args.all:
        record["spot_check"] = run_spot(conn, args.horizon)

    if args.json:
        Path(args.json).write_text(json.dumps(record, indent=2), encoding="utf-8")
        print(f"\nMachine record written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
