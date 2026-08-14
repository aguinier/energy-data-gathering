# ABL-442 — the 7-day re-fetch window freezes stored actuals on an unrevised vintage

**Read-only measurement and a costed proposal. No ingest behaviour was changed by this issue.**

Routed from ABL-439 (Forecasting Scientist, PR #71), which established the effect on one
country and one column and explicitly left the ingest question here.

- Probe: `scripts/abl442_revision_horizon_probe.py` (read-only, `mode=ro` +
  `PRAGMA query_only=ON`)
- Machine record: `reports/abl_442_revision_horizon.json`
- Measured against the workstation replica `C:/Code/able/data/energy_dashboard.db`,
  8.78 GiB, newest `energy_generation` target `2026-08-13 13:30:00`, on 2026-08-14.

Reproduce with:

```bash
ENERGY_DB_PATH=/path/to/energy_dashboard.db \
  python scripts/abl442_revision_horizon_probe.py --all --json out.json
```

---

## 1. The answer in four lines

1. **Confirmed, and it is not one country or one table.** The stored level in
   `energy_generation` is a function of how old the instant was when we last fetched it.
   The boundary is **28.00 days**, located from the data rather than assumed.
   **10 (country, column) pairs** confirm against an independent third series — ABL-439's
   three plus seven it never screened — and `energy_load` shows the same dependence at a
   smaller **+2.3%**, which is new.
2. **The cause is `UPDATE_DAYS_BACK = 7`** (`config.py:211`). Nothing on the routine
   schedule re-fetches past 7 days — confirmed by reading the cron, not inferred.
3. **3.10% of `energy_generation` is currently unrevised** — 98,582 rows, every target
   from **2026-07-01** onward, i.e. the whole of the most recent six weeks, which is
   the window every gate and accuracy read uses.
4. **Repair costs almost nothing in ENTSO-E calls.** A window widening adds **zero**
   requests, because nothing in the update path chunks by date. The real cost is
   database write volume and the blast radius of rewriting stored history.

**Recommendation, for the CEO's decision and not executed here:** a **weekly settle pass
over a trailing 42 days** (§7, Option B) — `+1.3%` ENTSO-E requests, `+11%` database
writes, routine passes untouched, and it provably re-fetches every row at least once past
the horizon even if a run is missed. Raising `UPDATE_DAYS_BACK` to 35 (Option A) reaches
the same place but pays a 5× write cost on all four daily passes to do once-per-row work
four times a day. **The separate one-time reconciliation of the 98,582 rows already
frozen is the decision that actually carries risk — see §8.**

---

## 2. Why the measurement is not just "old rows look different from new rows"

Comparing old rows against new rows compares January against July, and a wind series
really is different in January. Two controls remove that.

**Control 1 — the same instant, twice.** `energy_generation` and `energy_renewable` are
written from *one* A75 fetch (`src/fetch_renewable.py` →
`query_generation_and_renewable_with_metadata`), so for the columns they share they are
two recordings of one quantity, and their ratio at the same `(country, timestamp)` has
no weather in it. The ad-hoc `scripts/backfill_generation.py` writes only
`energy_generation`, so `energy_renewable` stayed on the unrevised vintage. Measured:
**100.00% of `energy_renewable` rows** with 2026 targets are on the unrevised side.

The reported statistic is therefore a ratio *of* ratios:

```
vintage_effect = (Σgen/Σren | age ≥ H) / (Σgen/Σren | age < H)
```

The inner ratio cancels weather. The outer ratio cancels any **constant** difference
between the two tables — which matters, because there is one: `energy_renewable` is
built from the *pre-netting* flatten while `energy_generation` stores the netted
`aggregated − consumption` (ABL-412). NL `wind_onshore` sits at 0.9949, not 1.0000, for
that reason alone. What survives the outer ratio is only the age-dependent part.

**Control 2 — a third, independent series.** The paired-table ratio is a **screen**, not
a finding: it also moves if the two tables simply mean different things and that
difference is not constant in time. So every screened pair is re-tested as
`generation / TSO day-ahead forecast` (`energy_generation_forecast`) — a separate fetch
into a separate table, not rewritten by the generation backfill, and nobody's revision
of anything. That is the design ABL-439 used on NL. See §4 for why this step changed
the answer.

---

## 3. The boundary is 28.00 days, and the profile shows it

`--boundary --country NL --column wind_onshore_mw`, magnitude-weighted `gen/ren` by age
at fetch:

| age (d) | n | gen/ren | | age (d) | n | gen/ren |
|---|---:|---:|---|---|---:|---:|
| 0–2 | 188 | 0.9976 | | 20–22 | 192 | 0.9794 |
| 2–4 | 192 | 0.9938 | | 22–24 | 192 | 1.0000 |
| 4–6 | 192 | 0.8260 | | 24–26 | 192 | 0.9999 |
| 6–8 | 1,616 | 0.9926 | | 26–28 | 192 | 0.9992 |
| 8–10 | 192 | 0.9987 | | **28–30** | 192 | **4.8264** |
| 10–12 | 192 | 1.0000 | | 30–32 | 192 | 3.0858 |
| 12–14 | 192 | 0.9979 | | 32–34 | 192 | 2.8479 |
| 14–16 | 192 | 1.0000 | | 34–36 | 192 | 3.3715 |
| 16–18 | 192 | 0.9992 | | 36–38 | 192 | 2.3178 |
| 18–20 | 192 | 0.9882 | | 38–40 | 192 | 2.9222 |

Flat at ~1.00 for 28 days, then a step. Detector (7-day aggregate either side, so a
whole weather week sits on each side) puts it at **age 28.00 days exactly**:
`gen/ren` 0.9997 younger → 3.1466 older, a **3.15×** change.

This agrees with ABL-439's independent measurement (boundary between 28.03 and 28.05
days, no exceptions in 19,968 rows) and was reached by a different route.

**The per-country exposure table shows the mechanism confirming itself.** Every
country's last settled target sits within a few hours of `2026-07-01`, and the exact
minute *staggers by country* — AT `14:15`, DE `15:15`, GR `16:15`, NL `17:45`, SK
`19:00`. That is the 2026-07-29 backfill walking country-by-country through its
afternoon: the boundary is pinned at 28.0 days behind whatever the clock said when each
country's turn came. A calendar-driven or upstream-driven step could not stagger that way.

---

## 4. What generalises — and the honest limit on it

The screen flagged **17** of 134 testable `(country, column)` pairs. Re-testing each
against the independent TSO forecast is what separates a revision from a mapping
difference, and it matters: EE `other_renewable_mw` reads max **0.9 MW** in
`energy_generation` against max **155.6 MW** in `energy_renewable` — that is not one
quantity recorded twice, and its screen "effect" of 418× is an artifact.

**All 10 pairs that have an independent reference confirmed.** `gen/TSO` is the stored
generation over the TSO's own day-ahead forecast for the same country and instant;
`confirm` is the ratio of those two, i.e. the same age-dependence measured without
`energy_renewable` in it at all.

| cc | column | screen | gen/TSO <28d | gen/TSO ≥28d | confirm | verdict |
|---|---|---:|---:|---:|---:|---|
| NL | `wind_onshore_mw` | 2.2968 | 0.3037 | 0.6536 | **2.1518** | CONFIRMED |
| CH | `wind_onshore_mw` | 1.5421 | 1.1331 | 1.8313 | **1.6163** | CONFIRMED |
| NL | `solar_mw` | 1.7647 | 0.0438 | 0.0648 | **1.4797** | CONFIRMED |
| EE | `solar_mw` | 1.1891 | 1.0613 | 1.3300 | **1.2532** | CONFIRMED |
| DK | `wind_offshore_mw` | 1.6041 | 1.0862 | 1.2981 | **1.1951** | CONFIRMED |
| LV | `solar_mw` | 1.1852 | 0.9659 | 1.1296 | **1.1695** | CONFIRMED |
| BE | `wind_onshore_mw` | 0.8743 | 1.0636 | 0.9564 | **0.8993** | CONFIRMED |
| GR | `solar_mw` | 0.8455 | 0.9200 | 0.7353 | **0.7992** | CONFIRMED |
| CY | `wind_onshore_mw` | 0.5700 | 1.0812 | 0.5334 | **0.4933** | CONFIRMED |
| CY | `solar_mw` | 0.2544 | 0.8880 | 0.1682 | **0.1894** | CONFIRMED |

The screen and the confirmation are computed from **disjoint** reference series and agree
in direction on all ten, and within ~20% on magnitude for eight. That is the result that
turns ABL-439's single-country finding into a fleet property.

ABL-439 found three affected pairs by screening the 37 committed ABL-316 pairs. This
sweep tests 134 and finds **all three of ABL-439's** — NL `wind_onshore`, NL `solar`,
GR `solar` — plus seven more that were never in that ledger's scope.



**Columns with no independent reference** — `biomass_mw`, `hydro_run_mw`,
`geothermal_mw`, `other_renewable_mw` — have no TSO forecast in this database, so they
are reported as **screened only**: neither confirmed nor dismissed. That is a statement
about our evidence, not about those pairs.

**The revision is not uniformly upward, which refutes the simplest story.** ABL-439's
natural reading was settlement metering replacing real-time metering, which predicts
revised > unrevised. Most confirmed pairs do go up — but **CY `solar_mw` goes the other
way**, to 0.25× — so "settlement adds previously unmetered generation" cannot be the
whole mechanism. Direction is per country and per type.

---

## 5. Spot-checks: load, price, renewable

`energy_load` and `energy_price` have no paired table, so the design is the other one
ABL-439 used: a single large backfill session that wrote targets on **both** sides of
the candidate horizon. Same session means same code and same mapper.

### `energy_load` — a small effect that survives a placebo

Session `2025-12-23` wrote 1,616,198 rows covering targets `2021-01-01 .. 2025-12-20`,
so its own boundary is `2025-11-25`. Control: `energy_load_forecast` day-ahead, a
separate fetch not rewritten by that session.

| band | n | actual / D+1 forecast |
|---|---:|---:|
| unrevised (age < 28d) | 21,072 | 0.9923 |
| revised (age ≥ 28d) | 21,120 | 1.0152 |

**vintage effect 1.0230 — a 2.30% level change.** Whether that is a revision or ordinary
drift in the actual/forecast ratio cannot be read off one number, so the probe runs
**placebo splits** at boundaries placed wholly inside a single band, where no vintage
change is possible by construction.

| split | position | effect | movement |
|---|---|---:|---:|
| **2025-11-25** | **the real boundary** | **1.0230** | **2.30%** |
| 2025-10-26 | placebo, revised band | 1.0003 | 0.03% |
| 2025-10-06 | placebo, revised band | 0.9950 | 0.50% |
| 2025-09-16 | placebo, revised band | 0.9945 | 0.55% |
| 2025-08-27 | placebo, revised band | 1.0012 | 0.12% |
| 2025-08-07 | placebo, revised band | 0.9937 | 0.63% |

**The real boundary moves 4× further than the largest placebo.** So `energy_load` is
age-dependent too, at roughly **+2.3%** — small beside generation's 60–130%, but not
noise, and **new**: ABL-439 established the effect for `energy_generation` only.

All placebos sit in the revised band because the unrevised side of this session spans
only ~25 days, which cannot hold a ±20-day window. The 4× factor is a judgement call and
is labelled as one in the code; what carries the argument is the two numbers.



One confound was checked and excluded rather than assumed: the ~26,405 rows carrying a
trailing `+02:00` offset (length 25) sit in `2025-11-13..28`, straddling this exact
boundary, and truncating one to 19 characters would yield *local* time and join it to
the wrong forecast hour. Measured: that backfill session rewrote every row in the window
to clean 19-character space form, so **0 rows were excluded**. The guard stays in the
code anyway, because "currently zero" is a fact about today's data.

### `energy_price` — bounded, not measured

Session `2025-11-25`, boundary `2025-10-28`. There is **no same-quantity control** for
price, and the probe says so rather than implying a null result. A day-ahead price is a
settled auction outcome, not a metered quantity, so there is no settlement mechanism
that would revise it — an a-priori argument, not a measurement.

| band | n | mean €/MWh | sd | s.e. |
|---|---:|---:|---:|---:|
| unrevised | 8,570 | 87.456 | 50.117 | 0.5414 |
| revised | 8,640 | 93.577 | 65.114 | 0.7005 |

Price levels are seasonal, so the raw difference is **not** a vintage estimate. What the
window supports is a bound: the smallest level shift it could resolve at ~2 s.e. is
**2.02%**. An effect of the size seen in generation (60–130%) is excluded; a small one
is not.

### `energy_renewable` — uniformly unrevised, by construction

**492,972 of 492,972 rows (100.00%)** with 2026 targets are on the unrevised side. No
ad-hoc backfill writes this table. That is what makes it usable as the control above,
and it means the table is uniformly stale rather than internally mixed — which is
better, not worse, for anything fitted against it.

---

## 6. Scope item 2 — confirmed from the code, not inferred

**Nothing on the routine schedule re-fetches past 7 days.**

| path | window | verdict |
|---|---|---|
| `docker/crontab` — 4 full passes daily (00:30/06:30/13:30/18:30 UTC) | `update.py` with no `--days` → `config.UPDATE_DAYS_BACK` = **7** | 7 days |
| `docker/crontab` — price passes (11:15, 12:15 UTC) | `update.py --types price --days 2` | **2** days |
| `docker/crontab` — weekly catch-up (05:00 UTC Sunday) | `catchup.py` | **cannot revise** — see below |
| `scripts/backfill*.py` | manual only, on no cron line | ad-hoc |

`scripts/catchup.py` is the one that looks like it might help and does not, for two
independent reasons. It is scoped to **one table** (`TABLE = "energy_load"`,
`catchup.py:71`; its only fetcher is `fetch_load`, `catchup.py:299`), so it never touches
`energy_generation` at all. And it targets **interior holes** — gaps bounded by real rows
on both sides — so it re-fetches instants we are *missing*, never instants we already
hold. A revision is a value change on a row that is present, which is invisible to it by
design. ABL-85 built it for coverage; this is a different defect wearing the same
constant.

The window is applied once per `(country, data_type)`: `pipeline.run_update` computes a
single `(start, end)` from `utils.get_recent_date_range(days_back)` and calls
`_fetch_data_chunk` once per pair. **There is no date-chunking loop anywhere in
`src/entsoe_client.py`** — no `while start < end`. This is the fact the whole cost
section turns on.

---

## 7. Scope item 3 — the costed proposal

### Measured baseline (2026-08-13, from `data_ingestion_log`)

| pass | runs | rows written | wall clock |
|---|---:|---:|---|
| 00:30 full | 288 | 133,049 | 23m 37s |
| 06:30 full | 288 | 131,485 | 27m 36s |
| 11:15 price | 36 | 7,372 | 5m 45s |
| 12:15 price | 36 | 8,608 | 9m 58s |
| 13:30 full | 288 | 134,341 | 25m 54s |
| 18:30 full | 288 | *(not yet on the replica)* | |
| **per day** | **1,224** | **~548,000** | |

The 18:30 pass is estimated at ~133,000 rows from the three measured full passes; the
replica's newest row is `13:30`, so that pass had not synced. Everything else in this
table is measured.

A full pass is 36 countries × 8 data types. Seven of those types issue exactly **2**
ENTSO-E document requests per `(country, window)` (raw XML for the publication stamp,
plus the pandas client); `crossborder_flows` fans out one request per neighbour leg — a
multiplier I did not enumerate, because it is **identical for every window width** and so
cancels out of this comparison.

### The load-bearing cost fact

**Widening the window adds zero ENTSO-E requests.** One request already carries the whole
window. Going 7 → 42 days multiplies the *payload* and the *rows upserted*, not the call
count. So "ENTSO-E calls/day" is the wrong axis to choose on; database write volume and
pass duration are the right ones.

### Option A — raise `UPDATE_DAYS_BACK` from 7 to 35

| | |
|---|---|
| ENTSO-E requests/day | **1,224 runs — unchanged (+0%)** |
| Rows upserted/day | ~548,000 → **~2,675,000 (4.9×)** |
| Pass duration | ~26 min → est. **1–2 h**, four times daily |
| Schedule collisions | the 06:30 pass would overrun into the 07:00 weather job |

Cheap in API terms and expensive everywhere else: it pays the full 5× write cost on
**every one of the four daily passes** to achieve something that only needs to happen
once per row. It also rewrites `fetched_at` on rows that were already settled, erasing
the provenance signal this entire measurement depended on.

### Option B — a separate low-frequency settle pass **(recommended)**

```cron
# Weekly settle pass: re-fetch a trailing 42 days so every row is asked about
# at least once AFTER ENTSO-E's ~28-day revision horizon.
0 2 * * 0  cd /app && python3 scripts/update.py --days 42 --types renewable,load,price
```

| | |
|---|---|
| ENTSO-E requests | **+108 runs/week ≈ +15/day (+1.3%)** |
| Rows upserted | +~418,000/week ≈ **+60,000/day (+11%)** |
| Routine passes | **unchanged** — still 7 days, four times a day |
| Duration | one pass of ~1–2 h, weekly, at 02:00 UTC Sunday |

**Why 42 days and not 35.** A settle pass at time *t* covers targets `[t−W, t]`. A row at
*T* is re-fetched past the horizon iff some pass falls in `[T+28, T+W]`. At `W=35` that
window is exactly 7 days wide and the cadence is exactly 7 days — so **one skipped run
breaks the guarantee silently**. At `W=42` the window is 14 days wide, two passes fall in
it, and the guarantee survives a missed run *and* leaves margin on the 28.0-day horizon
estimate itself.

**Why those three types.** They are the actuals this issue is about, and `renewable` is
the one that writes `energy_generation`. Costs, per settle pass, from the measured
per-type row counts:

| types | runs/week | rows/week | vs baseline writes |
|---|---:|---:|---:|
| all eight | 288 | ~798,000 | +21% |
| `renewable,load,price` | 108 | ~418,000 | **+11%** |
| `renewable,load` | 72 | ~296,000 | +7.7% |
| `renewable` only | 36 | ~194,000 | +5.1% |

The three forecast types are a different question, already owned by **ABL-278** (TSO
forecasts revise pre-delivery and freeze ~24–36 h after delivery), so paying to settle
them here would duplicate that issue's scope.

**`price` is a judgement call and I am flagging it rather than deciding it.** The effect
is *confirmed* only on `renewable`; `load` shows a small effect (§5); `price` is both
unmeasurable from stored data and a-priori settled, since a day-ahead price is an auction
outcome rather than a metered quantity. Dropping it saves ~29% of the settle pass's write cost
(+11% → +7.7%). Keeping it buys insurance against an assumption nobody has tested upstream. I lean
to **keeping it** at this price, but it is the CEO's call.

**One thing to trial rather than assume.** I have not verified that ENTSO-E serves a
42-day window in a single response for every document type — nothing in this codebase has
ever requested one, and I have no API token on this workstation (§9). Trial it on one
country before scheduling it; if any type refuses the width, that type needs chunking and
its request count would then rise in proportion.

**Optional refinement (needs a small code change).** `update.py` takes only `--days`, a
lookback. A `--days-from/--days-to` band of `[now−44d, now−26d]` would re-fetch only the
rows that have just crossed the horizon, cutting the write cost from ~418,000 to
~181,000 rows/week (+4.7% instead of +11%) for the same guarantee and the same request
count. Worth doing only if the write volume is the binding constraint.
`utils.get_recent_date_range` (`utils.py:151`) returns `(now − days_back, now)` and has
no band form, so this is a real if small code change, not a flag flip.

### The one-time reconciliation is separate, and it is the expensive one

Bringing the **98,582** already-frozen rows (targets from 2026-07-01) onto the settled
vintage needs one backfill over ~2026-07-01 onward: **36 countries × 2 requests ≈ 72
document requests**, minutes of runtime. Trivial in API terms. The cost is entirely §8.

---

## 8. Blast radius — this rewrites stored history

**A re-fetch that lets rows converge to the settled vintage changes values we have
already published.** It is not additive and it is not reversible without a backup.

- **It moves the actuals underneath every ABL-316 gate read.** ABL-439 predicts NL's
  gate-window actuals rise ~2.2×; this probe measures the NL `wind_onshore` factor at
  **2.2968** independently. Grades computed against the current numbers would not
  reproduce.
- **It moves the dashboard's own forecast-accuracy numbers**, for every confirmed pair,
  in both directions — up for most, **down** for CY `solar_mw` (0.25×).
- **It is not confined to the affected pairs.** Any row re-fetched gets whatever upstream
  now holds, and the 17 flagged pairs are only those large enough to clear a 10% screen.
- **`energy_renewable` would then disagree with `energy_generation` permanently** for the
  reconciled window, since no backfill writes it. Anything currently fitted against
  `energy_renewable` — which ABL-439 recommends for NL precisely because it is
  basis-consistent — keeps the old basis while `energy_generation` moves.

**Per the ABL-85 norm, this is a CEO decision and nothing here executes it.** Both the
schedule change and the one-time reconciliation are proposals.

A sequencing note that costs nothing and protects the evidence: the reconciliation
backfill destroys the natural experiment that made this measurable, because it rewrites
`fetched_at` on the rows currently on the unrevised side. If it is approved, capture the
current values for the affected window first — the machine record beside this report
already pins the aggregate ratios, but not row-level values.

---

## 9. Not done, and why

**The upstream "why" was not confirmed.** ABL-439 could not determine why ENTSO-E returns
a different number after four weeks, and asked whether re-querying the same instant at
two ages was cheap enough to settle here. **It is not cheap from this workstation:** there
is no ENTSO-E token in the local `.env` (0 matches for `ENTSOE`) and `entsoe-py` is not
installed. Doing it would mean executing against prod's credentials, which is a different
class of action than this read-only issue. Left, per the issue's own instruction.

Two things partially answer it at zero cost and are recorded above instead: the
**direction is not uniform** (CY `solar_mw` revises *down* to 0.25×), which refutes a
simple "settlement adds unmetered generation" story as the universal mechanism; and the
attribution does not depend on the answer, since whatever upstream is doing, the two
windows demonstrably hold different vintages.

**Out of scope and untouched:** no backfill was run; FR's July 2026 seam is ABL-328;
ABL-348's source registration is unchanged; no ABL-316 pair was re-graded.

---

## 10. A SQLite trap this cost an hour, worth carrying forward

Every timestamp column in this database is **declared `TIMESTAMP`**, and "TIMESTAMP"
contains none of `INT`/`CHAR`/`CLOB`/`TEXT`/`BLOB`/`REAL`/`FLOA`/`DOUB` — so under
SQLite's affinity rules it takes **NUMERIC** affinity. A comparison against a literal that
*looks* like a number converts the literal to an integer, and an integer sorts below
every text value:

```sql
SELECT typeof(timestamp_utc),           -- 'text'
       timestamp_utc < '9999',          -- 0   <-- matches nothing, silently
       timestamp_utc < '9999-12-31'     -- 1   <-- matches, as intended
FROM energy_generation LIMIT 1;
```

The first form returns an empty result set with **no error**. The first run of this
sweep reported "0 pairs affected" for exactly that reason, and it looked like a clean
bill of health. Any bound written against these columns needs at least one non-digit
character. Date-only bounds (`'2026-01-01'`) are safe, and are also format-safe across
the `T`/space separator split while still using the index.
