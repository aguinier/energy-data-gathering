> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

## Data Quality Guidelines

### Critical Data Quality Issues (as of 2025-12-23)

1. **Short Data Spans:** 6 countries have <1 week of load data (MD, MK, BA, CY, RS, ME)
2. **Outdated Data:** GB and UA have data from 2019-2022 only
3. **Missing Countries:** IS, MT, TR have no data at all

See `database_completeness.md` for detailed analysis.

### Data Validation Rules

When importing or modifying data:
1. **No duplicate timestamps per country** - Unique indexes enforce this
2. **UTC timestamps only** - All timestamps must be in UTC
3. **Valid country codes** - Must exist in `countries` table
4. **No negative energy values** - Energy metrics should be >= 0
5. **Renewable totals must match sum of components** - Validate calculations

### entsoe-py forward-fills sparse periods, and we used to store the fill

An ENTSO-E `Period` declares a `resolution` over a `timeInterval`, implying N
positions, but may carry fewer than N `Point` elements. **entsoe-py 0.8.0
expands such a period by forward-filling the last published value across every
missing position**, and `query_load_with_metadata` stored that expansion
verbatim as `data_quality = 'actual'`.

Measured against the live API on 2026-08-06 over
`2026-08-01T22:00Z .. 2026-08-02T22:00Z`, across 18 countries, this affects
exactly two:

| country | rows returned | positions published | forward-filled | exact zeros |
|---|---:|---:|---:|---:|
| MK | 24 (`PT60M`) | **1** | 23 | 24 |
| AL | 96 (`PT15M`) | 23 (hourly) | 73 | 0 |
| the other 16 | 24 or 96 | all of them | 0 | 0 |

MK's document carries one Point — `position 1, quantity 0.0` — and we wrote 24
rows of a measured national demand of 0 MW, 23 of which were ours. That is the
upstream source of the impossible-zero load rows the dashboard withholds at
read time (ABL-35); this is the writer-side half (ABL-50).

`src/published_points.py` is the rule, pure and with a colocated test
(`tests/test_published_points.py`). `query_load_with_metadata` applies it
before `remove_outliers`. It drops a row only when it is **both** a position
the document carried no Point for **and** exactly `0.0`:

- **Unpublished alone is not enough** — AL's 73 filled rows are the document's
  own hourly step function and are kept. `test_al_shape_is_untouched_by_this_rule`
  pins that, so the deferred follow-up (drop *every* forward-filled position,
  taking AL to its true 24 rows/day) lands as a deliberate diff.
- **Zero alone is not enough** — MK's `position 1` really was published as
  `0.0`. That is MEPSO's number, so it is stored, and `measuredLoadClause()`
  in the dashboard is what keeps it off a chart.
- **Everything fails open.** Unparseable XML, an unrecognised resolution, or a
  position grid matching none of the returned rows all keep every row and log
  a warning. A parser that silently deleted real readings would be worse than
  the defect it fixes.

**Measured caveat on `curveType`, which matters before extending this.** All
three documents sampled — MK, AL and DE — carry `curveType=A03`, "variable
sized block", whose contract is that a Point's value holds until the next
Point. So the forward fill *implements the document* rather than inventing a
value at random: MK's document does assert 0 MW across the whole day. It is
still not 24 measurements and a grid never draws 0 MW, which is why the zero
restriction is the operative half of the rule. But do not carry the phrase
"positions ENTSO-E never published" into the general case — under A03 an
absent position is an encoded repeat, not an unknown.

No second upstream request was added: the raw XML this reads is the document
already downloaded for the publication timestamp, so the one-fetch-per-country-per-window
invariant holds (`test_only_one_upstream_request_per_leg`). No schema change,
and no existing row was backfilled or deleted.

#### Second occurrence: net position (ABL-55)

The same mechanism ran in the A25 day-ahead net-position path.
`query_net_position_data_with_metadata` now applies the same rule through
`published_points.drop_unpublished_zeros_series` — the Series shape entsoe-py
returns for net position — so both tables are governed by one module rather
than two reimplementations. Tests: `tests/test_net_position_published_points.py`.

Measured against the live API on 2026-08-07, all `curveType=A03`:

| zone / day | declared | Points | stored before | stored after |
|---|---:|---:|---:|---:|
| GR 2026-07-24 | 24 (`PT60M`) | **1** (`quantity=0`) | 24 × `0.0` | **1** |
| GR 2026-03-14 | 24 (`PT60M`) | **1** (`quantity=0`) | 24 × `0.0` | **1** |
| IE 2026-03-14 | 24 (`PT60M`) | **1** (`quantity=0`) | 24 × `0.0` | **1** |
| IE 2026-07-24 | 24 (`PT60M`) | 24 | 24 | 24 |
| PT 2026-02-18 | 112 (`PT15M`) | sparse (7/47, 2/18) | 112 | 112 |
| ES 2026-02-08 | 112 (`PT15M`) | 51 | 112 | 112 |
| BE 2026-08-05 | 112 (`PT15M`) | 112 | 112 | 112 |

192 GR rows and 24 IE rows in `net_position` were manufactured this way, while
GR's own `crossborder_flows` showed a median net **export of 1,142 MW** over
the same hours.

**"Store only genuinely published Points" was proposed and is refuted.** It was
the obvious reading of the defect and it would have deleted real data. A25
documents are routinely and legitimately sparse under A03: PT 2026-02-18 has a
`Period` declaring 47 positions carrying 7 Points, and another declaring 18
carrying 2 — PT's interconnector sits at exactly 500 MW or 1500 MW for hours
and the document encodes the hold as one Point. That rule would have dropped
**more than half** of PT's and ES's genuine rows.
`test_pt_flat_interconnector_is_untouched` pins this. The zero half of the rule
is what separates GR's manufactured day from PT's flat interconnector: both are
forward-filled, only one is exactly `0.0`.

**Forward-looking impact, measured over 2026-07-31..08-07 across all 22 zones
in the table: 33 of 13,440 rows (0.25%), all of them PL.** Every other zone is
byte-identical; GR and IE now return no A25 at all (ABL-38). PL is the one zone
that genuinely publishes single-Point zero periods on otherwise-real days —
e.g. 2024-04-13 carries a `Period` declaring 14 positions with one Point,
`quantity=0`. Its published zeros are kept and only the fills are refused, and
its zero density is falling anyway (1,612 rows in 2023 → 166 in 2026).

Note this corrects a premise recorded on ABL-55 from stored rows alone: PL's
4,137 exact-`0.0` rows were assumed to be genuinely published, and the raw XML
shows a large share of them are our own forward-fill. That is why the rule was
measured against the documents rather than the table.

#### Third occurrence: generation (ABL-268)

The A75 actual-generation path ran the same mechanism, unguarded, on every
country every day. `query_generation_and_renewable_with_metadata` now applies
`published_points.blank_unpublished_zeros_by_series` to the MultiIndex frame
**before either flatten**, so one guarded document feeds both output frames.
Tests: `tests/test_generation_published_points.py`.

Two things are different here from the load and net-position cases, and both
are load-bearing.

**The unit is a cell, not a row.** `energy_load` and `net_position` have one
value column each, so refusing the value means refusing the row. An A75 row
carries up to 21 independently measured production types, and on 2026-08-12
Belgium's genuine 3.3 GW of gas sat on the same 24 rows as its manufactured
nuclear zero. Dropping the row would delete twenty real readings to suppress
one invented one. A blanked cell becomes `NaN`, which `_map_generation_columns`
already carries through to SQL `NULL` — "write no row" and "write no value" are
the same rule at the granularity the table actually has.

**The published set is per sub-series, not per document.**
`published_timestamps` unions every Point in the document, which is right for
a single-quantity document and wrong for A75: BE's Biomass publishes all 24
positions, so a document-wide union reports every position as published and
Nuclear's 23 invented zeros survive untouched.
`published_timestamps_by_series` keys on `(production type, 'Actual
Aggregated' | 'Actual Consumption')` — entsoe-py's own column identity, read
from the same two elements its parser reads — and unions across every
TimeSeries sharing a key, matching the concat-and-deduplicate entsoe-py
performs on same-named series.
`test_document_wide_union_would_have_missed_this` pins the distinction.

Measured against the live API on 2026-08-14 for the market day 2026-08-12, all
`curveType=A03`:

| zone / type | declared | Points | stored before | stored after |
|---|---:|---:|---:|---:|
| ES Fossil Hard coal | 96 (`PT15M`) | 26 | 59 × `0.0` | **1 × `0.0`** |
| BE Nuclear | 24 (`PT60M`) | **1** (`quantity=0`) | 24 × `0.0` | **1** |
| BE Hydro Run-of-river | 24 (`PT60M`) | **1** (`quantity=0`) | 24 × `0.0` | **1** |
| AT Fossil Hard coal | 96 (`PT15M`) | **1** (`quantity=0`) | 96 × `0.0` | **1** |
| AT Waste | 96 (`PT15M`) | **1** (`quantity=100`) | 96 × `100.0` | 96 (untouched) |
| DE Solar | 96 (`PT15M`) | 91 | 96 | 92 |
| SI (whole document) | — | — | 96 rows | 96 rows, 0 blanked |

**Spain's coal is the one that was a wrong number on a chart.** ES burns hard
coal — 126,379 of its 160,198 stored readings are positive and it reached
208 MW that very day — and 58 quarter-hours of a running fleet were stored as
a measured `0.0` that Red Eléctrica never published.

**Belgium's nuclear is the one to reason from.** 23 of those 24 rows are ours,
but BE's reactors really did shut down on 2026-04-04 (2,078 MW through January,
exactly `0.0` at every hour since), so the underlying reality is very likely
zero. They are refused anyway. The rule is about provenance, not plausibility:
the document cannot tell us which case we are in, and a pipeline that decides
which zeros to believe by how plausible they look is the one that wrote GR a
year of measured-looking `0.0` MW net position.

**AT's Waste is the generation-side twin of PT's flat interconnector** — one
Point at 100 MW held across 96 positions, forward-filled and not zero, so
untouched. Same reason "store only genuinely published Points" is refuted here
too.

**Forward-looking impact, measured through the real fetch path against live
documents for 2026-08-12 across 15 zones: 2,357 values blanked, and not one
non-zero value changed, disappeared or appeared.** The only transition anywhere
is `0.0` → `NULL`. Per zone: AT 343, ES 799, SK 239, FI 168, GR 164, CZ 150,
HU 137, NL 95, IE 88, BE 75, PL 69, FR 26, DE 4, **SI 0, DK 0** — the guard is
not indiscriminate. Expect `marine_mw`, `fossil_oil_shale_mw` and
`fossil_coal_derived_gas_mw` to go substantially `NULL`: they are 100%, 73.9%
and 36.3% exact-`0.0` today, largely from single-Point series.

**The one visible cost, stated rather than absorbed: overnight solar.** 206 of
2,671 blanks across 8 zones are `Solar`, and every one falls in UTC hours
20–05 — a fill from a genuinely published overnight `0.0`, where the A03 hold
is entirely plausible. They are refused on the same provenance rule, so a
generation-mix chart now shows a gap rather than a zero at those points. The
exception proves the rule is worth it: IE_SEM's 44 solar blanks span all 24
hours, because its document published one Point for the whole day, and Ireland
does generate solar.

For context on the population this sits in: 15.2% of every reported value in
`energy_generation` is exactly `0.0` — 5,302,173 cells of 34,849,767, measured
2026-08-14. **What fraction of the historical zeros are fabricated is not
measured and is deliberately not estimated from that number**; establishing it
would mean re-fetching every historical document, and ABL-210 showed that
verdict is not stable over time anyway. The guard is forward-only: it changes
what future passes write, and no existing row is backfilled or deleted.

**`energy_renewable` is deliberately unchanged**, verified byte-identical for
all 15 zones through the real fetch path and pinned by
`test_energy_renewable_output_is_byte_identical`. `_map_renewable_columns`
initialises every column to `0.0` and `fillna(0)`s what it maps, so a blanked
cell reaches the frozen table as the same `0.0` it held before. That table has
`DEFAULT 0` on every value column and has never been able to express "not
reported"; teaching it mid-life would leave one condition encoded two ways, and
its remaining consumer trains models on it. That is a contract change for its
owner to make deliberately, not a side effect of a guard — see ABL-268's
handover note for the separate, larger defect that leaves open.

### Completeness Cache

The `completeness_cache` table stores pre-computed data quality metrics:
- **Last Updated:** 2025-11-22 (may be outdated)
- **Recommendation:** Recompute after any bulk data operations
- Provides fast lookups for data coverage without scanning fact tables
