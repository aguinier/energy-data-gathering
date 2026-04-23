# Plan 1 — Coherence Tooling + Schema Migration (Phase 0 + Phase 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Phase 0 quick wins (broken-replica diagnosis + coherence script + doc refresh) and Phase 1 schema migration (`zone_type` + `capacity_mw` columns on `weather_location`), so that subsequent phases can extend `weather_observation` to all 39 countries × 3 tech types without coordinate-source drift going unnoticed.

**Architecture:** Coherence script compares schema constants (`OPEN_METEO_SOURCES`, `LOCATIONS`, `WEATHER_VARIABLE_COLUMNS`) to actual DB rows via set-difference; wired into existing `init_weather_observation.py --verify` so every deploy verifies coherence. Schema migration adds two columns via `ALTER TABLE ADD COLUMN` (online, O(1) per CLAUDE.md `EXTENDING.md` rules), backfills existing 5 BE rows, and renames `BE_LOCATIONS` → `LOCATIONS` schema constant in preparation for Phase 2's full population.

**Tech Stack:** Python 3.11+, sqlite3 (stdlib), pytest (new dev dep), existing `entsoe_pipeline` package.

**Spec reference:** `docs/superpowers/specs/2026-04-23-unified-weather-locations-design.md`, Phases 0 + 1.

---

## File Structure

### New files
- `scripts/check_weather_coherence.py` — coherence checker, CLI + importable
- `scripts/migrate_add_zone_type.py` — schema migration for existing DBs (adds `zone_type` + `capacity_mw`, backfills BE rows)
- `tests/__init__.py` — empty package marker
- `tests/conftest.py` — pytest fixtures (in-memory sqlite seeded with current schema)
- `tests/test_check_weather_coherence.py` — unit tests for coherence script
- `tests/test_migrate_add_zone_type.py` — unit tests for migration

### Modified files
- `requirements.txt` — add `pytest>=7.0` to dev deps
- `src/weather_schema.py`:
  - `SCHEMA_LOCATION` DDL gains `zone_type TEXT, capacity_mw REAL`
  - `BE_LOCATIONS` renamed to `LOCATIONS`, each row gets `zone_type` + `capacity_mw` fields appended
  - `__all__` updated
- `src/db.py`:
  - `create_weather_observation_tables()` import + seed INSERT updated for new columns + new constant name
- `scripts/init_weather_observation.py`:
  - `BE_LOCATIONS` import → `LOCATIONS`
  - `_verify()` extended to call coherence-check function and report drift
- `docker/crontab` — line 11 comment "4 NWP models" → "7 NWP models"
- `WEATHER_DB.md` — append "Replica recovery" runbook section (Task 1 output)

### Out-of-repo file (operator handles)
- `~/.claude/skills/weather-db-query/SKILL.md` — source-id reference table refresh; flagged for operator commit (covered by Task 3, but the file edit happens outside this repo)

---

## Task 1: Diagnose broken `/c/Code/able/data/` replica + document recovery

**Files:**
- Modify: `WEATHER_DB.md` (append "Replica recovery" section)

This is investigation, not TDD. Output is documented diagnosis + recovery steps committed to the runbook.

- [ ] **Step 1: Confirm the symptom**

Run:
```bash
ls -lh "/c/Code/able/data/energy_dashboard.db" && \
head -c 16 "/c/Code/able/data/energy_dashboard.db" | xxd && \
file "/c/Code/able/data/energy_dashboard.db"
```

Expected output: file is ~3.9 GB, header is `0000 0000 0000 0000 0000 0000 0000 0000`, file type reports `data` (not `SQLite 3.x database`). Confirms sparse-allocated / corrupted state.

- [ ] **Step 2: Confirm prod source-of-truth is intact**

Run on prod (operator action, document the command):
```bash
ssh clavain@192.168.86.36 'sqlite3 /home/clavain/energy-dashboard/data/energy_dashboard.db "PRAGMA quick_check;"'
```

Expected: `ok` (single line). If anything else, prod itself is the problem and recovery is a different conversation — escalate.

- [ ] **Step 3: Identify the sync mechanism**

Search for what writes to `/c/Code/able/data/energy_dashboard.db`:
```bash
ls -la "/c/Code/able/data/" && \
find /c/Users/guill -name "*.bat" -o -name "*.ps1" 2>/dev/null | xargs grep -l "energy_dashboard" 2>/dev/null | head -5
```

If a scheduled task is found (e.g. Windows Task Scheduler entry), record its name + trigger. If nothing found, the sync is either manual or broken-by-design — document either way.

- [ ] **Step 4: Repair the replica**

Run from the workstation:
```bash
# Move the broken file aside (don't delete in case forensics needed later)
mv "/c/Code/able/data/energy_dashboard.db" "/c/Code/able/data/energy_dashboard.db.broken-2026-04-23"

# Pull a fresh copy from prod (rsync over ssh; operator's existing key)
rsync -avh --progress \
  clavain@192.168.86.36:/home/clavain/energy-dashboard/data/energy_dashboard.db \
  "/c/Code/able/data/energy_dashboard.db"

# Verify
sqlite3 "/c/Code/able/data/energy_dashboard.db" "PRAGMA quick_check; SELECT COUNT(*) FROM weather_observation;"
```

Expected: `ok` from quick_check; row count > 0 from `weather_observation`.

- [ ] **Step 5: Document recovery in `WEATHER_DB.md`**

Append the following section to `WEATHER_DB.md`:

```markdown
## Replica recovery (workstation `/c/Code/able/data/energy_dashboard.db`)

If the workstation replica becomes unreadable (zero-byte header, `file` reports `data` instead of `SQLite 3.x database`), the daily sync from prod has been interrupted. Recovery:

```bash
# 1. Confirm prod source is intact
ssh clavain@192.168.86.36 'sqlite3 /home/clavain/energy-dashboard/data/energy_dashboard.db "PRAGMA quick_check;"'
# Expected: ok

# 2. Move the broken file aside (don't delete — keep for forensics)
mv "/c/Code/able/data/energy_dashboard.db" "/c/Code/able/data/energy_dashboard.db.broken-$(date +%F)"

# 3. Re-sync from prod
rsync -avh --progress \
  clavain@192.168.86.36:/home/clavain/energy-dashboard/data/energy_dashboard.db \
  "/c/Code/able/data/energy_dashboard.db"

# 4. Verify
sqlite3 "/c/Code/able/data/energy_dashboard.db" "PRAGMA quick_check;"
# Expected: ok
```

**Causes seen:** interrupted rsync (network drop mid-transfer), filesystem pre-allocation that never received data, antivirus quarantine.

**Prevention:** use `rsync --partial --inplace` so partial transfers can resume; add a post-sync `quick_check` that fails the scheduled task on corruption.
```

- [ ] **Step 6: Run full recovery + commit doc**

Execute the recovery steps (Step 4) yourself. Then:
```bash
git add WEATHER_DB.md
git commit -m "docs(weather): replica-recovery runbook for /c/Code/able/data/"
```

---

## Task 2: Refresh stale crontab comment (4 → 7 NWP models)

**Files:**
- Modify: `docker/crontab:11`

Trivial doc fix; no TDD.

- [ ] **Step 1: Read current crontab**

Run:
```bash
cat docker/crontab | head -15
```

Confirm line 11 reads:
```
# Pulls /v1/forecast for all 4 NWP models (best_match, ecmwf, icon, gfs)
```

- [ ] **Step 2: Apply fix**

Edit `docker/crontab` line 11–14 to reflect actual reality (7 models, 5 BE locations becomes "5 BE locations" only until Phase 3 expands; spec deliberately doesn't promise more than current):

Replace:
```
# Pulls /v1/forecast for all 4 NWP models (best_match, ecmwf, icon, gfs)
# × 5 BE locations at XX:30 UTC. Heliocast's :45 UTC runner reads
# straight from the DB (via GET /api/weather/latest) so the forecast
# it uses is ≤ 15 min old.
```

With:
```
# Pulls /v1/forecast for all 7 NWP models (best_match, ecmwf_ifs025,
# icon_seamless, gfs_seamless, knmi_harmonie_arome_europe,
# meteofrance_arome_france, icon_d2) × all weather_location rows at
# XX:30 UTC. Heliocast's :45 UTC runner reads straight from the DB
# (via GET /api/weather/latest) so the forecast it uses is ≤ 15 min old.
```

Also fix the line 18 comment ("Adds Previous Runs API (day1 + day3 × 4 NWP models)") to:
```
# Adds Previous Runs API (day1 + day2 + day3 × 7 NWP models) to the realtime
```

- [ ] **Step 3: Verify with grep**

Run:
```bash
grep -n "NWP models" docker/crontab
```

Expected: two hits, both saying "7 NWP models" (no more "4 NWP models").

- [ ] **Step 4: Commit**

```bash
git add docker/crontab
git commit -m "docs(crontab): correct NWP-model count (4 → 7) in comments"
```

---

## Task 3: Refresh `weather-db-query` skill source-id table

**Files:**
- Modify: `~/.claude/skills/weather-db-query/SKILL.md` (operator's machine path; lives outside this repo)

This file is outside the repo. Operator commits it via dotfiles repo or directly. Plan documents the required content; the actual file edit happens by the operator, not in this worktree.

- [ ] **Step 1: Generate the new source-id reference**

Run from the worktree (uses local repo to derive the table):
```bash
python3 -c "
from src.weather_schema import OPEN_METEO_SOURCES
for i, (provider, model_id, lead, desc) in enumerate(OPEN_METEO_SOURCES, start=1):
    print(f'| {i} | \`({provider}, {model_id}, {lead})\` |')
"
```

Expected: 29 lines (1 archive + 7 realtime + 21 previous_runs).

- [ ] **Step 2: Apply to skill SKILL.md**

In `~/.claude/skills/weather-db-query/SKILL.md`, replace the existing source-id reference table (currently shows 13 entries) with the freshly generated 29-row table from Step 1.

Add a header comment above the table:
```markdown
*Last refreshed: 2026-04-23. To regenerate, run the snippet in `able/energy-data-gathering/docs/superpowers/plans/2026-04-23-plan1-coherence-and-schema.md` Task 3.*
```

- [ ] **Step 3: Operator commits skill change separately**

This file is not in the worktree. Operator action: commit to dotfiles or wherever skills are version-controlled. Note in the next worktree commit that this is a follow-up.

(No git command in the worktree for this task. Operator handles separately.)

---

## Task 4: Bootstrap `tests/` directory + pytest

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Modify: `requirements.txt`

Repo has zero tests today. Set up the minimum pytest scaffolding so subsequent TDD tasks have somewhere to land.

- [ ] **Step 1: Add pytest to requirements**

Append to `requirements.txt`:
```
# Testing
pytest>=7.0
```

- [ ] **Step 2: Install**

Run:
```bash
pip install -r requirements.txt
```

Expected: pytest installed (no error).

- [ ] **Step 3: Create tests package marker**

Create `tests/__init__.py` (empty file):
```bash
touch tests/__init__.py
```

- [ ] **Step 4: Create conftest.py with shared fixtures**

Create `tests/conftest.py`:
```python
"""Shared pytest fixtures for the energy-data-gathering test suite."""

from __future__ import annotations

import sqlite3
from typing import Iterator

import pytest


@pytest.fixture
def in_memory_db() -> Iterator[sqlite3.Connection]:
    """Empty in-memory SQLite DB. Tests are responsible for creating their own schema."""
    conn = sqlite3.connect(":memory:")
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def seeded_observation_db(in_memory_db: sqlite3.Connection) -> sqlite3.Connection:
    """In-memory DB with the current production weather_observation schema applied + BE seed rows.

    Use this when testing migration scripts or coherence checks against the
    pre-Phase-1 schema state. Post-Phase-1 schema is the responsibility of the
    test that exercises the migration.
    """
    from src.weather_schema import (
        BE_LOCATIONS,
        OPEN_METEO_SOURCES,
        ALL_SCHEMA_SQL,
    )

    cursor = in_memory_db.cursor()
    for stmt in ALL_SCHEMA_SQL:
        cursor.execute(stmt)
    cursor.executemany(
        "INSERT OR IGNORE INTO weather_location "
        "(country_code, zone_id, lat, lon, weight, description) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        BE_LOCATIONS,
    )
    cursor.executemany(
        "INSERT OR IGNORE INTO weather_source "
        "(provider, model_id, lead_time_hours, description) "
        "VALUES (?, ?, ?, ?)",
        OPEN_METEO_SOURCES,
    )
    in_memory_db.commit()
    return in_memory_db
```

- [ ] **Step 5: Smoke test the fixture**

Create `tests/test_smoke.py`:
```python
"""Smoke test for the test scaffolding itself. Delete after Task 4 is verified."""


def test_in_memory_db_works(in_memory_db) -> None:
    cursor = in_memory_db.cursor()
    cursor.execute("CREATE TABLE t (x INTEGER)")
    cursor.execute("INSERT INTO t VALUES (1)")
    assert cursor.execute("SELECT x FROM t").fetchone() == (1,)


def test_seeded_observation_db_has_be_rows(seeded_observation_db) -> None:
    from src.weather_schema import BE_LOCATIONS

    cursor = seeded_observation_db.cursor()
    n = cursor.execute(
        "SELECT COUNT(*) FROM weather_location WHERE country_code = 'BE'"
    ).fetchone()[0]
    assert n == len(BE_LOCATIONS)
```

Run:
```bash
pytest tests/test_smoke.py -v
```

Expected: `2 passed`.

- [ ] **Step 6: Delete the smoke test + commit**

```bash
rm tests/test_smoke.py
git add requirements.txt tests/__init__.py tests/conftest.py
git commit -m "test: bootstrap pytest scaffolding for the data-gathering pipeline"
```

---

## Task 5: Coherence script — sources dimension (TDD)

**Files:**
- Create: `scripts/check_weather_coherence.py`
- Test: `tests/test_check_weather_coherence.py`

Start with the simplest dimension (sources) and lay down the script's overall shape. Locations + columns dimensions follow in Tasks 6 + 9.

- [ ] **Step 1: Write the failing test for sources drift**

Create `tests/test_check_weather_coherence.py`:
```python
"""Tests for scripts/check_weather_coherence.py."""

from __future__ import annotations

import pytest

from scripts.check_weather_coherence import check_sources_dimension


def test_check_sources_dimension_passes_when_db_matches_schema(
    seeded_observation_db,
) -> None:
    drift = check_sources_dimension(seeded_observation_db)
    assert drift == {"only_in_schema": set(), "only_in_db": set()}


def test_check_sources_dimension_detects_extra_db_row(
    seeded_observation_db,
) -> None:
    cursor = seeded_observation_db.cursor()
    cursor.execute(
        "INSERT INTO weather_source (provider, model_id, lead_time_hours, description) "
        "VALUES ('rogue_provider', 'rogue_model', 99, 'should-not-be-here')"
    )
    seeded_observation_db.commit()

    drift = check_sources_dimension(seeded_observation_db)
    assert drift["only_in_db"] == {("rogue_provider", "rogue_model", 99)}
    assert drift["only_in_schema"] == set()


def test_check_sources_dimension_detects_missing_db_row(
    seeded_observation_db,
) -> None:
    cursor = seeded_observation_db.cursor()
    # Delete the era5 archive source.
    cursor.execute(
        "DELETE FROM weather_source WHERE provider = 'open_meteo_archive'"
    )
    seeded_observation_db.commit()

    drift = check_sources_dimension(seeded_observation_db)
    assert drift["only_in_schema"] == {("open_meteo_archive", "era5", 0)}
    assert drift["only_in_db"] == set()
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
pytest tests/test_check_weather_coherence.py -v
```

Expected: 3 failures with `ModuleNotFoundError: No module named 'scripts.check_weather_coherence'` (or similar import error).

- [ ] **Step 3: Add `scripts/__init__.py` if missing**

Run:
```bash
ls scripts/__init__.py 2>&1
```

If absent, create it (empty file):
```bash
touch scripts/__init__.py
```

(scripts dir needs to be a package for the `from scripts...` import to work in tests.)

- [ ] **Step 4: Write minimal implementation**

Create `scripts/check_weather_coherence.py`:
```python
#!/usr/bin/env python3
"""Verify schema constants in weather_schema.py match actual DB rows.

Three dimensions checked:
1. weather_source vs OPEN_METEO_SOURCES
2. weather_location vs LOCATIONS (added in Task 9, post-Phase-1 migration)
3. weather_observation columns vs WEATHER_VARIABLE_COLUMNS

Exit 0 = coherent. Exit 1 = drift detected (full diff printed).

Usage:
    python scripts/check_weather_coherence.py [--db PATH]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import TypedDict

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.weather_schema import OPEN_METEO_SOURCES


class Drift(TypedDict):
    only_in_schema: set
    only_in_db: set


def check_sources_dimension(conn: sqlite3.Connection) -> Drift:
    """Compare OPEN_METEO_SOURCES (in code) to weather_source rows (in DB)."""
    schema_set: set[tuple[str, str, int]] = {
        (provider, model_id, lead) for provider, model_id, lead, _desc in OPEN_METEO_SOURCES
    }
    cursor = conn.cursor()
    cursor.execute(
        "SELECT provider, model_id, lead_time_hours FROM weather_source"
    )
    db_set: set[tuple[str, str, int]] = {
        (row[0], row[1], row[2]) for row in cursor.fetchall()
    }
    return {
        "only_in_schema": schema_set - db_set,
        "only_in_db": db_set - schema_set,
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run:
```bash
pytest tests/test_check_weather_coherence.py -v
```

Expected: `3 passed`.

- [ ] **Step 6: Commit**

```bash
git add scripts/__init__.py scripts/check_weather_coherence.py tests/test_check_weather_coherence.py
git commit -m "feat(coherence): script for sources-dimension drift detection (TDD)"
```

---

## Task 6: Coherence script — columns dimension (TDD)

**Files:**
- Modify: `scripts/check_weather_coherence.py`
- Modify: `tests/test_check_weather_coherence.py`

Add the columns check (compares `WEATHER_VARIABLE_COLUMNS` to `PRAGMA table_info(weather_observation)`).

- [ ] **Step 1: Write the failing tests for columns drift**

Append to `tests/test_check_weather_coherence.py`:
```python
from scripts.check_weather_coherence import check_columns_dimension


def test_check_columns_dimension_passes_when_db_matches_schema(
    seeded_observation_db,
) -> None:
    drift = check_columns_dimension(seeded_observation_db)
    assert drift == {"only_in_schema": set(), "only_in_db": set()}


def test_check_columns_dimension_detects_extra_db_column(
    seeded_observation_db,
) -> None:
    cursor = seeded_observation_db.cursor()
    cursor.execute(
        "ALTER TABLE weather_observation ADD COLUMN rogue_column REAL"
    )
    seeded_observation_db.commit()

    drift = check_columns_dimension(seeded_observation_db)
    assert drift["only_in_db"] == {"rogue_column"}
    assert drift["only_in_schema"] == set()
```

(We can't easily test "missing DB column" without dropping a column from the in-memory DB, which SQLite makes painful; the extra-column test exercises the same code path in the other direction. Trust the symmetric set-diff.)

- [ ] **Step 2: Run tests to verify failure**

Run:
```bash
pytest tests/test_check_weather_coherence.py::test_check_columns_dimension_passes_when_db_matches_schema -v
```

Expected: ImportError on `check_columns_dimension`.

- [ ] **Step 3: Implement the columns check**

Append to `scripts/check_weather_coherence.py`:
```python
from src.weather_schema import WEATHER_VARIABLE_COLUMNS


def check_columns_dimension(conn: sqlite3.Connection) -> Drift:
    """Compare WEATHER_VARIABLE_COLUMNS (in code) to weather_observation columns (in DB)."""
    schema_set: set[str] = set(WEATHER_VARIABLE_COLUMNS)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(weather_observation)")
    # PRAGMA returns (cid, name, type, notnull, dflt_value, pk); we want name.
    # Exclude the PK / metadata columns we don't track in WEATHER_VARIABLE_COLUMNS.
    metadata_cols = {"source_id", "location_id", "valid_at", "forecast_run_time", "fetched_at"}
    db_set: set[str] = {
        row[1] for row in cursor.fetchall() if row[1] not in metadata_cols
    }
    return {
        "only_in_schema": schema_set - db_set,
        "only_in_db": db_set - schema_set,
    }
```

Move the `from src.weather_schema import` line to the top of the file (single import block):
```python
from src.weather_schema import OPEN_METEO_SOURCES, WEATHER_VARIABLE_COLUMNS
```

(Replace the duplicate import.)

- [ ] **Step 4: Run all tests**

```bash
pytest tests/test_check_weather_coherence.py -v
```

Expected: `5 passed`.

- [ ] **Step 5: Commit**

```bash
git add scripts/check_weather_coherence.py tests/test_check_weather_coherence.py
git commit -m "feat(coherence): add columns-dimension drift detection"
```

---

## Task 7: Coherence script — CLI + integration with `init_weather_observation.py --verify`

**Files:**
- Modify: `scripts/check_weather_coherence.py` (add `main()` + CLI)
- Modify: `scripts/init_weather_observation.py` (call `run_all_checks()` from `_verify()`)
- Modify: `tests/test_check_weather_coherence.py` (test the CLI exit code + integration helper)

Wire the dimension functions together behind a `run_all_checks()` helper that returns 0/1, plus a CLI that calls it. `init_weather_observation.py --verify` then calls `run_all_checks()` directly.

- [ ] **Step 1: Write the failing test for `run_all_checks`**

Append to `tests/test_check_weather_coherence.py`:
```python
from scripts.check_weather_coherence import run_all_checks


def test_run_all_checks_returns_zero_on_clean_db(seeded_observation_db, capsys) -> None:
    exit_code = run_all_checks(seeded_observation_db)
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "PASS" in captured.out


def test_run_all_checks_returns_one_on_drift(seeded_observation_db, capsys) -> None:
    cursor = seeded_observation_db.cursor()
    cursor.execute(
        "INSERT INTO weather_source (provider, model_id, lead_time_hours, description) "
        "VALUES ('rogue', 'rogue', 1, 'drift')"
    )
    seeded_observation_db.commit()

    exit_code = run_all_checks(seeded_observation_db)
    captured = capsys.readouterr()
    assert exit_code == 1
    assert "DRIFT" in captured.out
    assert "rogue" in captured.out
```

- [ ] **Step 2: Run tests to verify failure**

```bash
pytest tests/test_check_weather_coherence.py::test_run_all_checks_returns_zero_on_clean_db -v
```

Expected: ImportError.

- [ ] **Step 3: Implement `run_all_checks()` + `main()`**

Append to `scripts/check_weather_coherence.py`:
```python
def _format_drift(name: str, drift: Drift) -> list[str]:
    """Return human-readable lines describing the drift for one dimension."""
    if not drift["only_in_schema"] and not drift["only_in_db"]:
        return [f"PASS: {name}"]
    lines = [f"DRIFT: {name}"]
    for item in sorted(drift["only_in_schema"], key=str):
        lines.append(f"  schema-only: {item}")
    for item in sorted(drift["only_in_db"], key=str):
        lines.append(f"  db-only:     {item}")
    return lines


def run_all_checks(conn: sqlite3.Connection) -> int:
    """Run all coherence checks. Print results, return 0/1."""
    sources_drift = check_sources_dimension(conn)
    columns_drift = check_columns_dimension(conn)

    has_drift = False
    for name, drift in [
        ("sources", sources_drift),
        ("columns", columns_drift),
    ]:
        for line in _format_drift(name, drift):
            print(line)
        if drift["only_in_schema"] or drift["only_in_db"]:
            has_drift = True

    return 1 if has_drift else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=None,
        help="Path to SQLite DB (default: read from config.DATABASE_PATH).",
    )
    args = parser.parse_args()

    if args.db is None:
        import config
        db_path = config.DATABASE_PATH
    else:
        db_path = args.db

    print(f"Database: {db_path}")
    with sqlite3.connect(db_path) as conn:
        return run_all_checks(conn)


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run all coherence tests**

```bash
pytest tests/test_check_weather_coherence.py -v
```

Expected: `7 passed`.

- [ ] **Step 5: Wire into `init_weather_observation.py`**

Edit `scripts/init_weather_observation.py`. Add import at the top with the others:
```python
from scripts.check_weather_coherence import run_all_checks
```

Replace the existing `_verify()` function's final `return 0` with:
```python
        # Coherence check: schema constants ↔ DB rows.
        print("---")
        coherence_exit = run_all_checks(conn)
        return coherence_exit
```

(Remove the `return 0` that was there.)

- [ ] **Step 6: Manual smoke test against the (good) workstation DB**

Run:
```bash
python scripts/check_weather_coherence.py --db "/c/Code/able/data/energy_dashboard.db"
```

Expected (post Task 1 recovery): two `PASS` lines (sources + columns), exit code 0. If the DB hasn't been recovered yet, this step is skipped — note it in the commit.

- [ ] **Step 7: Commit**

```bash
git add scripts/check_weather_coherence.py scripts/init_weather_observation.py tests/test_check_weather_coherence.py
git commit -m "feat(coherence): CLI + run_all_checks() + init_weather_observation hookup"
```

---

## Task 8: Phase 1 — extend `weather_schema.py` for `zone_type` + `capacity_mw`

**Files:**
- Modify: `src/weather_schema.py`

Two changes: (1) `SCHEMA_LOCATION` DDL gains the new columns (for fresh installs); (2) `BE_LOCATIONS` gets renamed to `LOCATIONS` and each row gets two new fields (`zone_type`, `capacity_mw`). Backfill values match what the migration script (Task 10) will write to existing DBs.

This task changes the schema constant only; DB consumers (db.py, init_weather_observation.py) update in Task 9.

- [ ] **Step 1: Update `SCHEMA_LOCATION` DDL**

In `src/weather_schema.py`, find `SCHEMA_LOCATION` (around line 167) and replace it with:
```python
SCHEMA_LOCATION = """
CREATE TABLE IF NOT EXISTS weather_location (
    location_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code  TEXT    NOT NULL,
    zone_id       TEXT    NOT NULL,
    zone_type     TEXT,
    lat           REAL    NOT NULL,
    lon           REAL    NOT NULL,
    weight        REAL,
    capacity_mw   REAL,
    description   TEXT,
    created_at    TEXT    DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(country_code, zone_id)
)
"""
```

- [ ] **Step 2: Add `zone_type` index to `SCHEMA_INDEXES`**

In `src/weather_schema.py`, find `SCHEMA_INDEXES` (around line 208) and append a third index:
```python
SCHEMA_INDEXES = [
    """
    CREATE INDEX IF NOT EXISTS idx_wx_replay
    ON weather_observation(location_id, valid_at, source_id, fetched_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_wx_source_latest
    ON weather_observation(source_id, location_id, fetched_at DESC, valid_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_weather_location_zone_type
    ON weather_location(zone_type)
    """,
]
```

- [ ] **Step 3: Rename `BE_LOCATIONS` → `LOCATIONS` and extend each row**

In `src/weather_schema.py`, find the `BE_LOCATIONS = [...]` block (around line 110) and replace it with:
```python
# Unified locations source of truth (Phase 1 — BE only; Phase 2 expands to all 39 countries).
# Tuple shape: (country_code, zone_id, zone_type, lat, lon, weight, capacity_mw, description)
#
# zone_type values:
#   'centroid'      — single-point country centroid (legacy weather_data semantics)
#   'solar'         — capacity-weighted PV cluster
#   'wind_onshore'  — capacity-weighted onshore wind cluster
#   'wind_offshore' — capacity-weighted offshore wind cluster
#
# Pre-Phase-2 BE rows: 'central'/'north'/'south'/'east' were originally
# PV-capacity-weighted zones, so they map to zone_type='solar'.
LOCATIONS = [
    # country, zone_id,    zone_type,  lat,   lon,  weight, capacity_mw, description
    ("BE",     "centroid", "centroid", 50.5,  4.45, 1.00,   None,        "Belgium centroid (legacy weather_data point)"),
    ("BE",     "central",  "solar",    50.8,  4.3,  0.40,   None,        "Central Belgium (40% of PV capacity)"),
    ("BE",     "north",    "solar",    51.1,  4.8,  0.30,   None,        "Northern Belgium (30%)"),
    ("BE",     "south",    "solar",    50.4,  4.0,  0.20,   None,        "Southern Belgium (20%)"),
    ("BE",     "east",     "solar",    50.2,  5.5,  0.10,   None,        "Eastern Belgium (10%)"),
]
```

(Note: `weight=1.0` for centroid (was None previously — the spec invariant is "every centroid has weight=1.0"). This is a deliberate semantic change. The migration in Task 10 backfills the same value.)

- [ ] **Step 4: Update `__all__`**

In `src/weather_schema.py`, find the `__all__` list (around line 223) and replace `"BE_LOCATIONS"` with `"LOCATIONS"`:
```python
__all__ = [
    "WEATHER_VARIABLE_COLUMNS",
    "OPENMETEO_TO_DB",
    "LOCATIONS",
    "OPEN_METEO_SOURCES",
    "SCHEMA_LOCATION",
    "SCHEMA_SOURCE",
    "SCHEMA_OBSERVATION",
    "SCHEMA_INDEXES",
    "ALL_SCHEMA_SQL",
]
```

- [ ] **Step 5: Verify imports still resolve**

Run:
```bash
python -c "from src.weather_schema import LOCATIONS, SCHEMA_LOCATION, SCHEMA_INDEXES; print(len(LOCATIONS), 'locations'); print(len(SCHEMA_INDEXES), 'indexes')"
```

Expected output: `5 locations` and `3 indexes` (centroid + solar zones × 4 = 5; replay + source_latest + zone_type = 3).

- [ ] **Step 6: Commit**

```bash
git add src/weather_schema.py
git commit -m "feat(schema): add zone_type + capacity_mw to weather_location; rename BE_LOCATIONS → LOCATIONS"
```

---

## Task 9: Update `db.py` + `init_weather_observation.py` for renamed constant + new columns

**Files:**
- Modify: `src/db.py`
- Modify: `scripts/init_weather_observation.py`

Both files import `BE_LOCATIONS` and use the old 6-column INSERT shape. Update to `LOCATIONS` + 8-column INSERT.

- [ ] **Step 1: Update `db.create_weather_observation_tables()`**

In `src/db.py`, find the import block in `create_weather_observation_tables` (around line 235-240) and update:
```python
    from src.weather_schema import (
        ALL_SCHEMA_SQL,
        LOCATIONS,
        OPEN_METEO_SOURCES,
    )
```

Then update the location INSERT (around line 249-256):
```python
        cursor.executemany(
            """
            INSERT OR IGNORE INTO weather_location
            (country_code, zone_id, zone_type, lat, lon, weight, capacity_mw, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            LOCATIONS,
        )
```

Update the log line at the bottom (around line 270-275):
```python
    logger.info(
        "weather_observation tables created/verified; "
        "seeded %d locations + %d OM sources",
        len(LOCATIONS),
        len(OPEN_METEO_SOURCES),
    )
```

- [ ] **Step 2: Update `init_weather_observation.py`**

Replace the `BE_LOCATIONS` import (line 29):
```python
from src.weather_schema import LOCATIONS, OPEN_METEO_SOURCES
```

In `_verify()` (around line 47-50), replace the BE-specific count check:
```python
        cursor.execute("SELECT COUNT(*) FROM weather_location")
        n_loc = cursor.fetchone()[0]
```

And update the print line + warning:
```python
        print(f"weather_location:      {n_loc} rows (expected {len(LOCATIONS)})")
```
```python
        if n_loc < len(LOCATIONS):
            print("WARN: fewer locations seeded than expected")
```

- [ ] **Step 3: Update conftest.py to use new constant name**

In `tests/conftest.py`, replace the `BE_LOCATIONS` import + INSERT with the renamed constant. Update the `seeded_observation_db` fixture:
```python
    from src.weather_schema import (
        LOCATIONS,
        OPEN_METEO_SOURCES,
        ALL_SCHEMA_SQL,
    )
```
And the INSERT:
```python
    cursor.executemany(
        "INSERT OR IGNORE INTO weather_location "
        "(country_code, zone_id, zone_type, lat, lon, weight, capacity_mw, description) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        LOCATIONS,
    )
```

- [ ] **Step 4: Run existing tests to confirm fixture + db.py changes are coherent**

```bash
pytest tests/ -v
```

Expected: `7 passed` (the existing coherence-script tests, now using the renamed LOCATIONS constant via the fixture).

- [ ] **Step 5: Commit**

```bash
git add src/db.py scripts/init_weather_observation.py tests/conftest.py
git commit -m "refactor(weather): use LOCATIONS constant + 8-column INSERT for weather_location seed"
```

---

## Task 10: Coherence script — locations dimension (TDD)

**Files:**
- Modify: `scripts/check_weather_coherence.py`
- Modify: `tests/test_check_weather_coherence.py`

Now that `LOCATIONS` exists with the new shape, add the third dimension to the coherence checker.

- [ ] **Step 1: Write the failing tests for locations drift**

Append to `tests/test_check_weather_coherence.py`:
```python
from scripts.check_weather_coherence import check_locations_dimension


def test_check_locations_dimension_passes_when_db_matches_schema(
    seeded_observation_db,
) -> None:
    drift = check_locations_dimension(seeded_observation_db)
    assert drift == {"only_in_schema": set(), "only_in_db": set()}


def test_check_locations_dimension_detects_extra_db_row(
    seeded_observation_db,
) -> None:
    cursor = seeded_observation_db.cursor()
    cursor.execute(
        "INSERT INTO weather_location "
        "(country_code, zone_id, zone_type, lat, lon, weight, capacity_mw, description) "
        "VALUES ('ZZ', 'rogue', 'solar', 0, 0, 1.0, 100, 'drift')"
    )
    seeded_observation_db.commit()

    drift = check_locations_dimension(seeded_observation_db)
    assert drift["only_in_db"] == {("ZZ", "rogue", "solar")}
    assert drift["only_in_schema"] == set()


def test_check_locations_dimension_detects_missing_db_row(
    seeded_observation_db,
) -> None:
    cursor = seeded_observation_db.cursor()
    cursor.execute("DELETE FROM weather_location WHERE zone_id = 'east'")
    seeded_observation_db.commit()

    drift = check_locations_dimension(seeded_observation_db)
    assert drift["only_in_schema"] == {("BE", "east", "solar")}
    assert drift["only_in_db"] == set()
```

- [ ] **Step 2: Run tests to verify failure**

```bash
pytest tests/test_check_weather_coherence.py::test_check_locations_dimension_passes_when_db_matches_schema -v
```

Expected: ImportError on `check_locations_dimension`.

- [ ] **Step 3: Implement `check_locations_dimension`**

In `scripts/check_weather_coherence.py`, update the schema import to include `LOCATIONS`:
```python
from src.weather_schema import LOCATIONS, OPEN_METEO_SOURCES, WEATHER_VARIABLE_COLUMNS
```

Add the new function after `check_columns_dimension`:
```python
def check_locations_dimension(conn: sqlite3.Connection) -> Drift:
    """Compare LOCATIONS (in code) to weather_location rows (in DB).

    Identity is (country_code, zone_id, zone_type) — the natural key plus
    zone_type, since zone_type is what fetchers filter on.
    """
    schema_set: set[tuple[str, str, str | None]] = {
        (country, zone_id, zone_type)
        for country, zone_id, zone_type, *_rest in LOCATIONS
    }
    cursor = conn.cursor()
    cursor.execute(
        "SELECT country_code, zone_id, zone_type FROM weather_location"
    )
    db_set: set[tuple[str, str, str | None]] = {
        (row[0], row[1], row[2]) for row in cursor.fetchall()
    }
    return {
        "only_in_schema": schema_set - db_set,
        "only_in_db": db_set - schema_set,
    }
```

Wire it into `run_all_checks()`. Replace the existing dimension loop:
```python
    sources_drift = check_sources_dimension(conn)
    columns_drift = check_columns_dimension(conn)
    locations_drift = check_locations_dimension(conn)

    has_drift = False
    for name, drift in [
        ("sources", sources_drift),
        ("locations", locations_drift),
        ("columns", columns_drift),
    ]:
        for line in _format_drift(name, drift):
            print(line)
        if drift["only_in_schema"] or drift["only_in_db"]:
            has_drift = True

    return 1 if has_drift else 0
```

- [ ] **Step 4: Run all coherence tests**

```bash
pytest tests/test_check_weather_coherence.py -v
```

Expected: `10 passed`.

- [ ] **Step 5: Commit**

```bash
git add scripts/check_weather_coherence.py tests/test_check_weather_coherence.py
git commit -m "feat(coherence): add locations-dimension drift detection"
```

---

## Task 11: Migration script for existing DBs (TDD)

**Files:**
- Create: `scripts/migrate_add_zone_type.py`
- Test: `tests/test_migrate_add_zone_type.py`

Existing prod DB has `weather_location` without `zone_type` / `capacity_mw`. Migration script: ALTER TABLE adds + idempotent backfill of the 5 BE rows. Idempotent (rerunnable safely).

- [ ] **Step 1: Write the failing test**

Create `tests/test_migrate_add_zone_type.py`:
```python
"""Tests for scripts/migrate_add_zone_type.py."""

from __future__ import annotations

import sqlite3

import pytest


# Pre-Phase-1 fixture: schema as it lived before this migration (no zone_type/capacity_mw).
@pytest.fixture
def pre_migration_db() -> sqlite3.Connection:
    """In-memory DB with the OLD weather_location schema (no zone_type / capacity_mw)."""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE weather_location (
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
    )
    # Seed the 5 pre-Phase-1 BE rows (matching what prod has today).
    pre_phase1_rows = [
        ("BE", "centroid", 50.5, 4.45, None, "Able centroid"),
        ("BE", "central",  50.8, 4.3,  0.40, "Central Belgium"),
        ("BE", "north",    51.1, 4.8,  0.30, "Northern Belgium"),
        ("BE", "south",    50.4, 4.0,  0.20, "Southern Belgium"),
        ("BE", "east",     50.2, 5.5,  0.10, "Eastern Belgium"),
    ]
    cursor.executemany(
        "INSERT INTO weather_location "
        "(country_code, zone_id, lat, lon, weight, description) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        pre_phase1_rows,
    )
    conn.commit()
    return conn


def test_migration_adds_zone_type_and_capacity_mw_columns(pre_migration_db) -> None:
    from scripts.migrate_add_zone_type import migrate

    migrate(pre_migration_db)

    cursor = pre_migration_db.cursor()
    cursor.execute("PRAGMA table_info(weather_location)")
    col_names = {row[1] for row in cursor.fetchall()}
    assert "zone_type" in col_names
    assert "capacity_mw" in col_names


def test_migration_backfills_existing_be_rows(pre_migration_db) -> None:
    from scripts.migrate_add_zone_type import migrate

    migrate(pre_migration_db)

    cursor = pre_migration_db.cursor()
    cursor.execute(
        "SELECT zone_id, zone_type FROM weather_location WHERE country_code = 'BE' "
        "ORDER BY zone_id"
    )
    rows = cursor.fetchall()
    assert ("centroid", "centroid") in rows
    assert ("central", "solar") in rows
    assert ("north", "solar") in rows
    assert ("south", "solar") in rows
    assert ("east", "solar") in rows


def test_migration_creates_zone_type_index(pre_migration_db) -> None:
    from scripts.migrate_add_zone_type import migrate

    migrate(pre_migration_db)

    cursor = pre_migration_db.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='weather_location'"
    )
    index_names = {row[0] for row in cursor.fetchall()}
    assert "idx_weather_location_zone_type" in index_names


def test_migration_is_idempotent(pre_migration_db) -> None:
    from scripts.migrate_add_zone_type import migrate

    migrate(pre_migration_db)
    # Second run must not raise (e.g., "duplicate column name") and must not corrupt data.
    migrate(pre_migration_db)

    cursor = pre_migration_db.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather_location")
    n = cursor.fetchone()[0]
    assert n == 5  # No duplicates introduced.
```

- [ ] **Step 2: Run tests to verify failure**

```bash
pytest tests/test_migrate_add_zone_type.py -v
```

Expected: 4 failures with ImportError or "no such module".

- [ ] **Step 3: Implement the migration**

Create `scripts/migrate_add_zone_type.py`:
```python
#!/usr/bin/env python3
"""Phase 1 migration: add zone_type + capacity_mw columns to weather_location.

Idempotent — safe to rerun. Uses ALTER TABLE ADD COLUMN (online, O(1) per
SQLite docs) plus an UPDATE backfill for the 5 pre-existing BE rows.

Usage:
    python scripts/migrate_add_zone_type.py [--db PATH]

Per CLAUDE.md `EXTENDING.md` rules:
  1. Backup prod DB before running.
  2. Test on a /tmp/scratch.db copy first.
  3. Verify with `init_weather_observation.py --verify` after.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def _index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?", (index_name,)
    )
    return cursor.fetchone() is not None


def migrate(conn: sqlite3.Connection) -> None:
    """Apply the Phase 1 migration. Idempotent."""
    cursor = conn.cursor()

    # Step 1: ALTER TABLE ADD COLUMN (skip if already added).
    if not _column_exists(conn, "weather_location", "zone_type"):
        cursor.execute("ALTER TABLE weather_location ADD COLUMN zone_type TEXT")
    if not _column_exists(conn, "weather_location", "capacity_mw"):
        cursor.execute("ALTER TABLE weather_location ADD COLUMN capacity_mw REAL")

    # Step 2: Backfill BE rows. Idempotent — reapplies same value if already set.
    cursor.execute(
        "UPDATE weather_location SET zone_type = 'centroid' "
        "WHERE country_code = 'BE' AND zone_id = 'centroid'"
    )
    cursor.execute(
        "UPDATE weather_location SET zone_type = 'solar' "
        "WHERE country_code = 'BE' AND zone_id IN ('central', 'north', 'south', 'east')"
    )

    # Step 3: Create the zone_type index (skip if exists).
    if not _index_exists(conn, "idx_weather_location_zone_type"):
        cursor.execute(
            "CREATE INDEX idx_weather_location_zone_type "
            "ON weather_location(zone_type)"
        )

    conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=None,
        help="Path to SQLite DB (default: read from config.DATABASE_PATH).",
    )
    args = parser.parse_args()

    if args.db is None:
        import config
        db_path = config.DATABASE_PATH
    else:
        db_path = args.db

    print(f"Database: {db_path}")
    print("Applying migration: add zone_type + capacity_mw to weather_location")
    with sqlite3.connect(db_path) as conn:
        migrate(conn)
    print("OK: migration complete (idempotent — safe to rerun)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run all migration tests**

```bash
pytest tests/test_migrate_add_zone_type.py -v
```

Expected: `4 passed`.

- [ ] **Step 5: Run full test suite (regression check)**

```bash
pytest tests/ -v
```

Expected: `14 passed` (4 migration + 10 coherence).

- [ ] **Step 6: Commit**

```bash
git add scripts/migrate_add_zone_type.py tests/test_migrate_add_zone_type.py
git commit -m "feat(schema): Phase 1 migration script for zone_type + capacity_mw (TDD, idempotent)"
```

---

## Task 12: Apply migration to scratch DB and verify

**Files:**
- (No code changes — this is a validation step)

Per CLAUDE.md `EXTENDING.md` rules: every schema change must be tested on `/tmp/scratch.db` before prod.

- [ ] **Step 1: Locate a prod-like DB**

The valid SQLite at `/c/Code/able/energy-data-gathering/energy_dashboard.db` is pre-Phase-1 (no `zone_type`). Verify:
```bash
sqlite3 "/c/Code/able/energy-data-gathering/energy_dashboard.db" "PRAGMA table_info(weather_location);"
```

Expected: no `zone_type` row in output. Note: this DB doesn't have `weather_observation` either (it's older than the migration that introduced the table) — that's OK for testing the migration script in isolation.

If `weather_location` doesn't exist either, skip to Step 2 with a fresh-init scratch instead.

- [ ] **Step 2: Make a scratch copy**

```bash
cp "/c/Code/able/energy-data-gathering/energy_dashboard.db" /tmp/scratch.db
```

If the source DB lacks `weather_location`, use the post-recovery `/c/Code/able/data/energy_dashboard.db` instead:
```bash
cp "/c/Code/able/data/energy_dashboard.db" /tmp/scratch.db
```

- [ ] **Step 3: Apply migration to scratch**

```bash
python scripts/migrate_add_zone_type.py --db /tmp/scratch.db
```

Expected output:
```
Database: /tmp/scratch.db
Applying migration: add zone_type + capacity_mw to weather_location
OK: migration complete (idempotent — safe to rerun)
```

- [ ] **Step 4: Verify schema + backfill**

```bash
sqlite3 /tmp/scratch.db "PRAGMA table_info(weather_location);"
```

Expected: rows include `zone_type TEXT` and `capacity_mw REAL`.

```bash
sqlite3 /tmp/scratch.db "SELECT country_code, zone_id, zone_type FROM weather_location WHERE country_code='BE' ORDER BY zone_id;"
```

Expected:
```
BE|central|solar
BE|centroid|centroid
BE|east|solar
BE|north|solar
BE|south|solar
```

```bash
sqlite3 /tmp/scratch.db "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='weather_location';"
```

Expected: includes `idx_weather_location_zone_type`.

- [ ] **Step 5: Verify coherence**

```bash
python scripts/check_weather_coherence.py --db /tmp/scratch.db
```

Expected: three `PASS:` lines (sources, locations, columns), exit code 0.

- [ ] **Step 6: Test idempotency on scratch**

```bash
python scripts/migrate_add_zone_type.py --db /tmp/scratch.db
```

Expected: same `OK:` output, no errors. Then re-verify coherence (same three PASS lines).

- [ ] **Step 7: No commit (validation only)**

This task produces no code changes. Move to Task 13.

---

## Task 13: Apply migration to prod (with backup)

**Files:**
- (No code changes — operator-driven deploy)

Operator action; spec/plan documents the exact commands. Per CLAUDE.md `EXTENDING.md`: backup first, ALTER, verify.

- [ ] **Step 1: Backup prod DB**

Run on prod:
```bash
ssh clavain@192.168.86.36 \
  'sqlite3 /home/clavain/energy-dashboard/data/energy_dashboard.db \
    ".backup /home/clavain/energy-dashboard/data/backup_2026-04-23_pre_zone_type.db"'
```

Expected: no output (success). Verify the backup file exists:
```bash
ssh clavain@192.168.86.36 'ls -lh /home/clavain/energy-dashboard/data/backup_2026-04-23_pre_zone_type.db'
```

Expected: file size matches the prod DB size (within a few MB).

- [ ] **Step 2: Copy migration script to prod (if not already deployed via Docker)**

If prod runs the data-gathering Docker container, the script lands when you bump the container image. For this single migration, run directly via `docker compose exec`:
```bash
ssh clavain@192.168.86.36 \
  'cd /home/clavain/energy-dashboard && docker compose exec data-gathering \
    python scripts/migrate_add_zone_type.py'
```

(This requires the new code already deployed in the running container. If not, build + redeploy first.)

- [ ] **Step 3: Verify schema + coherence on prod**

```bash
ssh clavain@192.168.86.36 \
  'cd /home/clavain/energy-dashboard && docker compose exec data-gathering \
    python scripts/init_weather_observation.py --verify'
```

Expected output (final lines):
```
Tables present: weather_location, weather_observation, weather_source
weather_location:      5 rows (expected 5)
weather_source:        29 rows (expected 29)
weather_observation indexes: ['idx_wx_replay', 'idx_wx_source_latest']
---
PASS: sources
PASS: locations
PASS: columns
```

Exit code 0.

- [ ] **Step 4: Smoke-check the HTTP endpoint**

```bash
curl -s "http://192.168.86.36:3001/api/weather/latest?country_code=BE&zones=central,north,south,east&provider=open_meteo_forecast&models=best_match&lead_time_hours=-1&valid_from=2026-04-22T00:00:00Z&valid_to=2026-04-23T00:00:00Z" | head -50
```

Expected: JSON response with non-empty data blocks for the 4 BE zones. Confirms the `weather_location` table change didn't break frontend reads.

- [ ] **Step 5: Re-sync workstation replica**

```bash
rsync -avh --progress \
  clavain@192.168.86.36:/home/clavain/energy-dashboard/data/energy_dashboard.db \
  "/c/Code/able/data/energy_dashboard.db"
```

- [ ] **Step 6: Final coherence check on workstation replica**

```bash
python scripts/check_weather_coherence.py --db "/c/Code/able/data/energy_dashboard.db"
```

Expected: three `PASS:` lines, exit code 0.

- [ ] **Step 7: No code commit needed**

All deploy actions are documented but produce no repo changes. This task wraps Plan 1.

---

## End of Plan 1

**Final state after Plan 1:**

- ✅ Broken workstation replica recovered + recovery runbook documented
- ✅ `docker/crontab` comments accurate (7 NWP models, not 4)
- ✅ `weather-db-query` skill source-id table refreshed (operator-handled outside repo)
- ✅ `scripts/check_weather_coherence.py` — 3-dimension drift detector with 10 passing tests
- ✅ `init_weather_observation.py --verify` runs coherence check on every deploy
- ✅ `weather_location` table has `zone_type` + `capacity_mw` columns + `idx_weather_location_zone_type` index
- ✅ 5 BE rows backfilled (centroid → `centroid`, others → `solar`)
- ✅ `LOCATIONS` schema constant ready for Phase 2 expansion
- ✅ `tests/` scaffolding in place for future TDD work

**Test count:** 14 (10 coherence + 4 migration).

**Next plan:** `Plan 2 — Sourcing pipeline (Phase 2)` — `scripts/build_weather_locations.py` consuming OPSD + GEM trackers, generating the full 39-country `LOCATIONS` block.
