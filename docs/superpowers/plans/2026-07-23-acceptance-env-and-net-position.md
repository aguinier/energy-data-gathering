# Acceptance Environment & Net-Position Forecast Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the workstation DB replica to cover all tables, stand up a prod-like acceptance environment (frontend in Docker + forecast in a native GPU venv), and get the net-position forecast (V010/V011) running on a daily schedule.

**Architecture:** The workstation is a **replica** of prod's SQLite DB — prod ingests, a daily sync pulls everything down (weather incrementally by rowid, all other tables by full refresh). The frontend container serves the replica read-only; forecast code reads the replica and writes **only** to a sidecar DB. Cross-border/net-position data collection is enabled on prod and backfilled once; the replica then receives it automatically.

**Tech Stack:** PowerShell 5.1 (sync + schedulers), Python 3.11 (sqlite3, chronos-forecasting, torch/CUDA), Docker Desktop (frontend), Windows Task Scheduler, ENTSO-E API (prod side only).

**Spec:** `docs/superpowers/specs/2026-07-23-acceptance-env-and-net-position-design.md` (same repo).

## Global Constraints

- Code flows **workstation → GitHub → prod** (`git pull` on prod). Never commit on prod.
- **Prod's `energy-forecast` container must not be touched** — no pull, no rebuild, no restart.
- **Replica purity:** nothing on the workstation writes to `C:\Code\able\data\energy_dashboard.db` except the sync script. All locally generated forecasts go to the sidecar `C:\Code\able\data\forecasts_local.db`.
- Prod SSH: `clavain@192.168.86.36` (key auth installed). Prod DB: `/home/clavain/energy-dashboard/data/energy_dashboard.db`.
- Scheduled tasks run under `powershell.exe` (PowerShell 5.1) — scripts must stay 5.1-compatible (no `&&`, no ternary).
- Only one backfill instance may run on prod at a time; interrupted runs resume with `--resume`.
- Logs for workstation jobs go to `C:\Code\able\logs\` (created on first use).
- Measured facts this plan relies on: all non-`weather_observation` tables together ≈ **15.3M rows** (transfer ≈ 1–3 GB); `weather_observation` ≈ 858M rows / ~340 GB (must stay incremental); prod box: 6 cores, 7 GB RAM, no GPU; workstation GPU: RTX 2060 SUPER 8 GB.

---

### Task 1: Launch cross-border + net-position backfill on prod

The longest pole — start it first; later tasks check on it. The running prod
`energy-data-gathering` image (built ≥ Jun 25 from commit `8c7433a`, May 18)
already contains `scripts/backfill_crossborder.py`, so **no deploy is needed
to start**.

**Files:** none (operational task, all on prod via SSH).

**Interfaces:**
- Produces: populated `crossborder_flows` + `net_position` tables in prod DB, 2023-01 → today. Task 9 gates on their completeness; Task 4's sync transports them to the replica automatically.

- [ ] **Step 1: Verify the script exists in the running container**

```bash
ssh clavain@192.168.86.36 "docker exec energy-data-gathering ls scripts/backfill_crossborder.py && docker exec energy-data-gathering python -c 'from src import fetch_crossborder_flows, fetch_net_position; print(\"imports ok\")'"
```
Expected: `scripts/backfill_crossborder.py` and `imports ok`.

- [ ] **Step 2: Launch the backfill in a tmux session on the prod host**

```bash
ssh clavain@192.168.86.36 "tmux new-session -d -s cbbackfill 'docker exec energy-data-gathering python scripts/backfill_crossborder.py --types all --countries all 2>&1 | tee -a /home/clavain/backfill_crossborder.log'"
```
Expected: exit 0, no output. (`--start-month` defaults to `2023-01`, `--end-month` to the current month; the script creates both tables itself.)

- [ ] **Step 3: After ~2 minutes, verify it is progressing**

```bash
ssh clavain@192.168.86.36 "tail -5 /home/clavain/backfill_crossborder.log"
```
Expected: per-country/per-month fetch log lines (inserted counts), no tracebacks.

- [ ] **Step 4: Verify tables exist and are filling**

```bash
ssh clavain@192.168.86.36 "python3 -c \"import sqlite3; c=sqlite3.connect('file:/home/clavain/energy-dashboard/data/energy_dashboard.db?mode=ro', uri=True); print('cbf', c.execute('SELECT count(*) FROM crossborder_flows').fetchone()[0]); print('np', c.execute('SELECT count(*) FROM net_position').fetchone()[0])\""
```
Expected: both counts > 0 and growing on a second invocation.

**Monitoring / recovery (for later tasks):** progress via the `tail` command above; if the tmux session dies, relaunch Step 2 with `--resume` appended. Completion = log shows the final summary and `SELECT max(timestamp_utc) FROM crossborder_flows` is within ~2 days of today.

---

### Task 2: Enable ongoing collection in prod's cron (`update.py`)

**Files:**
- Modify: `scripts/update.py:110` (energy-data-gathering repo)

**Interfaces:**
- Consumes: tables created by Task 1's backfill.
- Produces: `crossborder_flows` + `net_position` stay current via the existing 4×-daily cron.

- [ ] **Step 1: Edit the default data-types list**

In `scripts/update.py` line 110, change:
```python
        data_types = ['load', 'price', 'renewable', 'load_forecast_day_ahead', 'load_forecast_week_ahead', 'wind_solar_forecast']
```
to:
```python
        data_types = ['load', 'price', 'renewable', 'load_forecast_day_ahead', 'load_forecast_week_ahead', 'wind_solar_forecast', 'crossborder_flows', 'net_position']
```

- [ ] **Step 2: Syntax-check (do NOT run `update.py` on the workstation — it would ingest into the replica)**

```bash
cd /c/Code/able/energy-data-gathering && python -m py_compile scripts/update.py && echo OK
```
Expected: `OK`.

- [ ] **Step 3: Commit and push**

```bash
cd /c/Code/able/energy-data-gathering && git add scripts/update.py && git commit -m "feat: add crossborder_flows + net_position to scheduled update types" && git push origin main
```

- [ ] **Step 4: Deploy the data-gathering container on prod (NOT the forecast container)**

```bash
ssh clavain@192.168.86.36 "cd /home/clavain/energy-dashboard/repos/energy-data-gathering && git pull && cd docker && docker compose build && docker compose up -d --force-recreate"
```
Expected: pull fast-forwards to the new commit; container recreated. Concurrent writes with the running backfill are fine (SQLite busy handling already in place — prod containers write concurrently today).

- [ ] **Step 5: Verify after the next cron slot (00:30 / 06:30 / 13:30 / 18:30 UTC)**

```bash
ssh clavain@192.168.86.36 "python3 -c \"import sqlite3; c=sqlite3.connect('file:/home/clavain/energy-dashboard/data/energy_dashboard.db?mode=ro', uri=True); print(c.execute('SELECT data_type, max(created_at) FROM data_ingestion_log WHERE data_type IN (\\\"crossborder_flows\\\",\\\"net_position\\\") GROUP BY data_type').fetchall())\""
```
Expected: recent ingestion-log entries for both types. (If the log-table column differs, `docker exec energy-data-gathering tail -50 logs/cron_update.log` must show both types fetched without error.)

---

### Task 3: Push the stranded energy-forecast work

Ends the single-copy risk on the 12 unpushed commits.

**Files:**
- Modify: `experiments/registry.json` (energy-forecast repo — commit the existing uncommitted edit)

**Interfaces:**
- Produces: GitHub `origin/main` contains the Chronos-2 + net-position code (V010/V011 configs, loaders, `forecast_chronos2.py` net_position support) that Tasks 7–11 use.

- [ ] **Step 1: Review the pending diff (statuses V002 → ready, V003 → completed)**

```bash
cd /c/Code/able/energy-forecast && git diff experiments/registry.json
```
Expected: only the two status changes + missing trailing newline. If anything else appears, stop and ask.

- [ ] **Step 2: Commit and push everything**

```bash
cd /c/Code/able/energy-forecast && git add experiments/registry.json && git commit -m "chore: update experiment registry statuses (V002 ready, V003 completed)" && git push origin main
```

- [ ] **Step 3: Verify clean and synced**

```bash
cd /c/Code/able/energy-forecast && git status -sb
```
Expected: `## main...origin/main` — no ahead/behind, no modified tracked files.

---

### Task 4: Sync v2 — full-coverage replica script

**Files:**
- Create: `scripts/workstation/sync-db-v2.ps1` (energy-data-gathering repo)

**Interfaces:**
- Consumes: existing replica at `C:\Code\able\data\energy_dashboard.db` (rowid-aligned weather base), SSH access to prod.
- Produces: a replica where **every** table is ≤ 24 h behind prod. Stage 2's `sqlite_master` enumeration auto-includes `crossborder_flows` / `net_position` / any future table. Task 5 schedules this script; Task 9 depends on its data.

- [ ] **Step 1: Create the script**

Create `scripts/workstation/sync-db-v2.ps1` with exactly this content (Stage 1 is the proven logic from `C:\Code\able\scripts\sync-db-incremental.ps1`; Stage 2 and the transcript logging are new):

```powershell
<#
.SYNOPSIS
    Daily workstation replica sync v2 — full coverage.

.DESCRIPTION
    Stage 1  weather_observation: rowid-watermark incremental pull (unchanged
             from sync-db-incremental.ps1). ~340 GB table, append-only.
    Stage 2  every other table: full refresh. Prod exports all tables from
             sqlite_master EXCEPT weather_observation into a transfer DB
             (CTAS via read-only attach) together with their original DDL;
             locally each table is dropped and rebuilt from that DDL inside
             one transaction, then indexes are recreated. ~15M rows / 1-3 GB
             total - cheap, and immune to schema drift: new prod tables
             appear on the replica automatically.

    CAVEATS
      - A prod VACUUM renumbers rowids and invalidates the weather watermark
        -> full re-seed with the old sync-db-from-prod.ps1 required.
      - Replica-purity: this script is the ONLY writer to the replica.

.EXAMPLE
    powershell -File sync-db-v2.ps1            # both stages
    powershell -File sync-db-v2.ps1 -TablesOnly
#>
param(
    [int64]$SinceRowid = 0,
    [switch]$WeatherOnly,
    [switch]$TablesOnly
)

$ErrorActionPreference = "Stop"

$RemoteUser   = "clavain"
$RemoteHost   = "192.168.86.36"
$RemoteDb     = "/home/clavain/energy-dashboard/data/energy_dashboard.db"
$RemoteTmpDir = "/home/clavain/db_sync_tmp"
$RemoteDelta  = "$RemoteTmpDir/delta_export.db"
$RemoteRefresh= "$RemoteTmpDir/refresh_export.db"

$LocalDb     = "C:\Code\able\data\energy_dashboard.db"
$LocalDelta  = "$LocalDb.delta"
$LocalRefresh= "$LocalDb.refresh"
$LogDir      = "C:\Code\able\logs"

$Target = "$RemoteUser@$RemoteHost"
$Stamp  = { Get-Date -Format "yyyy-MM-dd HH:mm:ss" }

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Force $LogDir | Out-Null }
Start-Transcript -Path (Join-Path $LogDir "sync-db-v2.log") -Append | Out-Null

# Run a python snippet on prod via base64 (survives PS 5.1 quoting).
function Invoke-ProdPython([string]$Code) {
    $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Code))
    $out = ssh $Target "echo $b64 | base64 -d | python3 -"
    if ($LASTEXITCODE -ne 0) { throw "Remote python failed (exit $LASTEXITCODE)" }
    return $out
}

function Get-LocalMaxRowid {
    $code = @"
import sqlite3
c = sqlite3.connect(r'$LocalDb')
r = c.execute('SELECT max(rowid) FROM weather_observation').fetchone()[0]
c.close()
print('' if r is None else r)
"@
    $out = ($code | python -).Trim()
    if ($out -eq "") { throw "Local weather_observation is empty - run sync-db-from-prod.ps1 to seed a base replica first." }
    return [int64]$out
}

try {
    # =====================================================================
    # Stage 1 - weather_observation incremental (rowid watermark)
    # =====================================================================
    if (-not $TablesOnly) {
        if ($SinceRowid -gt 0) { $W = $SinceRowid } else { $W = Get-LocalMaxRowid }
        Write-Host "[$(& $Stamp)] Stage 1: weather watermark max(rowid) = $W"

        $py = @"
import sqlite3
dst = sqlite3.connect('file:${RemoteDelta}', uri=True)
dst.execute("ATTACH 'file:${RemoteDb}?mode=ro' AS prod")
dst.execute("CREATE TABLE wx_delta AS SELECT rowid AS rid, * FROM prod.weather_observation WHERE rowid > $W")
n  = dst.execute('SELECT count(*) FROM wx_delta').fetchone()[0]
dst.commit(); dst.close()
print(f'DELTA_ROWS={n}')
"@
        ssh $Target "set -e; mkdir -p $RemoteTmpDir; rm -f $RemoteDelta $RemoteDelta-wal $RemoteDelta-shm" | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Remote prep failed" }
        $exportOut = Invoke-ProdPython $py
        $exportOut | ForEach-Object { Write-Host "  $_" }
        $deltaRows = [int64](($exportOut | Select-String '^DELTA_ROWS=(\d+)').Matches.Groups[1].Value)

        if ($deltaRows -gt 0) {
            Write-Host "[$(& $Stamp)] Transferring weather delta ($deltaRows rows) ..."
            if (Test-Path $LocalDelta) { Remove-Item $LocalDelta -Force }
            scp "${Target}:$RemoteDelta" $LocalDelta
            if ($LASTEXITCODE -ne 0) { throw "scp failed (exit $LASTEXITCODE)" }

            Write-Host "[$(& $Stamp)] Merging weather delta ..."
            $mergeCode = @"
import sqlite3
con = sqlite3.connect(r'${LocalDb}')
con.execute('PRAGMA busy_timeout=300000')
con.execute("ATTACH ? AS d", (r'${LocalDelta}',))
cols = [r[1] for r in con.execute('PRAGMA table_info(weather_observation)').fetchall()]
cl = ','.join(cols)
mn, mx = con.execute('SELECT min(rid), max(rid) FROM d.wx_delta').fetchone()
total = 0
if mn is not None:
    BATCH = 5000000
    lo = mn
    while lo <= mx:
        hi = lo + BATCH
        cur = con.execute(f'INSERT OR IGNORE INTO weather_observation(rowid,{cl}) SELECT rid,{cl} FROM d.wx_delta WHERE rid >= ? AND rid < ?', (lo, hi))
        con.commit()
        total += cur.rowcount
        print(f'MERGE_BATCH rid>={lo} +{cur.rowcount} total={total}', flush=True)
        lo = hi
after = con.execute('SELECT max(rowid) FROM weather_observation').fetchone()[0]
con.execute('DETACH d'); con.close()
print(f'INSERTED={total}')
print(f'AFTER_MAX_ROWID={after}')
"@
            $mergeOut = $mergeCode | python -
            if ($LASTEXITCODE -ne 0) { throw "Weather merge failed (exit $LASTEXITCODE)" }
            $mergeOut | ForEach-Object { Write-Host "  $_" }
        } else {
            Write-Host "[$(& $Stamp)] Weather already current (0 new rows)."
        }
    }

    # =====================================================================
    # Stage 2 - all other tables: full refresh from prod
    # =====================================================================
    if (-not $WeatherOnly) {
        Write-Host "[$(& $Stamp)] Stage 2: exporting all non-weather tables from prod ..."
        $py2 = @"
import sqlite3
dst = sqlite3.connect('file:${RemoteRefresh}', uri=True)
dst.execute("ATTACH 'file:${RemoteDb}?mode=ro' AS prod")
skip = ('weather_observation',)
tables = [(r[0], r[1]) for r in dst.execute(
    "SELECT name, sql FROM prod.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN (?)", skip)]
dst.execute("CREATE TABLE _ddl(name TEXT, kind TEXT, sql TEXT)")
dst.execute("CREATE TABLE _meta(name TEXT, rows INTEGER)")
for name, sql in tables:
    dst.execute('CREATE TABLE "%s" AS SELECT * FROM prod."%s"' % (name, name))
    n = dst.execute('SELECT count(*) FROM "%s"' % name).fetchone()[0]
    dst.execute("INSERT INTO _ddl VALUES (?, 'table', ?)", (name, sql))
    dst.execute("INSERT INTO _meta VALUES (?, ?)", (name, n))
    for (isql,) in dst.execute(
        "SELECT sql FROM prod.sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL", (name,)):
        dst.execute("INSERT INTO _ddl VALUES (?, 'index', ?)", (name, isql))
dst.commit(); dst.close()
print(f'REFRESH_TABLES={len(tables)}')
"@
        ssh $Target "rm -f $RemoteRefresh $RemoteRefresh-wal $RemoteRefresh-shm" | Out-Null
        $refOut = Invoke-ProdPython $py2
        $refOut | ForEach-Object { Write-Host "  $_" }

        Write-Host "[$(& $Stamp)] Transferring refresh export ..."
        if (Test-Path $LocalRefresh) { Remove-Item $LocalRefresh -Force }
        scp "${Target}:$RemoteRefresh" $LocalRefresh
        if ($LASTEXITCODE -ne 0) { throw "scp failed (exit $LASTEXITCODE)" }

        Write-Host "[$(& $Stamp)] Replacing local tables (transactional) ..."
        $swapCode = @"
import sqlite3
con = sqlite3.connect(r'${LocalDb}')
con.execute('PRAGMA busy_timeout=300000')
con.execute("ATTACH ? AS d", (r'${LocalRefresh}',))
con.isolation_level = None
con.execute('BEGIN IMMEDIATE')
tables = con.execute("SELECT name, sql FROM d._ddl WHERE kind='table'").fetchall()
for name, sql in tables:
    con.execute('DROP TABLE IF EXISTS main."%s"' % name)
    con.execute(sql)
    con.execute('INSERT INTO main."%s" SELECT * FROM d."%s"' % (name, name))
for name, isql in con.execute("SELECT name, sql FROM d._ddl WHERE kind='index'").fetchall():
    con.execute(isql)
mismatch = []
for name, want in con.execute('SELECT name, rows FROM d._meta').fetchall():
    got = con.execute('SELECT count(*) FROM main."%s"' % name).fetchone()[0]
    if got != want:
        mismatch.append((name, want, got))
if mismatch:
    con.execute('ROLLBACK')
    raise SystemExit('ROWCOUNT MISMATCH: %s' % mismatch)
con.execute('COMMIT')
print(f'REFRESHED={len(tables)} tables')
for name, sql in tables:
    cols = [r[1] for r in con.execute('PRAGMA table_info("%s")' % name).fetchall()]
    ts = next((c for c in cols if 'timestamp' in c.lower() or c.lower() in ('datetime','date','created_at')), None)
    if ts:
        mx = con.execute('SELECT max("%s") FROM main."%s"' % (ts, name)).fetchone()[0]
        print(f'FRESH {name:32s} {ts:24s} {mx}')
con.execute('DETACH d'); con.close()
"@
        $swapOut = $swapCode | python -
        if ($LASTEXITCODE -ne 0) { throw "Table refresh failed (exit $LASTEXITCODE)" }
        $swapOut | ForEach-Object { Write-Host "  $_" }
    }

    Write-Host "[$(& $Stamp)] Done."
}
finally {
    Write-Host "[$(& $Stamp)] Cleanup"
    ssh $Target "rm -f $RemoteDelta $RemoteDelta-wal $RemoteDelta-shm $RemoteRefresh $RemoteRefresh-wal $RemoteRefresh-shm" 2>$null | Out-Null
    foreach ($f in @($LocalDelta, "$LocalDelta-wal", "$LocalDelta-shm", $LocalRefresh, "$LocalRefresh-wal", "$LocalRefresh-shm")) {
        if (Test-Path $f) { Remove-Item $f -Force }
    }
    Stop-Transcript | Out-Null
}
```

- [ ] **Step 2: Run Stage 2 alone first (small, fast, proves the new path)**

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File C:\Code\able\energy-data-gathering\scripts\workstation\sync-db-v2.ps1 -TablesOnly
```
Expected: `REFRESH_TABLES=18` (or +2 once Task 1's tables exist), `REFRESHED=… tables`, and `FRESH energy_load … <today or yesterday>` lines. Failure mode to watch: `ROWCOUNT MISMATCH` aborts with rollback (replica left on yesterday's tables).

- [ ] **Step 3: Verify replica freshness directly**

```powershell
python -c "import sqlite3; c=sqlite3.connect(r'C:\Code\able\data\energy_dashboard.db'); print('load ', c.execute('SELECT max(timestamp_utc) FROM energy_load').fetchone()[0]); print('price', c.execute('SELECT max(timestamp_utc) FROM energy_price').fetchone()[0])"
```
Expected: both within ~24 h of today (price may reach tomorrow — day-ahead).

- [ ] **Step 4: Run the full script (both stages)**

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File C:\Code\able\energy-data-gathering\scripts\workstation\sync-db-v2.ps1
```
Expected: Stage 1 pulls ~a day of weather rows (a few M) then Stage 2 reports 0-diff refresh; transcript appended to `C:\Code\able\logs\sync-db-v2.log`.

- [ ] **Step 5: Commit and push**

```bash
cd /c/Code/able/energy-data-gathering && git add scripts/workstation/sync-db-v2.ps1 && git commit -m "feat: workstation sync v2 - full-coverage daily replica (weather incremental + table refresh)" && git push origin main
```

---

### Task 5: Repoint the `able-db-sync` scheduled task

**Files:** none (Task Scheduler change).

**Interfaces:**
- Consumes: `sync-db-v2.ps1` from Task 4 (in-repo path).
- Produces: replica auto-refreshes daily at 07:00; Task 10's 08:00 forecast task can rely on fresh data.

- [ ] **Step 1: Repoint the task**

```powershell
schtasks /Change /TN able-db-sync /TR "powershell.exe -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File C:\Code\able\energy-data-gathering\scripts\workstation\sync-db-v2.ps1"
```
Expected: `SUCCESS`.

- [ ] **Step 2: Verify the registered action**

```powershell
schtasks /Query /TN able-db-sync /V /FO LIST | Select-String "Task To Run"
```
Expected: the new in-repo path.

- [ ] **Step 3: Trigger a run through the scheduler and check the log**

```powershell
schtasks /Run /TN able-db-sync
Start-Sleep -Seconds 90
Get-Content C:\Code\able\logs\sync-db-v2.log -Tail 15
```
Expected: a fresh transcript block ending in `Done.` (idempotent — rerunning right after Task 4 mostly no-ops).

---

### Task 6: Frontend container on Docker Desktop

**Files:**
- Create: `docker/.env` (energy-dashboard-frontend repo — untracked, matches prod convention)

**Interfaces:**
- Consumes: replica DB from Tasks 4–5.
- Produces: dashboard at `http://localhost:3001`, always-on, read-only on the replica (`HELIO_WRITE_TOKEN` unset → the sole write endpoint 503s).

- [ ] **Step 1: Create `docker/.env`**

`C:\Code\able\energy-dashboard-frontend\docker\.env`:
```
DB_DIR=C:/Code/able/data
```
(Forward slashes — Docker Desktop volume syntax. `HELIO_WRITE_TOKEN` deliberately absent.)

- [ ] **Step 2: Build and start**

```powershell
cd C:\Code\able\energy-dashboard-frontend\docker; docker compose up -d --build
```
Expected: image builds, `energy-dashboard-frontend` container starts.

- [ ] **Step 3: Verify health + data freshness through the API**

```powershell
curl.exe -s http://localhost:3001/api/health
curl.exe -s http://localhost:3001/api/data-freshness/DE
```
Expected: `"status":"healthy"`; freshness timestamps within ~24 h (proves it reads the replica, not a stale copy).

- [ ] **Step 4: Verify restart policy and enable Docker Desktop autostart**

```powershell
docker inspect energy-dashboard-frontend --format "{{.HostConfig.RestartPolicy.Name}}"
```
Expected: `unless-stopped`.
Manual step (GUI): Docker Desktop → Settings → General → enable **"Start Docker Desktop when you sign in"**. Confirm done before checking this box.

---

### Task 7: energy-forecast GPU venv

**Files:**
- Create: `.venv/` (energy-forecast repo — git-ignored)
- Create: `requirements-chronos.txt` (energy-forecast repo — committed; NOT merged into `requirements.txt`, which prod's CPU container installs)

**Interfaces:**
- Produces: `.venv\Scripts\python.exe` with CUDA torch + `chronos` importable — used by Tasks 8–11.

- [ ] **Step 1: Create `requirements-chronos.txt`**

```
# Chronos-2 extras for the workstation GPU venv (NOT installed in the prod
# CPU container image - keep out of requirements.txt).
# torch is installed separately with the CUDA index:
#   pip install torch --index-url https://download.pytorch.org/whl/cu121
chronos-forecasting>=1.4.0
pytest>=8.0.0
```

- [ ] **Step 2: Create the venv and install**

```powershell
cd C:\Code\able\energy-forecast
python -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\pip.exe install torch --index-url https://download.pytorch.org/whl/cu121
.venv\Scripts\pip.exe install -r requirements.txt -r requirements-chronos.txt
```
Expected: clean installs (torch wheel is ~2.5 GB; takes a few minutes).

- [ ] **Step 3: Verify CUDA + chronos**

```powershell
cd C:\Code\able\energy-forecast; .venv\Scripts\python.exe -c "import torch; from chronos import BaseChronosPipeline; print(torch.cuda.is_available(), torch.cuda.get_device_name(0))"
```
Expected: `True NVIDIA GeForce RTX 2060 SUPER`.

- [ ] **Step 4: Commit**

```bash
cd /c/Code/able/energy-forecast && git add requirements-chronos.txt && git commit -m "chore: add workstation GPU venv requirements (chronos extras)" && git push origin main
```

---

### Task 8: Sidecar output DB (replica purity) — TDD

**Files:**
- Modify: `config.py:18` (energy-forecast repo, after `DATABASE_PATH`)
- Modify: `src/db.py:27-52` (`get_connection`)
- Test: `tests/test_sidecar_db.py` (new — first test in this repo)

**Interfaces:**
- Consumes: `config.DATABASE_PATH` / `ENERGY_DB_PATH` (existing).
- Produces: env var **`FORECAST_OUTPUT_DB`** — when set, every `get_connection(readonly=False)` targets it; `readonly=True` connections still target `DATABASE_PATH`. All existing `db.create_*` / `save_*` functions transparently write to the sidecar. Tasks 9–11 set it to `C:\Code\able\data\forecasts_local.db`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_sidecar_db.py`:
```python
"""Sidecar output DB: with FORECAST_OUTPUT_DB set, writes go to the sidecar,
reads keep hitting the main (replica) DB."""
import importlib
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def _fresh_db_module(monkeypatch, tmp_path, sidecar: bool):
    monkeypatch.setenv("ENERGY_DB_PATH", str(tmp_path / "replica.db"))
    if sidecar:
        monkeypatch.setenv("FORECAST_OUTPUT_DB", str(tmp_path / "sidecar.db"))
    else:
        monkeypatch.delenv("FORECAST_OUTPUT_DB", raising=False)
    import config
    importlib.reload(config)
    from src import db
    importlib.reload(db)
    return db


def test_write_connection_targets_sidecar_when_set(monkeypatch, tmp_path):
    db = _fresh_db_module(monkeypatch, tmp_path, sidecar=True)
    with db.get_connection(readonly=False) as conn:
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
    side = sqlite3.connect(tmp_path / "sidecar.db")
    assert side.execute("SELECT count(*) FROM t").fetchone()[0] == 1


def test_replica_untouched_by_writes_when_sidecar_set(monkeypatch, tmp_path):
    # Seed a replica so we can prove it stays pristine.
    rep = sqlite3.connect(tmp_path / "replica.db")
    rep.execute("CREATE TABLE existing (x INTEGER)")
    rep.commit(); rep.close()
    db = _fresh_db_module(monkeypatch, tmp_path, sidecar=True)
    with db.get_connection(readonly=False) as conn:
        conn.execute("CREATE TABLE t (x INTEGER)")
    rep = sqlite3.connect(tmp_path / "replica.db")
    names = {r[0] for r in rep.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "t" not in names


def test_read_connection_targets_replica_even_with_sidecar(monkeypatch, tmp_path):
    rep = sqlite3.connect(tmp_path / "replica.db")
    rep.execute("CREATE TABLE marker (x INTEGER)")
    rep.commit(); rep.close()
    db = _fresh_db_module(monkeypatch, tmp_path, sidecar=True)
    with db.get_connection(readonly=True) as conn:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "marker" in names


def test_write_connection_targets_main_db_when_unset(monkeypatch, tmp_path):
    db = _fresh_db_module(monkeypatch, tmp_path, sidecar=False)
    with db.get_connection(readonly=False) as conn:
        conn.execute("CREATE TABLE t (x INTEGER)")
    rep = sqlite3.connect(tmp_path / "replica.db")
    assert rep.execute("SELECT count(*) FROM sqlite_master WHERE name='t'").fetchone()[0] == 1
```

- [ ] **Step 2: Run to verify it fails**

```powershell
cd C:\Code\able\energy-forecast; .venv\Scripts\python.exe -m pytest tests/test_sidecar_db.py -v
```
Expected: FAIL — writes land in `replica.db` (`test_write_connection_targets_sidecar_when_set` asserts on a missing `sidecar.db` table).

- [ ] **Step 3: Implement**

`config.py` — directly under the `DATABASE_PATH` line (line 18), add:
```python
# Sidecar DB for locally-generated forecasts (workstation replica-purity):
# when set, ALL write connections go here instead of DATABASE_PATH.
FORECAST_OUTPUT_DB = os.getenv('FORECAST_OUTPUT_DB')
```

`src/db.py` `get_connection` — replace the single connect line:
```python
        # Always use standard connection (SQLite URI mode can be unreliable)
        conn = sqlite3.connect(str(config.DATABASE_PATH), timeout=30.0)
```
with:
```python
        # Always use standard connection (SQLite URI mode can be unreliable).
        # Replica-purity: when FORECAST_OUTPUT_DB is set (workstation), all
        # write connections target the sidecar DB instead of the replica.
        target = config.DATABASE_PATH
        if not readonly and getattr(config, 'FORECAST_OUTPUT_DB', None):
            target = config.FORECAST_OUTPUT_DB
        conn = sqlite3.connect(str(target), timeout=30.0)
```

- [ ] **Step 4: Run tests to verify they pass**

```powershell
cd C:\Code\able\energy-forecast; .venv\Scripts\python.exe -m pytest tests/test_sidecar_db.py -v
```
Expected: 4 passed.

- [ ] **Step 5: Initialize the sidecar schema**

```powershell
cd C:\Code\able\energy-forecast
$env:ENERGY_DB_PATH = "C:\Code\able\data\energy_dashboard.db"
$env:FORECAST_OUTPUT_DB = "C:\Code\able\data\forecasts_local.db"
.venv\Scripts\python.exe -c "import sys; sys.path.insert(0, '.'); from src import db; db.create_forecasts_table(); db.create_forecast_quantiles_table(); print('sidecar ready')"
python -c "import sqlite3; print(sqlite3.connect(r'C:\Code\able\data\forecasts_local.db').execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall())"
```
Expected: `sidecar ready`, then both table names listed from `forecasts_local.db`.

- [ ] **Step 6: Commit and push**

```bash
cd /c/Code/able/energy-forecast && git add tests/test_sidecar_db.py config.py src/db.py && git commit -m "feat: FORECAST_OUTPUT_DB sidecar for write connections (replica purity)" && git push origin main
```

---

### Task 9: V010 zero-shot net-position smoke run

**Gate:** Task 1's backfill complete (`max(timestamp_utc)` in prod `crossborder_flows` within ~2 days of today) **and** a sync (Task 5) has run since — verify first, do not start otherwise.

**Files:** none (operational).

**Interfaces:**
- Consumes: replica `crossborder_flows`/`net_position` + weather + prices; venv (Task 7); sidecar env (Task 8).
- Produces: net-position forecast rows in the sidecar DB; proves the full inference path that Task 10 schedules.

- [ ] **Step 1: Verify the gate**

```powershell
python -c "import sqlite3; c=sqlite3.connect(r'C:\Code\able\data\energy_dashboard.db'); print('cbf rows', c.execute('SELECT count(*) FROM crossborder_flows').fetchone()[0]); print('cbf max ', c.execute('SELECT max(timestamp_utc) FROM crossborder_flows').fetchone()[0]); print('np  max ', c.execute('SELECT max(timestamp_utc) FROM net_position').fetchone()[0])"
```
Expected: millions of rows; both max timestamps within ~2 days of today. If the tables are missing locally, run Task 5's Step 3 (sync) after confirming the prod backfill finished.

- [ ] **Step 2: Dry run, one country, GPU**

```powershell
cd C:\Code\able\energy-forecast
$env:ENERGY_DB_PATH = "C:\Code\able\data\energy_dashboard.db"
$env:FORECAST_OUTPUT_DB = "C:\Code\able\data\forecasts_local.db"
.venv\Scripts\python.exe scripts\forecast_chronos2.py --experiment V010 --types net_position --countries BE --dry-run
```
Expected: model downloads `amazon/chronos-2` on first use, then prints 24 hourly net-position values for BE. No DB writes.

- [ ] **Step 3: Full run, all countries, saved**

```powershell
cd C:\Code\able\energy-forecast
$env:ENERGY_DB_PATH = "C:\Code\able\data\energy_dashboard.db"
$env:FORECAST_OUTPUT_DB = "C:\Code\able\data\forecasts_local.db"
.venv\Scripts\python.exe scripts\forecast_chronos2.py --experiment V010 --types net_position --countries all --save-to-db
```
Expected: per-country forecast + save log lines; countries without border data logged as skipped, not crashed.

- [ ] **Step 4: Verify rows landed in the sidecar (and only there)**

```powershell
python -c "import sqlite3; s=sqlite3.connect(r'C:\Code\able\data\forecasts_local.db'); print('sidecar', s.execute(\"SELECT count(*), count(DISTINCT country_code) FROM forecasts WHERE forecast_type='net_position'\").fetchone()); r=sqlite3.connect(r'C:\Code\able\data\energy_dashboard.db'); print('replica', r.execute(\"SELECT count(*) FROM forecasts WHERE forecast_type='net_position' AND model_name LIKE 'chronos%'\").fetchone())"
```
Expected: sidecar count > 0 across many countries; replica count 0 (purity holds).

---

### Task 10: Schedule the daily net-position forecast

**Files:**
- Create: `scripts/workstation/run-net-position.ps1` (energy-forecast repo)

**Interfaces:**
- Consumes: venv, sidecar env vars, `forecast_chronos2.py` (Tasks 7–9).
- Produces: Task Scheduler job `able-net-position-forecast`, daily 08:00 (one hour after the 07:00 sync), logging to `C:\Code\able\logs\net-position-forecast.log`. Task 11 may repoint `--experiment` to V011.

- [ ] **Step 1: Create the wrapper script**

`scripts/workstation/run-net-position.ps1`:
```powershell
# Daily net-position forecast on the workstation (acceptance).
# Reads the replica, writes ONLY to the sidecar DB (FORECAST_OUTPUT_DB).
# Scheduled at 08:00, after the 07:00 able-db-sync replica refresh.
$ErrorActionPreference = "Stop"
$Repo   = "C:\Code\able\energy-forecast"
$LogDir = "C:\Code\able\logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Force $LogDir | Out-Null }
Start-Transcript -Path (Join-Path $LogDir "net-position-forecast.log") -Append | Out-Null
try {
    $env:ENERGY_DB_PATH     = "C:\Code\able\data\energy_dashboard.db"
    $env:FORECAST_OUTPUT_DB = "C:\Code\able\data\forecasts_local.db"
    & "$Repo\.venv\Scripts\python.exe" "$Repo\scripts\forecast_chronos2.py" `
        --experiment V010 --types net_position --countries all --save-to-db
    if ($LASTEXITCODE -ne 0) { throw "forecast_chronos2.py exited $LASTEXITCODE" }
}
finally {
    Stop-Transcript | Out-Null
}
```

- [ ] **Step 2: Register the scheduled task**

```powershell
schtasks /Create /TN able-net-position-forecast /SC DAILY /ST 08:00 /TR "powershell.exe -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File C:\Code\able\energy-forecast\scripts\workstation\run-net-position.ps1" /F
```
Expected: `SUCCESS`.

- [ ] **Step 3: Trigger once and verify end-to-end**

```powershell
schtasks /Run /TN able-net-position-forecast
Start-Sleep -Seconds 180
Get-Content C:\Code\able\logs\net-position-forecast.log -Tail 10
schtasks /Query /TN able-net-position-forecast /V /FO LIST | Select-String "Last Result"
```
Expected: transcript shows a completed run; `Last Result: 0`.

- [ ] **Step 4: Commit and push**

```bash
cd /c/Code/able/energy-forecast && git add scripts/workstation/run-net-position.ps1 && git commit -m "feat: scheduled workstation net-position forecast (V010, sidecar output)" && git push origin main
```

---

### Task 11: V011 fine-tune, evaluate, conditional promotion

**Files:**
- Modify (conditional): `scripts/workstation/run-net-position.ps1` (`--experiment V010` → `V011`)

**Interfaces:**
- Consumes: everything above; GPU.
- Produces: fine-tuned checkpoint under `models/chronos2/finetuned/`; comparison JSON; possibly a promoted scheduled task.

- [ ] **Step 1: Launch the one-off fine-tune (long GPU run — do not run anything else heavy)**

```powershell
cd C:\Code\able\energy-forecast
$env:ENERGY_DB_PATH = "C:\Code\able\data\energy_dashboard.db"
$env:FORECAST_OUTPUT_DB = "C:\Code\able\data\forecasts_local.db"
.venv\Scripts\python.exe scripts\train_chronos2.py --experiment V011 --types net_position --countries all
```
Expected: 5000 steps with periodic checkpoints (config: batch 32, grad-accum 4, cosine LR). If VRAM overflows on 8 GB, retry with `--batch-size 16` (grad-accum compensates effective batch). Runtime: hours — leave it running.

- [ ] **Step 2: Verify the checkpoint exists**

```powershell
Get-ChildItem C:\Code\able\energy-forecast\models\chronos2\finetuned -Recurse | Select-Object -First 10
```
Expected: checkpoint directory with model files (safetensors/config), recent timestamps.

- [ ] **Step 3: Compare persistence vs V010 vs V011 on backtest weeks**

```powershell
cd C:\Code\able\energy-forecast
$env:ENERGY_DB_PATH = "C:\Code\able\data\energy_dashboard.db"
$env:FORECAST_OUTPUT_DB = "C:\Code\able\data\forecasts_local.db"
.venv\Scripts\python.exe scripts\compare_experiments.py --experiments persistence,V010,V011 --types net_position --countries all --output comparison_net_position.json
```
Expected: per-experiment metrics table + JSON written. `persistence` is a built-in pseudo-experiment (value at same hour 48 h ago).

- [ ] **Step 4: Promotion decision — STOP and apply the promotion gate**

Before claiming V011 "wins", run the `forecast-promotion-gate` checklist (serve-faithful evaluation, window stability, no look-ahead). Only if V011 beats **both** persistence and V010 on the held-out weeks: edit `scripts/workstation/run-net-position.ps1` changing `--experiment V010` to `--experiment V011`, then:

```bash
cd /c/Code/able/energy-forecast && git add scripts/workstation/run-net-position.ps1 comparison_net_position.json && git commit -m "feat: promote V011 fine-tuned model for scheduled net-position forecast" && git push origin main
```

If V011 does not clearly win, keep V010 scheduled, commit only the comparison JSON, and record the outcome in the registry (`experiments/registry.json` status field).

---

### Task 12: Documentation — WORKFLOWS.md reality update

**Files:**
- Modify: `C:\Code\able\WORKFLOWS.md` (unversioned able-root file)

**Interfaces:** none downstream — human documentation.

- [ ] **Step 1: Update the stale sections**

In `C:\Code\able\WORKFLOWS.md`:
1. **Environments table:** workstation row — replace "not used; training happens on prod" under Models with "GPU box (RTX 2060 SUPER 8 GB) — Chronos-2 training happens HERE; prod is CPU-only (6 cores / 7 GB)". Rename the "Test" column header to "Test / Acceptance".
2. **Database sync section:** replace the `sync-db-from-prod.ps1` daily-task description: `able-db-sync` (07:00) now runs `energy-data-gathering/scripts/workstation/sync-db-v2.ps1` — weather incremental + full refresh of all other tables; full-snapshot script retained only for re-seeding after a prod VACUUM. Log: `C:\Code\able\logs\sync-db-v2.log`.
3. **New "Acceptance services (workstation)" section:** frontend container (`docker compose` in `energy-dashboard-frontend/docker/`, port 3001, replica read-only, Docker Desktop autostart); forecast venv (`energy-forecast\.venv`, CUDA); scheduled tasks table: `able-db-sync` 07:00, `able-net-position-forecast` 08:00 (writes to sidecar `data\forecasts_local.db` — replica-purity rule stated explicitly).
4. **Model development loop:** note training now runs on the workstation GPU; prod containers only do scheduled XGBoost inference (unchanged Mar-era code, deliberately untouched).

- [ ] **Step 2: Verify accuracy against reality**

Re-read the edited file; every command and path mentioned must match what Tasks 1–11 actually built (task names, script paths, log paths, ports).

---

## Execution-order notes

- Task 1 first (longest pole; runs unattended for hours). Tasks 2–8 proceed while it runs.
- Task 9 is **gated** on Task 1 complete + a post-completion sync. Tasks 10–11 follow 9.
- Independent early tasks: 3 (push) can happen any time; 6 (frontend) only needs Task 4's first successful run.
