<#
.SYNOPSIS
    Daily workstation replica sync v2 — full coverage.

.DESCRIPTION
    Stage 1  weather_observation ONLY: rowid-watermark incremental pull, the
             same delta-CTAS + rowid-preserving merge mechanism as
             sync-db-incremental.ps1. Unlike that script, this stage does NOT
             also upsert the weather_location / weather_source dimension
             tables - those are now ordinary prod tables, so they are covered
             by Stage 2's generic full refresh below instead. ~340 GB table,
             append-only.
    Stage 2  every other table: full refresh. Prod exports all tables from
             sqlite_master EXCEPT weather_observation into a transfer DB
             (CTAS via read-only attach) together with their original DDL;
             locally each table is dropped and rebuilt from that DDL inside
             one transaction, then indexes are recreated. ~15M rows / 1-3 GB
             total - cheap, and immune to schema drift: new prod tables
             (including weather_location and weather_source) appear on the
             replica automatically.

    CAVEATS
      - A prod VACUUM renumbers rowids and invalidates the weather watermark
        -> full re-seed with the old sync-db-from-prod.ps1 required.
      - Replica-purity: this script is the ONLY writer to the replica.
      - Since 2026-07-26 the scheduled task runs -TablesOnly and the replica
        has NO weather_observation table: it was 325 GB of the 333 GB replica
        (868M rows + 3 indexes bigger than the table) and nothing on the
        workstation reads it - the dashboard now talks to prod's API, and
        energy-forecast never referenced it. The replica is ~4.6 GB.
        Dropping -TablesOnly will NOT work as-is: Stage 1 needs a rowid
        watermark and throws on the missing table. Re-seed with
        sync-db-from-prod.ps1 first, and check free disk - weather grew the
        replica by roughly 10 GB/day.

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
            $deltaBytes = [int64]((ssh $Target "stat -c %s $RemoteDelta").Trim())
            if ($LASTEXITCODE -ne 0) { throw "Remote stat failed (exit $LASTEXITCODE)" }

            # local free-space pre-flight for the delta (mirrors sync-db-incremental.ps1)
            $localDriveName = (Get-Item (Split-Path $LocalDb -Parent)).PSDrive.Name
            $freeBytes = (Get-PSDrive -Name $localDriveName).Free
            if ($freeBytes -lt ($deltaBytes * 1.10)) {
                throw "Not enough free space on ${localDriveName}: for delta ($([math]::Round($deltaBytes/1GB,1)) GB) - have $([math]::Round($freeBytes/1GB,1)) GB"
            }

            Write-Host "[$(& $Stamp)] Transferring weather delta ($deltaRows rows, $([math]::Round($deltaBytes/1GB,2)) GB) ..."
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
        ssh $Target "set -e; mkdir -p $RemoteTmpDir; rm -f $RemoteRefresh $RemoteRefresh-wal $RemoteRefresh-shm" | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Remote prep failed" }
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
    # BatchMode + timeouts: a prompt or a stalled connection here used to hang
    # the whole run forever. The task is MultipleInstances=IgnoreNew, so a hung
    # instance silently cancels every later run.
    $sshOpts = @('-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ServerAliveInterval=5','-o','ServerAliveCountMax=3')
    ssh @sshOpts $Target "rm -f $RemoteDelta $RemoteDelta-wal $RemoteDelta-shm $RemoteRefresh $RemoteRefresh-wal $RemoteRefresh-shm" 2>$null | Out-Null
    foreach ($f in @($LocalDelta, "$LocalDelta-wal", "$LocalDelta-shm", $LocalRefresh, "$LocalRefresh-wal", "$LocalRefresh-shm")) {
        if (Test-Path $f) { Remove-Item $f -Force }
    }
    Stop-Transcript | Out-Null
}
