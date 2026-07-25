# Session handoff — 2026-07-24/25

Acceptance environment, DB sync, and net-position forecasting. Everything below is
committed and pushed across the three repos.

## What was delivered

### 1. DB replica sync — fixed and automated
**Problem:** the workstation replica looked stale. Root cause: in late May the daily
`able-db-sync` task was switched to `sync-db-incremental.ps1`, which by design syncs
**only** `weather_observation`. Every other table froze at 2026-05-20/21. The task
never failed — it was just narrow.

**Fix:** `energy-data-gathering/scripts/workstation/sync-db-v2.ps1`
- Stage 1 — `weather_observation`: rowid-watermark incremental (unchanged mechanism).
- Stage 2 — every other table: full refresh, enumerated from `sqlite_master`, so new
  tables (`crossborder_flows`, `net_position`, future ones) are picked up
  automatically. Transactional drop-and-replace with per-table rowcount verification.
- `able-db-sync` (07:00 daily) repointed at it. Log: `C:\Code\able\logs\sync-db-v2.log`.
- Validated: energy tables went May-20 → current; 1.45M weather rows merged.

### 2. Acceptance environment — running
- **Frontend (Docker):** `energy-dashboard-frontend/docker`, port 3001, healthy,
  `restart: unless-stopped`, read-only on the replica.
  Required a real fix: the container crash-looped on Windows with
  `SQLITE_IOERR_SHMOPEN` because `writeDatabase.ts` opened a WAL **write** connection
  at import and Docker Desktop's bind mount can't back WAL shared memory. Now opened
  **lazily** (commit `ad99a19`), so token-less deploys start cleanly.
- **Frontend (dev):** `npm run dev` → 5173 + 3001. Needs `server/.env` with
  `ENERGY_DB_PATH` (see `server/.env.example`). After a Node upgrade, run
  `npm rebuild better-sqlite3` (native ABI mismatch).
- **Forecast (native venv):** `energy-forecast/.venv`, torch 2.13.0+**cu126**
  (cu121 has no cp314 wheel), CUDA on RTX 2060 SUPER.
- **Replica purity:** forecast processes read the replica, write ONLY to the sidecar
  `C:\Code\able\data\forecasts_local.db` via `FORECAST_OUTPUT_DB`. The sync script is
  the replica's only writer. Never set a global `ENERGY_DB_PATH`.

### 3. Net-position forecasting — live on V010
- **Data:** `crossborder_flows` + `net_position` backfilled 2023→now (4.1M records)
  and added to prod's 4×-daily cron.
- **Serving:** Task Scheduler `able-net-position-forecast`, 08:00 daily (after the
  07:00 sync), V010 zero-shot, all countries → sidecar DB.
  Target = **D+2**, 24 hourly points 00:00–23:00 UTC.
- **V011 fine-tune: REJECTED.** Pooled MAE 1374.5 vs V010's 1230.0 (+11.7% worse),
  worse in all 4 comparable countries, 17/48 country-weeks won. Registry updated.
  Full verdict: `energy-forecast/docs/2026-07-25_v011_net_position_verdict.md`.
- **Covariates:** cross-border flows are aggregated to 3 homogeneous features
  (`total_export`/`total_import`/`net`) — per-neighbour keys are heterogeneous across
  countries and break Chronos-2 global fine-tuning.

## Three bugs found by questioning results (all fixed)

1. **Scheduled ingest was 100% broken.** `pd.Timestamp(start, tz="UTC")` raises on
   tz-aware input; the scheduled path passes tz-aware, the backfill passes naive. Every
   cron fetch failed (`0 successful, 36 failed`) while the backfill worked. Two newest
   client methods were the only ones of 22 missing the codebase's existing idiom.
   Fixed + regression test (`943d9e8`). Verified live: `3 successful, 0 failed`.
2. **The "26h publication lag" was an artifact of bug 1** — tables frozen at the
   backfill's end. True lag measured after the fix: **~1h** (ENTSO-E publishes at H+1).
   The cross-border serve-lag constant was re-derived 96h → **72h**, now bounded by
   *ingest cadence* (cron 00:30 UTC + sync 05:00 UTC + 06:00 UTC run ⇒ flows only to
   ~T−48h, worst case 71h at target hour T 23:00). See
   `energy-forecast/docs/superpowers/specs/2026-07-25-crossborder-lag-parity-design.md`.
3. **DE had no net position** — the only Core CCR zone missing. Net positions are
   published per **bidding zone**; Germany's is **DE_LU** (the Core zone covering
   DE+LU). Plain `DE` → `NoMatchingDataError`. Added `NET_POSITION_BIDDING_ZONES`,
   deliberately separate from `PRICE_BIDDING_ZONES` (prices map `IT → IT_NORD`, wrong
   for a national net position). Core is now effectively **12/12**.

Also: `tests/` was never COPYed into the image although `pytest` was already a
dependency — the container had the runner, the workstation had the tests, neither
could run them. One `COPY tests/ tests/` fixed it; suite runs in-container (6 passed).

## Open items

- **DE backfill** (2023→now via DE_LU) was launched detached at the end of the session.
  Verify: `docker exec energy-data-gathering tail -5 /app/logs/backfill_de_netpos.log`
  and expect DE to reach ~31k rows like its peers. Then the next 07:00 sync brings it
  to the workstation and DE joins the forecast set (18 → 19 countries).
- **Data anomalies, uninvestigated** (all pre-date this session's bugs):
  - GR **and** IE net position both stop **2026-03-14** — same date, so likely one
    cause (zone/mapping change?) rather than coincidence.
  - Cross-border staleness: RO stops 2026-05-31; LV and SE stop 2026-06-30.
  - GB cross-border dead since 2023-07-31, IE since 2025-08-31 (post-Brexit / SEM?).
- **Net position is producer-only** — nothing consumes or displays it. Putting it on
  the dashboard needs a design decision first: does the dashboard read the sidecar
  read-only, or do these forecasts land in the replica (relaxing replica purity)?
- **Cross-border covariates contribute little** to V010 (removing 4 days of recency
  moved pooled MAE by −0.4%). An ablation would say whether they earn their keep.
- **Docker-vs-native for prod** remains deliberately parked. The Windows WAL crash is
  real evidence for that discussion.

## Verification commands

```bash
# prod services
ssh clavain@192.168.86.36 docker ps
# ingest health (expect "N successful, 0 failed")
ssh clavain@192.168.86.36 "docker exec energy-data-gathering tail -20 /app/logs/cron_update.log"
# tests (only runnable in-container)
ssh clavain@192.168.86.36 "docker exec energy-data-gathering python -m pytest tests/ -q"
```
```powershell
schtasks /Query /TN able-db-sync /FO LIST | Select-String "Last Result"
schtasks /Query /TN able-net-position-forecast /FO LIST | Select-String "Last Result"
```
