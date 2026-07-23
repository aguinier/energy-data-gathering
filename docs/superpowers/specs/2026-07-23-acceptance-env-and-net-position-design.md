# Acceptance Environment & Net-Position Forecast — Design

**Date:** 2026-07-23
**Status:** Approved (design review with Guillaume, 2026-07-23)
**Scope:** three user goals — (1) fix the stale acceptance DB, (2) stand up a
prod-like acceptance environment on the workstation, (3) get the net-position
forecast running.

---

## Context & findings

The able system runs on two machines (see `C:\Code\able\WORKFLOWS.md`):
prod = QuietlyConfident (Debian, 192.168.86.36, Docker, no GPU, 6 cores / 7 GB
RAM), workstation = Windows 11 (**has an RTX 2060 SUPER 8 GB** — WORKFLOWS.md
claims the opposite and must be corrected).

Diagnosis of the three goals:

1. **Acceptance DB staleness — root cause.** The daily `able-db-sync` task
   (07:00, Task Scheduler) never broke — it runs daily with exit 0. But in
   late May it was switched from the full-snapshot script to
   `sync-db-incremental.ps1`, which by design syncs **only**
   `weather_observation` (+ `weather_location`, `weather_source`). Every other
   table froze at 2026-05-20/21. Prod is current. Secondary finding: the
   scheduled task redirects no output, so runs are unlogged.
2. **Acceptance environment.** Nothing runs today; frontend/forecast are
   started manually. Prod runs three Docker containers with cron inside.
3. **Net-position forecast.** 12 energy-forecast commits (Chronos-2 pipeline,
   net_position loaders/covariates, V010/V011 configs) exist **only on the
   workstation, unpushed**, plus an uncommitted `experiments/registry.json`
   edit. The required input data (`crossborder_flows`, `net_position` tables)
   exists **nowhere** — collection code shipped to prod May 18 but was never
   added to the cron's data types and never backfilled. Training/inference are
   fully separated in the code: V010 is zero-shot (no training ever), V011
   fine-tunes **once** (5000 steps, GPU needed) then does CPU-viable inference
   from the checkpoint. No scheduled retraining exists anywhere.

## Decisions (made in design review)

| Question | Decision |
|---|---|
| Acceptance data source | **Replica of prod** — never ingests ENTSO-E itself |
| Services on acceptance | **Frontend (Docker) + forecast (native venv, GPU)** |
| Net-position serving | **Acceptance-only for now**; prod forecast container untouched |
| Cross-border ingestion | **Prod collects** (cron + one-time backfill); replica syncs down |
| Runtime shape | **Approach B** — frontend in Docker, forecast native; prod's Docker-vs-systemd question explicitly deferred |

---

## §1 Sync v2 — full-coverage daily replica

Successor to `sync-db-incremental.ps1`, same SSH/scp transport, two parts:

- **`weather_observation`**: unchanged rowid-watermark incremental merge
  (INSERT OR IGNORE preserving prod rowids).
- **All other tables**: prod-side export of **every table in `sqlite_master`
  except `weather_observation`** into a transfer DB (CTAS via read-only
  attach, same pattern as the existing delta export), scp down, local
  **drop-and-replace inside a single transaction** (schema + indexes copied
  from prod's DDL). At a few hundred MB total vs the 300+ GB weather table,
  daily full refresh is cheap and immune to schema drift.
- **Auto-inclusion**: because the export enumerates `sqlite_master`, new prod
  tables (`crossborder_flows`, `net_position`, future ones) appear on the
  replica automatically — no sync changes needed ever again for new tables.
- **Sizing caveat**: `weather_data` (legacy) is measured during
  implementation; if large (> ~2 GB) it moves to the rowid-incremental path
  instead of full refresh.
- **Observability**: the script appends to a dated log file itself (no
  reliance on task-level redirection) and ends with a per-table freshness
  report (local vs prod max timestamps). Failure exits non-zero so Task
  Scheduler records it.
- **Versioning**: sync v2 lives in this repo
  (`energy-data-gathering/scripts/workstation/`), not loose in
  `C:\Code\able\scripts\` — the current scripts are unversioned single copies.
  The Task Scheduler action points into the repo clone.
- **Known limits (documented in script header)**: a prod VACUUM invalidates
  the weather rowid watermark (requires full re-seed via the old snapshot
  script); brief writer-lock contention with the frontend container at 07:00
  is acceptable (busy_timeout, WAL-aware).

**Acceptance test:** run manually; per-table freshness report shows every
table within 24 h of prod; frontend `/api/data-freshness` shows current dates.

## §2 Acceptance services (Approach B)

- **Frontend**: existing image via `docker compose` (Docker Desktop,
  configured to start at login), `DB_DIR=C:\Code\able\data`, port 3001,
  `restart: unless-stopped`. `HELIO_WRITE_TOKEN` unset → the sole write
  endpoint returns 503, so the container is effectively read-only on the
  replica. The Vite dev server stays available on-demand for UI iteration.
- **Forecast**: native venv in `energy-forecast/` with CUDA torch (RTX 2060
  SUPER). Interactive training/experiments. `ENERGY_DB_PATH` → replica.
- **Replica-purity rule**: local forecast processes open the replica
  **read-only**; all local writes (forecast rows, run metadata) go to a
  sidecar DB `C:\Code\able\data\forecasts_local.db` (same schema as the
  forecasts family). Requires a small energy-forecast change (output-DB
  override). Rationale: §1's drop-and-replace would silently wipe local rows
  written into the replica.
- **Schedulers** (Windows Task Scheduler — already in use here):
  - `able-db-sync`: existing 07:00 task, repointed at sync v2 (in-repo path).
  - `able-net-position-forecast`: new, 08:00 daily (after sync), runs the
    venv's `forecast_chronos2.py` for `net_position` → sidecar DB, own log.
    Daily, not prod's 4×/day: the replica refreshes daily, so extra runs
    would re-forecast identical inputs.

**Acceptance test:** reboot workstation → frontend reachable on :3001 without
manual action; scheduled forecast task produces rows in the sidecar DB.

## §3 Cross-border collection on prod

- **Code change** (this repo): add `crossborder_flows` + `net_position` to
  `scripts/update.py`'s default data-types list (the tables themselves are
  created by the backfill, which runs first). Deploy: push → prod `git pull`
  → rebuild **data-gathering container only**.
- **One-time backfill on prod**: `backfill_crossborder.py --types all`
  covers **both** `crossborder_flows` and `net_position`, month-chunked from
  **2023-01** (its default — matching V010/V011's training window), supports
  `--resume`, and creates both tables itself. Run under tmux; ~1.3k ENTSO-E
  calls, well within rate limits. Verify per-country row counts afterwards.
- The next 07:00 sync delivers the new tables to acceptance automatically
  (§1 auto-inclusion).

**Acceptance test:** prod tables populated 2023-01-01 → today for all
countries with borders; replica has the same tables the following morning.

## §4 Net-position bring-up on acceptance

Ordered; each step gates the next.

1. **End the single-copy risk**: commit the `registry.json` edit, push the 12
   energy-forecast commits to GitHub. (Prod pulls nothing.)
2. **V010 zero-shot** once cross-border data has synced down: pure inference —
   validates the whole path (covariates, InputBuilder, sidecar writes) with
   zero GPU risk. Success = net-position forecasts for all countries with
   data, written to the sidecar DB.
3. **V011 fine-tune**: one-off 5000-step GPU run → checkpoint under
   `models/chronos2/finetuned/`.
4. **Evaluate** V011 vs V010 vs persistence baseline
   (`compare_experiments.py`). Only if V011 wins does the scheduled task
   switch to its checkpoint. (Run the promotion-gate checklist before
   claiming a winner.)
5. **Docs**: update `WORKFLOWS.md` — sync v2 scope, acceptance environment,
   corrected GPU/hardware claims.

## Out of scope (explicit)

- Net-position display in the dashboard UI.
- Promotion of net-position serving to prod (deliberately deferred; prod's
  CPU-only box can run checkpoint inference later, so nothing is lost).
- Prod's Docker-vs-systemd runtime question (parked; acceptance's native
  forecast run will generate evidence for it).
- `gas_prices` staleness (frozen 2026-02-20 **on prod too**) — pre-existing,
  unrelated collection issue; flagged, not fixed here.

## Error handling summary

- Sync v2: idempotent and re-runnable; transactional table swap (a failed run
  leaves yesterday's tables, never a half-replaced one); non-zero exit on
  failure; own log.
- Backfill: chunked/resumable; verified by per-country counts before §4
  proceeds.
- Scheduled forecast: skips gracefully when data is missing (existing
  `[SKIP] … not trained` / no-data paths); logs to file; failures visible in
  Task Scheduler history.
