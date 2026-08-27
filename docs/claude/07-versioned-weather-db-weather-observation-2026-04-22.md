> **Archived from `CLAUDE.md` on 2026-08-27 (ABL-574, following ABL-536).**
> Historical narrative, incident forensics and dated measurements, moved out of
> the auto-loaded root file verbatim. Figures and `file:line` citations here are
> frozen as of the archive date and are not re-checked. The durable rules
> distilled from this material live in the repo-root `CLAUDE.md`; where the two
> conflict, the root file wins.

## Versioned Weather DB (weather_observation — 2026-04-22)

Since 2026-04-22 this repo owns a versioned per-NWP-model per-zone
weather table, `weather_observation`, alongside the legacy
`weather_data` table (which continues to serve dashboards). It is
populated by **one hourly Docker cron** at `XX:30 UTC` — the only
Open-Meteo fetcher in the whole stack. Every downstream consumer
(dashboards, heliocast production inference, helio research
backtests) reads from this table, not from Open-Meteo directly.

**Architecture + deploy runbook:** [`WEATHER_DB.md`](WEATHER_DB.md)
**Step-by-step extension recipes:** [`EXTENDING.md`](EXTENDING.md)

### Current consumers (all break if the schema breaks)

- **Dashboards** (`energy-dashboard-frontend` on `:3001`) — serves
  `GET /api/weather/latest` to anything on the LAN.
- **Heliocast production** — hourly `:45 UTC` Windows Task reads
  the freshest forecast from `/api/weather/latest` and submits to
  Predico-Elia.
- **Helio research** — reads the workstation replica at
  `C:\Code\able\data\energy_dashboard.db` for backtests via
  `helioforge/src/data/weather_db_loader.py`.

### Extension rules (always-on, applies to any code touching this table)

1. **Never DROP `weather_observation` or its dims.** Always back up
   first:
   ```bash
   sqlite3 /home/clavain/energy-dashboard/data/energy_dashboard.db \
     ".backup /home/clavain/energy-dashboard/data/backup_$(date +%F).db"
   ```
2. **Schema changes via `ALTER TABLE ... ADD COLUMN`, never DROP-and-
   recreate.** SQLite's online ALTER handles add-column in O(1).
   Column removal requires the copy-table dance and a maintenance
   window — see `EXTENDING.md` "What NEVER to do".
3. **Always test on a scratch copy first.** Every single change
   must go through
   ```bash
   cp data/energy_dashboard.db /tmp/scratch.db
   ENERGY_DB_PATH=/tmp/scratch.db python scripts/<...>.py
   ```
   before it lands on prod.
4. **Verify after every deploy.** Always run
   ```bash
   docker compose exec data-gathering \
     python scripts/init_weather_observation.py --verify
   ```
   plus a `curl /api/weather/latest?...` smoke to confirm the
   frontend is still serving.
5. **Wire fetchers when adding a source.** If you append to
   `weather_schema.py::OPEN_METEO_SOURCES`, also add the model to
   the matching fetcher tuple (`REALTIME_NWP_MODELS` or
   `NWP_MODELS`). A seeded-but-unwired source is a silent dead end
   that produces no data.

### Workflows (Claude Code skills)

If you're an LLM driving this repo, three skills enforce the above
rules on specific workflows. They auto-invoke on intent match:

| Skill | Triggers on |
|---|---|
| `weather-db-extend` | "add a country / source / variable to the weather DB" |
| `weather-db-query` | "query the weather DB", "how do I read weather for X" |
| `weather-db-deploy` | "deploy weather DB change to prod", "ship weather DB" |
