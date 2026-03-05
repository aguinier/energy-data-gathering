# Energy Data Gathering

European energy market data collection pipeline. Fetches electricity load, prices, renewable generation, and weather data from ENTSO-E and Open-Meteo APIs into a shared SQLite database.

Migrated from the [energy-dashboard](https://github.com/aguinier/energy-dashboard) monorepo.

## Quick Start

```bash
pip install -r requirements.txt
echo "api_key_entsoe=YOUR_KEY" > .env

# Backfill historical data
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31 --types all --countries all

# Daily updates
python scripts/update.py
```

## Docker

```bash
cd docker
cp .env.example ../.env   # Edit with your ENTSO-E API key
docker compose up -d --build
```

The container runs cron jobs at 00:30, 06:30, 13:30, 18:30 UTC for energy data and 15:00 UTC for weather.

## Documentation

- [CLAUDE.md](CLAUDE.md) - Detailed module documentation
- [PIPELINE.md](PIPELINE.md) - Data pipeline architecture
- [database_structure.md](database_structure.md) - Database schema reference
