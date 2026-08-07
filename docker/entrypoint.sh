#!/bin/bash
set -e

# Pass environment variables to cron (cron runs in a clean env)
printenv | grep -E '^(api_key_entsoe|api_key_openmeteo|ENERGY_DB_PATH|ENERGY_LOGS_DIR|PATH)=' > /etc/environment

echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting energy data gathering scheduler" >> /app/logs/cron_update.log
echo "Schedule: full 00:30, 06:30, 13:30, 18:30 UTC; price-only 11:15, 12:15 UTC" >> /app/logs/cron_update.log

# Start cron in the foreground
exec cron -f
