#!/bin/sh
# Run an ENTSO-E update pass and make its exit code visible (ABL-61).
#
# cron's `>> log 2>&1` throws the exit code away, which is why teaching
# scripts/update.py to exit non-zero is not on its own enough: nothing reads it.
# This wrapper is what the crontab calls, and it puts a failed pass in the three
# places someone actually looks:
#
#   1. cron_update.log       -- one INGEST-PASS-FAILED line, greppable
#   2. `docker logs`         -- via /proc/1/fd/1, which is the container's stdout.
#                               `docker logs energy-data-gathering` was completely
#                               empty across the whole four-day ABL-630 window.
#   3. the wrapper's own exit code, for a supervisor that watches one
#
# The `data_ingestion_log` pass row written by update.py is the fourth, and the
# only one that survives log rotation.
#
# Exit codes are update.py's: 1 crash, 2 stored nothing, 3 volume collapse.

set -u

APP_DIR="${APP_DIR:-/app}"
LOG_DIR="${ENERGY_LOGS_DIR:-$APP_DIR/logs}"
LOG_FILE="$LOG_DIR/cron_update.log"
PYTHON="${PYTHON:-/usr/local/bin/python3}"

mkdir -p "$LOG_DIR"

cd "$APP_DIR" || exit 1
"$PYTHON" scripts/update.py "$@" >> "$LOG_FILE" 2>&1
rc=$?

if [ "$rc" -ne 0 ]; then
    msg="$(date -u '+%Y-%m-%dT%H:%M:%SZ') INGEST-PASS-FAILED rc=$rc args='$*' - see $LOG_FILE"
    echo "$msg" >> "$LOG_FILE"
    # Best effort: absent outside a container, and not worth failing over.
    if [ -w /proc/1/fd/1 ]; then
        echo "$msg" >> /proc/1/fd/1 2>/dev/null || true
    fi
fi

exit "$rc"
