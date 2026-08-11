FROM python:3.11-slim

WORKDIR /app

# Install cron
RUN apt-get update && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY config.py utils.py ./
COPY src/ src/
COPY scripts/ scripts/
# Tests ship too: pytest is already a runtime dependency (requirements.txt) and
# the container is the only environment with the full dependency set, so this is
# where `python -m pytest tests/` can actually run against real deps.
COPY tests/ tests/

# Install crontab. Full updates at 00:30, 06:30, 13:30, 18:30 UTC, plus a
# price-only day-ahead pass at 11:15 and 12:15 UTC, plus a weekly late-
# publisher catch-up (ABL-85) at 05:00 UTC Sunday — see docker/crontab.
COPY docker/crontab /etc/cron.d/energy-data
RUN chmod 0644 /etc/cron.d/energy-data && crontab /etc/cron.d/energy-data

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Database and logs live on mounted volumes
ENV ENERGY_DB_PATH=/data/energy_dashboard.db
ENV ENERGY_LOGS_DIR=/app/logs

RUN mkdir -p /app/logs /data

ENTRYPOINT ["/entrypoint.sh"]
