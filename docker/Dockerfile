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

# Install crontab: 00:30, 06:30, 13:30, 18:30
COPY docker/crontab /etc/cron.d/energy-data
RUN chmod 0644 /etc/cron.d/energy-data && crontab /etc/cron.d/energy-data

COPY docker/entrypoint.sh /entrypoint.sh

# Database and logs live on mounted volumes
ENV ENERGY_DB_PATH=/data/energy_dashboard.db
ENV ENERGY_LOGS_DIR=/app/logs

RUN mkdir -p /app/logs /data

ENTRYPOINT ["/entrypoint.sh"]
