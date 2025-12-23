#!/bin/bash
# ETL Cron Job Script

set -e

cd /app

echo "[$(date)] Starting ETL run..."

python -m scripts.run_etl --once

echo "[$(date)] ETL run completed"
