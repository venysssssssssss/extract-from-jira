#!/usr/bin/env bash
set -euo pipefail

cd /app
mkdir -p output/logs

timestamp() {
  date --iso-8601=seconds
}

exec 9>/tmp/jira-extractor.lock
if ! flock -n 9; then
  echo "[$(timestamp)] scheduled extraction skipped: another run is still active"
  exit 0
fi

echo "[$(timestamp)] scheduled extraction started"
python -m extractor.run --base all --mode api-first --format csv,parquet
echo "[$(timestamp)] scheduled extraction finished"
