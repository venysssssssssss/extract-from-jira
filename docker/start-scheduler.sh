#!/usr/bin/env bash
set -euo pipefail

timezone="${TZ:-America/Sao_Paulo}"
if [ -f "/usr/share/zoneinfo/${timezone}" ]; then
  ln -snf "/usr/share/zoneinfo/${timezone}" /etc/localtime
  echo "${timezone}" > /etc/timezone
fi

mkdir -p /app/output/logs
touch /app/output/logs/scheduler.log

echo "scheduler timezone=${timezone}"
echo "scheduler cron=0 8,11,14,17 * * *"

exec cron -f
