#!/usr/bin/env bash
set -euo pipefail

timezone="${TZ:-America/Sao_Paulo}"
if [ -f "/usr/share/zoneinfo/${timezone}" ]; then
  ln -snf "/usr/share/zoneinfo/${timezone}" /etc/localtime
  echo "${timezone}" > /etc/timezone
fi

echo "api timezone=${timezone}"

exec uvicorn api.main:app --host 0.0.0.0 --port 8000
