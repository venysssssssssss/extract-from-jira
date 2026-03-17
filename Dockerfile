FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VERSION=2.2.1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONPATH=/app \
    TZ=America/Sao_Paulo

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    cron \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

COPY pyproject.toml README.md /app/
RUN poetry install --only main --no-root

COPY extractor /app/extractor
COPY api /app/api
COPY docker /app/docker

RUN chmod +x /app/docker/run-scheduled-extraction.sh /app/docker/start-scheduler.sh /app/docker/start-api.sh \
    && chmod 0644 /app/docker/jira-extractor.cron \
    && cp /app/docker/jira-extractor.cron /etc/cron.d/jira-extractor \
    && crontab /etc/cron.d/jira-extractor

# Optional for fallback browser automation in production images:
# RUN poetry run playwright install --with-deps chromium

EXPOSE 8000

CMD ["/app/docker/start-api.sh"]
