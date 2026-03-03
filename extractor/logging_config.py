"""Centralized logging configuration and request context helpers."""

from __future__ import annotations

import json
import logging
import logging.config
from contextvars import ContextVar, Token
from pathlib import Path
from typing import Any


_REQUEST_ID: ContextVar[str] = ContextVar("request_id", default="-")
_LOGGING_CONFIGURED = False
_STANDARD_RECORD_ATTRS = set(logging.makeLogRecord({}).__dict__.keys())


class RequestContextFilter(logging.Filter):
    """Inject request-scoped identifiers into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = _REQUEST_ID.get("-")
        return True


class JsonLogFormatter(logging.Formatter):
    """Render logs as JSON lines preserving extra fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", "-"),
        }
        for key, value in record.__dict__.items():
            if key in _STANDARD_RECORD_ATTRS or key in {"message", "asctime"}:
                continue
            payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def bind_request_id(request_id: str) -> Token:
    """Bind request_id for the current context."""

    return _REQUEST_ID.set(request_id)


def reset_request_id(token: Token) -> None:
    """Restore previous request_id context value."""

    _REQUEST_ID.reset(token)


def _str_to_bool(value: str | bool | None, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def configure_logging(
    *,
    level: str = "INFO",
    json_format: bool = False,
    log_file: str | Path = "output/logs/application.log",
    max_bytes: int = 10_485_760,
    backup_count: int = 10,
) -> None:
    """Configure process-wide logging once with console + rotating file handlers."""

    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    file_path = Path(log_file)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    formatter_name = "json" if json_format else "plain"
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "request_context": {"()": "extractor.logging_config.RequestContextFilter"}
        },
        "formatters": {
            "plain": {
                "format": "%(asctime)s %(levelname)s [%(name)s] [request_id=%(request_id)s] %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "json": {"()": "extractor.logging_config.JsonLogFormatter"},
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": level,
                "filters": ["request_context"],
                "formatter": formatter_name,
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": level,
                "filters": ["request_context"],
                "formatter": formatter_name,
                "filename": str(file_path),
                "maxBytes": max_bytes,
                "backupCount": backup_count,
                "encoding": "utf-8",
            },
        },
        "root": {"level": level, "handlers": ["console", "file"]},
    }
    logging.config.dictConfig(config)
    logging.captureWarnings(True)
    _LOGGING_CONFIGURED = True


def configure_logging_from_env() -> None:
    """Configure logging using environment variables without strict app settings."""

    import os

    configure_logging(
        level=os.getenv("LOG_LEVEL", "INFO"),
        json_format=_str_to_bool(os.getenv("LOG_JSON"), default=False),
        log_file=os.getenv("LOG_FILE", "output/logs/application.log"),
        max_bytes=int(os.getenv("LOG_MAX_BYTES", "10485760")),
        backup_count=int(os.getenv("LOG_BACKUP_COUNT", "10")),
    )

