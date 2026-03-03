"""Domain models used across extraction modules.

This module is intentionally framework-agnostic and only stores data contracts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any


class BaseName(str, Enum):
    ENCERRADAS = "encerradas"
    ANALISADAS = "analisadas"
    INGRESSADAS = "ingressadas"


class SourceMode(str, Enum):
    API = "api"
    PLAYWRIGHT_FALLBACK = "playwright_fallback"


@dataclass(frozen=True)
class ExtractionWindow:
    from_date: date
    to_date: date


@dataclass(frozen=True)
class ExtractionRequest:
    base: str
    window: ExtractionWindow
    formats: tuple[str, ...] = ("csv", "parquet")
    mode: str = "api-first"


@dataclass
class RecordEnvelope:
    base: BaseName
    source_mode: SourceMode
    records: list[dict[str, Any]]
    raw_issues: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class BaseExecutionResult:
    base: BaseName
    source_mode: SourceMode
    total_records: int
    from_date: date
    to_date: date
    raw_path: str | None
    csv_path: str | None
    parquet_path: str | None
    started_at: datetime
    finished_at: datetime


@dataclass
class ExtractionRunResult:
    run_id: str
    request: ExtractionRequest
    base_results: list[BaseExecutionResult]
