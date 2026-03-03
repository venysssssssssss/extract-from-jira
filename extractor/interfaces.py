"""Dependency inversion interfaces used by orchestration services."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Protocol

from extractor.domain import BaseName, BaseExecutionResult, RecordEnvelope


class JiraGateway(Protocol):
    """Abstraction for Jira API interactions."""

    def resolve_field_ids(self, field_names: tuple[str, ...]) -> dict[str, str]:
        """Resolve custom fields by display name and return their Jira IDs."""

    def search_issues(
        self, jql: str, fields: tuple[str, ...], max_results: int
    ) -> list[dict[str, Any]]:
        """Run paginated search and return raw Jira issues."""


class FallbackGateway(Protocol):
    """Abstraction for UI export fallback flow."""

    def export_filter(
        self, base: BaseName, filter_url: str, run_date: date, output_dir: Path
    ) -> Path:
        """Export data from Jira UI and return CSV file path."""


class StorageGateway(Protocol):
    """Abstraction for data persistence."""

    def persist_raw(
        self,
        base: BaseName,
        from_date: date,
        to_date: date,
        issues: list[dict[str, Any]],
    ) -> Path | None:
        """Persist raw issue payloads and return location."""

    def persist_processed(
        self,
        base: BaseName,
        from_date: date,
        to_date: date,
        records: list[dict[str, Any]],
        formats: tuple[str, ...],
    ) -> dict[str, Path]:
        """Persist normalized records according to format list."""


class Auditor(Protocol):
    """Abstraction for writing execution audit events."""

    def write_event(self, event: dict[str, Any]) -> None:
        """Persist one structured audit event."""


class ExtractorService(Protocol):
    """Service contract consumed by CLI and FastAPI."""

    def run(
        self,
        request_base: str,
        from_date: date | None,
        to_date: date | None,
        formats: tuple[str, ...],
        mode: str,
    ) -> list[BaseExecutionResult]:
        """Run extraction and return per-base execution results."""


class Normalizer(Protocol):
    """Transforms raw Jira payloads into normalized records."""

    def normalize_api_issues(
        self,
        base: BaseName,
        issues: list[dict[str, Any]],
        field_ids: dict[str, str],
        extracted_at_iso: str,
    ) -> RecordEnvelope:
        """Normalize records sourced from API issues."""

    def normalize_fallback_csv(
        self, base: BaseName, csv_path: Path, extracted_at_iso: str
    ) -> RecordEnvelope:
        """Normalize records sourced from fallback CSV export."""
