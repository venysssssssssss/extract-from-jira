"""Application service coordinating API-first extraction with fallback."""

from __future__ import annotations

from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from extractor.business_rules import REQUIRED_FIELD_NAMES, RULES
from extractor.domain import BaseExecutionResult, BaseName, ExtractionWindow
from extractor.exceptions import (
    ApiAuthError,
    ApiSchemaError,
    ApiTransientError,
    FallbackExecutionError,
)
from extractor.interfaces import (
    Auditor,
    FallbackGateway,
    JiraGateway,
    Normalizer,
    StorageGateway,
)
from extractor.jql_builder import build_jql
from extractor.normalizer import utc_now_iso
from extractor.validators import validate_records


class ExtractionService:
    """Orchestrates end-to-end extraction from API to persisted artifacts."""

    def __init__(
        self,
        *,
        jira_gateway: JiraGateway,
        fallback_gateway: FallbackGateway,
        normalizer: Normalizer,
        storage: StorageGateway,
        auditor: Auditor,
        output_dir: Path,
        max_results: int,
        default_window_factory,
    ) -> None:
        self._jira = jira_gateway
        self._fallback = fallback_gateway
        self._normalizer = normalizer
        self._storage = storage
        self._auditor = auditor
        self._output_dir = output_dir
        self._max_results = max_results
        self._default_window_factory = default_window_factory

    def run(
        self,
        request_base: str,
        from_date: date | None,
        to_date: date | None,
        formats: tuple[str, ...],
        mode: str,
    ) -> list[BaseExecutionResult]:
        run_id = str(uuid4())
        window = self._resolve_window(from_date, to_date)
        bases = self._resolve_bases(request_base)

        field_ids = self._jira.resolve_field_ids(REQUIRED_FIELD_NAMES)

        results: list[BaseExecutionResult] = []
        for base in bases:
            started_at = datetime.now(UTC)
            try:
                result = self._run_base_via_api(
                    run_id=run_id,
                    base=base,
                    from_date=window.from_date,
                    to_date=window.to_date,
                    field_ids=field_ids,
                    formats=formats,
                    started_at=started_at,
                )
            except (ApiAuthError, ApiTransientError, ApiSchemaError) as exc:
                if mode != "api-first":
                    raise
                result = self._run_base_via_fallback(
                    run_id=run_id,
                    base=base,
                    from_date=window.from_date,
                    to_date=window.to_date,
                    formats=formats,
                    started_at=started_at,
                    fallback_reason=str(exc),
                )
            results.append(result)
        return results

    def _run_base_via_api(
        self,
        *,
        run_id: str,
        base: BaseName,
        from_date: date,
        to_date: date,
        field_ids: dict[str, str],
        formats: tuple[str, ...],
        started_at: datetime,
    ) -> BaseExecutionResult:
        rule = RULES[base]
        date_field_id = field_ids[rule.date_field_name]
        jql = build_jql(rule, date_field_id, self._resolve_window(from_date, to_date))

        fields = (
            "summary",
            "status",
            "created",
            "updated",
            date_field_id,
            field_ids["Espaço"],
            field_ids["Tipo do ticket"],
        )

        issues = self._jira.search_issues(
            jql=jql, fields=fields, max_results=self._max_results
        )

        envelope = self._normalizer.normalize_api_issues(
            base=base,
            issues=issues,
            field_ids=field_ids,
            extracted_at_iso=utc_now_iso(),
        )
        validate_records(envelope.records, from_date=from_date, to_date=to_date)

        raw_path = self._storage.persist_raw(base, to_date, envelope.raw_issues)
        processed = self._storage.persist_processed(
            base, to_date, envelope.records, formats
        )
        finished_at = datetime.now(UTC)

        self._auditor.write_event(
            {
                "run_id": run_id,
                "base": base.value,
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "query_hash": sha256(jql.encode("utf-8")).hexdigest(),
                "total_records": len(envelope.records),
                "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
                "source_mode": envelope.source_mode.value,
                "status": "success",
            }
        )

        return BaseExecutionResult(
            base=base,
            source_mode=envelope.source_mode,
            total_records=len(envelope.records),
            raw_path=str(raw_path) if raw_path else None,
            csv_path=str(processed.get("csv")) if processed.get("csv") else None,
            parquet_path=(
                str(processed.get("parquet")) if processed.get("parquet") else None
            ),
            started_at=started_at,
            finished_at=finished_at,
        )

    def _run_base_via_fallback(
        self,
        *,
        run_id: str,
        base: BaseName,
        from_date: date,
        to_date: date,
        formats: tuple[str, ...],
        started_at: datetime,
        fallback_reason: str,
    ) -> BaseExecutionResult:
        rule = RULES[base]
        csv_path = self._fallback.export_filter(
            base=base,
            filter_url=rule.filter_url,
            run_date=to_date,
            output_dir=self._output_dir,
        )

        envelope = self._normalizer.normalize_fallback_csv(
            base=base,
            csv_path=csv_path,
            extracted_at_iso=utc_now_iso(),
        )
        validate_records(envelope.records, from_date=from_date, to_date=to_date)

        processed = self._storage.persist_processed(
            base, to_date, envelope.records, formats
        )
        finished_at = datetime.now(UTC)

        self._auditor.write_event(
            {
                "run_id": run_id,
                "base": base.value,
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "query_hash": None,
                "total_records": len(envelope.records),
                "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
                "source_mode": envelope.source_mode.value,
                "status": "success",
                "fallback_reason": fallback_reason,
                "fallback_file": str(csv_path),
            }
        )

        return BaseExecutionResult(
            base=base,
            source_mode=envelope.source_mode,
            total_records=len(envelope.records),
            raw_path=str(csv_path),
            csv_path=str(processed.get("csv")) if processed.get("csv") else None,
            parquet_path=(
                str(processed.get("parquet")) if processed.get("parquet") else None
            ),
            started_at=started_at,
            finished_at=finished_at,
        )

    def _resolve_bases(self, request_base: str) -> list[BaseName]:
        if request_base == "all":
            return [BaseName.ENCERRADAS, BaseName.ANALISADAS, BaseName.INGRESSADAS]
        try:
            return [BaseName(request_base)]
        except ValueError as exc:
            raise ValueError(f"Unsupported base '{request_base}'") from exc

    def _resolve_window(self, from_date: date | None, to_date: date | None):
        if from_date and to_date:
            return ExtractionWindow(from_date=from_date, to_date=to_date)
        if from_date or to_date:
            raise ValueError("Both --from and --to must be provided together")
        return self._default_window_factory()
