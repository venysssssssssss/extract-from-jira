"""Application service coordinating API-first extraction with fallback."""

from __future__ import annotations

import logging
import shutil
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from extractor.business_rules import RULES, all_required_field_names
from extractor.domain import BaseExecutionResult, BaseName, ExtractionWindow
from extractor.exceptions import (
    ApiAuthError,
    ApiSchemaError,
    ApiTransientError,
    DatabaseWriteError,
    FallbackExecutionError,
)
from extractor.interfaces import (
    Auditor,
    DatabaseWriter,
    FallbackGateway,
    JiraGateway,
    Normalizer,
    StorageGateway,
)
from extractor.jql_builder import build_jql
from extractor.normalizer import utc_now_iso
from extractor.validators import validate_records

LOGGER = logging.getLogger(__name__)
CORE_API_FIELDS = ("summary", "status", "created", "updated", "project", "issuetype")


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
        clean_output_on_api_run: bool = True,
        database_writer: DatabaseWriter | None = None,
    ) -> None:
        self._jira = jira_gateway
        self._fallback = fallback_gateway
        self._normalizer = normalizer
        self._storage = storage
        self._auditor = auditor
        self._output_dir = output_dir
        self._max_results = max_results
        self._default_window_factory = default_window_factory
        self._clean_output_on_api_run = clean_output_on_api_run
        self._database_writer = database_writer

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
        LOGGER.info(
            "extraction_run_started run_id=%s base=%s mode=%s from=%s to=%s formats=%s",
            run_id,
            request_base,
            mode,
            window.from_date,
            window.to_date,
            formats,
        )
        if self._database_writer is not None:
            self._database_writer.check_connection()
            LOGGER.info("db_connection_check_ok run_id=%s", run_id)

        if mode == "api-first" and self._clean_output_on_api_run:
            self._cleanup_output_for_bases(bases)

        field_ids = self._jira.resolve_field_ids(tuple(sorted(all_required_field_names())))

        results: list[BaseExecutionResult] = []
        for base in bases:
            started_at = datetime.now(UTC)
            LOGGER.info("base_extraction_started run_id=%s base=%s", run_id, base.value)
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
                LOGGER.warning(
                    "api_extraction_failed_switching_to_fallback run_id=%s base=%s reason=%s",
                    run_id,
                    base.value,
                    exc,
                )
                try:
                    result = self._run_base_via_fallback(
                        run_id=run_id,
                        base=base,
                        from_date=window.from_date,
                        to_date=window.to_date,
                        formats=formats,
                        started_at=started_at,
                        fallback_reason=str(exc),
                    )
                except FallbackExecutionError:
                    LOGGER.exception(
                        "fallback_extraction_failed run_id=%s base=%s", run_id, base.value
                    )
                    raise
            results.append(result)
            LOGGER.info(
                "base_extraction_finished run_id=%s base=%s source=%s total=%s",
                run_id,
                result.base.value,
                result.source_mode.value,
                result.total_records,
            )
        LOGGER.info("extraction_run_finished run_id=%s base_count=%s", run_id, len(results))
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
        jql = build_jql(rule, self._resolve_window(from_date, to_date))
        fields = self._build_api_fields(base, field_ids)

        issues = self._jira.search_issues(
            jql=jql, fields=fields, max_results=self._max_results
        )

        envelope = self._normalizer.normalize_api_issues(
            base=base,
            issues=issues,
            field_ids=field_ids,
            extracted_at_iso=utc_now_iso(),
        )
        validate_records(
            envelope.records, base=base, from_date=from_date, to_date=to_date
        )

        raw_path = self._storage.persist_raw(
            base=base, from_date=from_date, to_date=to_date, issues=envelope.raw_issues
        )
        processed = self._storage.persist_processed(
            base=base,
            from_date=from_date,
            to_date=to_date,
            records=envelope.records,
            formats=formats,
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
        LOGGER.info(
            "api_persistence_finished run_id=%s base=%s raw=%s csv=%s parquet=%s",
            run_id,
            base.value,
            raw_path,
            processed.get("csv"),
            processed.get("parquet"),
        )
        self._write_to_database(
            run_id=run_id,
            base=base,
            from_date=from_date,
            to_date=to_date,
            records=envelope.records,
        )

        return BaseExecutionResult(
            base=base,
            source_mode=envelope.source_mode,
            total_records=len(envelope.records),
            from_date=from_date,
            to_date=to_date,
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
        validate_records(
            envelope.records, base=base, from_date=from_date, to_date=to_date
        )

        processed = self._storage.persist_processed(
            base=base,
            from_date=from_date,
            to_date=to_date,
            records=envelope.records,
            formats=formats,
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
        LOGGER.info(
            "fallback_persistence_finished run_id=%s base=%s fallback_file=%s csv=%s parquet=%s",
            run_id,
            base.value,
            csv_path,
            processed.get("csv"),
            processed.get("parquet"),
        )
        self._write_to_database(
            run_id=run_id,
            base=base,
            from_date=from_date,
            to_date=to_date,
            records=envelope.records,
        )

        return BaseExecutionResult(
            base=base,
            source_mode=envelope.source_mode,
            total_records=len(envelope.records),
            from_date=from_date,
            to_date=to_date,
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

    def _build_api_fields(
        self, base: BaseName, field_ids: dict[str, str]
    ) -> tuple[str, ...]:
        rule = RULES[base]
        resolved_custom_fields: list[str] = []
        missing_fields: list[str] = []

        for field_name in rule.custom_fields:
            field_id = field_ids.get(field_name)
            if field_id is None:
                missing_fields.append(field_name)
                continue
            resolved_custom_fields.append(field_id)

        if missing_fields:
            LOGGER.warning(
                "base_custom_fields_missing_ids base=%s fields=%s",
                base.value,
                ",".join(sorted(missing_fields)),
            )

        return tuple(dict.fromkeys((*CORE_API_FIELDS, *resolved_custom_fields)))

    def _cleanup_output_for_bases(self, bases: list[BaseName]) -> None:
        """Remove base-specific output folders before API extraction run."""

        for base in bases:
            for layer in ("raw", "processed", "fallback"):
                target = self._output_dir / layer / base.value
                if target.exists():
                    shutil.rmtree(target)
                    LOGGER.info(
                        "output_cleaned base=%s layer=%s path=%s",
                        base.value,
                        layer,
                        target,
                    )

    def _write_to_database(
        self,
        *,
        run_id: str,
        base: BaseName,
        from_date: date,
        to_date: date,
        records: list[dict],
    ) -> None:
        if self._database_writer is None:
            LOGGER.info("db_write_skipped_no_writer run_id=%s base=%s", run_id, base.value)
            return
        try:
            stats = self._database_writer.upsert_records(
                base=base,
                from_date=from_date,
                to_date=to_date,
                records=records,
            )
        except DatabaseWriteError:
            LOGGER.exception("db_write_failed run_id=%s base=%s", run_id, base.value)
            raise
        LOGGER.info(
            "db_write_finished run_id=%s base=%s table=%s rows=%s verified=%s",
            run_id,
            base.value,
            stats.get("table"),
            stats.get("inserted_rows"),
            stats.get("period_count"),
        )
