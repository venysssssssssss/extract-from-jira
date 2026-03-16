from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from extractor.audit import JsonlAuditor
from extractor.business_rules import RULES
from extractor.domain import BaseName, ExtractionWindow
from extractor.exceptions import ApiTransientError
from extractor.normalizer import JiraNormalizer
from extractor.service import ExtractionService
from extractor.storage import FileStorage
from extractor.utils import canonicalize_column_name


class FakeJiraGateway:
    def __init__(self) -> None:
        self.last_fields: tuple[str, ...] | None = None

    def resolve_field_ids(self, field_names: tuple[str, ...]) -> dict[str, str]:
        return {
            field_name: f"customfield_{index}"
            for index, field_name in enumerate(field_names, start=1)
        }

    def search_issues(self, jql: str, fields: tuple[str, ...], max_results: int):
        self.last_fields = fields
        raise ApiTransientError("forced transient")


class FakeJiraGatewaySuccess:
    def __init__(self, missing_fields: set[str] | None = None) -> None:
        self.missing_fields = missing_fields or set()
        self.last_fields: tuple[str, ...] | None = None

    def resolve_field_ids(self, field_names: tuple[str, ...]) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for index, field_name in enumerate(field_names, start=1):
            if field_name in self.missing_fields:
                continue
            if field_name == "DATA FECHOU SALESFORCE":
                mapping[field_name] = "customfield_data_fechou"
                continue
            mapping[field_name] = f"customfield_{index}"
        return mapping

    def search_issues(self, jql: str, fields: tuple[str, ...], max_results: int):
        self.last_fields = fields
        return [
            {
                "key": "ATEN-1",
                "fields": {
                    "summary": "Linha API",
                    "status": {"name": "ENCERRADO"},
                    "created": "2026-03-02T10:00:00Z",
                    "updated": "2026-03-02T11:00:00Z",
                    "project": {"key": "ATEN", "name": "ATEN"},
                    "issuetype": {"name": "ATENDIMENTO"},
                    "customfield_data_fechou": "2026-03-02",
                    "customfield_1": "Tema API",
                },
            }
        ]


class FakeFallbackGateway:
    def export_filter(
        self, base: BaseName, filter_url: str, run_date: date, output_dir: Path
    ) -> Path:
        path = output_dir / "fallback" / base.value / run_date.isoformat()
        path.mkdir(parents=True, exist_ok=True)
        csv_file = path / "export.csv"
        frame = pd.DataFrame(
            [
                {
                    "Issue key": "TEL-99",
                    "Summary": "Linha fallback",
                    "Status": "EM ANDAMENTO",
                    "Created": "2026-03-01T10:00:00Z",
                    "Updated": "2026-03-01T11:00:00Z",
                    "DATA ULTIMA ANALISE": "2026-03-01",
                    "Espaço": "Atendimento Ouv",
                    "Tipo do ticket": "ATENDIMENTO",
                }
            ]
        )
        frame.to_csv(csv_file, index=False)
        return csv_file


class FakeDatabaseWriter:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.preflight_called = 0

    def check_connection(self) -> None:
        self.preflight_called += 1

    def upsert_records(self, *, base, from_date, to_date, records):
        self.calls.append(
            {
                "base": base.value,
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "rows": len(records),
            }
        )
        return {
            "table": f"dbo.jira_{base.value}",
            "inserted_rows": len(records),
            "period_count": len(records),
        }


def test_service_runs_fallback_when_api_fails(tmp_path: Path) -> None:
    service = ExtractionService(
        jira_gateway=FakeJiraGateway(),
        fallback_gateway=FakeFallbackGateway(),
        normalizer=JiraNormalizer(),
        storage=FileStorage(tmp_path),
        auditor=JsonlAuditor(tmp_path),
        output_dir=tmp_path,
        max_results=100,
        default_window_factory=lambda: ExtractionWindow(
            from_date=date(2026, 2, 1), to_date=date(2026, 3, 1)
        ),
    )

    results = service.run(
        request_base="analisadas",
        from_date=date(2026, 2, 1),
        to_date=date(2026, 3, 1),
        formats=("csv",),
        mode="api-first",
    )

    assert len(results) == 1
    assert results[0].source_mode.value == "playwright_fallback"
    assert results[0].total_records == 1
    assert (
        tmp_path / "processed" / "analisadas" / "2026-02-01__2026-03-01.csv"
    ).exists()


def test_service_cleans_base_output_before_api_run(tmp_path: Path) -> None:
    stale_raw = tmp_path / "raw" / "encerradas"
    stale_processed = tmp_path / "processed" / "encerradas"
    stale_fallback = tmp_path / "fallback" / "encerradas"
    for directory in (stale_raw, stale_processed, stale_fallback):
        directory.mkdir(parents=True, exist_ok=True)
    (stale_raw / "stale.jsonl").write_text("{\"x\":1}\n", encoding="utf-8")
    (stale_processed / "stale.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    (stale_fallback / "stale.csv").write_text("a,b\n1,2\n", encoding="utf-8")

    service = ExtractionService(
        jira_gateway=FakeJiraGatewaySuccess(),
        fallback_gateway=FakeFallbackGateway(),
        normalizer=JiraNormalizer(),
        storage=FileStorage(tmp_path),
        auditor=JsonlAuditor(tmp_path),
        output_dir=tmp_path,
        max_results=100,
        default_window_factory=lambda: ExtractionWindow(
            from_date=date(2026, 2, 2), to_date=date(2026, 3, 3)
        ),
        clean_output_on_api_run=True,
    )

    results = service.run(
        request_base="encerradas",
        from_date=date(2026, 2, 2),
        to_date=date(2026, 3, 3),
        formats=("csv",),
        mode="api-first",
    )

    assert len(results) == 1
    assert results[0].source_mode.value == "api"
    assert not (stale_raw / "stale.jsonl").exists()
    assert not (stale_processed / "stale.csv").exists()
    assert not (stale_fallback / "stale.csv").exists()


def test_service_writes_records_to_database(tmp_path: Path) -> None:
    db_writer = FakeDatabaseWriter()
    service = ExtractionService(
        jira_gateway=FakeJiraGatewaySuccess(),
        fallback_gateway=FakeFallbackGateway(),
        normalizer=JiraNormalizer(),
        storage=FileStorage(tmp_path),
        auditor=JsonlAuditor(tmp_path),
        output_dir=tmp_path,
        max_results=100,
        default_window_factory=lambda: ExtractionWindow(
            from_date=date(2026, 2, 2), to_date=date(2026, 3, 2)
        ),
        clean_output_on_api_run=True,
        database_writer=db_writer,
    )

    results = service.run(
        request_base="encerradas",
        from_date=date(2026, 2, 2),
        to_date=date(2026, 3, 2),
        formats=("csv",),
        mode="api-first",
    )

    assert len(results) == 1
    assert len(db_writer.calls) == 1
    assert db_writer.preflight_called == 1
    assert db_writer.calls[0]["base"] == "encerradas"
    assert db_writer.calls[0]["from_date"] == "2026-02-02"
    assert db_writer.calls[0]["to_date"] == "2026-03-02"
    assert db_writer.calls[0]["rows"] == results[0].total_records


def test_service_uses_expanded_fields_and_tolerates_missing_field_ids(
    tmp_path: Path,
) -> None:
    gateway = FakeJiraGatewaySuccess(missing_fields={"TEMA"})
    service = ExtractionService(
        jira_gateway=gateway,
        fallback_gateway=FakeFallbackGateway(),
        normalizer=JiraNormalizer(),
        storage=FileStorage(tmp_path),
        auditor=JsonlAuditor(tmp_path),
        output_dir=tmp_path,
        max_results=100,
        default_window_factory=lambda: ExtractionWindow(
            from_date=date(2026, 2, 2), to_date=date(2026, 3, 2)
        ),
        clean_output_on_api_run=False,
    )

    results = service.run(
        request_base="encerradas",
        from_date=date(2026, 2, 2),
        to_date=date(2026, 3, 2),
        formats=("csv",),
        mode="api-first",
    )

    assert len(results) == 1
    assert gateway.last_fields is not None
    assert "customfield_data_fechou" in gateway.last_fields
    assert "summary" in gateway.last_fields

    output = pd.read_csv(
        tmp_path / "processed" / "encerradas" / "2026-02-02__2026-03-02.csv"
    )
    assert canonicalize_column_name("DATA FECHOU SALESFORCE") in output.columns
    assert canonicalize_column_name("TEMA") in output.columns
    assert pd.isna(output.loc[0, canonicalize_column_name("TEMA")])
    assert output.loc[0, canonicalize_column_name("DATA FECHOU SALESFORCE")] == "2026-03-02"

    expected_columns = {
        canonicalize_column_name(field_name)
        for field_name in RULES[BaseName.ENCERRADAS].custom_fields
    }
    assert expected_columns.issubset(set(output.columns))
