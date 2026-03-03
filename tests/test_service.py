from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from extractor.audit import JsonlAuditor
from extractor.domain import BaseName, ExtractionWindow
from extractor.exceptions import ApiTransientError
from extractor.normalizer import JiraNormalizer
from extractor.service import ExtractionService
from extractor.storage import FileStorage


class FakeJiraGateway:
    def resolve_field_ids(self, field_names: tuple[str, ...]) -> dict[str, str]:
        return {
            "DATA FECHOU SALESFORCE": "customfield_data_fechou",
            "DATA ÚLTIMA ANÁLISE": "customfield_data_analise",
            "DATA DE ABERTURA": "customfield_data_abertura",
        }

    def search_issues(self, jql: str, fields: tuple[str, ...], max_results: int):
        raise ApiTransientError("forced transient")


class FakeJiraGatewaySuccess:
    def resolve_field_ids(self, field_names: tuple[str, ...]) -> dict[str, str]:
        return {
            "DATA FECHOU SALESFORCE": "customfield_data_fechou",
            "DATA ÚLTIMA ANÁLISE": "customfield_data_analise",
            "DATA DE ABERTURA": "customfield_data_abertura",
        }

    def search_issues(self, jql: str, fields: tuple[str, ...], max_results: int):
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
                    "DATA ÚLTIMA ANÁLISE": "2026-03-01",
                    "Espaço": "Atendimento Ouv",
                    "Tipo do ticket": "ATENDIMENTO",
                }
            ]
        )
        frame.to_csv(csv_file, index=False)
        return csv_file


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
