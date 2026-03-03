from datetime import UTC, datetime

import api.main as api_main
from api.schemas import RunExtractionRequest
from extractor.domain import BaseExecutionResult, BaseName, SourceMode


class FakeService:
    def run(self, request_base, from_date, to_date, formats, mode):
        return [
            BaseExecutionResult(
                base=BaseName.ENCERRADAS,
                source_mode=SourceMode.API,
                total_records=10,
                from_date=datetime(2026, 2, 2, 0, 0, tzinfo=UTC).date(),
                to_date=datetime(2026, 3, 2, 0, 0, tzinfo=UTC).date(),
                raw_path="output/raw/encerradas/2026-03-01.jsonl",
                csv_path="output/processed/encerradas/2026-03-01.csv",
                parquet_path="output/processed/encerradas/2026-03-01.parquet",
                started_at=datetime(2026, 3, 3, 12, 0, tzinfo=UTC),
                finished_at=datetime(2026, 3, 3, 12, 1, tzinfo=UTC),
            )
        ]


def test_run_extraction_endpoint(monkeypatch):
    monkeypatch.setattr(api_main, "get_service", lambda: FakeService())

    payload = api_main.run_extraction(RunExtractionRequest(base="encerradas"))

    assert payload.results[0].base == "encerradas"
    assert payload.results[0].source_mode == "api"
    assert payload.results[0].from_date.isoformat() == "2026-02-02"
    assert payload.results[0].to_date.isoformat() == "2026-03-02"
