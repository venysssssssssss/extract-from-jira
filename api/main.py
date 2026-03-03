"""FastAPI server exposing extraction endpoints."""

from __future__ import annotations

from functools import lru_cache

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from api.schemas import BaseRunResponse, RunExtractionRequest, RunExtractionResponse
from extractor.bootstrap import build_service
from extractor.config import Settings
from extractor.exceptions import ExtractionError


@lru_cache(maxsize=1)
def get_service():
    load_dotenv()
    return build_service(Settings())


app = FastAPI(title="Jira Extractor API", version="0.1.0")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/extractions/run", response_model=RunExtractionResponse)
def run_extraction(payload: RunExtractionRequest) -> RunExtractionResponse:
    service = get_service()
    try:
        results = service.run(
            request_base=payload.base,
            from_date=payload.from_date,
            to_date=payload.to_date,
            formats=tuple(payload.formats),
            mode=payload.mode,
        )
    except (ValueError, ExtractionError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    response_items = [
        BaseRunResponse(
            base=item.base.value,
            source_mode=item.source_mode.value,
            total_records=item.total_records,
            raw_path=item.raw_path,
            csv_path=item.csv_path,
            parquet_path=item.parquet_path,
            started_at=item.started_at.isoformat(),
            finished_at=item.finished_at.isoformat(),
        )
        for item in results
    ]
    return RunExtractionResponse(results=response_items)


def run() -> None:
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    run()
