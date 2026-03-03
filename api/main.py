"""FastAPI server exposing extraction endpoints."""

from __future__ import annotations

import logging
from contextlib import suppress
from functools import lru_cache
from time import perf_counter
from uuid import uuid4

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request

from api.schemas import BaseRunResponse, RunExtractionRequest, RunExtractionResponse
from extractor.bootstrap import build_service
from extractor.config import Settings
from extractor.exceptions import ExtractionError
from extractor.logging_config import (
    bind_request_id,
    configure_logging,
    configure_logging_from_env,
    reset_request_id,
)


configure_logging_from_env()
LOGGER = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    load_dotenv()
    settings = Settings()
    configure_logging(
        level=settings.log_level,
        json_format=settings.log_json,
        log_file=settings.log_file,
        max_bytes=settings.log_max_bytes,
        backup_count=settings.log_backup_count,
    )
    return settings


@lru_cache(maxsize=1)
def get_service():
    return build_service(get_settings())


app = FastAPI(title="Jira Extractor API", version="0.1.0")


@app.middleware("http")
async def log_http_requests(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid4()))
    token = bind_request_id(request_id)
    started = perf_counter()
    LOGGER.info("http_request_started method=%s path=%s", request.method, request.url.path)
    response = None
    try:
        response = await call_next(request)
    except Exception:
        LOGGER.exception(
            "http_request_failed method=%s path=%s duration_ms=%d",
            request.method,
            request.url.path,
            int((perf_counter() - started) * 1000),
        )
        raise
    else:
        with suppress(Exception):
            response.headers["X-Request-ID"] = request_id
        LOGGER.info(
            "http_request_finished method=%s path=%s status=%s duration_ms=%d",
            request.method,
            request.url.path,
            response.status_code,
            int((perf_counter() - started) * 1000),
        )
        return response
    finally:
        reset_request_id(token)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/extractions/run", response_model=RunExtractionResponse)
def run_extraction(payload: RunExtractionRequest) -> RunExtractionResponse:
    service = get_service()
    LOGGER.info(
        "api_extraction_requested base=%s mode=%s formats=%s from=%s to=%s",
        payload.base,
        payload.mode,
        payload.formats,
        payload.from_date,
        payload.to_date,
    )
    try:
        results = service.run(
            request_base=payload.base,
            from_date=payload.from_date,
            to_date=payload.to_date,
            formats=tuple(payload.formats),
            mode=payload.mode,
        )
    except (ValueError, ExtractionError) as exc:
        LOGGER.exception("api_extraction_failed reason=%s", exc)
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
    LOGGER.info("api_extraction_finished bases=%d", len(response_items))
    return RunExtractionResponse(results=response_items)


def run() -> None:
    settings = get_settings()
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        access_log=False,
        log_config=None,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    run()
