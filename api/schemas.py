"""FastAPI request/response schemas."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field


class RunExtractionRequest(BaseModel):
    base: str = Field(
        default="all", pattern="^(all|encerradas|analisadas|ingressadas)$"
    )
    from_date: date | None = None
    to_date: date | None = None
    mode: str = Field(default="api-first", pattern="^(api-first)$")
    formats: list[str] = Field(default_factory=lambda: ["csv", "parquet"])


class BaseRunResponse(BaseModel):
    base: str
    source_mode: str
    total_records: int
    from_date: date
    to_date: date
    raw_path: str | None
    csv_path: str | None
    parquet_path: str | None
    started_at: str
    finished_at: str


class RunExtractionResponse(BaseModel):
    results: list[BaseRunResponse]
