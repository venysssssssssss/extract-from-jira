"""Filesystem storage gateway for raw and normalized outputs."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from extractor.domain import BaseName
from extractor.interfaces import StorageGateway
from extractor.validators import REQUIRED_COLUMNS


class FileStorage(StorageGateway):
    """Persists extraction artifacts into deterministic folder structure."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def persist_raw(
        self, base: BaseName, run_date: date, issues: list[dict[str, Any]]
    ) -> Path | None:
        if not issues:
            return None
        raw_dir = self._root / "raw" / base.value
        raw_dir.mkdir(parents=True, exist_ok=True)
        path = raw_dir / f"{run_date.isoformat()}.jsonl"
        with path.open("w", encoding="utf-8") as handler:
            for issue in issues:
                handler.write(json.dumps(issue, ensure_ascii=False) + "\n")
        return path

    def persist_processed(
        self,
        base: BaseName,
        run_date: date,
        records: list[dict[str, Any]],
        formats: tuple[str, ...],
    ) -> dict[str, Path]:
        processed_dir = self._root / "processed" / base.value
        processed_dir.mkdir(parents=True, exist_ok=True)

        frame = pd.DataFrame.from_records(records)
        if frame.empty:
            frame = pd.DataFrame(columns=REQUIRED_COLUMNS)
        else:
            frame = frame.drop_duplicates(subset=["issue_key", "updated"], keep="last")
            frame = frame.reindex(columns=REQUIRED_COLUMNS)

        outputs: dict[str, Path] = {}
        date_token = run_date.isoformat()

        if "csv" in formats:
            csv_path = processed_dir / f"{date_token}.csv"
            frame.to_csv(csv_path, index=False)
            outputs["csv"] = csv_path

        if "parquet" in formats:
            parquet_path = processed_dir / f"{date_token}.parquet"
            frame.to_parquet(parquet_path, index=False)
            outputs["parquet"] = parquet_path

        return outputs
