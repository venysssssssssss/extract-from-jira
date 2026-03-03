"""Filesystem storage gateway for raw and normalized outputs."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
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

    @staticmethod
    def _period_token(from_date: date, to_date: date) -> str:
        return f"{from_date.isoformat()}__{to_date.isoformat()}"

    def persist_raw(
        self,
        base: BaseName,
        from_date: date,
        to_date: date,
        issues: list[dict[str, Any]],
    ) -> Path | None:
        if not issues:
            return None
        raw_dir = self._root / "raw" / base.value
        raw_dir.mkdir(parents=True, exist_ok=True)
        path = raw_dir / f"{self._period_token(from_date, to_date)}.jsonl"
        with path.open("w", encoding="utf-8") as handler:
            for issue in issues:
                handler.write(json.dumps(issue, ensure_ascii=False) + "\n")
        return path

    def persist_processed(
        self,
        base: BaseName,
        from_date: date,
        to_date: date,
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
        period_token = self._period_token(from_date, to_date)

        if "csv" in formats:
            csv_path = processed_dir / f"{period_token}.csv"
            frame.to_csv(csv_path, index=False)
            outputs["csv"] = csv_path

        if "parquet" in formats:
            parquet_path = processed_dir / f"{period_token}.parquet"
            frame.to_parquet(parquet_path, index=False)
            outputs["parquet"] = parquet_path

        period_ref_path = processed_dir / f"periodo_{period_token}.json"
        with period_ref_path.open("w", encoding="utf-8") as handler:
            json.dump(
                {
                    "base": base.value,
                    "from_date": from_date.isoformat(),
                    "to_date": to_date.isoformat(),
                    "generated_at": datetime.now(UTC).isoformat(),
                    "record_count": int(len(frame.index)),
                    "formats": list(formats),
                },
                handler,
                ensure_ascii=False,
                indent=2,
            )
        outputs["period_ref"] = period_ref_path

        return outputs
