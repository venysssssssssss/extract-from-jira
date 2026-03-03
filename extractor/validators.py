"""Data quality validation rules for normalized records."""

from __future__ import annotations

from datetime import date

import pandas as pd

from extractor.exceptions import ValidationError

REQUIRED_COLUMNS = (
    "issue_key",
    "summary",
    "status",
    "created",
    "updated",
    "base_origem",
    "data_referencia",
    "espaco",
    "tipo_ticket",
    "extracted_at",
    "source_mode",
)


def validate_records(
    records: list[dict[str, object]], from_date: date, to_date: date
) -> None:
    """Validate schema and date window for normalized records."""

    if not records:
        return

    missing_columns = [col for col in REQUIRED_COLUMNS if col not in records[0]]
    if missing_columns:
        raise ValidationError(f"Missing required columns: {', '.join(missing_columns)}")

    df = pd.DataFrame.from_records(records)
    missing_keys = df["issue_key"].isna().sum()
    missing_status = df["status"].isna().sum()
    if missing_keys > 0 or missing_status > 0:
        raise ValidationError("Critical columns issue_key/status contain null values")

    ref_dates = pd.to_datetime(df["data_referencia"], errors="coerce", utc=True)
    if ref_dates.notna().any():
        outside = (
            ref_dates.dropna()
            .dt.tz_convert(None)
            .dt.date.apply(lambda d: d < from_date or d > to_date)
        )
        if outside.any():
            raise ValidationError(
                "data_referencia values are outside extraction window"
            )
