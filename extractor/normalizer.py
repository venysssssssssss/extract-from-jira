"""Normalization module that adapts API/fallback payloads to a single schema."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from extractor.business_rules import RULES
from extractor.domain import BaseName, RecordEnvelope, SourceMode
from extractor.interfaces import Normalizer
from extractor.utils import canonicalize, canonicalize_column_name


class JiraNormalizer(Normalizer):
    """Converts heterogeneous Jira payloads into normalized records."""

    @staticmethod
    def _pick_scalar(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float, bool)):
            return str(value)
        if isinstance(value, dict):
            for key in ("value", "name", "displayName"):
                current = value.get(key)
                if current is not None:
                    return str(current)
            return str(value)
        if isinstance(value, list):
            if not value:
                return None
            first = JiraNormalizer._pick_scalar(value[0])
            return first
        return str(value)

    @staticmethod
    def _status_name(fields: dict[str, Any]) -> str | None:
        status = fields.get("status")
        if isinstance(status, dict):
            name = status.get("name")
            return str(name) if name is not None else None
        return JiraNormalizer._pick_scalar(status)

    @staticmethod
    def _series_value(series: pd.Series | None, index: int) -> str | None:
        if series is None:
            return None
        value = series.iloc[index]
        if pd.isna(value):
            return None
        return str(value)

    def normalize_api_issues(
        self,
        base: BaseName,
        issues: list[dict[str, Any]],
        field_ids: dict[str, str],
        extracted_at_iso: str,
    ) -> RecordEnvelope:
        rule = RULES[base]

        records: list[dict[str, Any]] = []
        for issue in issues:
            fields = issue.get("fields", {})
            if not isinstance(fields, dict):
                continue
            project = fields.get("project")
            issuetype = fields.get("issuetype")

            record = {
                "issue_key": issue.get("key"),
                "summary": self._pick_scalar(fields.get("summary")),
                "status": self._status_name(fields),
                "created": self._pick_scalar(fields.get("created")),
                "updated": self._pick_scalar(fields.get("updated")),
                "base_origem": base.value,
                "data_referencia": (
                    self._pick_scalar(fields.get(field_ids[rule.date_field_name]))
                    if rule.date_field_name in field_ids
                    else None
                ),
                "espaco": self._pick_scalar(project),
                "tipo_ticket": self._pick_scalar(issuetype),
                "extracted_at": extracted_at_iso,
                "source_mode": SourceMode.API.value,
            }
            for field_name in rule.custom_fields:
                column_name = canonicalize_column_name(field_name)
                field_id = field_ids.get(field_name)
                record[column_name] = (
                    self._pick_scalar(fields.get(field_id)) if field_id else None
                )

            records.append(record)

        return RecordEnvelope(
            base=base, source_mode=SourceMode.API, records=records, raw_issues=issues
        )

    def normalize_fallback_csv(
        self, base: BaseName, csv_path: Path, extracted_at_iso: str
    ) -> RecordEnvelope:
        df = pd.read_csv(csv_path)
        columns = {canonicalize(col): col for col in df.columns}

        def pick_column(*names: str) -> pd.Series | None:
            for name in names:
                key = canonicalize(name)
                if key in columns:
                    return df[columns[key]]
            return None

        summary = pick_column("Summary", "Resumo")
        status = pick_column("Status")
        created = pick_column("Created", "Data de criacao")
        updated = pick_column("Updated", "Atualizado")
        key = pick_column("Issue key", "Chave")
        espaco = pick_column("Espaco", "Espaco - custom", "Project key", "Project")
        tipo = pick_column("Tipo do ticket", "Ticket type", "Issue Type", "Tipo de item")
        rule = RULES[base]
        data_referencia = pick_column(
            rule.date_field_name,
            "DATA ABERTURA" if base is BaseName.INGRESSADAS else rule.date_field_name,
        )

        normalized: list[dict[str, Any]] = []
        for index in range(len(df.index)):
            record = {
                "issue_key": self._series_value(key, index),
                "summary": self._series_value(summary, index),
                "status": self._series_value(status, index),
                "created": self._series_value(created, index),
                "updated": self._series_value(updated, index),
                "base_origem": base.value,
                "data_referencia": self._series_value(data_referencia, index),
                "espaco": self._series_value(espaco, index),
                "tipo_ticket": self._series_value(tipo, index),
                "extracted_at": extracted_at_iso,
                "source_mode": SourceMode.PLAYWRIGHT_FALLBACK.value,
            }
            for field_name in rule.custom_fields:
                record[canonicalize_column_name(field_name)] = self._series_value(
                    pick_column(field_name), index
                )
            normalized.append(record)

        return RecordEnvelope(
            base=base, source_mode=SourceMode.PLAYWRIGHT_FALLBACK, records=normalized
        )


def utc_now_iso() -> str:
    """Centralized timestamp format for normalized records."""

    return datetime.now(UTC).isoformat()
