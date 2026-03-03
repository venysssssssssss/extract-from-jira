"""Normalization module that adapts API/fallback payloads to a single schema."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from extractor.business_rules import RULES
from extractor.domain import BaseName, RecordEnvelope, SourceMode
from extractor.interfaces import Normalizer
from extractor.utils import canonicalize


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

    def normalize_api_issues(
        self,
        base: BaseName,
        issues: list[dict[str, Any]],
        field_ids: dict[str, str],
        extracted_at_iso: str,
    ) -> RecordEnvelope:
        rule = RULES[base]
        date_field_id = field_ids[rule.date_field_name]

        records: list[dict[str, Any]] = []
        for issue in issues:
            fields = issue.get("fields", {})
            if not isinstance(fields, dict):
                continue
            project = fields.get("project")
            issuetype = fields.get("issuetype")

            records.append(
                {
                    "issue_key": issue.get("key"),
                    "summary": self._pick_scalar(fields.get("summary")),
                    "status": self._status_name(fields),
                    "created": self._pick_scalar(fields.get("created")),
                    "updated": self._pick_scalar(fields.get("updated")),
                    "base_origem": base.value,
                    "data_referencia": self._pick_scalar(fields.get(date_field_id)),
                    "espaco": self._pick_scalar(project),
                    "tipo_ticket": self._pick_scalar(issuetype),
                    "extracted_at": extracted_at_iso,
                    "source_mode": SourceMode.API.value,
                }
            )

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

        data_referencia = (
            pick_column("DATA FECHOU SALESFORCE")
            if base is BaseName.ENCERRADAS
            else (
                pick_column("DATA ULTIMA ANALISE")
                if base is BaseName.ANALISADAS
                else pick_column("DATA DE ABERTURA", "DATA ABERTURA")
            )
        )

        normalized: list[dict[str, Any]] = []
        for index in range(len(df.index)):
            normalized.append(
                {
                    "issue_key": None if key is None else str(key.iloc[index]),
                    "summary": None if summary is None else str(summary.iloc[index]),
                    "status": None if status is None else str(status.iloc[index]),
                    "created": None if created is None else str(created.iloc[index]),
                    "updated": None if updated is None else str(updated.iloc[index]),
                    "base_origem": base.value,
                    "data_referencia": (
                        None
                        if data_referencia is None
                        else str(data_referencia.iloc[index])
                    ),
                    "espaco": None if espaco is None else str(espaco.iloc[index]),
                    "tipo_ticket": None if tipo is None else str(tipo.iloc[index]),
                    "extracted_at": extracted_at_iso,
                    "source_mode": SourceMode.PLAYWRIGHT_FALLBACK.value,
                }
            )

        return RecordEnvelope(
            base=base, source_mode=SourceMode.PLAYWRIGHT_FALLBACK, records=normalized
        )


def utc_now_iso() -> str:
    """Centralized timestamp format for normalized records."""

    return datetime.now(UTC).isoformat()
