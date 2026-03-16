from __future__ import annotations

from datetime import date

import pytest

from extractor.domain import BaseName
from extractor.exceptions import ValidationError
from extractor.utils import canonicalize_column_name
from extractor.validators import CORE_COLUMNS, get_required_columns, validate_records


def _base_record() -> dict[str, object]:
    return {
        "issue_key": "ATEN-1",
        "summary": "Teste",
        "status": "ABERTO",
        "created": "2026-03-01T10:00:00Z",
        "updated": "2026-03-01T11:00:00Z",
        "base_origem": "encerradas",
        "data_referencia": "2026-03-01",
        "espaco": "Atendimento Ouv",
        "tipo_ticket": "ATENDIMENTO",
        "extracted_at": "2026-03-03T10:00:00+00:00",
        "source_mode": "api",
    }


def test_get_required_columns_extends_core_columns_per_base() -> None:
    columns = get_required_columns(BaseName.ENCERRADAS)

    assert columns[: len(CORE_COLUMNS)] == CORE_COLUMNS
    assert canonicalize_column_name("TEMA") in columns
    assert canonicalize_column_name("FaixaDiasUteis_Simples") in columns


def test_validate_records_accepts_null_custom_fields() -> None:
    record = _base_record()
    for column in get_required_columns(BaseName.ENCERRADAS):
        record.setdefault(column, None)

    validate_records(
        [record],
        base=BaseName.ENCERRADAS,
        from_date=date(2026, 2, 1),
        to_date=date(2026, 3, 5),
    )


def test_validate_records_rejects_missing_schema_columns() -> None:
    record = _base_record()

    with pytest.raises(ValidationError, match="Missing required columns"):
        validate_records(
            [record],
            base=BaseName.ENCERRADAS,
            from_date=date(2026, 2, 1),
            to_date=date(2026, 3, 5),
        )


def test_validate_records_rejects_null_core_values() -> None:
    record = _base_record()
    for column in get_required_columns(BaseName.ANALISADAS):
        record.setdefault(column, None)
    record["issue_key"] = None

    with pytest.raises(ValidationError, match="Critical columns"):
        validate_records(
            [record],
            base=BaseName.ANALISADAS,
            from_date=date(2026, 2, 1),
            to_date=date(2026, 3, 5),
        )
