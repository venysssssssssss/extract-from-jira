from __future__ import annotations

from datetime import date

from extractor.domain import BaseName
from extractor.sql_server_writer import SqlServerWriter


class FakeCursor:
    def __init__(self, existing_columns: list[tuple[str, str, int | None]]) -> None:
        self.existing_columns = existing_columns
        self.execute_calls: list[tuple[str, tuple]] = []
        self._rows: list[tuple[str, str, int | None]] = []

    def execute(self, sql: str, params: tuple = ()) -> "FakeCursor":
        self.execute_calls.append((sql, params))
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            self._rows = list(self.existing_columns)
        return self

    def fetchall(self) -> list[tuple[str, str, int | None]]:
        return self._rows


def _writer() -> SqlServerWriter:
    return SqlServerWriter(
        server="localhost",
        port=1433,
        driver="ODBC Driver 18 for SQL Server",
        database="db",
        user="user",
        password="pass",
    )


def test_build_create_table_sql_contains_dynamic_custom_columns() -> None:
    writer = _writer()
    column_defs = writer._get_column_defs(BaseName.ENCERRADAS)

    create_sql = writer._build_create_table_sql(BaseName.ENCERRADAS, column_defs)

    assert "[resumo] NVARCHAR(MAX) NULL" in create_sql
    assert "[tema] NVARCHAR(MAX) NULL" in create_sql
    assert "[data_fechou_salesforce] DATE NULL" in create_sql
    assert "[faixa_dias_uteis_simples] NVARCHAR(MAX) NULL" in create_sql
    assert "PRIMARY KEY ([chave_ticket], [data_atualizacao])" in create_sql


def test_migrate_schema_adds_missing_columns() -> None:
    writer = _writer()
    cursor = FakeCursor(
        existing_columns=[
            ("chave_ticket", "nvarchar", 128),
            ("resumo", "nvarchar", -1),
            ("status", "nvarchar", 255),
        ]
    )
    column_defs = writer._get_column_defs(BaseName.ANALISADAS)

    writer._migrate_schema(cursor, BaseName.ANALISADAS, column_defs)

    alter_statements = [
        sql for sql, _params in cursor.execute_calls if sql.startswith("ALTER TABLE")
    ]

    assert any("ADD [data_limite] DATE NULL" in sql for sql in alter_statements)
    assert any("ADD [relato] NVARCHAR(MAX) NULL" in sql for sql in alter_statements)


def test_migrate_schema_alters_existing_nvarchar_4000_to_max() -> None:
    writer = _writer()
    cursor = FakeCursor(
        existing_columns=[
            ("chave_ticket", "nvarchar", 128),
            ("resumo", "nvarchar", 4000),
            ("status", "nvarchar", 255),
            ("relato", "nvarchar", 4000),
        ]
    )
    column_defs = writer._get_column_defs(BaseName.ANALISADAS)

    writer._migrate_schema(cursor, BaseName.ANALISADAS, column_defs)

    alter_statements = [
        sql for sql, _params in cursor.execute_calls if "ALTER COLUMN" in sql
    ]

    assert any("ALTER COLUMN [resumo] NVARCHAR(MAX) NULL" in sql for sql in alter_statements)
    assert any("ALTER COLUMN [relato] NVARCHAR(MAX) NULL" in sql for sql in alter_statements)


def test_migrate_schema_sanitizes_and_alters_existing_text_date_columns() -> None:
    writer = _writer()
    cursor = FakeCursor(
        existing_columns=[
            ("chave_ticket", "nvarchar", 128),
            ("data_fechou_salesforce", "nvarchar", 4000),
        ]
    )
    column_defs = writer._get_column_defs(BaseName.ENCERRADAS)

    writer._migrate_schema(cursor, BaseName.ENCERRADAS, column_defs)

    executed_sql = [sql for sql, _params in cursor.execute_calls]

    assert any(
        "UPDATE [dbo].[jira_encerradas] SET [data_fechou_salesforce] = NULL" in sql
        for sql in executed_sql
    )
    assert any(
        "ALTER COLUMN [data_fechou_salesforce] DATE NULL" in sql for sql in executed_sql
    )


def test_build_rows_includes_dynamic_custom_values() -> None:
    writer = _writer()
    rows = writer._build_rows(
        BaseName.ENCERRADAS,
        [
            {
                "issue_key": "ATEN-1",
                "summary": "Resumo",
                "status": "ENCERRADO",
                "created": "2026-03-01T10:00:00Z",
                "updated": "2026-03-01T11:00:00Z",
                "base_origem": "encerradas",
                "data_referencia": "2026-03-01",
                "espaco": "Atendimento Ouv",
                "tipo_ticket": "ATENDIMENTO",
                "extracted_at": "2026-03-03T10:00:00+00:00",
                "source_mode": "api",
                "tema": "Tema X",
                "data_fechou_salesforce": "2026-03-01",
                "faixa_dias_uteis_simples": "5",
            }
        ],
        from_date=date(2026, 2, 1),
        to_date=date(2026, 3, 1),
    )

    assert len(rows) == 1
    assert "Tema X" in rows[0]
    assert "5" in rows[0]
    assert date(2026, 3, 1) in rows[0]


def test_has_large_text_payload_detects_values_over_4000_chars() -> None:
    writer = _writer()

    assert writer._has_large_text_payload([("a" * 4001,)]) is True
    assert writer._has_large_text_payload([("ok", 1, None)]) is False
