from __future__ import annotations

from datetime import date

from extractor.domain import BaseName
from extractor.sql_server_writer import SqlServerWriter


class FakeCursor:
    def __init__(self, existing_columns: list[str]) -> None:
        self.existing_columns = existing_columns
        self.execute_calls: list[tuple[str, tuple]] = []
        self._rows: list[tuple[str]] = []

    def execute(self, sql: str, params: tuple = ()) -> "FakeCursor":
        self.execute_calls.append((sql, params))
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            self._rows = [(column,) for column in self.existing_columns]
        return self

    def fetchall(self) -> list[tuple[str]]:
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

    assert "[tema] NVARCHAR(4000) NULL" in create_sql
    assert "[faixa_dias_uteis_simples] NVARCHAR(4000) NULL" in create_sql
    assert "PRIMARY KEY ([chave_ticket], [data_atualizacao])" in create_sql


def test_migrate_schema_adds_missing_columns() -> None:
    writer = _writer()
    cursor = FakeCursor(existing_columns=["chave_ticket", "resumo", "status"])
    column_defs = writer._get_column_defs(BaseName.ANALISADAS)

    writer._migrate_schema(cursor, BaseName.ANALISADAS, column_defs)

    alter_statements = [
        sql for sql, _params in cursor.execute_calls if sql.startswith("ALTER TABLE")
    ]

    assert any("[data_limite] NVARCHAR(4000) NULL" in sql for sql in alter_statements)
    assert any("[relato] NVARCHAR(4000) NULL" in sql for sql in alter_statements)


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
                "faixa_dias_uteis_simples": "5",
            }
        ],
        from_date=date(2026, 2, 1),
        to_date=date(2026, 3, 1),
    )

    assert len(rows) == 1
    assert "Tema X" in rows[0]
    assert "5" in rows[0]
