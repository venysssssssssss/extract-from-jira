"""SQL Server writer for loading processed records into per-base tables."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from datetime import date, datetime
from typing import Any, Callable

import pandas as pd

from extractor.business_rules import RULES
from extractor.domain import BaseName
from extractor.exceptions import DatabaseWriteError
from extractor.utils import canonicalize_column_name

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ColumnDef:
    sql_name: str
    sql_type: str
    record_key: str
    converter: Callable[[Any], Any]


class SqlServerWriter:
    """Create/upsert base tables in SQL Server using DB_* settings."""

    def __init__(
        self,
        *,
        server: str,
        port: int | None,
        driver: str,
        database: str,
        user: str,
        password: str,
        schema: str = "dbo",
        encrypt: bool = False,
        trust_server_certificate: bool = True,
        connect_timeout: int = 30,
    ) -> None:
        self._server = server
        self._port = port
        self._driver = driver
        self._database = database
        self._user = user
        self._password = password
        self._schema = schema
        self._encrypt = encrypt
        self._trust_server_certificate = trust_server_certificate
        self._connect_timeout = connect_timeout

    @staticmethod
    def _safe_identifier(value: str) -> str:
        if not value or any(ch in value for ch in ("[", "]", ";", "--", ".")):
            raise DatabaseWriteError(f"Unsafe SQL identifier: {value!r}")
        return value

    def _table_name(self, base: BaseName) -> str:
        return self._safe_identifier(f"jira_{base.value}")

    def _qualified_table(self, base: BaseName) -> str:
        schema = self._safe_identifier(self._schema)
        table = self._table_name(base)
        return f"[{schema}].[{table}]"

    def _quoted_identifier(self, value: str) -> str:
        return f"[{self._safe_identifier(value)}]"

    def _connection_string(self) -> str:
        encrypt = "yes" if self._encrypt else "no"
        trust = "yes" if self._trust_server_certificate else "no"
        server = self._server
        if self._port:
            host = self._server.split("\\", maxsplit=1)[0]
            server = f"{host},{self._port}"
        return (
            f"DRIVER={{{self._driver}}};"
            f"SERVER={server};"
            f"DATABASE={self._database};"
            f"UID={self._user};"
            f"PWD={self._password};"
            f"Encrypt={encrypt};"
            f"TrustServerCertificate={trust};"
            f"Connection Timeout={self._connect_timeout};"
        )

    def check_connection(self) -> None:
        """Fail fast if database is unreachable before extraction starts."""

        try:
            import pyodbc
        except Exception as exc:
            raise DatabaseWriteError(
                "pyodbc is required to write data into SQL Server. Install it and retry."
            ) from exc

        try:
            with pyodbc.connect(self._connection_string(), autocommit=True) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                _ = cursor.fetchone()
        except Exception as exc:
            hint = ""
            if "\\" in self._server and not self._port:
                hint = (
                    " Hint: DB_SERVER uses named instance; set DB_PORT with the SQL Server TCP port "
                    "and keep DB_SERVER as host/IP only."
                )
            raise DatabaseWriteError(f"Database connection check failed: {exc}.{hint}") from exc

    @staticmethod
    def _to_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        try:
            parsed = pd.to_datetime(value, errors="coerce", utc=False)
        except Exception:
            return None
        if pd.isna(parsed):
            return None
        if hasattr(parsed, "to_pydatetime"):
            return parsed.to_pydatetime()
        return None

    @staticmethod
    def _to_date(value: Any) -> date | None:
        if value is None:
            return None
        try:
            parsed = pd.to_datetime(value, errors="coerce")
        except Exception:
            return None
        if pd.isna(parsed):
            return None
        return parsed.date()

    @staticmethod
    def _normalize_text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    @staticmethod
    def _identity(value: Any) -> Any:
        return value

    def _get_column_defs(self, base: BaseName) -> list[ColumnDef]:
        core_defs = [
            ColumnDef("chave_ticket", "NVARCHAR(128) NOT NULL", "issue_key", self._normalize_text),
            ColumnDef("resumo", "NVARCHAR(MAX) NULL", "summary", self._normalize_text),
            ColumnDef("status", "NVARCHAR(255) NULL", "status", self._normalize_text),
            ColumnDef("data_criacao", "DATETIME2 NULL", "created", self._to_datetime),
            ColumnDef(
                "data_atualizacao",
                "DATETIME2 NOT NULL",
                "__resolved_updated__",
                self._identity,
            ),
            ColumnDef("base_origem", "NVARCHAR(64) NOT NULL", "base_origem", self._normalize_text),
            ColumnDef("data_referencia", "DATE NULL", "data_referencia", self._to_date),
            ColumnDef("espaco", "NVARCHAR(255) NULL", "espaco", self._normalize_text),
            ColumnDef("tipo_ticket", "NVARCHAR(255) NULL", "tipo_ticket", self._normalize_text),
            ColumnDef(
                "extraido_em",
                "DATETIME2 NOT NULL",
                "__resolved_extracted_at__",
                self._identity,
            ),
            ColumnDef("modo_origem", "NVARCHAR(64) NOT NULL", "source_mode", self._normalize_text),
            ColumnDef("periodo_inicio", "DATE NOT NULL", "__from_date__", self._identity),
            ColumnDef("periodo_fim", "DATE NOT NULL", "__to_date__", self._identity),
        ]
        custom_defs = [
            ColumnDef(
                canonicalize_column_name(field_name),
                "NVARCHAR(MAX) NULL",
                canonicalize_column_name(field_name),
                self._normalize_text,
            )
            for field_name in RULES[base].custom_fields
        ]
        return [*core_defs, *custom_defs]

    def _build_create_table_sql(self, base: BaseName, column_defs: list[ColumnDef]) -> str:
        table = self._qualified_table(base)
        table_name = self._table_name(base)
        column_lines = [
            f"        {self._quoted_identifier(column.sql_name)} {column.sql_type}"
            for column in column_defs
        ]
        column_lines.extend(
            [
                (
                    f"        [carga_em] DATETIME2 NOT NULL CONSTRAINT "
                    f"DF_{table_name}_carga_em DEFAULT SYSUTCDATETIME()"
                ),
                (
                    f"        CONSTRAINT PK_{table_name} PRIMARY KEY "
                    f"([chave_ticket], [data_atualizacao])"
                ),
            ]
        )
        joined_columns = ",\n".join(column_lines)
        return (
            f"IF OBJECT_ID(N'{table}', N'U') IS NULL\n"
            f"BEGIN\n"
            f"    CREATE TABLE {table} (\n"
            f"{joined_columns}\n"
            f"    );\n"
            f"END"
        )

    def _fetch_existing_columns(self, cursor: Any, base: BaseName) -> dict[str, dict[str, Any]]:
        cursor.execute(
            """
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
""",
            (self._schema, self._table_name(base)),
        )
        existing: dict[str, dict[str, Any]] = {}
        for row in cursor.fetchall():
            if not row or row[0] is None:
                continue
            existing[str(row[0]).strip().lower()] = {
                "data_type": str(row[1]).strip().lower() if row[1] is not None else None,
                "character_maximum_length": row[2],
            }
        return existing

    @staticmethod
    def _column_supports_expected_type(existing: dict[str, Any], expected_sql_type: str) -> bool:
        normalized_expected = expected_sql_type.strip().lower()
        existing_type = existing.get("data_type")
        existing_length = existing.get("character_maximum_length")

        if normalized_expected.startswith("nvarchar(max)"):
            return existing_type == "nvarchar" and existing_length == -1
        if normalized_expected.startswith("nvarchar("):
            start = normalized_expected.find("(") + 1
            end = normalized_expected.find(")")
            expected_length = int(normalized_expected[start:end])
            return existing_type == "nvarchar" and existing_length == expected_length
        if normalized_expected.startswith("datetime2"):
            return existing_type == "datetime2"
        if normalized_expected.startswith("date"):
            return existing_type == "date"
        return False

    def _migrate_schema(
        self, cursor: Any, base: BaseName, column_defs: list[ColumnDef]
    ) -> None:
        existing_columns = self._fetch_existing_columns(cursor, base)
        table = self._qualified_table(base)
        for column in column_defs:
            existing = existing_columns.get(column.sql_name.lower())
            if existing is None:
                cursor.execute(
                    f"ALTER TABLE {table} ADD {self._quoted_identifier(column.sql_name)} {column.sql_type};"
                )
                continue
            if self._column_supports_expected_type(existing, column.sql_type):
                continue
            cursor.execute(
                f"ALTER TABLE {table} ALTER COLUMN {self._quoted_identifier(column.sql_name)} {column.sql_type};"
            )

    def _build_delete_sql(self, base: BaseName) -> str:
        return (
            f"DELETE FROM {self._qualified_table(base)} "
            f"WHERE [periodo_inicio] = ? AND [periodo_fim] = ?;"
        )

    def _build_insert_sql(self, base: BaseName, column_defs: list[ColumnDef]) -> str:
        columns_sql = ",\n    ".join(
            self._quoted_identifier(column.sql_name) for column in column_defs
        )
        placeholders = ", ".join("?" for _ in column_defs)
        return (
            f"INSERT INTO {self._qualified_table(base)} (\n"
            f"    {columns_sql}\n"
            f") VALUES ({placeholders})"
        )

    def _build_count_sql(self, base: BaseName) -> str:
        return (
            f"SELECT COUNT(1) FROM {self._qualified_table(base)} "
            f"WHERE [periodo_inicio] = ? AND [periodo_fim] = ?;"
        )

    @staticmethod
    def _has_large_text_payload(rows: list[tuple]) -> bool:
        for row in rows:
            for value in row:
                if isinstance(value, str) and len(value) > 4000:
                    return True
        return False

    def _build_rows(
        self,
        base: BaseName,
        records: list[dict[str, Any]],
        from_date: date,
        to_date: date,
    ) -> list[tuple]:
        """Convert normalized record dictionaries into DB tuples."""

        column_defs = self._get_column_defs(base)
        dedup: dict[tuple[str, datetime], tuple] = {}
        for item in records:
            chave_ticket = self._normalize_text(item.get("issue_key"))
            if not chave_ticket:
                continue

            data_criacao = self._to_datetime(item.get("created"))
            data_atualizacao = (
                self._to_datetime(item.get("updated"))
                or data_criacao
                or self._to_datetime(item.get("extracted_at"))
                or datetime.utcnow()
            )
            resolved_extracted_at = self._to_datetime(item.get("extracted_at")) or datetime.utcnow()
            row_source = {
                **item,
                "__resolved_updated__": data_atualizacao,
                "__resolved_extracted_at__": resolved_extracted_at,
                "__from_date__": from_date,
                "__to_date__": to_date,
            }
            key = (chave_ticket, data_atualizacao)
            dedup[key] = tuple(
                column.converter(row_source.get(column.record_key)) for column in column_defs
            )
        return list(dedup.values())

    def upsert_records(
        self,
        *,
        base: BaseName,
        from_date: date,
        to_date: date,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        column_defs = self._get_column_defs(base)
        rows = self._build_rows(base, records, from_date, to_date)
        table = self._qualified_table(base)

        create_sql = self._build_create_table_sql(base, column_defs)
        delete_sql = self._build_delete_sql(base)
        insert_sql = self._build_insert_sql(base, column_defs)
        count_sql = self._build_count_sql(base)

        try:
            import pyodbc
        except Exception as exc:
            raise DatabaseWriteError(
                "pyodbc is required to write data into SQL Server. Install it and retry."
            ) from exc

        try:
            with pyodbc.connect(self._connection_string(), autocommit=False) as conn:
                cursor = conn.cursor()
                cursor.execute(create_sql)
                self._migrate_schema(cursor, base, column_defs)
                if not rows:
                    conn.commit()
                    LOGGER.info("db_upsert_skipped_no_rows base=%s", base.value)
                    return {"table": table, "inserted_rows": 0, "period_count": 0}
                cursor.execute(delete_sql, (from_date, to_date))
                if self._has_large_text_payload(rows):
                    for row in rows:
                        cursor.execute(insert_sql, row)
                else:
                    cursor.fast_executemany = True
                    cursor.executemany(insert_sql, rows)
                cursor.execute(count_sql, (from_date, to_date))
                period_count = int(cursor.fetchone()[0])
                if period_count != len(rows):
                    raise DatabaseWriteError(
                        f"DB verification failed for {table}: expected {len(rows)} rows, found {period_count}."
                    )
                conn.commit()
        except DatabaseWriteError:
            raise
        except Exception as exc:
            raise DatabaseWriteError(
                f"Failed writing data to SQL Server table {table}: {exc}"
            ) from exc

        LOGGER.info(
            "db_upsert_finished base=%s table=%s rows=%s from=%s to=%s",
            base.value,
            table,
            len(rows),
            from_date,
            to_date,
        )
        return {"table": table, "inserted_rows": len(rows), "period_count": period_count}
