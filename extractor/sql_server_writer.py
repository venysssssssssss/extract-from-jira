"""SQL Server writer for loading processed records into per-base tables."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import pandas as pd

from extractor.domain import BaseName
from extractor.exceptions import DatabaseWriteError

LOGGER = logging.getLogger(__name__)


class SqlServerWriter:
    """Create/upsert base tables in SQL Server using DB_* settings."""

    def __init__(
        self,
        *,
        server: str,
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

    def _qualified_table(self, base: BaseName) -> str:
        schema = self._safe_identifier(self._schema)
        table = self._safe_identifier(f"jira_{base.value}")
        return f"[{schema}].[{table}]"

    def _connection_string(self) -> str:
        encrypt = "yes" if self._encrypt else "no"
        trust = "yes" if self._trust_server_certificate else "no"
        return (
            f"DRIVER={{{self._driver}}};"
            f"SERVER={self._server};"
            f"DATABASE={self._database};"
            f"UID={self._user};"
            f"PWD={self._password};"
            f"Encrypt={encrypt};"
            f"TrustServerCertificate={trust};"
            f"Connection Timeout={self._connect_timeout};"
        )

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

    def _build_rows(
        self, records: list[dict[str, Any]], from_date: date, to_date: date
    ) -> list[tuple]:
        """Convert normalized record dictionaries into DB tuples."""

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
            key = (chave_ticket, data_atualizacao)
            dedup[key] = (
                chave_ticket,
                self._normalize_text(item.get("summary")),
                self._normalize_text(item.get("status")),
                data_criacao,
                data_atualizacao,
                self._normalize_text(item.get("base_origem")),
                self._to_date(item.get("data_referencia")),
                self._normalize_text(item.get("espaco")),
                self._normalize_text(item.get("tipo_ticket")),
                self._to_datetime(item.get("extracted_at")) or datetime.utcnow(),
                self._normalize_text(item.get("source_mode")),
                from_date,
                to_date,
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
        rows = self._build_rows(records, from_date, to_date)
        table = self._qualified_table(base)

        if not rows:
            LOGGER.info("db_upsert_skipped_no_rows base=%s", base.value)
            return {"table": table, "inserted_rows": 0, "period_count": 0}

        create_sql = f"""
IF OBJECT_ID(N'{table}', N'U') IS NULL
BEGIN
    CREATE TABLE {table} (
        chave_ticket NVARCHAR(128) NOT NULL,
        resumo NVARCHAR(4000) NULL,
        status NVARCHAR(255) NULL,
        data_criacao DATETIME2 NULL,
        data_atualizacao DATETIME2 NOT NULL,
        base_origem NVARCHAR(64) NOT NULL,
        data_referencia DATE NULL,
        espaco NVARCHAR(255) NULL,
        tipo_ticket NVARCHAR(255) NULL,
        extraido_em DATETIME2 NOT NULL,
        modo_origem NVARCHAR(64) NOT NULL,
        periodo_inicio DATE NOT NULL,
        periodo_fim DATE NOT NULL,
        carga_em DATETIME2 NOT NULL CONSTRAINT DF_{base.value}_carga_em DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_{base.value}_jira PRIMARY KEY (chave_ticket, data_atualizacao)
    );
END
"""
        delete_sql = (
            f"DELETE FROM {table} WHERE periodo_inicio = ? AND periodo_fim = ?;"
        )
        insert_sql = f"""
INSERT INTO {table} (
    chave_ticket,
    resumo,
    status,
    data_criacao,
    data_atualizacao,
    base_origem,
    data_referencia,
    espaco,
    tipo_ticket,
    extraido_em,
    modo_origem,
    periodo_inicio,
    periodo_fim
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
        count_sql = (
            f"SELECT COUNT(1) FROM {table} WHERE periodo_inicio = ? AND periodo_fim = ?;"
        )

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
                cursor.execute(delete_sql, (from_date, to_date))
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

