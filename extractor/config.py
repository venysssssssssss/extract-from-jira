"""Application settings and default runtime values."""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from extractor.domain import ExtractionWindow


class Settings(BaseSettings):
    """Typed settings loaded from environment variables or `.env`."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    jira_base_url: str = Field(validation_alias="JIRA_BASE_URL")
    jira_email: str = Field(validation_alias="JIRA_EMAIL")
    jira_api_token: str = Field(validation_alias="JIRA_API_TOKEN")

    jira_web_password: str | None = Field(
        default=None, validation_alias="JIRA_WEB_PASSWORD"
    )

    output_dir: Path = Field(default=Path("output"), validation_alias="OUTPUT_DIR")
    timezone: str = Field(default="America/Bahia", validation_alias="TIMEZONE")
    max_results: int = Field(default=100, validation_alias="MAX_RESULTS")
    retry_attempts: int = Field(default=4, validation_alias="RETRY_ATTEMPTS")
    retry_backoff_seconds: float = Field(
        default=2.0, validation_alias="RETRY_BACKOFF_SECONDS"
    )
    playwright_headless: bool = Field(
        default=True, validation_alias="PLAYWRIGHT_HEADLESS"
    )
    clean_output_on_api_run: bool = Field(
        default=True, validation_alias="CLEAN_OUTPUT_ON_API_RUN"
    )
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")
    log_json: bool = Field(default=False, validation_alias="LOG_JSON")
    log_file: Path = Field(
        default=Path("output/logs/application.log"), validation_alias="LOG_FILE"
    )
    log_max_bytes: int = Field(default=10_485_760, validation_alias="LOG_MAX_BYTES")
    log_backup_count: int = Field(default=10, validation_alias="LOG_BACKUP_COUNT")
    db_server: str | None = Field(default=None, validation_alias="DB_SERVER")
    db_driver: str | None = Field(default=None, validation_alias="DB_DRIVER")
    db_database: str | None = Field(default=None, validation_alias="DB_DATABASE")
    db_user: str | None = Field(default=None, validation_alias="DB_USER")
    db_password: str | None = Field(default=None, validation_alias="DB_PASSWORD")
    db_schema: str = Field(default="dbo", validation_alias="DB_SCHEMA")
    db_encrypt: bool = Field(default=False, validation_alias="DB_ENCRYPT")
    db_trust_server_certificate: bool = Field(
        default=True, validation_alias="DB_TRUST_SERVER_CERTIFICATE"
    )
    db_connect_timeout: int = Field(default=30, validation_alias="DB_CONNECT_TIMEOUT")
    db_enabled: bool = Field(default=True, validation_alias="DB_ENABLED")

    @staticmethod
    def _subtract_one_month(reference_date: date) -> date:
        """Subtract one calendar month preserving day when possible."""

        year = reference_date.year
        month = reference_date.month - 1
        if month == 0:
            month = 12
            year -= 1
        last_day = calendar.monthrange(year, month)[1]
        day = min(reference_date.day, last_day)
        return reference_date.replace(year=year, month=month, day=day)

    def default_window(self) -> ExtractionWindow:
        """Return window from (D-1 minus 1 month) up to D-1."""

        tz = ZoneInfo(self.timezone)
        today = datetime.now(tz).date()
        reference_d_minus_1 = today - timedelta(days=1)
        from_date = self._subtract_one_month(reference_d_minus_1)
        to_date = reference_d_minus_1
        return ExtractionWindow(from_date=from_date, to_date=to_date)

    @property
    def database_configured(self) -> bool:
        """Whether DB_* credentials are present and non-empty."""

        required = (
            self.db_server,
            self.db_driver,
            self.db_database,
            self.db_user,
            self.db_password,
        )
        return all(value is not None and str(value).strip() for value in required)
