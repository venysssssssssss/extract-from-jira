"""Application settings and default runtime values."""

from __future__ import annotations

from datetime import datetime, timedelta
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

    def default_window(self) -> ExtractionWindow:
        """Return the rolling window D-30..D-1 in configured timezone."""

        tz = ZoneInfo(self.timezone)
        today = datetime.now(tz).date()
        to_date = today - timedelta(days=1)
        from_date = to_date - timedelta(days=29)
        return ExtractionWindow(from_date=from_date, to_date=to_date)
