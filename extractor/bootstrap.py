"""Composition root for wiring concrete implementations."""

from __future__ import annotations

import logging

from extractor.audit import JsonlAuditor
from extractor.config import Settings
from extractor.jira_api_client import JiraApiClient
from extractor.normalizer import JiraNormalizer
from extractor.playwright_fallback import PlaywrightFallback
from extractor.service import ExtractionService
from extractor.sql_server_writer import SqlServerWriter
from extractor.storage import FileStorage

LOGGER = logging.getLogger(__name__)


def build_service(settings: Settings | None = None) -> ExtractionService:
    """Create fully wired extraction service with production defaults."""

    cfg = settings or Settings()
    jira = JiraApiClient(
        base_url=cfg.jira_base_url,
        email=cfg.jira_email,
        api_token=cfg.jira_api_token,
        retry_attempts=cfg.retry_attempts,
        retry_backoff_seconds=cfg.retry_backoff_seconds,
    )
    fallback = PlaywrightFallback(
        email=cfg.jira_email,
        password=cfg.jira_web_password,
        headless=cfg.playwright_headless,
    )
    normalizer = JiraNormalizer()
    storage = FileStorage(cfg.output_dir)
    auditor = JsonlAuditor(cfg.output_dir)
    db_writer = None
    if cfg.db_enabled and cfg.database_configured:
        db_writer = SqlServerWriter(
            server=cfg.db_server or "",
            driver=cfg.db_driver or "",
            database=cfg.db_database or "",
            user=cfg.db_user or "",
            password=cfg.db_password or "",
            schema=cfg.db_schema,
            encrypt=cfg.db_encrypt,
            trust_server_certificate=cfg.db_trust_server_certificate,
            connect_timeout=cfg.db_connect_timeout,
        )
    elif cfg.db_enabled and not cfg.database_configured:
        LOGGER.warning(
            "database_enabled_but_not_configured_missing_db_vars db_server/db_driver/db_database/db_user/db_password"
        )

    return ExtractionService(
        jira_gateway=jira,
        fallback_gateway=fallback,
        normalizer=normalizer,
        storage=storage,
        auditor=auditor,
        output_dir=cfg.output_dir,
        max_results=cfg.max_results,
        default_window_factory=cfg.default_window,
        clean_output_on_api_run=cfg.clean_output_on_api_run,
        database_writer=db_writer,
    )
