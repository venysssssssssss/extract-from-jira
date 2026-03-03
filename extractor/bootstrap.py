"""Composition root for wiring concrete implementations."""

from __future__ import annotations

from extractor.audit import JsonlAuditor
from extractor.config import Settings
from extractor.jira_api_client import JiraApiClient
from extractor.normalizer import JiraNormalizer
from extractor.playwright_fallback import PlaywrightFallback
from extractor.service import ExtractionService
from extractor.storage import FileStorage


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
    )
