"""Playwright fallback implementation for Jira UI export."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from extractor.domain import BaseName
from extractor.exceptions import FallbackExecutionError
from extractor.interfaces import FallbackGateway


class PlaywrightFallback(FallbackGateway):
    """Uses browser automation to export CSV data from Jira issue navigator."""

    def __init__(self, email: str, password: str | None, headless: bool) -> None:
        self._email = email
        self._password = password
        self._headless = headless

    def export_filter(
        self, base: BaseName, filter_url: str, run_date: date, output_dir: Path
    ) -> Path:
        if not self._password:
            raise FallbackExecutionError(
                "JIRA_WEB_PASSWORD is required for Playwright fallback"
            )

        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # pragma: no cover - dependency/runtime concern
            raise FallbackExecutionError("Playwright runtime is not available") from exc

        fallback_dir = output_dir / "fallback" / base.value / run_date.isoformat()
        fallback_dir.mkdir(parents=True, exist_ok=True)

        filter_id = parse_qs(urlparse(filter_url).query).get("filter", [base.value])[0]
        target = fallback_dir / f"filter_{filter_id}.csv"

        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=self._headless)
                context = browser.new_context(accept_downloads=True)
                page = context.new_page()
                page.goto(filter_url, wait_until="domcontentloaded", timeout=120000)

                self._attempt_login(page)
                self._trigger_export(page)

                with page.expect_download(timeout=120000) as download_event:
                    self._click_csv_option(page)
                download = download_event.value
                download.save_as(str(target))

                context.close()
                browser.close()
        except PlaywrightTimeoutError as exc:
            raise FallbackExecutionError(
                "Playwright timeout while exporting Jira data"
            ) from exc
        except FallbackExecutionError:
            raise
        except Exception as exc:
            raise FallbackExecutionError(f"Playwright fallback failed: {exc}") from exc

        if not target.exists():
            raise FallbackExecutionError("Playwright did not produce export file")

        return target

    def _attempt_login(self, page: object) -> None:
        """Try Atlassian login flow only when login form is present."""

        try:
            if page.locator("input[name='username']").count() > 0:
                page.fill("input[name='username']", self._email)
                page.click("button[type='submit']")
                page.wait_for_timeout(1500)
        except Exception:
            return

        try:
            if page.locator("input[name='password']").count() > 0:
                page.fill("input[name='password']", self._password or "")
                page.click("button[type='submit']")
                page.wait_for_load_state("networkidle", timeout=120000)
        except Exception:
            return

    @staticmethod
    def _trigger_export(page: object) -> None:
        """Click the export menu using a selector cascade."""

        selectors = (
            "button:has-text('Export')",
            "[data-testid='issue-navigator-export-button']",
            "[aria-label='Export']",
        )
        for selector in selectors:
            try:
                if page.locator(selector).count() > 0:
                    page.click(selector)
                    page.wait_for_timeout(700)
                    return
            except Exception:
                continue

        raise FallbackExecutionError("Could not locate Jira export button")

    @staticmethod
    def _click_csv_option(page: object) -> None:
        options = (
            "text=CSV (all fields)",
            "text=CSV",
            "text=Export Excel CSV",
        )
        for selector in options:
            try:
                if page.locator(selector).count() > 0:
                    page.click(selector)
                    return
            except Exception:
                continue

        raise FallbackExecutionError("Could not locate CSV export option")
