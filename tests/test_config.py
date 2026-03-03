from __future__ import annotations

from datetime import date, datetime

from extractor.config import Settings


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: D401
        return cls(2026, 3, 3, 10, 0, 0, tzinfo=tz)


def test_default_window_uses_d_minus_1_minus_one_month_to_today(monkeypatch):
    monkeypatch.setattr("extractor.config.datetime", FixedDateTime)
    settings = Settings(
        JIRA_BASE_URL="https://example.atlassian.net",
        JIRA_EMAIL="user@example.com",
        JIRA_API_TOKEN="token",
    )

    window = settings.default_window()

    assert window.from_date == date(2026, 2, 2)
    assert window.to_date == date(2026, 3, 3)
