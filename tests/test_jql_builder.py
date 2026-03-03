from datetime import date

from extractor.business_rules import RULES
from extractor.domain import BaseName, ExtractionWindow
from extractor.jql_builder import build_jql


def test_build_jql_contains_window_and_ordering() -> None:
    rule = RULES[BaseName.ENCERRADAS]
    window = ExtractionWindow(from_date=date(2026, 2, 1), to_date=date(2026, 3, 1))

    jql = build_jql(rule, window)

    assert "filter = 10719" in jql
    assert '"data fechou salesforce[date]" >= "2026-02-01"' in jql
    assert '"data fechou salesforce[date]" <= "2026-03-01"' in jql
    assert "ORDER BY created DESC" in jql
