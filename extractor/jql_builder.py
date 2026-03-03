"""JQL construction helpers based on business rules and date windows."""

from __future__ import annotations

from extractor.business_rules import BaseRule
from extractor.domain import ExtractionWindow


def build_jql(rule: BaseRule, window: ExtractionWindow) -> str:
    """Build deterministic JQL using business clauses and rolling date window."""

    return (
        f"filter = {rule.filter_id} "
        f'AND "{rule.date_jql_name}" >= "{window.from_date.isoformat()}" '
        f'AND "{rule.date_jql_name}" <= "{window.to_date.isoformat()}" '
        "ORDER BY created DESC"
    )
