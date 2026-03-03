"""JQL construction helpers based on business rules and date windows."""

from __future__ import annotations

from extractor.business_rules import BaseRule
from extractor.domain import ExtractionWindow


def build_jql(rule: BaseRule, date_field_id: str, window: ExtractionWindow) -> str:
    """Build deterministic JQL using business clauses and rolling date window."""

    clauses = list(rule.fixed_clauses)
    clauses.append(f'"{date_field_id}" >= "{window.from_date.isoformat()}"')
    clauses.append(f'"{date_field_id}" <= "{window.to_date.isoformat()}"')
    return " AND ".join(clauses) + f' ORDER BY "{date_field_id}" ASC, key ASC'
