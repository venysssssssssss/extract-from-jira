"""Business rule catalog for each extraction base."""

from __future__ import annotations

from dataclasses import dataclass

from extractor.domain import BaseName


@dataclass(frozen=True)
class BaseRule:
    """Describes a single extraction base and its filtering constraints."""

    base: BaseName
    filter_url: str
    filter_id: int
    date_field_name: str
    date_jql_name: str

RULES: dict[BaseName, BaseRule] = {
    BaseName.ENCERRADAS: BaseRule(
        base=BaseName.ENCERRADAS,
        filter_url="https://ouvid.atlassian.net/issues/?filter=10719",
        filter_id=10719,
        date_field_name="DATA FECHOU SALESFORCE",
        date_jql_name="data fechou salesforce[date]",
    ),
    BaseName.ANALISADAS: BaseRule(
        base=BaseName.ANALISADAS,
        filter_url="https://ouvid.atlassian.net/issues/?filter=10720",
        filter_id=10720,
        date_field_name="DATA ÚLTIMA ANÁLISE",
        date_jql_name="data última análise[date]",
    ),
    BaseName.INGRESSADAS: BaseRule(
        base=BaseName.INGRESSADAS,
        filter_url="https://ouvid.atlassian.net/issues/?filter=10721",
        filter_id=10721,
        date_field_name="DATA DE ABERTURA",
        date_jql_name="data de abertura[date]",
    ),
}

REQUIRED_FIELD_NAMES = (
    "DATA FECHOU SALESFORCE",
    "DATA ÚLTIMA ANÁLISE",
    "DATA DE ABERTURA",
)
