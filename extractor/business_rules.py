"""Business rule catalog for each extraction base."""

from __future__ import annotations

from dataclasses import dataclass

from extractor.domain import BaseName


@dataclass(frozen=True)
class BaseRule:
    """Describes a single extraction base and its filtering constraints."""

    base: BaseName
    filter_url: str
    date_field_name: str
    fixed_clauses: tuple[str, ...]


ANALISADAS_STATUS = (
    "ABERTO",
    "ANALISAR",
    "ANÁLISE",
    "CIRADO",
    "DESIGNADA",
    "DEVOLVIDO",
    "EM ABERTO",
    "EM TRATAMENTO",
    "EM TRATATIVA",
    "EM VERIFICAÇÃO",
    "EM ANDAMENTO",
    "PENDENTE RETORNO",
    "POSTERGADO",
    "RECLASSIFICAR",
    "RESPONDIDO",
    "RETORNO",
    "REITERADAS EM ABERTO",
    "RECORRER ANEEL",
    "PROCEDENTES",
)

RULES: dict[BaseName, BaseRule] = {
    BaseName.ENCERRADAS: BaseRule(
        base=BaseName.ENCERRADAS,
        filter_url="https://ouvid.atlassian.net/issues/?filter=10719",
        date_field_name="DATA FECHOU SALESFORCE",
        fixed_clauses=(
            '"Espaço" = "Atendimento Ouv"',
            'status = "ENCERRADO"',
        ),
    ),
    BaseName.ANALISADAS: BaseRule(
        base=BaseName.ANALISADAS,
        filter_url="https://ouvid.atlassian.net/issues/?filter=10720",
        date_field_name="DATA ÚLTIMA ANÁLISE",
        fixed_clauses=(
            '"Tipo do ticket" = "ATENDIMENTO"',
            "status IN (" + ", ".join(f'"{item}"' for item in ANALISADAS_STATUS) + ")",
        ),
    ),
    BaseName.INGRESSADAS: BaseRule(
        base=BaseName.INGRESSADAS,
        filter_url="https://ouvid.atlassian.net/issues/?filter=10721",
        date_field_name="DATA ABERTURA",
        fixed_clauses=('"Espaço" = "Atendimento Ouv"',),
    ),
}

REQUIRED_FIELD_NAMES = (
    "DATA FECHOU SALESFORCE",
    "DATA ÚLTIMA ANÁLISE",
    "DATA ABERTURA",
    "Espaço",
    "Tipo do ticket",
)
