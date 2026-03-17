"""Business rule catalog for each extraction base."""

from __future__ import annotations

from dataclasses import dataclass

from extractor.domain import BaseName

DATE_CUSTOM_FIELD_NAMES = frozenset(
    {
        "DATA DE ABERTURA",
        "DATA INGRESSO ORDEM",
        "DATA DA ACAO/ ENVIO AREA",
        "DATA DO RETORNO DA AREA",
        "DATA FINALIZACAO DA ORDEM",
        "1 PRAZO POSTERGADO AO CLIENTE",
        "DATA COMPROMISSO",
        "DATA ULTIMA ANALISE",
        "DATA DE ENTRADA",
        "DATA ATENDIMENTO COMPROMISSO",
        "DATA FECHOU SALESFORCE",
        "Data limite",
    }
)


INGRESSADAS_FIELDS = (
    "DATA DE ABERTURA",
    "TEMA",
    "ANALISTA",
    "DEFINICAO DA ACAO",
    "ACAO REALIZADA",
    "ACAO DO RESPONSAVEL",
    "N DA ORDEM",
    "DATA INGRESSO ORDEM",
    "TIPOLOGIA DA ORDEM",
    "DATA DA ACAO/ ENVIO AREA",
    "DATA DO RETORNO DA AREA",
    "DATA FINALIZACAO DA ORDEM",
    "1 PRAZO POSTERGADO AO CLIENTE",
    "DATA COMPROMISSO",
    "DATA ULTIMA ANALISE",
    "DATA DE ENTRADA",
    "PRAZO",
    "DESTINATARIO",
    "AREA PENDENTE",
    "DATA ATENDIMENTO COMPROMISSO",
    "COMPROMISSO GERADO:",
    "CAUSA RAIZ",
    "DATA FECHOU SALESFORCE",
    "ASSUNTO PRINCIPAL",
)

ANALISADAS_FIELDS = (
    "Data limite",
    "1 PRAZO POSTERGADO AO CLIENTE",
    "TEMA",
    "NÚMERO DE CASO PAI",
    "OFICIO (somente consultorias)",
    "PENDENCIA DO CASO",
    "AREA PENDENTE",
    "N DA ORDEM",
    "N ORDEM INGRESSADA (OUV)",
    "NUMERO DA ORDEM",
    "Itens associados",
    "DATA COMPROMISSO",
    "CAUSA RAIZ",
    "RESULTADO",
    "CONTA CONTRATO",
    "NUMERO DO PONTO DE FORNECIMENTO",
    "REGIONAL",
    "ANALISTA",
    "MUNICIPALIDADE",
    "RELATO",
    "DATA ULTIMA ANALISE",
    "DATA DE ABERTURA",
)

ENCERRADAS_FIELDS = (
    "DATA DE ABERTURA",
    "TEMA",
    "ANALISTA",
    "DEFINICAO DA ACAO",
    "ACAO REALIZADA",
    "ACAO DO RESPONSAVEL",
    "N DA ORDEM",
    "DATA INGRESSO ORDEM",
    "TIPOLOGIA DA ORDEM",
    "DATA DA ACAO/ ENVIO AREA",
    "DATA DO RETORNO DA AREA",
    "DATA FINALIZACAO DA ORDEM",
    "1 PRAZO POSTERGADO AO CLIENTE",
    "DATA COMPROMISSO",
    "DATA ULTIMA ANALISE",
    "DATA DE ENTRADA",
    "PRAZO",
    "DESTINATARIO",
    "AREA PENDENTE",
    "DATA ATENDIMENTO COMPROMISSO",
    "COMPROMISSO GERADO:",
    "CAUSA RAIZ",
    "DATA FECHOU SALESFORCE",
    "ASSUNTO PRINCIPAL",
    "FaixaDiasUteis_Simples",
)


@dataclass(frozen=True)
class BaseRule:
    """Describes a single extraction base and its filtering constraints."""

    base: BaseName
    filter_url: str
    filter_id: int
    date_field_name: str
    date_jql_name: str
    custom_fields: tuple[str, ...]


def all_required_field_names() -> set[str]:
    """Return the union of all Jira custom fields required by the pipeline."""

    return {field_name for rule in RULES.values() for field_name in rule.custom_fields}


def custom_field_is_date(field_name: str) -> bool:
    """Return whether a custom field must be stored as SQL DATE."""

    return field_name in DATE_CUSTOM_FIELD_NAMES

RULES: dict[BaseName, BaseRule] = {
    BaseName.ENCERRADAS: BaseRule(
        base=BaseName.ENCERRADAS,
        filter_url="https://ouvid.atlassian.net/issues/?filter=10719",
        filter_id=10719,
        date_field_name="DATA FECHOU SALESFORCE",
        date_jql_name="data fechou salesforce[date]",
        custom_fields=ENCERRADAS_FIELDS,
    ),
    BaseName.ANALISADAS: BaseRule(
        base=BaseName.ANALISADAS,
        filter_url="https://ouvid.atlassian.net/issues/?filter=10720",
        filter_id=10720,
        date_field_name="DATA ULTIMA ANALISE",
        date_jql_name="data última análise[date]",
        custom_fields=ANALISADAS_FIELDS,
    ),
    BaseName.INGRESSADAS: BaseRule(
        base=BaseName.INGRESSADAS,
        filter_url="https://ouvid.atlassian.net/issues/?filter=10721",
        filter_id=10721,
        date_field_name="DATA DE ABERTURA",
        date_jql_name="data de abertura[date]",
        custom_fields=INGRESSADAS_FIELDS,
    ),
}

REQUIRED_FIELD_NAMES = tuple(sorted(all_required_field_names()))
