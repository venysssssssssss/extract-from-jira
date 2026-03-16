from __future__ import annotations

from extractor.business_rules import RULES, all_required_field_names, custom_field_is_date
from extractor.domain import BaseName
from extractor.utils import canonicalize_column_name


def test_all_required_field_names_covers_all_base_custom_fields() -> None:
    field_names = all_required_field_names()

    assert len(RULES[BaseName.INGRESSADAS].custom_fields) == 24
    assert len(RULES[BaseName.ANALISADAS].custom_fields) == 21
    assert len(RULES[BaseName.ENCERRADAS].custom_fields) == 25
    assert "TEMA" in field_names
    assert "FaixaDiasUteis_Simples" in field_names


def test_canonicalized_custom_field_names_do_not_collide() -> None:
    collisions: dict[str, set[str]] = {}
    for field_name in all_required_field_names():
        key = canonicalize_column_name(field_name)
        collisions.setdefault(key, set()).add(field_name)

    duplicated = {key: names for key, names in collisions.items() if len(names) > 1}

    assert duplicated == {}


def test_custom_field_is_date_flags_known_date_fields() -> None:
    assert custom_field_is_date("DATA FECHOU SALESFORCE") is True
    assert custom_field_is_date("Data limite") is True
    assert custom_field_is_date("TEMA") is False
