from pathlib import Path

import pandas as pd

from extractor.business_rules import RULES
from extractor.normalizer import JiraNormalizer
from extractor.utils import canonicalize_column_name


def test_normalize_api_issues_maps_core_and_custom_fields_for_all_bases() -> None:
    normalizer = JiraNormalizer()

    for base, rule in RULES.items():
        field_ids = {
            field_name: f"customfield_{index}"
            for index, field_name in enumerate(rule.custom_fields, start=1)
        }
        issues = [
            {
                "key": f"{base.value.upper()}-1",
                "fields": {
                    "summary": "Teste",
                    "status": {"name": "EM ANDAMENTO"},
                    "created": "2026-03-01T01:00:00.000+0000",
                    "updated": "2026-03-01T03:00:00.000+0000",
                    "project": {"key": "ATEN", "name": "Atendimento Ouv"},
                    "issuetype": {"name": "ATENDIMENTO"},
                    **{
                        field_id: f"valor-{canonicalize_column_name(field_name)}"
                        for field_name, field_id in field_ids.items()
                    },
                },
            }
        ]

        envelope = normalizer.normalize_api_issues(
            base=base,
            issues=issues,
            field_ids=field_ids,
            extracted_at_iso="2026-03-03T10:00:00+00:00",
        )

        assert envelope.source_mode.value == "api"
        assert envelope.records[0]["issue_key"] == f"{base.value.upper()}-1"
        assert envelope.records[0]["espaco"] == "Atendimento Ouv"
        for field_name in rule.custom_fields:
            key = canonicalize_column_name(field_name)
            assert envelope.records[0][key] == f"valor-{key}"


def test_normalize_fallback_csv_maps_custom_fields_and_missing_columns_to_none(
    tmp_path: Path,
) -> None:
    normalizer = JiraNormalizer()

    for base, rule in RULES.items():
        missing_field = rule.custom_fields[-1]
        csv_path = tmp_path / f"{base.value}.csv"
        frame = pd.DataFrame(
            [
                {
                    "Issue key": f"{base.value.upper()}-2",
                    "Summary": "Fallback",
                    "Status": "ABERTO",
                    "Created": "2026-03-01T01:00:00.000+0000",
                    "Updated": "2026-03-01T03:00:00.000+0000",
                    "Espaço": "Atendimento Ouv",
                    "Tipo do ticket": "ATENDIMENTO",
                    **{
                        field_name: f"csv-{canonicalize_column_name(field_name)}"
                        for field_name in rule.custom_fields[:-1]
                    },
                }
            ]
        )
        frame.to_csv(csv_path, index=False)

        envelope = normalizer.normalize_fallback_csv(
            base=base,
            csv_path=csv_path,
            extracted_at_iso="2026-03-03T10:00:00+00:00",
        )

        assert envelope.source_mode.value == "playwright_fallback"
        assert envelope.records[0]["issue_key"] == f"{base.value.upper()}-2"
        for field_name in rule.custom_fields[:-1]:
            key = canonicalize_column_name(field_name)
            assert envelope.records[0][key] == f"csv-{key}"
        assert envelope.records[0][canonicalize_column_name(missing_field)] is None
