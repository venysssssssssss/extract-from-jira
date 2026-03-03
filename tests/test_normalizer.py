from extractor.domain import BaseName
from extractor.normalizer import JiraNormalizer


def test_normalize_api_issues_maps_core_fields() -> None:
    normalizer = JiraNormalizer()
    issues = [
        {
            "key": "TEL-1",
            "fields": {
                "summary": "Teste",
                "status": {"name": "ENCERRADO"},
                "created": "2026-03-01T01:00:00.000+0000",
                "updated": "2026-03-01T03:00:00.000+0000",
                "customfield_data": "2026-03-01",
                "customfield_espaco": {"value": "Atendimento Ouv"},
                "customfield_tipo": {"value": "ATENDIMENTO"},
            },
        }
    ]

    field_ids = {
        "DATA FECHOU SALESFORCE": "customfield_data",
        "DATA ÚLTIMA ANÁLISE": "customfield_data_an",
        "DATA ABERTURA": "customfield_data_ing",
        "Espaço": "customfield_espaco",
        "Tipo do ticket": "customfield_tipo",
    }

    envelope = normalizer.normalize_api_issues(
        base=BaseName.ENCERRADAS,
        issues=issues,
        field_ids=field_ids,
        extracted_at_iso="2026-03-03T10:00:00+00:00",
    )

    assert envelope.source_mode.value == "api"
    assert envelope.records[0]["issue_key"] == "TEL-1"
    assert envelope.records[0]["data_referencia"] == "2026-03-01"
    assert envelope.records[0]["espaco"] == "Atendimento Ouv"
