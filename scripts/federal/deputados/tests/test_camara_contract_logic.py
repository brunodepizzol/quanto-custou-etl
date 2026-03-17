import json
import shutil
from pathlib import Path
from typing import cast

from ..camara_common import JsonDict
from ..camara_domain import (
    build_entity_contract,
    build_resumos_deputados_from_month_entities,
    daily_summary_from_highlights,
)
from ..camara_validate import (
    _validate_home_insight_catalog_semantics,
    _validate_insight_item,
    _validate_overview_schema,
)


def expect_runtime_error(fn, expected_substring: str) -> None:
    try:
        fn()
    except RuntimeError as exc:
        message = str(exc)
        assert expected_substring in message, message
        return
    raise AssertionError(f"expected RuntimeError containing: {expected_substring}")


def test_build_entity_contract_is_canonical_only() -> None:
    entity = build_entity_contract(
        {
            "id": 1,
            "name": "Nome Canonico",
            "stateCode": "SP",
            "party": "ABC",
            "photoUrl": "https://example.com/photo.jpg",
            "amountNet": 12.34,
        }
    )
    assert entity["name"] == "Nome Canonico"
    assert entity["stateCode"] == "SP"
    assert entity["party"] == "ABC"
    assert entity["photoUrl"] == "https://example.com/photo.jpg"
    assert entity["amountGross"] == 12.34
    assert entity["amountAdjustments"] == 0.0

    non_canonical_only = build_entity_contract(
        {
            "id": 2,
            "fullName": "Fora do contrato",
            "regionCode": "RJ",
            "partyCode": "LEG",
            "imageUrl": "https://example.com/noncanonical.jpg",
            "netValue": 99.0,
        }
    )
    assert non_canonical_only["name"] is None
    assert non_canonical_only["stateCode"] is None
    assert non_canonical_only["party"] is None
    assert non_canonical_only["photoUrl"] is None
    assert non_canonical_only["amountNet"] == 0.0


def test_daily_summary_requires_canonical_daily_summary_fields() -> None:
    canonical = daily_summary_from_highlights(
        {
            "dailyTotals": {
                "referenceDate": "2026-03-11",
                "amountNet": 194.15,
                "previousAmountNet": 120.0,
                "deltaAmountNet": 74.15,
                "deltaPercent": 61.79,
                "deltaDirection": "up",
                "trend7d": [{"date": "2026-03-11", "amountNet": 194.15}],
            },
            "dailyTopExpense": {
                "referenceDate": "2026-03-11",
                "entity": {"id": 1, "name": "Deputado"},
                "amountNet": 80.0,
                "supplier": "Fornecedor",
            },
        }
    )
    assert canonical is not None
    assert canonical["referenceDate"] == "2026-03-11"
    assert canonical["totals"]["amountNet"] == 194.15
    assert canonical["topExpense"]["amountNet"] == 80.0

    unsupported_shape = daily_summary_from_highlights(
        {
            "unexpectedDailySummary": {
                "referenceDate": "2026-03-11",
                "totalAmountNet": 194.15,
                "topAmountNet": 80.0,
            }
        }
    )
    assert unsupported_shape is None


def test_validate_overview_schema_rejects_non_canonical_highlight_keys() -> None:
    valid_overview: JsonDict = {
        "meta": {
            "scope": "federal/camara",
            "type": "overview",
            "period": "month-2026-03",
            "generatedAt": "2026-03-13T00:00:00Z",
            "schemaVersion": "2.2.2",
        },
        "base": {
            "amountNet": 10.0,
            "amountGross": 10.0,
            "amountAdjustments": 0.0,
            "recordsCount": 1,
            "entitiesCount": 1,
            "entitiesWithSpending": 1,
            "entitiesWithoutSpending": 0,
        },
        "highlights": {
            "topSpender": {},
            "topCategories": [],
            "topStates": [],
            "concentrationTop10": {},
            "averages": {},
            "dailyTotals": {"referenceDate": "2026-03-11", "amountNet": 10.0},
            "dailyTopExpense": {"referenceDate": "2026-03-11", "entity": {}, "amountNet": 5.0},
        },
    }
    _validate_overview_schema(Path("overview.json"), valid_overview, "2.2.2")

    invalid_overview = {
        **valid_overview,
        "highlights": {
            **cast(JsonDict, valid_overview["highlights"]),
            "unexpectedDailySummary": {"referenceDate": "2026-03-11"},
        },
    }
    expect_runtime_error(
        lambda: _validate_overview_schema(Path("overview.json"), invalid_overview, "2.2.2"),
        "highlights keys must match the canonical overview contract",
    )


def test_validate_home_insight_catalog_semantics() -> None:
    valid_items = [
        {
            "id": "federal-deputies-day-total-amount",
            "type": "aggregate",
            "level": "federal",
            "period": "day",
            "tag": "X",
            "title": "X",
            "context": "Y",
            "source": "Z",
            "enabled": True,
            "priority": 1,
            "weight": 0.9,
            "qualityScore": 0.9,
            "generatedAt": "2026-03-13T00:00:00Z",
            "freshUntil": "2026-03-14T00:00:00Z",
            "value": {"amount": 1, "currency": "BRL"},
            "variationText": "Base: 1",
        },
        {
            "id": "federal-deputies-day-top-spender",
            "type": "person",
            "level": "federal",
            "period": "day",
            "tag": "X",
            "title": "X",
            "context": "Y",
            "source": "Z",
            "enabled": True,
            "priority": 1,
            "weight": 0.9,
            "qualityScore": 0.9,
            "generatedAt": "2026-03-13T00:00:00Z",
            "freshUntil": "2026-03-14T00:00:00Z",
            "entity": {"id": 1, "name": "A"},
            "value": {"amount": 1, "currency": "BRL"},
        },
        {
            "id": "federal-deputies-day-vs-previous-day",
            "type": "comparison",
            "level": "federal",
            "period": "day",
            "tag": "X",
            "title": "X",
            "context": "Y",
            "source": "Z",
            "enabled": True,
            "priority": 1,
            "weight": 0.9,
            "qualityScore": 0.9,
            "generatedAt": "2026-03-13T00:00:00Z",
            "freshUntil": "2026-03-14T00:00:00Z",
            "left": {"amount": 1},
            "right": {"amount": 1},
            "delta": {"amount": 0, "percent": 0, "direction": "flat"},
            "variationText": "Dia anterior: R$ 1,00",
        },
        {
            "id": "federal-deputies-day-top-share",
            "type": "comparison",
            "level": "federal",
            "period": "day",
            "tag": "X",
            "title": "X",
            "context": "Y",
            "source": "Z",
            "enabled": True,
            "priority": 1,
            "weight": 0.9,
            "qualityScore": 0.9,
            "generatedAt": "2026-03-13T00:00:00Z",
            "freshUntil": "2026-03-14T00:00:00Z",
            "left": {"amount": 1},
            "right": {"amount": 1},
            "delta": {"amount": 0, "percent": 0, "direction": "flat"},
            "variationText": "Valor líder: R$ 1,00",
        },
        {
            "id": "federal-deputies-rolling7d-trend",
            "type": "timeline",
            "level": "federal",
            "period": "rolling7d",
            "tag": "X",
            "title": "X",
            "context": "Y",
            "source": "Z",
            "enabled": True,
            "priority": 1,
            "weight": 0.9,
            "qualityScore": 0.9,
            "generatedAt": "2026-03-13T00:00:00Z",
            "freshUntil": "2026-03-14T00:00:00Z",
            "series": [{"date": "2026-03-11", "amount": 1}],
            "variationText": "Janela 7d: R$ 1,00",
        },
    ]
    for idx, item in enumerate(valid_items):
        _validate_insight_item(item, idx)
    _validate_home_insight_catalog_semantics(valid_items)

    invalid_items = [dict(item) for item in valid_items]
    invalid_items[-1]["type"] = "aggregate"
    expect_runtime_error(
        lambda: _validate_home_insight_catalog_semantics(invalid_items),
        "federal-deputies-rolling7d-trend type must be timeline",
    )


def test_build_resumos_from_month_entities_accumulates_canonical_totals() -> None:
    deputados = [
        {
            "id": 101,
            "nome": "Deputado Teste",
            "siglaUf": "SP",
            "siglaPartido": "ABC",
            "urlFoto": "https://example.com/photo.jpg",
        }
    ]
    month_payload = {
        "meta": {"scope": "federal/camara", "type": "entities", "period": "month-2026-03", "generatedAt": "2026-03-13T00:00:00Z", "schemaVersion": "2.2.2"},
        "items": [
            {
                "id": 101,
                "name": "Deputado Teste",
                "stateCode": "SP",
                "party": "ABC",
                "photoUrl": "https://example.com/photo.jpg",
                "amountNet": 150.0,
                "amountGross": 170.0,
                "amountAdjustments": -20.0,
                "recordsCount": 3,
                "byCategoryNet": {"TRANSPORTE": 100.0, "HOSPEDAGEM": 50.0},
                "byCategoryGross": {"TRANSPORTE": 120.0, "HOSPEDAGEM": 50.0},
                "byCategoryAdjustments": {"TRANSPORTE": -20.0, "HOSPEDAGEM": 0.0},
            }
        ],
    }
    tmp_dir = Path(__file__).resolve().parents[4] / "tmp-test-camara-contract"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    try:
        month_file = tmp_dir / "entities" / "month-2026-03" / "entities.json"
        month_file.parent.mkdir(parents=True, exist_ok=True)
        month_file.write_text(json.dumps(month_payload), encoding="utf-8")
        resumos = build_resumos_deputados_from_month_entities(deputados, [month_file], 2023)
    finally:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)

    resumo = resumos[101]
    assert resumo["mandateTotals"]["amountNet"] == 150.0
    assert resumo["mandateTotals"]["amountGross"] == 170.0
    assert resumo["mandateTotals"]["amountAdjustments"] == -20.0
    assert resumo["mandateTotals"]["recordsCount"] == 3
    assert resumo["yearTotals"]["2026"] == 150.0
    assert resumo["monthTotals"]["2026-03"] == 150.0
    assert resumo["topCategory"]["category"] == "TRANSPORTE"
    assert resumo["topCategory"]["amountNet"] == 100.0


def main() -> None:
    test_build_entity_contract_is_canonical_only()
    test_daily_summary_requires_canonical_daily_summary_fields()
    test_validate_overview_schema_rejects_non_canonical_highlight_keys()
    test_validate_home_insight_catalog_semantics()
    test_build_resumos_from_month_entities_accumulates_canonical_totals()
    print("OK: camara contract logic")


if __name__ == "__main__":
    main()
