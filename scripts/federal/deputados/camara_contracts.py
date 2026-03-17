from pathlib import Path
from typing import Dict, List, Optional, cast

from .camara_common import CategoryMap, JsonDict, artifact_schema_version, now_iso, period_key, safe_slug, write_json
from .camara_domain import CanonicalRow, DailySummary, ExpenseTypeAggregate, build_entity_contract


def build_entities_contract_from_rows(rows: List[CanonicalRow], pkey: str, generated_at: Optional[str] = None) -> JsonDict:
    items: List[JsonDict] = []
    for row in rows:
        totals = row.get("totais") or {}
        items.append(
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "stateCode": row.get("stateCode"),
                "party": row.get("party"),
                "photoUrl": row.get("photoUrl"),
                "amountNet": round(float(totals.get("amountNet", 0.0) or 0.0), 2),
                "amountGross": round(float(totals.get("amountGross", 0.0) or 0.0), 2),
                "amountAdjustments": round(float(totals.get("amountAdjustments", 0.0) or 0.0), 2),
                "recordsCount": int(totals.get("recordsCount", 0) or 0),
                "byCategoryNet": {k: round(float(v or 0.0), 2) for k, v in (totals.get("byCategoryNet") or {}).items()},
                "byCategoryGross": {k: round(float(v or 0.0), 2) for k, v in (totals.get("byCategoryGross") or {}).items()},
                "byCategoryAdjustments": {k: round(float(v or 0.0), 2) for k, v in (totals.get("byCategoryAdjustments") or {}).items()},
            }
        )
    return {
        "meta": {
            "scope": "federal/camara",
            "type": "entities",
            "period": pkey,
            "generatedAt": str(generated_at or now_iso()),
            "schemaVersion": artifact_schema_version(),
        },
        "items": items,
    }


def build_expense_types_contract(expense_type_rows: List[ExpenseTypeAggregate], pkey: str, generated_at: Optional[str] = None) -> JsonDict:
    items = [
        {
            "expenseType": item.get("expenseType"),
            "amountNet": round(float(item.get("amountNet", 0.0) or 0.0), 2),
            "amountGross": round(float(item.get("amountGross", 0.0) or 0.0), 2),
            "amountAdjustments": round(float(item.get("amountAdjustments", 0.0) or 0.0), 2),
            "recordsCount": int(item.get("recordsCount", 0) or 0),
        }
        for item in (expense_type_rows or [])
    ]
    return {
        "meta": {
            "scope": "federal/camara",
            "type": "analytics-expense-types",
            "period": pkey,
            "generatedAt": str(generated_at or now_iso()),
            "schemaVersion": artifact_schema_version(),
        },
        "items": items,
    }


def build_pending_categories_contract(expense_type_rows: List[ExpenseTypeAggregate], pendentes: List[str], pkey: str, generated_at: Optional[str] = None) -> JsonDict:
    pend_set = set(pendentes or [])
    items = [
        {
            "expenseType": item.get("expenseType"),
            "amountNet": round(float(item.get("amountNet", 0.0) or 0.0), 2),
            "recordsCount": int(item.get("recordsCount", 0) or 0),
        }
        for item in (expense_type_rows or [])
        if item.get("expenseType") in pend_set
    ]
    return {
        "meta": {
            "scope": "federal/camara",
            "type": "analytics-pending-categories",
            "period": pkey,
            "generatedAt": str(generated_at or now_iso()),
            "schemaVersion": artifact_schema_version(),
        },
        "items": items,
    }


def build_ranking_total_contract_from_rows(rows: List[CanonicalRow], pkey: str) -> JsonDict:
    active_rows = [row for row in rows if (row["totais"]["recordsCount"] > 0 or row["totais"]["amountNet"] > 0)]

    def map_item(row: CanonicalRow) -> JsonDict:
        totals = row.get("totais") or {}
        amount_net = round(float(totals.get("amountNet", 0.0) or 0.0), 2)
        return {
            "id": row.get("id"),
            "name": row.get("name"),
            "stateCode": row.get("stateCode"),
            "party": row.get("party"),
            "amountNet": amount_net,
            "amountGross": round(float(totals.get("amountGross", max(amount_net, 0.0)) or 0.0), 2),
            "amountAdjustments": round(float(totals.get("amountAdjustments", min(amount_net, 0.0)) or 0.0), 2),
            "recordsCount": int(totals.get("recordsCount", 0) or 0),
        }

    top = [map_item(row) for row in sorted(active_rows, key=lambda row: float((row.get("totais") or {}).get("amountNet", 0.0) or 0.0), reverse=True)[:10]]
    bottom = [map_item(row) for row in sorted(active_rows, key=lambda row: float((row.get("totais") or {}).get("amountNet", 0.0) or 0.0))[:10]]
    return {
        "meta": {
            "scope": "federal/camara",
            "type": "ranking-total",
            "period": pkey,
            "generatedAt": now_iso(),
            "schemaVersion": artifact_schema_version(),
        },
        "top": top,
        "bottom": bottom,
    }


def build_overview_from_rows(rows: List[CanonicalRow], periodo_meta: dict, cmap: CategoryMap, daily_summary: Optional[DailySummary] = None) -> JsonDict:
    categories = sorted({cat for _, cat in cmap.rules} | {cmap.default})

    total_amount_net = 0.0
    total_amount_gross = 0.0
    total_amount_adjustments = 0.0
    category_totals_net = {category: 0.0 for category in categories}
    category_totals_gross = {category: 0.0 for category in categories}
    category_totals_adjustments = {category: 0.0 for category in categories}
    state_totals: Dict[str, JsonDict] = {}
    total_records_count = 0

    active_rows = [row for row in rows if (row["totais"]["recordsCount"] > 0 or row["totais"]["amountNet"] > 0)]
    entities_count = len(rows)
    entities_with_spending = len(active_rows)
    entities_without_spending = max(0, entities_count - entities_with_spending)

    for row in active_rows:
        entity_amount_net = float(row["totais"].get("amountNet", 0.0))
        entity_amount_gross = float(row["totais"].get("amountGross", max(entity_amount_net, 0.0)))
        entity_amount_adjustments = float(row["totais"].get("amountAdjustments", min(entity_amount_net, 0.0)))
        total_amount_net += entity_amount_net
        total_amount_gross += entity_amount_gross
        total_amount_adjustments += entity_amount_adjustments
        total_records_count += int(row["totais"]["recordsCount"])
        state_code = row.get("stateCode") or "?"
        state_totals.setdefault(state_code, {"amountNet": 0.0, "amountGross": 0.0, "amountAdjustments": 0.0})
        state_totals[state_code]["amountNet"] += entity_amount_net
        state_totals[state_code]["amountGross"] += entity_amount_gross
        state_totals[state_code]["amountAdjustments"] += entity_amount_adjustments
        for category in categories:
            category_amount_net = float((row["totais"].get("byCategoryNet") or {}).get(category, 0.0))
            category_amount_gross = float((row["totais"].get("byCategoryGross") or {}).get(category, max(category_amount_net, 0.0)))
            category_amount_adjustments = float((row["totais"].get("byCategoryAdjustments") or {}).get(category, min(category_amount_net, 0.0)))
            category_totals_net[category] += category_amount_net
            category_totals_gross[category] += category_amount_gross
            category_totals_adjustments[category] += category_amount_adjustments

    top_spender = {}
    if active_rows:
        top_entity_row = max(active_rows, key=lambda row: row["totais"]["amountNet"])
        top_amount_net = float(top_entity_row["totais"].get("amountNet", 0.0))
        top_amount_gross = float(top_entity_row["totais"].get("amountGross", max(top_amount_net, 0.0)))
        top_amount_adjustments = float(top_entity_row["totais"].get("amountAdjustments", min(top_amount_net, 0.0)))
        top_spender = build_entity_contract(
            {
                "id": top_entity_row["id"],
                "name": top_entity_row["name"],
                "stateCode": top_entity_row["stateCode"],
                "party": top_entity_row["party"],
                "photoUrl": top_entity_row.get("photoUrl"),
                "amountNet": round(top_amount_net, 2),
                "amountGross": round(top_amount_gross, 2),
                "amountAdjustments": round(top_amount_adjustments, 2),
            }
        )

    top_categories_raw: List[JsonDict] = sorted(
        [
            {
                "category": category,
                "amountNet": float(category_totals_net[category]),
                "amountGross": float(category_totals_gross[category]),
                "amountAdjustments": float(category_totals_adjustments[category]),
            }
            for category in categories
        ],
        key=lambda item: float(item["amountNet"]),
        reverse=True,
    )
    top_n = 8
    top_categories_list = [
        {
            "category": item["category"],
            "amountNet": round(float(item["amountNet"]), 2),
            "amountGross": round(float(item["amountGross"]), 2),
            "amountAdjustments": round(float(item["amountAdjustments"]), 2),
        }
        for item in top_categories_raw[:top_n]
    ]
    top_categories_amount_net = sum(float(item["amountNet"]) for item in top_categories_raw[:top_n])
    top_categories_amount_gross = sum(float(item["amountGross"]) for item in top_categories_raw[:top_n])
    top_categories_amount_adjustments = sum(float(item["amountAdjustments"]) for item in top_categories_raw[:top_n])
    top_categories = top_categories_list + [
        {
            "category": "DEMAIS CATEGORIAS",
            "amountNet": round(float(total_amount_net) - float(top_categories_amount_net), 2),
            "amountGross": round(float(total_amount_gross) - float(top_categories_amount_gross), 2),
            "amountAdjustments": round(float(total_amount_adjustments) - float(top_categories_amount_adjustments), 2),
        }
    ]
    top_categories_contract: List[JsonDict] = [
        {
            "category": item.get("category"),
            "amountNet": round(float(item.get("amountNet", 0.0) or 0.0), 2),
            "amountGross": round(float(item.get("amountGross", 0.0) or 0.0), 2),
            "amountAdjustments": round(float(item.get("amountAdjustments", 0.0) or 0.0), 2),
        }
        for item in top_categories
    ]

    top_states: List[JsonDict] = [
        {
            "stateCode": state_code,
            "amountNet": round(float(values.get("amountNet", 0.0)), 2),
            "amountGross": round(float(values.get("amountGross", 0.0)), 2),
            "amountAdjustments": round(float(values.get("amountAdjustments", 0.0)), 2),
        }
        for state_code, values in sorted(state_totals.items(), key=lambda item: item[1]["amountNet"], reverse=True)[:12]
    ]

    top10_total = sum(float(row["totais"].get("amountNet", 0.0)) for row in sorted(active_rows, key=lambda item: float(item["totais"]["amountNet"]), reverse=True)[:10])
    top10_pct = (top10_total / total_amount_net * 100.0) if total_amount_net > 0 else 0.0
    average_per_entity = (total_amount_net / entities_with_spending) if entities_with_spending > 0 else 0.0
    average_per_record = (total_amount_net / total_records_count) if total_records_count > 0 else 0.0

    daily_reference = daily_summary.get("referenceDate") if daily_summary else None
    daily_totals = cast(JsonDict, daily_summary.get("totals") or {}) if daily_summary else {}
    daily_top_expense = cast(JsonDict, daily_summary.get("topExpense") or {}) if daily_summary else {}

    return {
        "meta": {
            "scope": "federal/camara",
            "type": "overview",
            "period": period_key(periodo_meta),
            "generatedAt": now_iso(),
            "schemaVersion": artifact_schema_version(),
        },
        "base": {
            "amountNet": round(total_amount_net, 2),
            "amountGross": round(total_amount_gross, 2),
            "amountAdjustments": round(total_amount_adjustments, 2),
            "recordsCount": int(total_records_count),
            "entitiesCount": int(entities_count),
            "entitiesWithSpending": int(entities_with_spending),
            "entitiesWithoutSpending": int(entities_without_spending),
        },
        "highlights": {
            "topSpender": top_spender,
            "topCategories": top_categories_contract,
            "topStates": top_states,
            "concentrationTop10": {"amountNet": round(top10_total, 2), "percent": round(top10_pct, 2)},
            "averages": {"perEntityWithSpending": round(average_per_entity, 2), "perRecord": round(average_per_record, 2)},
            "dailyTotals": {
                "referenceDate": daily_reference,
                "amountNet": round(float(daily_totals.get("amountNet", 0.0) or 0.0), 2),
                "previousAmountNet": round(float(daily_totals.get("previousAmountNet", 0.0) or 0.0), 2),
                "deltaAmountNet": round(float(daily_totals.get("deltaAmountNet", 0.0) or 0.0), 2),
                "deltaPercent": round(float(daily_totals.get("deltaPercent", 0.0) or 0.0), 2),
                "deltaDirection": str(daily_totals.get("deltaDirection") or "flat"),
                "trend7d": daily_totals.get("trend7d") if isinstance(daily_totals.get("trend7d"), list) else [],
            },
            "dailyTopExpense": {
                "referenceDate": daily_reference,
                "entity": daily_top_expense.get("entity") or {},
                "amountNet": round(float(daily_top_expense.get("amountNet", 0.0) or 0.0), 2),
                "category": daily_top_expense.get("category"),
                "expenseType": daily_top_expense.get("expenseType"),
                "supplier": daily_top_expense.get("supplier"),
                "documentType": daily_top_expense.get("documentType"),
                "documentUrl": daily_top_expense.get("documentUrl"),
            },
        },
    }


def period_contract_paths(out_dir: Path, pkey: str) -> Dict[str, Path]:
    base = out_dir / "federal/camara"
    return {
        "overview": base / f"overview/{pkey}/overview.json",
        "entities": base / f"entities/{pkey}/entities.json",
        "ranking": base / f"rankings/{pkey}/ranking-total.json",
        "expense_types": base / f"analytics/{pkey}/expense-types.json",
        "pending_categories": base / f"analytics/{pkey}/pending-categories.json",
    }


def write_period_contracts(
    out_dir: Path,
    pkey: str,
    overview_obj: JsonDict,
    entities_obj: JsonDict,
    ranking_obj: JsonDict,
    expense_types_obj: JsonDict,
    pending_categories_obj: JsonDict,
) -> None:
    paths = period_contract_paths(out_dir, pkey)
    write_json(paths["overview"], overview_obj)
    write_json(paths["entities"], entities_obj)
    write_json(paths["ranking"], ranking_obj)
    write_json(paths["expense_types"], expense_types_obj)
    write_json(paths["pending_categories"], pending_categories_obj)

def write_profiles_and_index(out_dir: Path, resumos: Dict[int, JsonDict]) -> None:
    profiles_dir = out_dir / "federal/camara/entities/profiles"
    index_items = []
    for dep_id, summary in sorted(resumos.items(), key=lambda kv: kv[0]):
        slug = safe_slug(summary.get("name") or str(dep_id)) or str(dep_id)
        profile = {
            "meta": {
                "scope": "federal/camara",
                "type": "entity-profile",
                "entityId": dep_id,
                "generatedAt": now_iso(),
                "schemaVersion": artifact_schema_version(),
            },
            "entity": {
                "id": dep_id,
                "name": summary.get("name"),
                "stateCode": summary.get("stateCode"),
                "party": summary.get("party"),
                "photoUrl": summary.get("photoUrl"),
                "slug": slug,
            },
            "mandateTotals": summary.get("mandateTotals") or {},
            "yearTotals": summary.get("yearTotals") or {},
            "monthTotals": summary.get("monthTotals") or {},
            "topCategory": summary.get("topCategory"),
        }
        write_json(profiles_dir / f"{dep_id}.json", profile)
        mandate_totals = summary.get("mandateTotals") or {}
        index_items.append(
            {
                "id": dep_id,
                "slug": slug,
                "name": summary.get("name"),
                "stateCode": summary.get("stateCode"),
                "party": summary.get("party"),
                "photoUrl": summary.get("photoUrl"),
                "amountNet": round(float(mandate_totals.get("amountNet", 0.0) or 0.0), 2),
                "recordsCount": int(mandate_totals.get("recordsCount", 0) or 0),
                "yearTotals": {str(key): round(float(value or 0.0), 2) for key, value in (summary.get("yearTotals") or {}).items()},
            }
        )
    write_json(
        out_dir / "federal/camara/entities/index.json",
        {
            "meta": {
                "scope": "federal/camara",
                "type": "entities-index",
                "generatedAt": now_iso(),
                "schemaVersion": artifact_schema_version(),
                "itemsCount": len(index_items),
            },
            "items": index_items,
        },
    )
