import re
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, NotRequired, Optional, Tuple, TypedDict, cast

from .camara_common import (
    CategoryMap,
    JsonDict,
    all_categories,
    categorize,
    normalize_doc_date,
    now_iso,
    parse_iso_utc,
    read_json,
    split_financial_values,
)
from .camara_source import fetch_despesas


class EntityIdentity(TypedDict):
    id: int
    name: Optional[str]
    stateCode: Optional[str]
    party: Optional[str]
    photoUrl: Optional[str]


class CanonicalTotals(TypedDict):
    amountNet: float
    amountGross: float
    amountAdjustments: float
    recordsCount: int
    byCategoryNet: Dict[str, float]
    byCategoryGross: Dict[str, float]
    byCategoryAdjustments: Dict[str, float]


class CanonicalRow(EntityIdentity):
    totais: CanonicalTotals


class ExpenseTypeAggregate(TypedDict):
    expenseType: str
    amountNet: float
    amountGross: float
    amountAdjustments: float
    recordsCount: int


class DailySeriesPoint(TypedDict):
    date: str
    amountNet: float


class DailyTotalsSummary(TypedDict):
    amountNet: float
    previousAmountNet: float
    deltaAmountNet: float
    deltaPercent: float
    deltaDirection: str
    trend7d: List[DailySeriesPoint]


class DailyTopExpenseSummary(TypedDict):
    entity: JsonDict
    amountNet: float
    category: NotRequired[Optional[str]]
    expenseType: NotRequired[Optional[str]]
    supplier: NotRequired[Optional[str]]
    documentType: NotRequired[Optional[str]]
    documentUrl: NotRequired[Optional[str]]


class DailySummary(TypedDict):
    referenceDate: str
    totals: NotRequired[DailyTotalsSummary]
    topExpense: NotRequired[DailyTopExpenseSummary]


def build_canonical_identity(dep_id: int, name: Optional[str], state_code: Optional[str], party: Optional[str], photo_url: Optional[str]) -> EntityIdentity:
    return {
        "id": dep_id,
        "name": name,
        "stateCode": state_code,
        "party": party,
        "photoUrl": photo_url,
    }


def build_canonical_identity_from_source(dep: Mapping[str, Any]) -> EntityIdentity:
    return build_canonical_identity(
        dep_id=int(dep.get("id") or 0),
        name=dep.get("nome") or dep.get("nomeCivil") or dep.get("nomeParlamentar"),
        state_code=dep.get("siglaUf") or dep.get("uf"),
        party=dep.get("siglaPartido") or dep.get("partido"),
        photo_url=dep.get("urlFoto"),
    )


def empty_totals_bucket(categories: Optional[List[str]] = None) -> CanonicalTotals:
    by_category_net = {}
    by_category_gross = {}
    by_category_adjustments = {}
    for category in (categories or []):
        by_category_net[category] = 0.0
        by_category_gross[category] = 0.0
        by_category_adjustments[category] = 0.0
    return {
        "amountNet": 0.0,
        "amountGross": 0.0,
        "amountAdjustments": 0.0,
        "recordsCount": 0,
        "byCategoryNet": by_category_net,
        "byCategoryGross": by_category_gross,
        "byCategoryAdjustments": by_category_adjustments,
    }


def add_amounts_to_totals(totals: CanonicalTotals, amount_net: float, amount_gross: float, amount_adjustments: float, records_count: int) -> None:
    totals["amountNet"] = float(totals.get("amountNet", 0.0) or 0.0) + float(amount_net or 0.0)
    totals["amountGross"] = float(totals.get("amountGross", 0.0) or 0.0) + float(amount_gross or 0.0)
    totals["amountAdjustments"] = float(totals.get("amountAdjustments", 0.0) or 0.0) + float(amount_adjustments or 0.0)
    totals["recordsCount"] = int(totals.get("recordsCount", 0) or 0) + int(records_count or 0)


def add_category_amounts_to_totals(
    totals: CanonicalTotals,
    by_category_net: Optional[Mapping[str, float]] = None,
    by_category_gross: Optional[Mapping[str, float]] = None,
    by_category_adjustments: Optional[Mapping[str, float]] = None,
) -> None:
    target_net = totals.setdefault("byCategoryNet", {})
    target_gross = totals.setdefault("byCategoryGross", {})
    target_adjustments = totals.setdefault("byCategoryAdjustments", {})
    for key, value in (by_category_net or {}).items():
        target_net[key] = float(target_net.get(key, 0.0) or 0.0) + float(value or 0.0)
    for key, value in (by_category_gross or {}).items():
        target_gross[key] = float(target_gross.get(key, 0.0) or 0.0) + float(value or 0.0)
    for key, value in (by_category_adjustments or {}).items():
        target_adjustments[key] = float(target_adjustments.get(key, 0.0) or 0.0) + float(value or 0.0)


def finalize_totals_bucket(totals: CanonicalTotals) -> CanonicalTotals:
    return {
        "amountNet": round(float(totals.get("amountNet", 0.0) or 0.0), 2),
        "amountGross": round(float(totals.get("amountGross", 0.0) or 0.0), 2),
        "amountAdjustments": round(float(totals.get("amountAdjustments", 0.0) or 0.0), 2),
        "recordsCount": int(totals.get("recordsCount", 0) or 0),
        "byCategoryNet": {k: round(float(v or 0.0), 2) for k, v in (totals.get("byCategoryNet") or {}).items()},
        "byCategoryGross": {k: round(float(v or 0.0), 2) for k, v in (totals.get("byCategoryGross") or {}).items()},
        "byCategoryAdjustments": {k: round(float(v or 0.0), 2) for k, v in (totals.get("byCategoryAdjustments") or {}).items()},
    }


def new_canonical_row(identity: EntityIdentity, categories: Optional[List[str]] = None) -> CanonicalRow:
    return {
        "id": int(identity.get("id") or 0),
        "name": identity.get("name"),
        "stateCode": identity.get("stateCode"),
        "party": identity.get("party"),
        "photoUrl": identity.get("photoUrl"),
        "totais": empty_totals_bucket(categories),
    }


def build_entity_contract(raw: Mapping[str, Any]) -> JsonDict:
    src = raw or {}
    amount_net = round(float(src.get("amountNet", 0.0) or 0.0), 2)
    amount_gross = round(float(src.get("amountGross", max(amount_net, 0.0)) or 0.0), 2)
    amount_adjustments = round(float(src.get("amountAdjustments", min(amount_net, 0.0)) or 0.0), 2)
    return {
        "id": src.get("id"),
        "name": src.get("name"),
        "stateCode": src.get("stateCode"),
        "party": src.get("party"),
        "photoUrl": src.get("photoUrl"),
        "amountNet": amount_net,
        "amountGross": amount_gross,
        "amountAdjustments": amount_adjustments,
    }


def build_month_aggregates(
    deputados: List[JsonDict],
    ano: int,
    mes: int,
    cmap: CategoryMap,
    sleep_s: float = 0.05,
) -> Tuple[List[CanonicalRow], List[ExpenseTypeAggregate], List[str], Optional[DailySummary]]:
    dep_map: Dict[int, JsonDict] = {int(d["id"]): d for d in deputados}
    cats = all_categories(cmap)

    expense_type_summary: Dict[str, ExpenseTypeAggregate] = {}
    pendentes_set: set[str] = set()
    max_dia_por_data: Dict[str, DailySummary] = {}
    total_dia_por_data: Dict[str, float] = {}

    rows: List[CanonicalRow] = []
    for index, dep in enumerate(deputados, start=1):
        dep_id = int(dep["id"])
        despesas = fetch_despesas(dep_id, ano, mes)
        identity = build_canonical_identity(
            dep_id=dep_id,
            name=dep_map[dep_id].get("nome"),
            state_code=dep_map[dep_id].get("siglaUf"),
            party=dep_map[dep_id].get("siglaPartido"),
            photo_url=dep_map[dep_id].get("urlFoto"),
        )
        row = new_canonical_row(identity, cats)
        totals = row["totais"]

        for despesa in despesas:
            value_gross, value_adjustments, value_net = split_financial_values(despesa)
            add_amounts_to_totals(totals, value_net, value_gross, value_adjustments, 1)

            expense_type = (despesa.get("tipoDespesa") or "").strip() or "(Sem tipo)"
            category = categorize(expense_type, cmap)
            add_category_amounts_to_totals(
                totals,
                by_category_net={category: value_net},
                by_category_gross={category: value_gross},
                by_category_adjustments={category: value_adjustments},
            )

            data_doc = normalize_doc_date(despesa.get("dataDocumento"))
            if data_doc:
                total_dia_por_data[data_doc] = float(total_dia_por_data.get(data_doc, 0.0)) + float(value_net)
                candidate: DailySummary = {
                    "referenceDate": data_doc,
                    "topExpense": {
                        "entity": build_entity_contract(identity),
                        "amountNet": round(value_net, 2),
                        "category": category,
                        "expenseType": expense_type,
                        "supplier": (despesa.get("nomeFornecedor") or "").strip() or None,
                        "documentType": (despesa.get("tipoDocumento") or "").strip() or None,
                        "documentUrl": (despesa.get("urlDocumento") or "").strip() or None,
                    },
                }
                previous = max_dia_por_data.get(data_doc)
                previous_top: JsonDict = cast(JsonDict, (previous.get("topExpense") or {}) if previous else {})
                previous_amount = float(previous_top.get("amountNet", 0.0) or 0.0)
                candidate_top: JsonDict = cast(JsonDict, candidate.get("topExpense") or {})
                candidate_amount = float(candidate_top.get("amountNet", 0.0) or 0.0)
                if (not previous) or (candidate_amount > previous_amount):
                    max_dia_por_data[data_doc] = candidate

            bucket = expense_type_summary.setdefault(
                expense_type,
                {
                    "expenseType": expense_type,
                    "amountNet": 0.0,
                    "amountGross": 0.0,
                    "amountAdjustments": 0.0,
                    "recordsCount": 0,
                },
            )
            bucket["amountNet"] += value_net
            bucket["amountGross"] += value_gross
            bucket["amountAdjustments"] += value_adjustments
            bucket["recordsCount"] += 1
            if category == cmap.default:
                pendentes_set.add(expense_type)

        row["totais"] = finalize_totals_bucket(totals)
        rows.append(row)

        if sleep_s > 0:
            time.sleep(sleep_s)

        if index % 50 == 0:
            print(f"[{ano}-{mes:02d}] processados {index}/{len(deputados)} deputados...")

    expense_type_rows = list(expense_type_summary.values())
    for item in expense_type_rows:
        item["amountNet"] = round(float(item.get("amountNet", 0.0)), 2)
        item["amountGross"] = round(float(item.get("amountGross", 0.0)), 2)
        item["amountAdjustments"] = round(float(item.get("amountAdjustments", 0.0)), 2)
    expense_type_rows.sort(key=lambda x: x["amountNet"], reverse=True)

    daily_summary: Optional[DailySummary] = None
    if max_dia_por_data:
        data_mais_recente = max(max_dia_por_data.keys())
        daily_summary = cast(DailySummary, dict(max_dia_por_data.get(data_mais_recente) or {}))
        top_expense = cast(DailyTopExpenseSummary, daily_summary.get("topExpense") or {})
        total_dia = round(float(total_dia_por_data.get(data_mais_recente, top_expense.get("amountNet", 0.0)) or 0.0), 2)
        datas_ordenadas = sorted(total_dia_por_data.keys())
        idx_data = datas_ordenadas.index(data_mais_recente) if data_mais_recente in datas_ordenadas else -1
        data_anterior = datas_ordenadas[idx_data - 1] if idx_data > 0 else None
        total_dia_anterior = round(float(total_dia_por_data.get(data_anterior, 0.0) or 0.0), 2) if data_anterior else 0.0
        delta = round(total_dia - total_dia_anterior, 2)
        pct = round((delta / total_dia_anterior * 100.0), 2) if total_dia_anterior > 0 else 0.0
        direction = "up" if delta > 0 else ("down" if delta < 0 else "flat")
        trend_dates = datas_ordenadas[-7:]
        trend7d: List[DailySeriesPoint] = [{"date": date, "amountNet": round(float(total_dia_por_data.get(date, 0.0) or 0.0), 2)} for date in trend_dates]
        daily_summary["totals"] = {
            "amountNet": total_dia,
            "previousAmountNet": total_dia_anterior,
            "deltaAmountNet": delta,
            "deltaPercent": pct,
            "deltaDirection": direction,
            "trend7d": trend7d,
        }

    pendentes = sorted(p for p in pendentes_set if p not in ("(Sem tipo)",))
    return rows, expense_type_rows, pendentes, daily_summary


def list_month_entity_periods(out_dir: Path) -> List[Tuple[int, int, Path]]:
    base = out_dir / "federal/camara/entities"
    out: List[Tuple[int, int, Path]] = []
    if not base.exists():
        return out
    for path in sorted(base.glob("month-*/entities.json")):
        try:
            match = re.match(r"^month-(\d{4})-(\d{2})$", path.parent.name)
            if not match:
                continue
            year = int(match.group(1))
            month = int(match.group(2))
            if month < 1 or month > 12:
                continue
            out.append((year, month, path))
        except Exception:
            continue
    return out


def sum_entity_files(entity_files: List[Path]) -> List[CanonicalRow]:
    summed: Dict[int, CanonicalRow] = {}
    for file_path in entity_files:
        obj = read_json(file_path) or {}
        for item in cast(List[JsonDict], obj.get("items") or []):
            dep_id_raw = item.get("id")
            if dep_id_raw is None:
                continue
            try:
                dep_id = int(dep_id_raw)
            except Exception:
                continue
            if dep_id not in summed:
                summed[dep_id] = new_canonical_row(
                    build_canonical_identity(
                        dep_id=dep_id,
                        name=item.get("name"),
                        state_code=item.get("stateCode"),
                        party=item.get("party"),
                        photo_url=item.get("photoUrl"),
                    )
                )
            summed_row = summed[dep_id]
            summed_row["name"] = summed_row.get("name") or item.get("name")
            summed_row["stateCode"] = summed_row.get("stateCode") or item.get("stateCode")
            summed_row["party"] = summed_row.get("party") or item.get("party")
            summed_row["photoUrl"] = summed_row.get("photoUrl") or item.get("photoUrl")
            totals = summed_row["totais"]

            amount_net = float(item.get("amountNet", 0.0) or 0.0)
            amount_gross = float(item.get("amountGross", max(amount_net, 0.0)) or 0.0)
            amount_adjustments = float(item.get("amountAdjustments", min(amount_net, 0.0)) or 0.0)
            add_amounts_to_totals(totals, amount_net, amount_gross, amount_adjustments, int(item.get("recordsCount", 0) or 0))
            add_category_amounts_to_totals(
                totals,
                by_category_net=(item.get("byCategoryNet") or {}),
                by_category_gross=(item.get("byCategoryGross") or {}),
                by_category_adjustments=(item.get("byCategoryAdjustments") or {}),
            )

    out = []
    for row in summed.values():
        row["totais"] = finalize_totals_bucket(row["totais"])
        out.append(row)
    return out


def daily_summary_from_highlights(highlights: Mapping[str, Any]) -> Optional[DailySummary]:
    if not isinstance(highlights, dict):
        return None
    daily_totals = cast(Mapping[str, Any], highlights.get("dailyTotals") or {})
    daily_top = cast(Mapping[str, Any], highlights.get("dailyTopExpense") or {})
    ref = str(daily_totals.get("referenceDate") or daily_top.get("referenceDate") or "").strip()
    if not ref:
        return None
    trend7d_raw = daily_totals.get("trend7d")
    trend7d: List[DailySeriesPoint] = []
    if isinstance(trend7d_raw, list):
        for point in trend7d_raw:
            if not isinstance(point, dict):
                continue
            trend7d.append(
                {
                    "date": str(point.get("date") or ""),
                    "amountNet": round(float(point.get("amountNet", 0.0) or 0.0), 2),
                }
            )
    return {
        "referenceDate": ref,
        "totals": {
            "amountNet": round(float(daily_totals.get("amountNet", 0.0) or 0.0), 2),
            "previousAmountNet": round(float(daily_totals.get("previousAmountNet", 0.0) or 0.0), 2),
            "deltaAmountNet": round(float(daily_totals.get("deltaAmountNet", 0.0) or 0.0), 2),
            "deltaPercent": round(float(daily_totals.get("deltaPercent", 0.0) or 0.0), 2),
            "deltaDirection": str(daily_totals.get("deltaDirection") or "flat"),
            "trend7d": trend7d,
        },
        "topExpense": {
            "entity": cast(JsonDict, daily_top.get("entity") or {}),
            "amountNet": round(float(daily_top.get("amountNet", 0.0) or 0.0), 2),
            "category": daily_top.get("category"),
            "expenseType": daily_top.get("expenseType"),
            "supplier": daily_top.get("supplier"),
            "documentType": daily_top.get("documentType"),
            "documentUrl": daily_top.get("documentUrl"),
        },
    }


def pick_latest_daily_insight_from_month_overview_files(overview_files: List[Path]) -> Optional[DailySummary]:
    latest: Optional[DailySummary] = None
    latest_date: Optional[str] = None
    for overview_file in overview_files:
        obj = read_json(overview_file) or {}
        daily = daily_summary_from_highlights((obj.get("highlights") or {}))
        if daily is None:
            continue
        ref = str(daily.get("referenceDate") or "").strip()
        if not ref:
            continue
        normalized_date = normalize_doc_date(ref)
        if not normalized_date:
            continue
        daily_top: JsonDict = cast(JsonDict, daily.get("topExpense") or {})
        score = float(daily_top.get("amountNet", 0.0) or 0.0)
        if (latest_date is None) or (normalized_date > latest_date):
            latest_date = normalized_date
            latest = daily
            continue
        latest_top: JsonDict = cast(JsonDict, (latest.get("topExpense") or {}) if latest else {})
        latest_score = float(latest_top.get("amountNet", 0.0) or 0.0)
        if normalized_date == latest_date and score > latest_score:
            latest = daily
    return latest


def build_resumos_deputados_from_month_entities(
    deputados: List[JsonDict],
    month_entity_files: List[Path],
    mandate_start_year: int,
) -> Dict[int, dict]:
    dep_map: Dict[int, EntityIdentity] = {}
    for deputado in deputados:
        try:
            dep_id = int(deputado.get("id") or 0)
        except Exception:
            continue
        dep_map[dep_id] = build_canonical_identity_from_source(deputado)

    acc: Dict[int, dict] = {}

    def ensure(dep_id: int) -> dict:
        if dep_id not in acc:
            base = dep_map.get(dep_id, {"id": dep_id})
            acc[dep_id] = {
                "id": dep_id,
                "name": base.get("name"),
                "stateCode": base.get("stateCode"),
                "party": base.get("party"),
                "photoUrl": base.get("photoUrl"),
                "mandateTotals": empty_totals_bucket(),
                "yearTotals": {},
                "monthTotals": {},
            }
        return acc[dep_id]

    for entity_file in month_entity_files:
        try:
            parts = entity_file.parts
            entity_idx = parts.index("entities")
            period_name = str(parts[entity_idx + 1])
            match = re.match(r"^month-(\d{4})-(\d{2})$", period_name)
            if not match:
                continue
            year = int(match.group(1))
            month = int(match.group(2))
        except Exception:
            year = None
            month = None

        obj = read_json(entity_file)
        if not obj:
            continue
        rows = obj.get("items") or []
        ym_key = f"{year:04d}-{month:02d}" if year and month else None

        for row in rows:
            dep_id = row.get("id")
            if dep_id is None:
                continue
            try:
                dep_id = int(dep_id)
            except Exception:
                continue

            acc_row = ensure(dep_id)
            acc_row["name"] = acc_row.get("name") or row.get("name")
            acc_row["stateCode"] = acc_row.get("stateCode") or row.get("stateCode")
            acc_row["party"] = acc_row.get("party") or row.get("party")
            acc_row["photoUrl"] = acc_row.get("photoUrl") or row.get("photoUrl")

            amount_net = float(row.get("amountNet", 0.0) or 0.0)
            amount_gross = float(row.get("amountGross", max(amount_net, 0.0)) or 0.0)
            amount_adjustments = float(row.get("amountAdjustments", min(amount_net, 0.0)) or 0.0)
            records_count = int(row.get("recordsCount") or 0)
            add_amounts_to_totals(acc_row["mandateTotals"], amount_net, amount_gross, amount_adjustments, records_count)
            add_category_amounts_to_totals(
                acc_row["mandateTotals"],
                by_category_net=(row.get("byCategoryNet") or {}),
                by_category_gross=(row.get("byCategoryGross") or {}),
                by_category_adjustments=(row.get("byCategoryAdjustments") or {}),
            )

            if year is not None and year >= mandate_start_year:
                acc_row["yearTotals"][str(year)] = float(acc_row["yearTotals"].get(str(year)) or 0.0) + amount_net
            if ym_key:
                acc_row["monthTotals"][ym_key] = float(acc_row["monthTotals"].get(ym_key) or 0.0) + amount_net

    for summary in acc.values():
        mandate_totals = finalize_totals_bucket(summary["mandateTotals"])
        summary["mandateTotals"] = mandate_totals
        summary["yearTotals"] = {key: round(float(value), 2) for key, value in (summary.get("yearTotals") or {}).items()}
        summary["monthTotals"] = {key: round(float(value), 2) for key, value in (summary.get("monthTotals") or {}).items()}

        by_category_net = mandate_totals.get("byCategoryNet") or {}
        if by_category_net:
            category, value = max(by_category_net.items(), key=lambda kv: kv[1])
            total_amount = mandate_totals.get("amountNet") or 0.0
            pct = (float(value) / float(total_amount) * 100.0) if total_amount else 0.0
            summary["topCategory"] = {"category": category, "amountNet": round(float(value), 2), "percent": round(pct, 2)}
        else:
            summary["topCategory"] = None

    return acc


def aggregate_analytics_from_month_periods(out_dir: Path, month_pkeys: List[str]) -> Tuple[List[ExpenseTypeAggregate], List[str], str]:
    merged: Dict[str, ExpenseTypeAggregate] = {}
    pend_set: set[str] = set()
    latest_generated_at: Optional[str] = None
    latest_generated_dt = None

    for month_key in month_pkeys:
        expense_types_obj = read_json(out_dir / f"federal/camara/analytics/{month_key}/expense-types.json") or {}
        pending_obj = read_json(out_dir / f"federal/camara/analytics/{month_key}/pending-categories.json") or {}

        generated_expense = parse_iso_utc((expense_types_obj.get("meta") or {}).get("generatedAt"))
        if generated_expense and (latest_generated_dt is None or generated_expense > latest_generated_dt):
            latest_generated_dt = generated_expense
            latest_generated_at = generated_expense.isoformat().replace("+00:00", "Z")
        generated_pending = parse_iso_utc((pending_obj.get("meta") or {}).get("generatedAt"))
        if generated_pending and (latest_generated_dt is None or generated_pending > latest_generated_dt):
            latest_generated_dt = generated_pending
            latest_generated_at = generated_pending.isoformat().replace("+00:00", "Z")

        for item in cast(List[JsonDict], expense_types_obj.get("items", []) or []):
            expense_type = str(item.get("expenseType") or "").strip()
            if not expense_type:
                continue
            current = merged.setdefault(
                expense_type,
                {
                    "expenseType": expense_type,
                    "amountNet": 0.0,
                    "amountGross": 0.0,
                    "amountAdjustments": 0.0,
                    "recordsCount": 0,
                },
            )
            current["amountNet"] += float(item.get("amountNet", 0.0) or 0.0)
            current["amountGross"] += float(item.get("amountGross", 0.0) or 0.0)
            current["amountAdjustments"] += float(item.get("amountAdjustments", 0.0) or 0.0)
            current["recordsCount"] += int(item.get("recordsCount", 0) or 0)

        for item in cast(List[JsonDict], pending_obj.get("items", []) or []):
            expense_type = str(item.get("expenseType") or "").strip()
            if expense_type:
                pend_set.add(expense_type)

    rows: List[ExpenseTypeAggregate] = list(merged.values())
    for row in rows:
        row["amountNet"] = round(float(row.get("amountNet", 0.0)), 2)
        row["amountGross"] = round(float(row.get("amountGross", 0.0)), 2)
        row["amountAdjustments"] = round(float(row.get("amountAdjustments", 0.0)), 2)
    rows.sort(key=lambda x: float(x.get("amountNet", 0.0)), reverse=True)

    return rows, sorted(pend_set), (latest_generated_at or now_iso())
