import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, cast

from .camara_common import JsonDict, fresh_until_from, home_insights_schema_version, parse_iso_utc


@dataclass
class HomeInsightsContext:
    generated_at: str
    fresh_until: str
    base: JsonDict
    top_spender: JsonDict
    top_categories: List[JsonDict]
    top_states: List[JsonDict]
    concentration: JsonDict
    averages: JsonDict
    entities_count: int
    entities_with_spending: int
    entities_without_spending: int
    daily_totals: JsonDict
    daily_top_expense: JsonDict
    daily_entity: JsonDict
    daily_amount: float
    daily_total: float
    previous_total: float
    delta_total: float
    delta_percent: float
    delta_direction: str
    trend7d: Optional[List[JsonDict]]
    top_cat: JsonDict
    top_state: JsonDict
    share: float
    daily_supplier: str
    entity_photo_by_id: Dict[str, str]
    insights_cfg: JsonDict
    templates_cfg: JsonDict
    default_tag: str
    default_source: str


def official_home_insight_ids() -> set:
    return {
        "federal-deputies-mandate-total-amount",
        "federal-deputies-mandate-top-spender",
        "federal-deputies-mandate-top10-concentration",
        "federal-deputies-day-total-amount",
        "federal-deputies-day-top-spender",
        "federal-deputies-day-vs-previous-day",
        "federal-deputies-mandate-top-category",
        "federal-deputies-mandate-top-state",
        "federal-deputies-mandate-average-per-entity",
        "federal-deputies-day-top-share",
        "federal-deputies-rolling7d-trend",
    }


def home_to_ptbr_date(value: str) -> str:
    s = str(value or "").strip()
    match = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if match:
        return f"{match.group(3)}/{match.group(2)}/{match.group(1)}"
    return s


def home_fmt_brl(amount: float) -> str:
    number = round(float(amount or 0.0), 2)
    formatted = f"{number:,.2f}"
    formatted = formatted.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {formatted}"


def home_render_template(tpl: str, values: Dict[str, str]) -> str:
    out = str(tpl or "")
    for key, value in (values or {}).items():
        out = out.replace("{" + str(key) + "}", str(value))
    return out


def resolve_home_insight_text(ctx: HomeInsightsContext, iid: str, fallback_title: str, fallback_context: str) -> JsonDict:
    base = {"tag": ctx.default_tag, "source": ctx.default_source, "title": fallback_title, "context": fallback_context}
    entry_raw = ctx.insights_cfg.get(iid)
    entry = cast(JsonDict, entry_raw) if isinstance(entry_raw, dict) else {}
    template_name = str(entry.get("useTemplate") or "").strip()
    template_raw = ctx.templates_cfg.get(template_name) if template_name else None
    template_obj = cast(JsonDict, template_raw) if isinstance(template_raw, dict) else {}
    merged: JsonDict = dict(base)
    for src in [template_obj, entry]:
        for key in ["tag", "source", "title", "context", "contextTemplate", "variationTemplate"]:
            value = src.get(key)
            if isinstance(value, str) and value.strip():
                merged[key] = value.strip()
    return merged


def home_with_photo_fallback(entity_obj: JsonDict, entity_photo_by_id: Dict[str, str]) -> JsonDict:
    entity = dict(entity_obj or {})
    entity_id = str(entity.get("id") or "").strip()
    direct = str(entity.get("photoUrl") or "").strip()
    if direct:
        entity["photoUrl"] = direct
        return entity
    if entity_id:
        mapped = str(entity_photo_by_id.get(entity_id) or "").strip()
        if mapped:
            entity["photoUrl"] = mapped
    return entity


def calc_home_insight_quality(item: JsonDict) -> float:
    now_dt = datetime.now(timezone.utc)
    generated_at = parse_iso_utc(item.get("generatedAt"))
    fresh_until = parse_iso_utc(item.get("freshUntil"))
    if not generated_at or not fresh_until:
        freshness = 0.0
    else:
        delta_hours = max(0.0, (fresh_until - generated_at).total_seconds() / 3600.0)
        if delta_hours <= 24:
            freshness = 1.0
        elif delta_hours <= 72:
            freshness = 0.8
        elif delta_hours <= 168:
            freshness = 0.6
        else:
            freshness = 0.4
        if fresh_until < now_dt:
            freshness = min(freshness, 0.5)
    required = ["id", "type", "level", "period", "tag", "title", "context", "source", "enabled", "priority", "weight", "generatedAt", "freshUntil"]
    insight_type = str(item.get("type") or "")
    if insight_type == "person":
        required += ["entity", "value"]
    elif insight_type == "aggregate":
        required += ["value"]
    elif insight_type == "comparison":
        required += ["left", "right", "delta"]
    elif insight_type == "timeline":
        required += ["series"]
    present = 0
    for key in required:
        value = item.get(key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        present += 1
    completeness = (present / len(required)) if required else 0.0
    enum_ok = str(item.get("type") or "") in {"person", "aggregate", "comparison", "alert", "timeline"} and str(item.get("period") or "") in {"day", "month", "year", "mandate", "rolling7d"}
    priority = float(item.get("priority") or -1.0)
    weight = float(item.get("weight") or -1.0)
    range_ok = 0.0 <= priority <= 100.0 and 0.0 <= weight <= 1.0
    consistency = ((1.0 if enum_ok else 0.0) + (1.0 if range_ok else 0.0)) / 2.0
    quality = (0.45 * freshness) + (0.35 * completeness) + (0.20 * consistency)
    return round(max(0.0, min(1.0, quality)), 4)


def mk_home_insight_base(ctx: HomeInsightsContext, iid: str, itype: str, period: str, title: str, context: str, priority: int, weight: float) -> JsonDict:
    text_cfg = resolve_home_insight_text(ctx, iid, title, context)
    return {
        "id": iid,
        "type": itype,
        "level": "federal",
        "period": period,
        "tag": str(text_cfg.get("tag") or ctx.default_tag),
        "title": str(text_cfg.get("title") or title),
        "context": str(text_cfg.get("context") or context),
        "source": str(text_cfg.get("source") or ctx.default_source),
        "enabled": True,
        "priority": priority,
        "weight": weight,
        "generatedAt": ctx.generated_at,
        "freshUntil": ctx.fresh_until,
        "qualityScore": 0.0,
    }


def home_deputy_subtitle(entity: JsonDict) -> str:
    party = str(entity.get("party") or "").strip()
    state_code = str(entity.get("stateCode") or "").strip()
    suffix = f"{party}{'/' if party and state_code else ''}{state_code}".strip()
    return f"Deputado federal • {suffix}".strip(" •")


def home_pct_of_base(ctx: HomeInsightsContext, amount: float) -> float:
    base_amount = float(ctx.base.get("amountNet", 0.0) or 0.0)
    if base_amount <= 0:
        return 0.0
    return (float(amount or 0.0) / base_amount) * 100.0


def set_home_brl_value(item: JsonDict, amount: float) -> None:
    item["value"] = {"amount": round(float(amount or 0.0), 2), "currency": "BRL"}


def build_insight_mandate_total_amount(ctx: HomeInsightsContext) -> dict:
    item = mk_home_insight_base(ctx, "federal-deputies-mandate-total-amount", "aggregate", "mandate", "Total acumulado no mandato", "Valor líquido total acumulado no mandato.", 100, 1.0)
    set_home_brl_value(item, float(ctx.base.get("amountNet", 0.0) or 0.0))
    item["tags"] = ["Dados federais"]
    item["referenceDate"] = f"{int(ctx.base.get('recordsCount', 0) or 0)} lançamentos no período"
    item["variationText"] = f"Base: {ctx.entities_count} • Com gasto: {ctx.entities_with_spending} • Sem gasto: {ctx.entities_without_spending}"
    return item


def build_insight_mandate_top_spender(ctx: HomeInsightsContext) -> dict:
    item = mk_home_insight_base(ctx, "federal-deputies-mandate-top-spender", "person", "mandate", "Maior gasto no mandato", "Parlamentar com maior valor líquido acumulado no mandato.", 95, 0.95)
    item["entity"] = home_with_photo_fallback(
        {
            "id": ctx.top_spender.get("id"),
            "name": ctx.top_spender.get("name"),
            "party": ctx.top_spender.get("party"),
            "stateCode": ctx.top_spender.get("stateCode"),
            "photoUrl": ctx.top_spender.get("photoUrl"),
            "subtitle": home_deputy_subtitle(ctx.top_spender),
        },
        ctx.entity_photo_by_id,
    )
    set_home_brl_value(item, float(ctx.top_spender.get("amountNet", 0.0) or 0.0))
    item["referenceDate"] = "Deputado com maior gasto no mandato"
    item["variationText"] = f"Base: {ctx.entities_count} • Com gasto: {ctx.entities_with_spending}"
    return item


def build_insight_mandate_top10_concentration(ctx: HomeInsightsContext) -> dict:
    item = mk_home_insight_base(ctx, "federal-deputies-mandate-top10-concentration", "comparison", "mandate", "Concentração dos 10 maiores no mandato", "Participação dos 10 maiores no total líquido acumulado do mandato.", 92, 0.90)
    concentration_amount = float(ctx.concentration.get("amountNet", 0.0) or 0.0)
    concentration_percent = round(float(ctx.concentration.get("percent", 0.0) or 0.0), 2)
    item["left"] = {"amount": round(concentration_amount, 2), "currency": "BRL"}
    item["right"] = {"amount": round(float(ctx.base.get("amountNet", 0.0) or 0.0), 2), "currency": "BRL"}
    item["delta"] = {"amount": round(concentration_amount - float(ctx.base.get("amountNet", 0.0) or 0.0), 2), "percent": concentration_percent, "direction": "up"}
    item["tags"] = ["Top 10 deputados"]
    item["referenceDate"] = f"{ctx.entities_with_spending} deputados com gasto no período"
    item["variationText"] = f"Montante Top 10: {home_fmt_brl(concentration_amount)}"
    return item


def build_insight_day_total_amount(ctx: HomeInsightsContext) -> dict:
    item = mk_home_insight_base(ctx, "federal-deputies-day-total-amount", "aggregate", "day", "Movimento do dia", "Valor líquido total no dia mais recente disponível.", 90, 0.88)
    set_home_brl_value(item, ctx.daily_total)
    item["tags"] = ["Movimento do dia"]
    item["referenceDate"] = ctx.daily_totals.get("referenceDate")
    item["variationText"] = f"Base: {ctx.entities_count} • Com gasto: {ctx.entities_with_spending}"
    return item


def build_insight_day_top_spender(ctx: HomeInsightsContext) -> dict:
    item = mk_home_insight_base(ctx, "federal-deputies-day-top-spender", "person", "day", "Despesa mais alta do dia", "Parlamentar com maior valor líquido no dia mais recente disponível.", 89, 0.86)
    item["entity"] = home_with_photo_fallback(
        {
            "id": ctx.daily_entity.get("id"),
            "name": ctx.daily_entity.get("name"),
            "party": ctx.daily_entity.get("party"),
            "stateCode": ctx.daily_entity.get("stateCode"),
            "photoUrl": ctx.daily_entity.get("photoUrl"),
            "subtitle": home_deputy_subtitle(ctx.daily_entity),
        },
        ctx.entity_photo_by_id,
    )
    set_home_brl_value(item, ctx.daily_amount)
    item["referenceDate"] = ctx.daily_top_expense.get("referenceDate")
    text_cfg = resolve_home_insight_text(ctx, "federal-deputies-day-top-spender", "Despesa mais alta do dia", "Parlamentar com maior valor líquido no dia mais recente disponível.")
    variation_tpl = str(text_cfg.get("variationTemplate") or "").strip()
    context_tpl = str(text_cfg.get("contextTemplate") or "").strip()
    daily_ref_br = home_to_ptbr_date(str(ctx.daily_top_expense.get("referenceDate") or ""))
    if ctx.daily_supplier and variation_tpl:
        item["variationText"] = home_render_template(variation_tpl, {"supplier": ctx.daily_supplier})
    if daily_ref_br and context_tpl:
        item["context"] = home_render_template(context_tpl, {"referenceDateBr": daily_ref_br})
    return item


def build_insight_day_vs_previous_day(ctx: HomeInsightsContext) -> dict:
    item = mk_home_insight_base(ctx, "federal-deputies-day-vs-previous-day", "comparison", "day", "Oscilação diária", "Variação diária comparando o último dia disponível com o dia anterior.", 85, 0.82)
    item["left"] = {"amount": ctx.daily_total, "currency": "BRL"}
    item["right"] = {"amount": ctx.previous_total, "currency": "BRL"}
    item["delta"] = {"amount": ctx.delta_total, "percent": ctx.delta_percent, "direction": ctx.delta_direction}
    item["tags"] = ["Variação diária"]
    item["referenceDate"] = ctx.daily_totals.get("referenceDate")
    item["variationText"] = f"Dia anterior: {home_fmt_brl(ctx.previous_total)}"
    return item


def build_insight_mandate_top_category(ctx: HomeInsightsContext) -> dict:
    item = mk_home_insight_base(ctx, "federal-deputies-mandate-top-category", "aggregate", "mandate", "Categoria líder no mandato", "Categoria com maior valor líquido acumulado no mandato.", 82, 0.78)
    top_cat_amount = float(ctx.top_cat.get("amountNet", 0.0) or 0.0)
    set_home_brl_value(item, top_cat_amount)
    item["tags"] = [str(ctx.top_cat.get("category") or "")]
    item["referenceDate"] = "Participação no total do mandato"
    item["variationText"] = f"Participação: {home_pct_of_base(ctx, top_cat_amount):.1f}%".replace(".", ",")
    return item


def build_insight_mandate_top_state(ctx: HomeInsightsContext) -> dict:
    item = mk_home_insight_base(ctx, "federal-deputies-mandate-top-state", "aggregate", "mandate", "UF líder no mandato", "UF com maior valor líquido acumulado no mandato.", 80, 0.76)
    top_state_amount = float(ctx.top_state.get("amountNet", 0.0) or 0.0)
    set_home_brl_value(item, top_state_amount)
    item["tags"] = [str(ctx.top_state.get("stateCode") or "")]
    item["referenceDate"] = f"{ctx.entities_with_spending} deputados com gasto no período"
    item["variationText"] = f"Participação: {home_pct_of_base(ctx, top_state_amount):.1f}%".replace(".", ",")
    return item


def build_insight_mandate_average_per_entity(ctx: HomeInsightsContext) -> dict:
    item = mk_home_insight_base(ctx, "federal-deputies-mandate-average-per-entity", "aggregate", "mandate", "Média por parlamentar com gasto no mandato", "Média de valor líquido por parlamentar com gasto no mandato.", 78, 0.72)
    set_home_brl_value(item, float(ctx.averages.get("perEntityWithSpending", 0.0) or 0.0))
    item["tags"] = ["Média por agente"]
    item["referenceDate"] = f"{ctx.entities_with_spending} deputados com gasto no período"
    item["variationText"] = f"Montante: {home_fmt_brl(float(ctx.base.get('amountNet', 0.0) or 0.0))}"
    return item


def build_insight_day_top_share(ctx: HomeInsightsContext) -> dict:
    item = mk_home_insight_base(ctx, "federal-deputies-day-top-share", "comparison", "day", "Peso do líder no dia", "Participação percentual do maior gasto diário sobre o total do dia.", 74, 0.68)
    item["left"] = {"amount": ctx.daily_amount, "currency": "BRL"}
    item["right"] = {"amount": ctx.daily_total, "currency": "BRL"}
    item["delta"] = {
        "amount": round(ctx.daily_amount - ctx.daily_total, 2),
        "percent": round(ctx.share, 2),
        "direction": ("up" if ctx.daily_amount > ctx.daily_total else ("down" if ctx.daily_amount < ctx.daily_total else "flat")),
    }
    item["tags"] = ["Peso do maior gasto no dia"]
    item["referenceDate"] = ctx.daily_totals.get("referenceDate")
    item["variationText"] = f"Valor líder: {home_fmt_brl(ctx.daily_amount)}"
    return item


def build_insight_rolling7d_trend(ctx: HomeInsightsContext) -> Optional[dict]:
    item = mk_home_insight_base(ctx, "federal-deputies-rolling7d-trend", "timeline", "rolling7d", "Tendência dos últimos 7 dias", "Tendência dos totais diários nos últimos 7 dias disponíveis.", 72, 0.66)
    if not ctx.trend7d:
        return None
    item["tags"] = ["Totais diários"]
    item["series"] = [
        {"date": str(point.get("date") or ""), "amount": round(float(point.get("amountNet", 0.0) or 0.0), 2), "currency": "BRL"}
        for point in ctx.trend7d
        if str(point.get("date") or "").strip()
    ]
    if not item["series"]:
        return None
    last_point = item["series"][-1]
    item["referenceDate"] = last_point.get("date")
    window_total = round(sum(float(point.get("amount", 0.0) or 0.0) for point in item["series"]), 2)
    item["variationText"] = f"Janela 7d: {home_fmt_brl(window_total)}"
    return item


def _validate_editorial_fields(it: JsonDict, idx: int, assert_ok) -> None:
    tags = it.get("tags")
    if tags is not None:
        assert_ok(isinstance(tags, list), f"home-insights item[{idx}] tags must be a list")
        for tag_idx, tag in enumerate(tags):
            txt = str(tag or "").strip()
            assert_ok(bool(txt), f"home-insights item[{idx}] tags[{tag_idx}] is empty")
            assert_ok(len(txt) <= 40, f"home-insights item[{idx}] tags[{tag_idx}] too long")
    context = str(it.get("context") or "").strip()
    title = str(it.get("title") or "").strip()
    if context and title:
        assert_ok(context.lower() != title.lower(), f"home-insights item[{idx}] context duplicates title")
    variation = str(it.get("variationText") or "").strip()
    insight_type = str(it.get("type") or "")
    if insight_type == "comparison":
        assert_ok(bool(variation), f"home-insights item[{idx}] comparison should include variationText")
    elif insight_type == "timeline":
        assert_ok(bool(variation), f"home-insights item[{idx}] timeline should include variationText")
    elif insight_type == "aggregate":
        assert_ok(bool(variation), f"home-insights item[{idx}] aggregate should include variationText")


def build_home_insights(
    overview_mandate: JsonDict,
    entity_photo_by_id: Optional[Dict[str, str]] = None,
    insights_text_catalog: Optional[JsonDict] = None,
) -> JsonDict:
    generated_at = str(((overview_mandate or {}).get("meta") or {}).get("generatedAt") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    fresh_until = fresh_until_from(generated_at, 24)
    base = cast(JsonDict, (overview_mandate or {}).get("base") or {})
    highlights = cast(JsonDict, (overview_mandate or {}).get("highlights") or {})
    top_spender = cast(JsonDict, highlights.get("topSpender") or {})
    top_categories = cast(List[JsonDict], highlights.get("topCategories") or [])
    top_states = cast(List[JsonDict], highlights.get("topStates") or [])
    concentration = cast(JsonDict, highlights.get("concentrationTop10") or {})
    averages = cast(JsonDict, highlights.get("averages") or {})
    entities_count = int(base.get("entitiesCount", 0) or 0)
    entities_with_spending = int(base.get("entitiesWithSpending", 0) or 0)
    entities_without_spending = int(base.get("entitiesWithoutSpending", max(0, entities_count - entities_with_spending)) or 0)
    daily_totals = cast(JsonDict, highlights.get("dailyTotals") or {})
    daily_top_expense = cast(JsonDict, highlights.get("dailyTopExpense") or {})
    daily_entity = cast(JsonDict, daily_top_expense.get("entity") or {})
    daily_amount = round(float(daily_top_expense.get("amountNet", 0.0) or 0.0), 2)
    daily_total = round(float(daily_totals.get("amountNet", daily_amount) or 0.0), 2)
    previous_total = round(float(daily_totals.get("previousAmountNet", 0.0) or 0.0), 2)
    delta_total = round(float(daily_totals.get("deltaAmountNet", daily_total - previous_total) or 0.0), 2)
    delta_percent = round(float(daily_totals.get("deltaPercent", ((delta_total / previous_total) * 100.0 if previous_total > 0 else 0.0)) or 0.0), 2)
    delta_direction = str(daily_totals.get("deltaDirection") or ("up" if delta_total > 0 else ("down" if delta_total < 0 else "flat")))
    trend7d = cast(Optional[List[JsonDict]], daily_totals.get("trend7d") if isinstance(daily_totals.get("trend7d"), list) and daily_totals.get("trend7d") else None)
    top_cat = top_categories[0] if top_categories else cast(JsonDict, {})
    top_state = top_states[0] if top_states else cast(JsonDict, {})
    share = (daily_amount / daily_total * 100.0) if daily_total > 0 else 0.0
    daily_supplier = str(daily_top_expense.get("supplier") or "").strip()
    entity_photo_by_id = entity_photo_by_id or {}
    text_catalog = insights_text_catalog or {}
    defaults_raw = text_catalog.get("defaults")
    templates_raw = text_catalog.get("templates")
    insights_raw = text_catalog.get("insights")
    defaults_cfg = cast(JsonDict, defaults_raw) if isinstance(defaults_raw, dict) else {}
    templates_cfg = cast(JsonDict, templates_raw) if isinstance(templates_raw, dict) else {}
    insights_cfg = cast(JsonDict, insights_raw) if isinstance(insights_raw, dict) else {}
    default_tag = str(defaults_cfg.get("tag") or "INSIGHT DO DIA").strip()
    source_by_orgao_raw = defaults_cfg.get("sourceByOrgao")
    source_by_orgao = cast(JsonDict, source_by_orgao_raw) if isinstance(source_by_orgao_raw, dict) else {}
    default_source = str(source_by_orgao.get("camara") or "Câmara dos Deputados").strip()
    ctx = HomeInsightsContext(
        generated_at=generated_at,
        fresh_until=fresh_until,
        base=base,
        top_spender=top_spender,
        top_categories=top_categories,
        top_states=top_states,
        concentration=concentration,
        averages=averages,
        entities_count=entities_count,
        entities_with_spending=entities_with_spending,
        entities_without_spending=entities_without_spending,
        daily_totals=daily_totals,
        daily_top_expense=daily_top_expense,
        daily_entity=daily_entity,
        daily_amount=daily_amount,
        daily_total=daily_total,
        previous_total=previous_total,
        delta_total=delta_total,
        delta_percent=delta_percent,
        delta_direction=delta_direction,
        trend7d=trend7d,
        top_cat=top_cat,
        top_state=top_state,
        share=share,
        daily_supplier=daily_supplier,
        entity_photo_by_id=entity_photo_by_id,
        insights_cfg=insights_cfg,
        templates_cfg=templates_cfg,
        default_tag=default_tag,
        default_source=default_source,
    )
    items = [
        build_insight_mandate_total_amount(ctx),
        build_insight_mandate_top_spender(ctx),
        build_insight_mandate_top10_concentration(ctx),
        build_insight_day_total_amount(ctx),
        build_insight_day_top_spender(ctx),
        build_insight_day_vs_previous_day(ctx),
        build_insight_mandate_top_category(ctx),
        build_insight_mandate_top_state(ctx),
        build_insight_mandate_average_per_entity(ctx),
        build_insight_day_top_share(ctx),
        build_insight_rolling7d_trend(ctx),
    ]
    final_items = []
    for item in items:
        if not item:
            continue
        item["qualityScore"] = calc_home_insight_quality(item)
        if float(item.get("qualityScore") or 0.0) < 0.60:
            continue
        final_items.append(item)
    final_items.sort(key=lambda item: (-float(item.get("priority", 0) or 0.0), -float(item.get("weight", 0) or 0.0), str(item.get("id") or "")))
    return {
        "meta": {
            "scope": "federal/camara",
            "type": "home-insights",
            "generatedAt": generated_at,
            "freshUntil": fresh_until,
            "schemaVersion": home_insights_schema_version(),
        },
        "items": final_items,
    }
