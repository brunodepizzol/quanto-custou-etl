import fnmatch
import re
from pathlib import Path
from typing import List, Sequence, cast

from .camara_common import JsonDict, artifact_schema_version, home_insights_schema_version, load_category_map, read_json
from .camara_insights import _validate_editorial_fields, official_home_insight_ids


def _missing(value: object) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _assert_ok(cond: bool, msg: str) -> None:
    if not cond:
        raise RuntimeError(msg)


def _as_dict(value: object, msg: str) -> JsonDict:
    _assert_ok(isinstance(value, dict), msg)
    return cast(JsonDict, value)


def _as_list(value: object, msg: str) -> List[object]:
    _assert_ok(isinstance(value, list), msg)
    return cast(List[object], value)


def _get_path(obj: JsonDict, path: str) -> object:
    current: object = obj
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current.get(part)
    return current


def _validate_insight_item(it: JsonDict, idx: int) -> None:
    required_common = [
        "id",
        "type",
        "level",
        "period",
        "tag",
        "title",
        "context",
        "source",
        "enabled",
        "priority",
        "weight",
        "qualityScore",
        "generatedAt",
        "freshUntil",
    ]
    for key in required_common:
        _assert_ok(not _missing(it.get(key)), f"home-insights item[{idx}] missing field: {key}")
    _assert_ok(str(it.get("type")) in {"person", "aggregate", "comparison", "alert", "timeline"}, f"home-insights item[{idx}] invalid type")
    _assert_ok(str(it.get("period")) in {"day", "month", "year", "mandate", "rolling7d"}, f"home-insights item[{idx}] invalid period")
    priority = float(it.get("priority") or 0.0)
    weight = float(it.get("weight") or 0.0)
    quality = float(it.get("qualityScore") or 0.0)
    _assert_ok(0.0 <= priority <= 100.0, f"home-insights item[{idx}] invalid priority range")
    _assert_ok(0.0 <= weight <= 1.0, f"home-insights item[{idx}] invalid weight range")
    _assert_ok(0.0 <= quality <= 1.0, f"home-insights item[{idx}] invalid qualityScore range")
    _assert_ok(quality >= 0.6, f"home-insights item[{idx}] blocked by qualityScore < 0.60")
    insight_type = str(it.get("type"))
    if insight_type == "person":
        for key in ["entity.id", "entity.name", "value.amount", "value.currency"]:
            _assert_ok(not _missing(_get_path(it, key)), f"home-insights item[{idx}] missing required person field: {key}")
        for key in ["left", "right", "delta", "series"]:
            _assert_ok(_missing(it.get(key)), f"home-insights item[{idx}] person must not include {key}")
    elif insight_type == "aggregate":
        for key in ["value.amount", "value.currency"]:
            _assert_ok(not _missing(_get_path(it, key)), f"home-insights item[{idx}] missing required aggregate field: {key}")
        for key in ["entity", "left", "right", "delta", "series"]:
            _assert_ok(_missing(it.get(key)), f"home-insights item[{idx}] aggregate must not include {key}")
    elif insight_type == "comparison":
        for key in ["left.amount", "right.amount", "delta.amount", "delta.percent", "delta.direction"]:
            _assert_ok(not _missing(_get_path(it, key)), f"home-insights item[{idx}] missing required comparison field: {key}")
        for key in ["entity", "value", "series"]:
            _assert_ok(_missing(it.get(key)), f"home-insights item[{idx}] comparison must not include {key}")
    elif insight_type == "alert":
        for key in ["severity", "baseline.amount"]:
            _assert_ok(not _missing(_get_path(it, key)), f"home-insights item[{idx}] missing required alert field: {key}")
    elif insight_type == "timeline":
        series = _as_list(it.get("series"), f"home-insights item[{idx}] missing series")
        _assert_ok(len(series) > 0, f"home-insights item[{idx}] missing series")
        for key in ["entity", "value", "left", "right", "delta"]:
            _assert_ok(_missing(it.get(key)), f"home-insights item[{idx}] timeline must not include {key}")
        for point_idx, point in enumerate(series):
            safe_point = cast(JsonDict, point or {})
            _assert_ok(not _missing(safe_point.get("date")), f"home-insights item[{idx}] series[{point_idx}] missing date")
            _assert_ok(not _missing(safe_point.get("amount")), f"home-insights item[{idx}] series[{point_idx}] missing amount")


def _validate_daily_highlights_semantics(path: Path, highlights: JsonDict) -> None:
    expected_highlight_keys = {
        "topSpender",
        "topCategories",
        "topStates",
        "concentrationTop10",
        "averages",
        "dailyTotals",
        "dailyTopExpense",
    }
    _assert_ok(set(highlights.keys()) == expected_highlight_keys, f"{path} highlights keys must match the canonical overview contract")
    daily_totals = _as_dict(highlights.get("dailyTotals") or {}, f"{path} highlights.dailyTotals must be an object")
    daily_top_expense = _as_dict(highlights.get("dailyTopExpense") or {}, f"{path} highlights.dailyTopExpense must be an object")
    for forbidden_key in ["entity", "category", "expenseType", "supplier", "documentType", "documentUrl"]:
        _assert_ok(forbidden_key not in daily_totals, f"{path} highlights.dailyTotals must not include {forbidden_key}")
    for forbidden_key in [
        "previousAmountNet",
        "deltaAmountNet",
        "deltaPercent",
        "deltaDirection",
        "trend7d",
        "totalAmountNet",
        "previousTotalAmountNet",
        "deltaTotalAmountNet",
        "deltaPercentTotalAmountNet",
    ]:
        _assert_ok(forbidden_key not in daily_top_expense, f"{path} highlights.dailyTopExpense must not include {forbidden_key}")
    totals_ref = str(daily_totals.get("referenceDate") or "").strip()
    top_ref = str(daily_top_expense.get("referenceDate") or "").strip()
    if totals_ref and top_ref:
        _assert_ok(totals_ref == top_ref, f"{path} daily highlight reference dates must match")


def _validate_home_insight_catalog_semantics(items: Sequence[object]) -> None:
    by_id = {
        str((item or {}).get("id") or "").strip(): cast(JsonDict, (item or {}))
        for item in items
        if isinstance(item, dict)
    }
    expected = {
        "federal-deputies-day-total-amount": ("aggregate", "day"),
        "federal-deputies-day-top-spender": ("person", "day"),
        "federal-deputies-day-vs-previous-day": ("comparison", "day"),
        "federal-deputies-day-top-share": ("comparison", "day"),
        "federal-deputies-rolling7d-trend": ("timeline", "rolling7d"),
    }
    for insight_id, (expected_type, expected_period) in expected.items():
        item = by_id.get(insight_id)
        _assert_ok(item is not None, f"home-insights missing required item {insight_id}")
        assert item is not None
        _assert_ok(str(item.get("type") or "") == expected_type, f"{insight_id} type must be {expected_type}")
        _assert_ok(str(item.get("period") or "") == expected_period, f"{insight_id} period must be {expected_period}")


def _validate_overview_schema(path: Path, obj: JsonDict, required_schema: str) -> None:
    meta = _as_dict(obj.get("meta") or {}, f"{path} meta must be an object")
    base = _as_dict(obj.get("base") or {}, f"{path} base must be an object")
    highlights = _as_dict(obj.get("highlights") or {}, f"{path} highlights must be an object")
    for key in ["scope", "type", "period", "generatedAt", "schemaVersion"]:
        _assert_ok(not _missing(meta.get(key)), f"{path} missing meta.{key}")
    _assert_ok(str(meta.get("schemaVersion") or "").strip() == required_schema, f"{path} meta.schemaVersion must be {required_schema}")
    for key in ["amountNet", "amountGross", "amountAdjustments", "recordsCount", "entitiesCount", "entitiesWithSpending", "entitiesWithoutSpending"]:
        _assert_ok(not _missing(base.get(key)), f"{path} missing base.{key}")
    for key in ["topSpender", "topCategories", "topStates", "concentrationTop10", "averages", "dailyTotals", "dailyTopExpense"]:
        _assert_ok(key in highlights, f"{path} missing highlights.{key}")
    _validate_daily_highlights_semantics(path, highlights)


def _validate_entities_schema(path: Path, obj: JsonDict, required_schema: str) -> None:
    meta = _as_dict(obj.get("meta") or {}, f"{path} meta must be an object")
    items = _as_list(obj.get("items"), f"{path} missing items array")
    for key in ["scope", "type", "period", "generatedAt", "schemaVersion"]:
        _assert_ok(not _missing(meta.get(key)), f"{path} missing meta.{key}")
    _assert_ok(str(meta.get("schemaVersion") or "").strip() == required_schema, f"{path} meta.schemaVersion must be {required_schema}")
    for idx, item in enumerate(items):
        safe_item = cast(JsonDict, item or {})
        for key in ["id", "name", "stateCode", "party", "amountNet", "amountGross", "amountAdjustments", "recordsCount"]:
            _assert_ok(not _missing(safe_item.get(key)), f"{path} item[{idx}] missing {key}")


def _validate_ranking_schema(path: Path, obj: JsonDict, required_schema: str) -> None:
    meta = _as_dict(obj.get("meta") or {}, f"{path} meta must be an object")
    for key in ["scope", "type", "period", "generatedAt", "schemaVersion"]:
        _assert_ok(not _missing(meta.get(key)), f"{path} missing meta.{key}")
    _assert_ok(str(meta.get("schemaVersion") or "").strip() == required_schema, f"{path} meta.schemaVersion must be {required_schema}")
    _as_list(obj.get("top"), f"{path} missing top array")
    _as_list(obj.get("bottom"), f"{path} missing bottom array")


def _validate_analytics_schema(path: Path, obj: JsonDict, required_schema: str) -> None:
    meta = _as_dict(obj.get("meta") or {}, f"{path} meta must be an object")
    for key in ["scope", "type", "period", "generatedAt", "schemaVersion"]:
        _assert_ok(not _missing(meta.get(key)), f"{path} missing meta.{key}")
    _assert_ok(str(meta.get("schemaVersion") or "").strip() == required_schema, f"{path} meta.schemaVersion must be {required_schema}")
    _as_list(obj.get("items"), f"{path} missing items array")


def _scan_disallowed_contract_keys(obj: object, disallowed: set[str], out: set[str]) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in disallowed:
                out.add(key)
            _scan_disallowed_contract_keys(value, disallowed, out)
    elif isinstance(obj, list):
        for item in obj:
            _scan_disallowed_contract_keys(item, disallowed, out)


def _validate_artifact_governance(out_dir: Path) -> None:
    policy = {
        "core_ui": [
            "metodologia.json",
            "federal/camara/metodologia_scope.json",
            "federal/camara/insights/home-insights.json",
            "federal/camara/entities/index.json",
            "federal/camara/mapping/categoria/category_map.json",
            "federal/camara/entities/mandate/entities.json",
            "federal/camara/overview/mandate/overview.json",
            "federal/camara/entities/profiles/*.json",
            "federal/camara/analytics/month-*/expense-types.json",
            "federal/camara/analytics/month-*/pending-categories.json",
        ],
        "scale_contract": [
            "federal/camara/rankings/mandate/ranking-total.json",
            "federal/camara/rankings/year-*/ranking-total.json",
            "federal/camara/rankings/month-*/ranking-total.json",
            "federal/camara/analytics/year-*/expense-types.json",
            "federal/camara/analytics/year-*/pending-categories.json",
            "federal/camara/analytics/mandate/expense-types.json",
            "federal/camara/analytics/mandate/pending-categories.json",
        ],
    }
    rel_files = [fp.relative_to(out_dir).as_posix() for fp in sorted(out_dir.rglob("*.json"))]
    for class_name, patterns in policy.items():
        for pattern in patterns:
            has_match = any(fnmatch.fnmatch(path, pattern) for path in rel_files)
            _assert_ok(has_match, f"artifact governance missing [{class_name}] pattern: {pattern}")


def validate_contract_outputs(out_dir: Path) -> None:
    required_artifact_schema = artifact_schema_version()
    required_home_schema = home_insights_schema_version()
    top_files = [
        out_dir / "federal/camara/insights/home-insights.json",
        out_dir / "federal/camara/entities/index.json",
    ]
    for path in top_files:
        _assert_ok(path.exists(), f"missing required artifact: {path}")

    insights_obj = read_json(out_dir / "federal/camara/insights/home-insights.json") or {}
    insights_meta = _as_dict(insights_obj.get("meta") or {}, "home-insights meta must be object")
    _assert_ok(str(insights_meta.get("schemaVersion") or "").strip() == required_home_schema, f"home-insights schemaVersion must be {required_home_schema}")
    items = _as_list(insights_obj.get("items"), "home-insights missing items array")
    ids = [str(cast(JsonDict, item or {}).get("id") or "") for item in items]
    _assert_ok(len(ids) == len(set(ids)), "home-insights has duplicated id")
    official_ids = official_home_insight_ids()
    _assert_ok(set(ids) == official_ids, "home-insights IDs are not the official 11 set")
    for idx, item in enumerate(items):
        safe_item = cast(JsonDict, item or {})
        _validate_insight_item(safe_item, idx)
        _validate_editorial_fields(safe_item, idx, _assert_ok)
    _validate_home_insight_catalog_semantics(items)

    overview_root = out_dir / "federal/camara/overview"
    _assert_ok(overview_root.exists(), "missing overview root")
    period_keys = sorted([path.name for path in overview_root.iterdir() if path.is_dir()])
    _assert_ok(len(period_keys) > 0, "missing overview periods")
    for period_key in period_keys:
        overview_path = out_dir / f"federal/camara/overview/{period_key}/overview.json"
        entities_path = out_dir / f"federal/camara/entities/{period_key}/entities.json"
        ranking_path = out_dir / f"federal/camara/rankings/{period_key}/ranking-total.json"
        analytics_expense_path = out_dir / f"federal/camara/analytics/{period_key}/expense-types.json"
        analytics_pending_path = out_dir / f"federal/camara/analytics/{period_key}/pending-categories.json"
        for path in [overview_path, entities_path, ranking_path, analytics_expense_path, analytics_pending_path]:
            _assert_ok(path.exists(), f"missing period artifact: {path}")
        _validate_overview_schema(overview_path, read_json(overview_path) or {}, required_artifact_schema)
        _validate_entities_schema(entities_path, read_json(entities_path) or {}, required_artifact_schema)
        _validate_ranking_schema(ranking_path, read_json(ranking_path) or {}, required_artifact_schema)
        _validate_analytics_schema(analytics_expense_path, read_json(analytics_expense_path) or {}, required_artifact_schema)
        _validate_analytics_schema(analytics_pending_path, read_json(analytics_pending_path) or {}, required_artifact_schema)

    disallowed_contract_keys = {
        "geradoEm",
        "periodo",
        "top1Gasto",
        "topCategorias",
        "topUFs",
        "agentesComGasto",
        "agentesSemGasto",
        "totalLancamentos",
        "valorLiquido",
        "valorBruto",
        "valorAjustes",
        "uf",
        "partido",
        "nome",
        "urlFoto",
    }
    contract_roots = [
        out_dir / "federal/camara/insights/home-insights.json",
        out_dir / "federal/camara/overview",
        out_dir / "federal/camara/entities",
        out_dir / "federal/camara/rankings",
        out_dir / "federal/camara/analytics",
    ]
    for root in contract_roots:
        files = [root] if root.is_file() else sorted(root.rglob("*.json"))
        for file_path in files:
            obj = read_json(file_path) or {}
            found: set[str] = set()
            _scan_disallowed_contract_keys(obj, disallowed_contract_keys, found)
            _assert_ok(not found, f"non-canonical keys found in {file_path}: {sorted(found)}")

    _validate_artifact_governance(out_dir)


def validate_category_map_file(mapping_path: Path) -> None:
    _assert_ok(mapping_path.exists(), f"CATEGORY_MAP not found: {mapping_path}")
    cfg = read_json(mapping_path)
    _assert_ok(isinstance(cfg, dict), "CATEGORY_MAP must be a JSON object")
    assert cfg is not None
    _assert_ok(isinstance(cfg.get("default"), str) and bool(str(cfg.get("default") or "").strip()), "CATEGORY_MAP default must be a non-empty string")
    rules = _as_list(cfg.get("rules"), "CATEGORY_MAP rules must be an array")
    for idx, rule in enumerate(rules):
        safe_rule = cast(JsonDict, rule or {})
        _assert_ok(isinstance(rule, dict), f"CATEGORY_MAP rule[{idx}] must be an object")
        match = safe_rule.get("match")
        category = safe_rule.get("category")
        _assert_ok(isinstance(match, str) and bool(match.strip()), f"CATEGORY_MAP rule[{idx}] missing match")
        _assert_ok(isinstance(category, str) and bool(category.strip()), f"CATEGORY_MAP rule[{idx}] missing category")
        assert isinstance(match, str)
        re.compile(match, re.IGNORECASE)
    load_category_map(mapping_path)
