import os
from pathlib import Path
from typing import Dict

from federal.deputados.camara_common import (
    ensure_dir,
    load_category_map,
    load_insights_text_catalog,
    parse_int_env,
    parse_months,
    read_json,
    validate_out_dir,
    write_json,
    write_metodologia_docs,
)
from federal.deputados.camara_contracts import (
    build_entities_contract_from_rows,
    build_expense_types_contract,
    build_overview_from_rows,
    build_pending_categories_contract,
    build_ranking_total_contract_from_rows,
    write_period_contracts,
    write_profiles_and_index,
)
from federal.deputados.camara_domain import (
    aggregate_analytics_from_month_periods,
    build_month_aggregates,
    build_resumos_deputados_from_month_entities,
    list_month_entity_periods,
    pick_latest_daily_insight_from_month_overview_files,
    sum_entity_files,
)
from federal.deputados.camara_insights import build_home_insights, official_home_insight_ids
from federal.deputados.camara_source import fetch_deputados
from federal.deputados.camara_validate import (
    _assert_ok,
    validate_category_map_file,
    validate_contract_outputs,
)


def main():
    mode = os.environ.get("MODE", "single").strip().lower()
    _assert_ok(mode in {"single", "validate", "validate-mapping"}, f"Unsupported MODE: {mode!r}")
    year = parse_int_env("YEAR", 2026, 2000, 2100)
    months = parse_months(os.environ.get("MONTHS", "1"))
    mandate_start_year = parse_int_env("MANDATE_START_YEAR", 2023, 2000, 2100)
    _assert_ok(mandate_start_year <= year, f"MANDATE_START_YEAR ({mandate_start_year}) cannot be greater than YEAR ({year})")

    out_dir = Path(os.environ.get("OUT_DIR", "data")).resolve()
    validate_out_dir(out_dir)
    repo_root = Path(__file__).resolve().parents[3]
    mapping_env = os.environ.get("CATEGORY_MAP", "").strip()
    mapping_path = Path(mapping_env).resolve() if mapping_env else (repo_root / "mapping/federal/deputados/category_map.json").resolve()
    insights_catalog_env = os.environ.get("INSIGHTS_TEXT_CATALOG", "").strip()
    insights_catalog_path = Path(insights_catalog_env).resolve() if insights_catalog_env else (repo_root / "mapping/insights/texts_ptbr.json").resolve()

    if mode == "validate-mapping":
        validate_category_map_file(mapping_path)
        print("OK: mapping validation complete.")
        return

    if mode == "validate":
        validate_contract_outputs(out_dir)
        print("OK: validation complete.")
        return

    if not mapping_path.exists():
        raise FileNotFoundError(f"CATEGORY_MAP not found: {mapping_path}")
    if not insights_catalog_path.exists():
        raise FileNotFoundError(f"INSIGHTS_TEXT_CATALOG not found: {insights_catalog_path}")

    cmap = load_category_map(mapping_path)
    insights_text_catalog = load_insights_text_catalog(insights_catalog_path)
    missing_insight_texts = official_home_insight_ids().difference(set((insights_text_catalog.get("insights") or {}).keys()))
    _assert_ok(not missing_insight_texts, f"INSIGHTS_TEXT_CATALOG missing IDs: {sorted(missing_insight_texts)}")

    out_mapping_path = out_dir / "federal/camara/mapping/categoria/category_map.json"
    ensure_dir(out_mapping_path.parent)
    out_mapping_path.write_text(mapping_path.read_text(encoding="utf-8"), encoding="utf-8")
    write_metodologia_docs(out_dir, cmap, mandate_start_year)

    deputados = fetch_deputados()
    period_entities: Dict[str, dict] = {}
    period_overviews: Dict[str, dict] = {}
    period_rankings: Dict[str, dict] = {}

    for month in months:
        rows, expense_type_rows, pendentes, daily_summary_month = build_month_aggregates(deputados, year, month, cmap)
        periodo_mes = {"tipo": "mes", "ano": year, "mes": month}
        overview = build_overview_from_rows(rows, periodo_mes, cmap, daily_summary=daily_summary_month)
        period_key_month = f"month-{year:04d}-{month:02d}"
        entities_contract = build_entities_contract_from_rows(rows, period_key_month)
        ranking_contract = build_ranking_total_contract_from_rows(rows, period_key_month)
        period_overviews[period_key_month] = overview
        period_entities[period_key_month] = entities_contract
        period_rankings[period_key_month] = ranking_contract
        write_period_contracts(
            out_dir=out_dir,
            pkey=period_key_month,
            overview_obj=overview,
            entities_obj=entities_contract,
            ranking_obj=ranking_contract,
            expense_types_obj=build_expense_types_contract(expense_type_rows, period_key_month),
            pending_categories_obj=build_pending_categories_contract(expense_type_rows, pendentes, period_key_month),
        )

    month_periods = list_month_entity_periods(out_dir)
    year_month_periods = [(y, mm, p) for (y, mm, p) in month_periods if y == year]
    if year_month_periods:
        year_entity_files = [p for (_, _, p) in year_month_periods]
        rows_year = sum_entity_files(year_entity_files)
        periodo_ano = {"tipo": "ano", "ano": year, "mesesIncluidos": [mm for (_, mm, _) in year_month_periods]}
        year_month_overview_files = [out_dir / f"federal/camara/overview/month-{y:04d}-{mm:02d}/overview.json" for (y, mm, _) in year_month_periods]
        daily_year = pick_latest_daily_insight_from_month_overview_files(year_month_overview_files)
        overview_year = build_overview_from_rows(rows_year, periodo_ano, cmap, daily_summary=daily_year)
        period_key_year = f"year-{year:04d}"
        period_overviews[period_key_year] = overview_year
        period_entities[period_key_year] = build_entities_contract_from_rows(rows_year, period_key_year)
        period_rankings[period_key_year] = build_ranking_total_contract_from_rows(rows_year, period_key_year)
        month_pkeys_year = [f"month-{y:04d}-{mm:02d}" for (y, mm, _) in year_month_periods]
        analytics_year_rows, pendentes_year, analytics_year_generated_at = aggregate_analytics_from_month_periods(out_dir, month_pkeys_year)
        write_period_contracts(
            out_dir=out_dir,
            pkey=period_key_year,
            overview_obj=period_overviews[period_key_year],
            entities_obj=period_entities[period_key_year],
            ranking_obj=period_rankings[period_key_year],
            expense_types_obj=build_expense_types_contract(analytics_year_rows, period_key_year, analytics_year_generated_at),
            pending_categories_obj=build_pending_categories_contract(analytics_year_rows, pendentes_year, period_key_year, analytics_year_generated_at),
        )

    mandate_periods = [(y, mm, p) for (y, mm, p) in month_periods if mandate_start_year <= y <= year]
    overview_mandate_contract = None
    if mandate_periods:
        mandate_entity_files = [p for (_, _, p) in mandate_periods]
        rows_mandato = sum_entity_files(mandate_entity_files)
        mandate_month_overview_files = [out_dir / f"federal/camara/overview/month-{y:04d}-{mm:02d}/overview.json" for (y, mm, _) in mandate_periods]
        daily_mandato = pick_latest_daily_insight_from_month_overview_files(mandate_month_overview_files)
        periodo_mandato = {"tipo": "mandato", "inicioAno": mandate_start_year, "fimAno": year, "totalMesesIncluidos": len(mandate_periods)}
        overview_mandato = build_overview_from_rows(rows_mandato, periodo_mandato, cmap, daily_summary=daily_mandato)
        period_key_mandate = "mandate"
        period_overviews[period_key_mandate] = overview_mandato
        period_entities[period_key_mandate] = build_entities_contract_from_rows(rows_mandato, period_key_mandate)
        period_rankings[period_key_mandate] = build_ranking_total_contract_from_rows(rows_mandato, period_key_mandate)
        overview_mandate_contract = period_overviews[period_key_mandate]
        month_pkeys_mandato = [f"month-{y:04d}-{mm:02d}" for (y, mm, _) in mandate_periods]
        analytics_mandato_rows, pendentes_mandato, analytics_mandato_generated_at = aggregate_analytics_from_month_periods(out_dir, month_pkeys_mandato)
        write_period_contracts(
            out_dir=out_dir,
            pkey=period_key_mandate,
            overview_obj=period_overviews[period_key_mandate],
            entities_obj=period_entities[period_key_mandate],
            ranking_obj=period_rankings[period_key_mandate],
            expense_types_obj=build_expense_types_contract(analytics_mandato_rows, period_key_mandate, analytics_mandato_generated_at),
            pending_categories_obj=build_pending_categories_contract(analytics_mandato_rows, pendentes_mandato, period_key_mandate, analytics_mandato_generated_at),
        )

    entity_photo_by_id: Dict[str, str] = {}
    if mandate_periods:
        mandate_entity_files = [p for (_, _, p) in mandate_periods]
        resumos = build_resumos_deputados_from_month_entities(deputados, mandate_entity_files, mandate_start_year)
        write_profiles_and_index(out_dir, resumos)
    entities_index_obj = read_json(out_dir / "federal/camara/entities/index.json") or {}
    for item in (entities_index_obj.get("items") or []):
        if not isinstance(item, dict):
            continue
        pid = str(item.get("id") or "").strip()
        photo = str(item.get("photoUrl") or "").strip()
        if pid and photo and pid not in entity_photo_by_id:
            entity_photo_by_id[pid] = photo

    home_insights = build_home_insights(
        overview_mandate_contract or {},
        entity_photo_by_id=entity_photo_by_id,
        insights_text_catalog=insights_text_catalog,
    )
    write_json(out_dir / "federal/camara/insights/home-insights.json", home_insights)

    print("OK: generation complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
