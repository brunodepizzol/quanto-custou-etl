import argparse
import json
from pathlib import Path


def _assert_ok(cond: bool, msg: str) -> None:
    if not cond:
        raise RuntimeError(msg)


def _is_non_empty_str(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_non_empty_scalar(value) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return isinstance(value, (int, float))


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _has_first_tag(it: dict) -> bool:
    tags = it.get("tags")
    if not isinstance(tags, list) or not tags:
        return False
    return _is_non_empty_str(tags[0])


def _read_json(path: Path) -> dict:
    _assert_ok(path.exists(), f"missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def validate(data_dir: Path) -> None:
    catalog_path = data_dir / "catalog.json"
    index_path = data_dir / "home-insights-index.json"
    feed_path = data_dir / "home-insights-feed.json"

    catalog = _read_json(catalog_path)
    index = _read_json(index_path)
    feed = _read_json(feed_path)

    datasets = catalog.get("datasets")
    _assert_ok(isinstance(datasets, list), "catalog.datasets must be an array")

    index_items = index.get("items")
    _assert_ok(isinstance(index_items, list), "home-insights-index.items must be an array")

    feed_items = feed.get("items")
    _assert_ok(isinstance(feed_items, list), "home-insights-feed.items must be an array")

    dataset_ids = []
    for i, ds in enumerate(datasets):
        _assert_ok(isinstance(ds, dict), f"catalog dataset[{i}] must be object")
        for k in [
            "id",
            "pathBaseData",
            "pathInsightsHome",
            "pathOverviewRoot",
            "pathEntitiesRoot",
            "pathRankingsRoot",
            "pathAnalyticsRoot",
        ]:
            _assert_ok(str(ds.get(k) or "").strip(), f"catalog dataset[{i}] missing {k}")
        dataset_ids.append(str(ds.get("id")).strip())
    _assert_ok(len(dataset_ids) == len(set(dataset_ids)), "catalog has duplicated dataset id")

    index_ids = []
    for i, it in enumerate(index_items):
        _assert_ok(isinstance(it, dict), f"index item[{i}] must be object")
        for k in ["id", "level", "scope", "orgao", "path", "enabled"]:
            _assert_ok(str(it.get(k) or "").strip() if k != "enabled" else it.get("enabled") is not None,
                       f"index item[{i}] missing {k}")
        item_id = str(it.get("id")).strip()
        index_ids.append(item_id)
        rel = str(it.get("path")).strip().lstrip("/")
        _assert_ok((data_dir / rel).exists(), f"index item[{i}] path does not exist: {rel}")
    _assert_ok(len(index_ids) == len(set(index_ids)), "home-insights-index has duplicated id")

    ranked = []
    seen_keys = set()
    for i, it in enumerate(feed_items):
        _assert_ok(isinstance(it, dict), f"feed item[{i}] must be object")
        for k in [
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
            "sourceId",
            "sourceLevel",
            "sourceScope",
            "sourceOrgao",
            "scoreFinal",
            "rank",
        ]:
            _assert_ok(it.get(k) is not None and (k == "enabled" or str(it.get(k)).strip()),
                       f"feed item[{i}] missing {k}")

        source_id = str(it.get("sourceId")).strip()
        _assert_ok(source_id in index_ids, f"feed item[{i}] sourceId not found in index: {source_id}")
        _assert_ok(
            str(it.get("level") or "").strip() == str(it.get("sourceLevel") or "").strip(),
            f"feed item[{i}] level/sourceLevel mismatch"
        )

        quality = float(it.get("qualityScore"))
        weight = float(it.get("weight"))
        priority = float(it.get("priority"))
        score_final = float(it.get("scoreFinal"))
        rank = int(it.get("rank"))
        _assert_ok(0.0 <= quality <= 1.0, f"feed item[{i}] qualityScore out of range")
        _assert_ok(weight >= 0.0, f"feed item[{i}] weight out of range")
        _assert_ok(0.0 <= priority <= 100.0, f"feed item[{i}] priority out of range")
        _assert_ok(score_final >= 0.0, f"feed item[{i}] scoreFinal out of range")
        _assert_ok(rank >= 1, f"feed item[{i}] rank must be >= 1")

        key = f"{source_id}:{str(it.get('id')).strip()}"
        _assert_ok(key not in seen_keys, f"feed has duplicated insight key: {key}")
        seen_keys.add(key)
        ranked.append(rank)

        insight_type = str(it.get("type") or "").strip()
        _assert_ok(insight_type in {"person", "aggregate", "comparison", "alert", "timeline"}, f"feed item[{i}] invalid type")

        if insight_type == "person":
            entity = it.get("entity")
            _assert_ok(isinstance(entity, dict), f"feed item[{i}] person.entity must be object")
            _assert_ok(_is_non_empty_scalar(entity.get("id")), f"feed item[{i}] missing person.entity.id")
            for k in ["name", "photoUrl"]:
                _assert_ok(_is_non_empty_str(entity.get(k)), f"feed item[{i}] missing person.entity.{k}")
            value = it.get("value")
            _assert_ok(isinstance(value, dict), f"feed item[{i}] person.value must be object")
            _assert_ok(_is_number(value.get("amount")), f"feed item[{i}] person.value.amount must be number")
            _assert_ok(_is_non_empty_str(it.get("referenceDate")), f"feed item[{i}] missing referenceDate")
            _assert_ok(_is_non_empty_str(it.get("variationText")), f"feed item[{i}] missing variationText")

        elif insight_type == "aggregate":
            value = it.get("value")
            _assert_ok(isinstance(value, dict), f"feed item[{i}] aggregate.value must be object")
            _assert_ok(_is_number(value.get("amount")), f"feed item[{i}] aggregate.value.amount must be number")
            _assert_ok(_has_first_tag(it), f"feed item[{i}] aggregate missing tags[0]")
            _assert_ok(_is_non_empty_str(it.get("referenceDate")), f"feed item[{i}] missing referenceDate")
            _assert_ok(_is_non_empty_str(it.get("variationText")), f"feed item[{i}] missing variationText")

        elif insight_type == "comparison":
            delta = it.get("delta")
            _assert_ok(isinstance(delta, dict), f"feed item[{i}] comparison.delta must be object")
            _assert_ok(_is_number(delta.get("percent")), f"feed item[{i}] comparison.delta.percent must be number")
            _assert_ok(_has_first_tag(it), f"feed item[{i}] comparison missing tags[0]")
            _assert_ok(_is_non_empty_str(it.get("referenceDate")), f"feed item[{i}] missing referenceDate")
            _assert_ok(_is_non_empty_str(it.get("variationText")), f"feed item[{i}] missing variationText")

        elif insight_type == "timeline":
            series = it.get("series")
            _assert_ok(isinstance(series, list) and len(series) > 0, f"feed item[{i}] timeline missing series")
            for j, point in enumerate(series):
                _assert_ok(isinstance(point, dict), f"feed item[{i}] timeline.series[{j}] must be object")
                _assert_ok(_is_non_empty_str(point.get("date")), f"feed item[{i}] timeline.series[{j}].date missing")
                _assert_ok(_is_number(point.get("amount")), f"feed item[{i}] timeline.series[{j}].amount must be number")
            _assert_ok(_is_non_empty_str(it.get("referenceDate")), f"feed item[{i}] missing referenceDate")
            _assert_ok(_is_non_empty_str(it.get("variationText")), f"feed item[{i}] missing variationText")

    if ranked:
        _assert_ok(ranked == list(range(1, len(ranked) + 1)), "feed ranks must be contiguous from 1")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    validate(data_dir)
    print("OK: global insights feed validation complete.")


if __name__ == "__main__":
    main()
