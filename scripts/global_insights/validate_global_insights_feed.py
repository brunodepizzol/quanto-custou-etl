import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, cast

JsonDict = dict[str, Any]


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


def _parse_iso_utc(value: object):
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _assert_valid_iso(value, field_name: str) -> None:
    _assert_ok(_is_non_empty_str(value), f"{field_name} must be non-empty string")
    _assert_ok(_parse_iso_utc(value) is not None, f"{field_name} must be valid ISO-8601 UTC timestamp")


def _has_first_tag(it: JsonDict) -> bool:
    tags = it.get("tags")
    if not isinstance(tags, list) or not tags:
        return False
    return _is_non_empty_str(tags[0])


def _read_json(path: Path) -> JsonDict:
    _assert_ok(path.exists(), f"missing required file: {path}")
    return cast(JsonDict, json.loads(path.read_text(encoding="utf-8")))


def _as_dict(value: object, msg: str) -> JsonDict:
    _assert_ok(isinstance(value, dict), msg)
    return cast(JsonDict, value)


def _as_list(value: object, msg: str) -> List[object]:
    _assert_ok(isinstance(value, list), msg)
    return cast(List[object], value)


def validate(data_dir: Path) -> None:
    required_schema = str(os.getenv("HOME_INSIGHTS_SCHEMA_VERSION", "1.0.0")).strip() or "1.0.0"
    catalog_path = data_dir / "catalog.json"
    index_path = data_dir / "home-insights-index.json"
    feed_path = data_dir / "home-insights-feed.json"

    catalog = _read_json(catalog_path)
    index = _read_json(index_path)
    feed = _read_json(feed_path)

    for name, obj in [("catalog", catalog), ("home-insights-index", index), ("home-insights-feed", feed)]:
        meta = _as_dict(obj.get("meta"), f"{name}.meta must be an object")
        _assert_valid_iso(meta.get("generatedAt"), f"{name}.meta.generatedAt")
        _assert_ok(
            str(meta.get("schemaVersion") or "").strip() == required_schema,
            f"{name}.meta.schemaVersion must be {required_schema}",
        )

    datasets = _as_list(catalog.get("datasets"), "catalog.datasets must be an array")

    index_items = _as_list(index.get("items"), "home-insights-index.items must be an array")

    feed_items = _as_list(feed.get("items"), "home-insights-feed.items must be an array")

    dataset_ids = []
    for i, ds in enumerate(datasets):
        safe_ds = _as_dict(ds, f"catalog dataset[{i}] must be object")
        for k in [
            "id",
            "pathBaseData",
            "pathInsightsHome",
            "pathOverviewRoot",
            "pathEntitiesRoot",
            "pathRankingsRoot",
            "pathAnalyticsRoot",
        ]:
            _assert_ok(bool(str(safe_ds.get(k) or "").strip()), f"catalog dataset[{i}] missing {k}")
        dataset_ids.append(str(safe_ds.get("id")).strip())
    _assert_ok(len(dataset_ids) == len(set(dataset_ids)), "catalog has duplicated dataset id")

    index_ids = []
    for i, it in enumerate(index_items):
        safe_it = _as_dict(it, f"index item[{i}] must be object")
        for k in ["id", "level", "scope", "orgao", "path", "enabled"]:
            _assert_ok(
                bool(str(safe_it.get(k) or "").strip()) if k != "enabled" else safe_it.get("enabled") is not None,
                f"index item[{i}] missing {k}",
            )
        item_id = str(safe_it.get("id")).strip()
        index_ids.append(item_id)
        rel = str(safe_it.get("path")).strip().lstrip("/")
        _assert_ok((data_dir / rel).exists(), f"index item[{i}] path does not exist: {rel}")
    _assert_ok(len(index_ids) == len(set(index_ids)), "home-insights-index has duplicated id")

    ranked = []
    seen_keys = set()
    for i, it in enumerate(feed_items):
        safe_it = _as_dict(it, f"feed item[{i}] must be object")
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
            _assert_ok(
                bool(safe_it.get(k) is not None and (k == "enabled" or str(safe_it.get(k)).strip())),
                f"feed item[{i}] missing {k}",
            )
        _assert_valid_iso(safe_it.get("generatedAt"), f"feed item[{i}].generatedAt")
        _assert_valid_iso(safe_it.get("freshUntil"), f"feed item[{i}].freshUntil")

        source_id = str(safe_it.get("sourceId")).strip()
        _assert_ok(source_id in index_ids, f"feed item[{i}] sourceId not found in index: {source_id}")
        _assert_ok(
            str(safe_it.get("level") or "").strip() == str(safe_it.get("sourceLevel") or "").strip(),
            f"feed item[{i}] level/sourceLevel mismatch"
        )

        quality = float(safe_it.get("qualityScore") or 0.0)
        weight = float(safe_it.get("weight") or 0.0)
        priority = float(safe_it.get("priority") or 0.0)
        score_final = float(safe_it.get("scoreFinal") or 0.0)
        rank = int(safe_it.get("rank") or 0)
        _assert_ok(0.0 <= quality <= 1.0, f"feed item[{i}] qualityScore out of range")
        _assert_ok(weight >= 0.0, f"feed item[{i}] weight out of range")
        _assert_ok(0.0 <= priority <= 100.0, f"feed item[{i}] priority out of range")
        _assert_ok(score_final >= 0.0, f"feed item[{i}] scoreFinal out of range")
        _assert_ok(rank >= 1, f"feed item[{i}] rank must be >= 1")

        key = f"{source_id}:{str(safe_it.get('id')).strip()}"
        _assert_ok(key not in seen_keys, f"feed has duplicated insight key: {key}")
        seen_keys.add(key)
        ranked.append(rank)

        insight_type = str(safe_it.get("type") or "").strip()
        _assert_ok(insight_type in {"person", "aggregate", "comparison", "alert", "timeline"}, f"feed item[{i}] invalid type")

        if insight_type == "person":
            entity = _as_dict(safe_it.get("entity"), f"feed item[{i}] person.entity must be object")
            _assert_ok(_is_non_empty_scalar(entity.get("id")), f"feed item[{i}] missing person.entity.id")
            for k in ["name", "photoUrl"]:
                _assert_ok(_is_non_empty_str(entity.get(k)), f"feed item[{i}] missing person.entity.{k}")
            value = _as_dict(safe_it.get("value"), f"feed item[{i}] person.value must be object")
            _assert_ok(_is_number(value.get("amount")), f"feed item[{i}] person.value.amount must be number")
            _assert_ok(_is_non_empty_str(safe_it.get("referenceDate")), f"feed item[{i}] missing referenceDate")
            _assert_ok(_is_non_empty_str(safe_it.get("variationText")), f"feed item[{i}] missing variationText")

        elif insight_type == "aggregate":
            value = _as_dict(safe_it.get("value"), f"feed item[{i}] aggregate.value must be object")
            _assert_ok(_is_number(value.get("amount")), f"feed item[{i}] aggregate.value.amount must be number")
            _assert_ok(_has_first_tag(safe_it), f"feed item[{i}] aggregate missing tags[0]")
            _assert_ok(_is_non_empty_str(safe_it.get("referenceDate")), f"feed item[{i}] missing referenceDate")
            _assert_ok(_is_non_empty_str(safe_it.get("variationText")), f"feed item[{i}] missing variationText")

        elif insight_type == "comparison":
            delta = _as_dict(safe_it.get("delta"), f"feed item[{i}] comparison.delta must be object")
            _assert_ok(_is_number(delta.get("percent")), f"feed item[{i}] comparison.delta.percent must be number")
            _assert_ok(_has_first_tag(safe_it), f"feed item[{i}] comparison missing tags[0]")
            _assert_ok(_is_non_empty_str(safe_it.get("referenceDate")), f"feed item[{i}] missing referenceDate")
            _assert_ok(_is_non_empty_str(safe_it.get("variationText")), f"feed item[{i}] missing variationText")

        elif insight_type == "timeline":
            series = _as_list(safe_it.get("series"), f"feed item[{i}] timeline missing series")
            _assert_ok(len(series) > 0, f"feed item[{i}] timeline missing series")
            for j, point in enumerate(series):
                safe_point = _as_dict(point, f"feed item[{i}] timeline.series[{j}] must be object")
                _assert_ok(_is_non_empty_str(safe_point.get("date")), f"feed item[{i}] timeline.series[{j}].date missing")
                _assert_ok(_is_number(safe_point.get("amount")), f"feed item[{i}] timeline.series[{j}].amount must be number")
            _assert_ok(_is_non_empty_str(safe_it.get("referenceDate")), f"feed item[{i}] missing referenceDate")
            _assert_ok(_is_non_empty_str(safe_it.get("variationText")), f"feed item[{i}] missing variationText")

        elif insight_type == "alert":
            _assert_ok(_is_non_empty_str(safe_it.get("severity")), f"feed item[{i}] missing severity")
            baseline = _as_dict(safe_it.get("baseline"), f"feed item[{i}] alert.baseline must be object")
            _assert_ok(_is_number(baseline.get("amount")), f"feed item[{i}] alert.baseline.amount must be number")

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
