import argparse
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

JsonDict = dict[str, Any]

def parse_iso_utc(value):
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def has_invalid_iso(value):
    s = str(value or "").strip()
    if not s:
        return False
    return parse_iso_utc(s) is None


def aggregate_items(raw_items, max_items=300, now_utc=None):
    now = now_utc or datetime.now(timezone.utc)
    filtered = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        if item.get("enabled") is False:
            continue
        if has_invalid_iso(item.get("generatedAt")) or has_invalid_iso(item.get("freshUntil")):
            continue
        quality = float(item.get("qualityScore", 0.0) or 0.0)
        if quality < 0.6:
            continue
        fresh_until = parse_iso_utc(item.get("freshUntil"))
        if fresh_until is not None and fresh_until < now:
            continue
        filtered.append(dict(item))

    dedup: dict[str, JsonDict] = {}
    for item in filtered:
        key = f"{str(item.get('sourceId') or '')}::{str(item.get('id') or '')}"
        prev = dedup.get(key)
        if prev is None:
            dedup[key] = item
            continue
        cur_score = float(item.get("scoreFinal", 0.0) or 0.0)
        prev_score = float(prev.get("scoreFinal", 0.0) or 0.0)
        if cur_score > prev_score:
            dedup[key] = item

    items = list(dedup.values())
    items.sort(key=lambda x: (float(x.get("scoreFinal", 0.0)), str(x.get("id", ""))), reverse=True)

    if max_items > 0 and len(items) > max_items:
        items = items[:max_items]
    return items, (len(filtered) - len(dedup))


def _infer_source_from_file(src_meta, base, base_parts):
    source_scope = str((src_meta or {}).get("scope") or base).strip().strip("/")
    if not source_scope:
        source_scope = base
    scope_parts = [p for p in source_scope.split("/") if p]
    source_level = str(
        (src_meta or {}).get("level")
        or (scope_parts[0] if len(scope_parts) > 0 else (base_parts[0] if len(base_parts) > 0 else "unknown"))
    ).strip() or "unknown"
    source_orgao = str(
        (src_meta or {}).get("orgao")
        or (scope_parts[1] if len(scope_parts) > 1 else (base_parts[1] if len(base_parts) > 1 else source_level))
    ).strip() or source_level
    return source_scope, source_level, source_orgao


def build_catalog_index_feed(data_dir: Path) -> None:
    strict = str(os.getenv("VALIDATE_STRICT_GOVERNANCE", "0")).strip() == "1"
    required_schema = str(os.getenv("HOME_INSIGHTS_SCHEMA_VERSION", "1.0.0")).strip() or "1.0.0"
    max_items = int((os.getenv("HOME_INSIGHTS_FEED_MAX_ITEMS") or "300").strip())
    max_drop_rate = float((os.getenv("HOME_INSIGHTS_FEED_MAX_DROP_RATE") or "0.80").strip())

    out_root = data_dir
    out_root.mkdir(parents=True, exist_ok=True)

    insights_files = sorted(out_root.glob("**/insights/home-insights.json"))
    print(f"[aggregate] insights files found: {len(insights_files)}")

    datasets = {}
    index_items = {}
    candidates = []
    dropped_non_object = 0
    dropped_disabled = 0
    dropped_low_quality = 0
    dropped_expired = 0
    dropped_invalid_timestamp = 0
    now_utc = datetime.now(timezone.utc)

    for fp in insights_files:
        rel = fp.relative_to(out_root).as_posix()
        parts = rel.split("/")
        if len(parts) < 4:
            continue
        base_parts = parts[:-2]
        base = "/".join(base_parts).strip("/")
        if not base:
            continue

        src_obj = json.loads(fp.read_text(encoding="utf-8"))
        src_meta = src_obj.get("meta") if isinstance(src_obj, dict) else {}
        source_scope, source_level, source_orgao = _infer_source_from_file(src_meta, base, base_parts)
        dataset_id = base

        index_items[dataset_id] = {
            "id": dataset_id,
            "level": source_level,
            "scope": source_scope,
            "orgao": source_orgao,
            "path": f"{base}/insights/home-insights.json",
            "enabled": True,
        }

        datasets[dataset_id] = {
            "id": dataset_id,
            "description": f"{dataset_id} ETL outputs",
            "pathBaseData": base,
            "pathInsightsHome": f"{base}/insights/home-insights.json",
            "pathOverviewRoot": f"{base}/overview",
            "pathEntitiesRoot": f"{base}/entities",
            "pathRankingsRoot": f"{base}/rankings",
            "pathAnalyticsRoot": f"{base}/analytics",
            "pathCategoryMap": f"{base}/mapping/categoria/category_map.json",
        }

        src_schema = str((src_meta or {}).get("schemaVersion") or "")
        if src_schema != required_schema:
            raise SystemExit(
                f"schemaVersion invalido em {rel}: esperado={required_schema} recebido={src_schema or '(vazio)'}"
            )

        for raw in (src_obj.get("items") or []):
            if not isinstance(raw, dict):
                dropped_non_object += 1
                continue
            if raw.get("enabled") is False:
                dropped_disabled += 1
                continue
            if has_invalid_iso(raw.get("generatedAt")) or has_invalid_iso(raw.get("freshUntil")):
                dropped_invalid_timestamp += 1
                continue
            quality = float(raw.get("qualityScore", 0.0) or 0.0)
            if quality < 0.6:
                dropped_low_quality += 1
                continue
            fresh_until = parse_iso_utc(raw.get("freshUntil"))
            if fresh_until is not None and fresh_until < now_utc:
                dropped_expired += 1
                continue

            weight = float(raw.get("weight", 0.0) or 0.0)
            priority = float(raw.get("priority", 0.0) or 0.0)
            score = quality * max(0.0001, weight) * (1.0 + (priority / 100.0))
            obj = dict(raw)
            obj["level"] = str(obj.get("level") or source_level).strip() or source_level
            obj["sourceId"] = dataset_id
            obj["sourceLevel"] = source_level
            obj["sourceScope"] = source_scope
            obj["sourceOrgao"] = source_orgao
            obj["scoreFinal"] = round(float(score), 6)
            candidates.append(obj)

    before_filters = (
        len(candidates)
        + dropped_non_object
        + dropped_disabled
        + dropped_low_quality
        + dropped_expired
        + dropped_invalid_timestamp
    )
    print(f"[aggregate] candidate insights: {len(candidates)}")
    print(f"[aggregate] dropped non-object: {dropped_non_object}")
    print(f"[aggregate] dropped disabled: {dropped_disabled}")
    print(f"[aggregate] dropped low-quality: {dropped_low_quality}")
    print(f"[aggregate] dropped expired: {dropped_expired}")
    print(f"[aggregate] dropped invalid timestamp: {dropped_invalid_timestamp}")

    feed_sorted, dedup_removed = aggregate_items(candidates, max_items=0, now_utc=now_utc)
    if max_items > 0 and len(feed_sorted) > max_items:
        print(f"[aggregate] truncating feed: {len(feed_sorted)} -> {max_items}")
        feed_sorted = feed_sorted[:max_items]

    feed_items = []
    rest = list(feed_sorted)
    last_level = ""
    streak = 0
    while rest:
        pick_idx = None
        for i, it in enumerate(rest):
            lvl = str(it.get("level") or "")
            if streak >= 2 and lvl == last_level:
                continue
            pick_idx = i
            break
        if pick_idx is None:
            pick_idx = 0
        picked = rest.pop(pick_idx)
        lvl = str(picked.get("level") or "")
        if lvl == last_level:
            streak += 1
        else:
            last_level = lvl
            streak = 1
        feed_items.append(picked)

    for rank, it in enumerate(feed_items, start=1):
        it["rank"] = rank

    by_level = Counter(str(x.get("level") or "") for x in feed_items)
    by_orgao = Counter(str(x.get("sourceOrgao") or "") for x in feed_items)
    print(f"[aggregate] feed insights: {len(feed_items)}")
    print(f"[aggregate] datasets: {len(datasets)}")
    print(f"[aggregate] distribution by level: {dict(by_level)}")
    print(f"[aggregate] distribution by orgao: {dict(by_orgao)}")
    top_ids = [f"{str(x.get('sourceId') or '')}::{str(x.get('id') or '')}" for x in feed_items[:10]]
    print(f"[aggregate] top10 ids: {top_ids}")

    total_dropped = (
        dropped_non_object
        + dropped_disabled
        + dropped_low_quality
        + dropped_expired
        + dropped_invalid_timestamp
        + max(dedup_removed, 0)
    )
    drop_rate = (float(total_dropped) / float(before_filters)) if before_filters else 0.0
    print(f"[aggregate] drop rate: {drop_rate:.4f} ({total_dropped}/{before_filters})")
    if drop_rate > max_drop_rate:
        msg = f"drop rate acima do limite: {drop_rate:.4f} > {max_drop_rate:.4f}"
        print(f"::warning::{msg}")
        if strict:
            raise SystemExit(f"{msg} VALIDATE_STRICT_GOVERNANCE=1")

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    (out_root / "catalog.json").write_text(
        json.dumps({"meta": {"generatedAt": now, "schemaVersion": required_schema}, "datasets": [datasets[k] for k in sorted(datasets.keys())]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out_root / "home-insights-index.json").write_text(
        json.dumps({"meta": {"generatedAt": now, "schemaVersion": required_schema}, "items": [index_items[k] for k in sorted(index_items.keys())]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out_root / "home-insights-feed.json").write_text(
        json.dumps({"meta": {"generatedAt": now, "schemaVersion": required_schema}, "items": feed_items}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()
    build_catalog_index_feed(Path(args.data_dir).resolve())


if __name__ == "__main__":
    main()
