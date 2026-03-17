import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Mapping, Optional, Tuple, cast


CAMARA_BASE = "https://dadosabertos.camara.leg.br/api/v2"
DEFAULT_SCHEMA_VERSION = "1.0.0"
JsonDict = dict[str, Any]


def artifact_schema_version() -> str:
    return str(os.environ.get("SCHEMA_VERSION", DEFAULT_SCHEMA_VERSION)).strip() or DEFAULT_SCHEMA_VERSION


def home_insights_schema_version() -> str:
    return str(os.environ.get("HOME_INSIGHTS_SCHEMA_VERSION", artifact_schema_version())).strip() or artifact_schema_version()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso_utc(value: object) -> Optional[datetime]:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, obj: object) -> None:
    ensure_dir(path.parent)
    payload = json.dumps(obj, ensure_ascii=False, indent=2)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path) -> Optional[JsonDict]:
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    return cast(JsonDict, raw)


def load_insights_text_catalog(path: Path) -> JsonDict:
    if not path.exists():
        fail(f"Insights text catalog not found: {path}")
    obj = cast(JsonDict, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(obj, dict):
        fail("Insights text catalog must be a JSON object")
    if not isinstance(obj.get("defaults"), dict):
        fail("Insights text catalog missing defaults object")
    if not isinstance(obj.get("templates"), dict):
        fail("Insights text catalog missing templates object")
    if not isinstance(obj.get("insights"), dict):
        fail("Insights text catalog missing insights object")
    return obj


def fail(msg: str) -> None:
    raise RuntimeError(msg)


def parse_int_env(name: str, default: int, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
    raw = str(os.environ.get(name, str(default))).strip()
    try:
        value = int(raw)
    except Exception:
        fail(f"Invalid integer for {name}: {raw!r}")
    if min_value is not None and value < min_value:
        fail(f"{name} out of range: {value} < {min_value}")
    if max_value is not None and value > max_value:
        fail(f"{name} out of range: {value} > {max_value}")
    return value


def validate_out_dir(path: Path) -> None:
    s = str(path).strip().lower()
    if not s or s in {"/", "\\", "c:\\", "c:"}:
        fail(f"Unsafe OUT_DIR: {path}")


def safe_slug(text: object) -> str:
    text = str(text or "")
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s]+", "-", text)
    return text.strip("-")


def normalize_doc_date(value: object) -> Optional[str]:
    s = str(value or "").strip()
    if not s:
        return None
    if len(s) >= 10:
        cand = s[:10]
        try:
            datetime.strptime(cand, "%Y-%m-%d")
            return cand
        except Exception:
            pass
    try:
        d = datetime.strptime(s, "%d/%m/%Y")
        return d.strftime("%Y-%m-%d")
    except Exception:
        return None


def parse_monetary(value: object) -> float:
    if value is None:
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def split_financial_values(item: Mapping[str, Any]) -> Tuple[float, float, float]:
    raw_liq = item.get("valorLiquido")
    has_liq = raw_liq is not None and str(raw_liq).strip() != ""
    valor_liquido = parse_monetary(raw_liq if has_liq else item.get("valorDocumento"))
    valor_bruto = valor_liquido if valor_liquido > 0 else 0.0
    valor_ajustes = valor_liquido if valor_liquido < 0 else 0.0
    return valor_bruto, valor_ajustes, valor_liquido


@dataclass
class CategoryMap:
    version: str
    rules: List[Tuple[re.Pattern, str]]
    default: str


def load_category_map(path: Path) -> CategoryMap:
    cfg = cast(JsonDict, json.loads(path.read_text(encoding="utf-8")))
    rules: List[Tuple[re.Pattern, str]] = []
    for rule in cast(List[JsonDict], cfg.get("rules", [])):
        rules.append((re.compile(rule["match"], re.IGNORECASE), rule["category"]))
    default = str(cfg.get("default", "OUTROS") or "OUTROS")
    version = str(cfg.get("version", "1.0.0") or "1.0.0")
    return CategoryMap(version=version, rules=rules, default=default)


def categorize(tipo_despesa: str, cmap: CategoryMap) -> str:
    t = (tipo_despesa or "").strip()
    for rx, cat in cmap.rules:
        if rx.search(t):
            return cat
    return cmap.default


def all_categories(cmap: CategoryMap) -> List[str]:
    cats = {cat for _, cat in cmap.rules}
    cats.add(cmap.default)
    return sorted(cats)


def build_metodologia_global() -> dict:
    return {
        "meta": {
            "id": "methodology-global",
            "scope": "global",
            "generatedAt": now_iso(),
            "schemaVersion": artifact_schema_version(),
            "documentVersion": "1.0.0",
        },
        "principles": [
            "Only official public datasets are used.",
            "This output does not replace official source systems.",
            "Processing is deterministic and reproducible.",
            "No subjective judgment about individuals is applied.",
            "Formulas, versions and criteria are explicit.",
        ],
        "baseOperations": [
            {"id": "sum", "description": "Monetary sums by period and segment."},
            {"id": "sort", "description": "Ascending or descending sort by metric."},
            {"id": "filter", "description": "Explicit filter application."},
            {"id": "group", "description": "Grouping by category rules and official keys."},
            {"id": "round", "description": "Monetary rounding to 2 decimals."},
        ],
        "governance": {
            "divergenceRule": "If any divergence is found, official source data prevails.",
            "minimumAudit": [
                "Official source used.",
                "Category mapping version.",
                "Artifact schema version.",
                "Generation timestamp.",
            ],
        },
    }


def build_metodologia_scope_camara(cmap: CategoryMap, mandate_start_year: int) -> dict:
    return {
        "meta": {
            "id": "methodology-federal-camara",
            "scope": "federal/camara",
            "generatedAt": now_iso(),
            "schemaVersion": artifact_schema_version(),
            "documentVersion": "1.0.0",
        },
        "source": {
            "organization": "Camara dos Deputados",
            "apiBase": CAMARA_BASE,
            "endpoints": ["/deputados", "/deputados/{id}/despesas"],
        },
        "periodization": {
            "month": "Queries API by year/month and produces monthly entities/contracts.",
            "year": "Sum of available monthly entities for the year.",
            "mandate": f"Sum of monthly entities from {mandate_start_year} forward.",
        },
        "categorization": {
            "pathCategoryMap": "federal/camara/mapping/categoria/category_map.json",
            "categoryMapVersion": cmap.version,
            "default": cmap.default,
            "description": "Official expense type grouped by versioned regex rules.",
        },
        "criteria": {
            "activeEntityInPeriod": "Deputy with recordsCount > 0 or amountNet > 0 in the period.",
        },
        "outputArtifacts": [
            {"path": "federal/camara/overview/{period}/overview.json", "description": "Aggregated KPIs and highlights by period."},
            {"path": "federal/camara/entities/{period}/entities.json", "description": "Entities list and metrics by period."},
            {"path": "federal/camara/rankings/{period}/ranking-total.json", "description": "Top and bottom ranking by total amount."},
            {"path": "federal/camara/analytics/{period}/expense-types.json", "description": "Distribution by expense type."},
            {"path": "federal/camara/analytics/{period}/pending-categories.json", "description": "Expense types mapped to default category."},
            {"path": "federal/camara/entities/profiles/{id}.json", "description": "Entity profile with mandate totals."},
            {"path": "federal/camara/entities/index.json", "description": "Index of entities for search and filters."},
            {"path": "federal/camara/insights/home-insights.json", "description": "Home insights pool for this scope."},
        ],
        "indicators": [
            {
                "id": "totalAccumulatedInMandate",
                "outputSource": "overview/mandate/overview.json:base.amountNet",
                "formula": "sum(amountNet by active entity in period)",
            },
            {
                "id": "topSpenderInMandate",
                "outputSource": "overview/mandate/overview.json:highlights.topSpender",
                "formula": "argmax(amountNet by active entity in period)",
            },
            {
                "id": "top10Concentration",
                "outputSource": "overview/mandate/overview.json:highlights.concentrationTop10",
                "formula": "sum(top 10 entities amountNet) and share over base.amountNet",
            },
            {
                "id": "dailySummary",
                "outputSource": "overview/mandate/overview.json:highlights.dailyTotals + highlights.dailyTopExpense",
                "formula": "latest document date; daily total, top expense and deltas",
            },
        ],
    }


def write_metodologia_docs(out_dir: Path, cmap: CategoryMap, mandate_start_year: int) -> None:
    write_json(out_dir / "metodologia.json", build_metodologia_global())
    write_json(out_dir / "federal/camara/metodologia_scope.json", build_metodologia_scope_camara(cmap, mandate_start_year))


def parse_months(s: str) -> List[int]:
    raw = str(s or "").strip()
    if not raw:
        fail("MONTHS is empty")
    values: List[int]
    if "-" in raw:
        a, b = raw.split("-", 1)
        if not a.strip() or not b.strip():
            fail(f"Invalid MONTHS range: {raw!r}")
        start = int(a)
        end = int(b)
        if start > end:
            fail(f"Invalid MONTHS range (start > end): {raw!r}")
        values = list(range(start, end + 1))
    elif "," in raw:
        values = [int(x.strip()) for x in raw.split(",") if x.strip()]
    else:
        values = [int(raw)]
    if not values:
        fail("MONTHS did not resolve to any month")
    for month in values:
        if month < 1 or month > 12:
            fail(f"MONTHS has out-of-range value: {month}")
    return sorted(set(values))


def period_key(periodo_meta: Mapping[str, Any]) -> str:
    t = str((periodo_meta or {}).get("tipo") or "").strip().lower()
    if t == "mes":
        ano = int(periodo_meta.get("ano") or 0)
        mes = int(periodo_meta.get("mes") or 0)
        return f"month-{ano:04d}-{mes:02d}"
    if t == "ano":
        ano = int(periodo_meta.get("ano") or 0)
        return f"year-{ano:04d}"
    return "mandate"


def fresh_until_from(generated_at: str, hours: int = 24) -> str:
    base = datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
    return (base + timedelta(hours=hours)).isoformat().replace("+00:00", "Z")
