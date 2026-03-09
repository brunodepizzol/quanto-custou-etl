import os
import json
import re
import time
import shutil
import fnmatch
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests

CAMARA_BASE = "https://dadosabertos.camara.leg.br/api/v2"

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def parse_iso_utc(value: str) -> Optional[datetime]:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_json(path: Path, obj) -> None:
    ensure_dir(path.parent)
    payload = json.dumps(obj, ensure_ascii=False, indent=2)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)

def remove_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()

def read_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))

def load_insights_text_catalog(path: Path) -> dict:
    if not path.exists():
        fail(f"Insights text catalog not found: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
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

def safe_slug(text):
    text = text or ""
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s]+", "-", text)
    return text.strip("-")

def normalize_doc_date(value: str) -> Optional[str]:
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

def parse_monetary(value) -> float:
    if value is None:
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0

def split_financial_values(item: dict) -> Tuple[float, float, float]:
    raw_liq = item.get("valorLiquido")
    has_liq = raw_liq is not None and str(raw_liq).strip() != ""
    valor_liquido = parse_monetary(raw_liq if has_liq else item.get("valorDocumento"))
    valor_bruto = valor_liquido if valor_liquido > 0 else 0.0
    valor_ajustes = valor_liquido if valor_liquido < 0 else 0.0
    return valor_bruto, valor_ajustes, valor_liquido
    
def build_metodologia_global() -> dict:
    return {
        "meta": {
            "id": "methodology-global",
            "scope": "global",
            "generatedAt": now_iso(),
            "schemaVersion": "1.0.0",
            "documentVersion": "1.0.0"
        },
        "principles": [
            "Only official public datasets are used.",
            "This output does not replace official source systems.",
            "Processing is deterministic and reproducible.",
            "No subjective judgment about individuals is applied.",
            "Formulas, versions and criteria are explicit."
        ],
        "baseOperations": [
            {"id": "sum", "description": "Monetary sums by period and segment."},
            {"id": "sort", "description": "Ascending or descending sort by metric."},
            {"id": "filter", "description": "Explicit filter application."},
            {"id": "group", "description": "Grouping by category rules and official keys."},
            {"id": "round", "description": "Monetary rounding to 2 decimals."}
        ],
        "governance": {
            "divergenceRule": "If any divergence is found, official source data prevails.",
            "minimumAudit": [
                "Official source used.",
                "Category mapping version.",
                "Artifact schema version.",
                "Generation timestamp."
            ]
        }
    }

def build_metodologia_scope_camara(cmap: "CategoryMap", mandate_start_year: int) -> dict:
    return {
        "meta": {
            "id": "methodology-federal-camara",
            "scope": "federal/camara",
            "generatedAt": now_iso(),
            "schemaVersion": "1.0.0",
            "documentVersion": "1.0.0"
        },
        "source": {
            "organization": "Camara dos Deputados",
            "apiBase": CAMARA_BASE,
            "endpoints": [
                "/deputados",
                "/deputados/{id}/despesas"
            ]
        },
        "periodization": {
            "month": "Queries API by year/month and produces monthly entities/contracts.",
            "year": "Sum of available monthly entities for the year.",
            "mandate": f"Sum of monthly entities from {mandate_start_year} forward."
        },
        "categorization": {
            "pathCategoryMap": "federal/camara/mapping/categoria/category_map.json",
            "categoryMapVersion": cmap.version,
            "default": cmap.default,
            "description": "Official expense type grouped by versioned regex rules."
        },
        "criteria": {
            "activeEntityInPeriod": "Deputy with recordsCount > 0 or amountNet > 0 in the period."
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
            {"path": "home-insights-index.json", "description": "Global index of home insights pools."},
            {"path": "catalog.json", "description": "Global data catalog for all scopes."}
        ],
        "indicators": [
            {
                "id": "totalAccumulatedInMandate",
                "outputSource": "overview/mandate/overview.json:base.amountNet",
                "formula": "sum(amountNet by active entity in period)"
            },
            {
                "id": "topSpenderInMandate",
                "outputSource": "overview/mandate/overview.json:highlights.topSpender",
                "formula": "argmax(amountNet by active entity in period)"
            },
            {
                "id": "top10Concentration",
                "outputSource": "overview/mandate/overview.json:highlights.concentrationTop10",
                "formula": "sum(top 10 entities amountNet) and share over base.amountNet"
            },
            {
                "id": "dailyHighlights",
                "outputSource": "overview/mandate/overview.json:highlights.dailyHighlight",
                "formula": "latest document date; daily total, top spender and deltas"
            }
        ]
    }

def write_metodologia_docs(out_dir: Path, cmap: "CategoryMap", mandate_start_year: int) -> None:
    write_json(out_dir / "metodologia.json", build_metodologia_global())
    write_json(out_dir / "federal/camara/metodologia_scope.json", build_metodologia_scope_camara(cmap, mandate_start_year))


def http_get_json(url: str, params: dict = None, retries: int = 8, timeout_s: int = 120) -> dict:
    """GET JSON with retry/backoff for transient HTTP failures (429/5xx)."""
    headers = {"Accept": "application/json"}
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout_s)
            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                if ra and ra.isdigit():
                    time.sleep(min(int(ra), 30))
                else:
                    time.sleep(min(2 ** attempt, 30))
                last_err = requests.exceptions.HTTPError(f"{r.status_code} for {r.url}")
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            last_err = e
            time.sleep(min(2 ** attempt, 30))
        except Exception as e:
            last_err = e
            time.sleep(min(2 ** attempt, 30))
    raise last_err

def paginate(endpoint: str, params: dict) -> List[dict]:
    """Paginate Camara API. If timeouts happen, auto-reduce page size."""
    itens = int(params.get("itens", 100))
    pagina = 1
    out: List[dict] = []
    while True:
        p = dict(params)
        p["itens"] = itens
        p["pagina"] = pagina
        try:
            data = http_get_json(endpoint, params=p)
        except Exception as e:
            msg = str(e)
            if ("504" in msg or "Gateway Timeout" in msg) and itens > 25:
                itens = 50 if itens > 50 else 25
                print(f"WARN: 504 response, reducing page size to {itens} and retrying (page={pagina}).")
                continue
            raise
        dados = data.get("dados", [])
        out.extend(dados)
        links = data.get("links", [])
        has_next = any(l.get("rel") == "next" for l in links)
        if not has_next or not dados:
            break
        pagina += 1
    return out
@dataclass
class CategoryMap:
    version: str
    rules: List[Tuple[re.Pattern, str]]
    default: str

def load_category_map(path: Path) -> CategoryMap:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    rules: List[Tuple[re.Pattern, str]] = []
    for rule in cfg.get("rules", []):
        rules.append((re.compile(rule["match"], re.IGNORECASE), rule["category"]))
    default = cfg.get("default", "OUTROS")
    version = cfg.get("version", "1.0.0")
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

def fetch_deputados() -> List[dict]:
    return paginate(f"{CAMARA_BASE}/deputados", {
        "ordem": "ASC",
        "ordenarPor": "nome",
        "itens": 100
    })

def fetch_despesas(dep_id: int, ano: int, mes: int) -> List[dict]:
    return paginate(f"{CAMARA_BASE}/deputados/{dep_id}/despesas", {
        "ano": ano,
        "mes": mes,
        "itens": 100
    })

def build_month_aggregates(
    deputados: List[dict],
    ano: int,
    mes: int,
    cmap: CategoryMap,
    sleep_s: float = 0.05
) -> Tuple[dict, dict, List[str], Optional[dict]]:
    """
    Returns:
      aggregates_obj: meta + data (per dep totals)
      tipo_resumo_obj: meta + data (sum by tipoDespesa)
      pendentes: list of tipoDespesa that fell into default category
    """
    dep_map = {int(d["id"]): d for d in deputados}
    cats = all_categories(cmap)

    tipo_resumo: Dict[str, dict] = {}
    pendentes_set = set()
    max_dia_por_data: Dict[str, dict] = {}
    total_dia_por_data: Dict[str, float] = {}

    rows = []
    for i, dep in enumerate(deputados, start=1):
        dep_id = int(dep["id"])
        despesas = fetch_despesas(dep_id, ano, mes)

        total_liquido = 0.0
        total_bruto = 0.0
        total_ajustes = 0.0
        por_cat_liquido = {c: 0.0 for c in cats}
        por_cat_bruto = {c: 0.0 for c in cats}
        por_cat_ajustes = {c: 0.0 for c in cats}
        qtd = 0

        for x in despesas:
            qtd += 1
            v_bruto, v_ajustes, v_liquido = split_financial_values(x)
            total_liquido += v_liquido
            total_bruto += v_bruto
            total_ajustes += v_ajustes

            tipo = (x.get("tipoDespesa") or "").strip() or "(Sem tipo)"
            cat = categorize(tipo, cmap)
            por_cat_liquido.setdefault(cat, 0.0)
            por_cat_liquido[cat] += v_liquido
            por_cat_bruto.setdefault(cat, 0.0)
            por_cat_bruto[cat] += v_bruto
            por_cat_ajustes.setdefault(cat, 0.0)
            por_cat_ajustes[cat] += v_ajustes

            data_doc = normalize_doc_date(x.get("dataDocumento"))
            if data_doc:
                total_dia_por_data[data_doc] = float(total_dia_por_data.get(data_doc, 0.0)) + float(v_liquido)
                cand = {
                    "dataReferencia": data_doc,
                    "deputado": {
                        "id": dep_id,
                        "nome": dep_map[dep_id].get("nome"),
                        "uf": dep_map[dep_id].get("siglaUf"),
                        "partido": dep_map[dep_id].get("siglaPartido")
                    },
                    "valor": round(v_liquido, 2),
                    "tipoDespesa": tipo,
                    "categoriaQC": cat,
                    "fornecedor": (x.get("nomeFornecedor") or "").strip() or None,
                    "tipoDocumento": (x.get("tipoDocumento") or "").strip() or None,
                    "urlDocumento": (x.get("urlDocumento") or "").strip() or None
                }
                prev = max_dia_por_data.get(data_doc)
                if (not prev) or (float(cand["valor"]) > float(prev.get("valor", 0.0))):
                    max_dia_por_data[data_doc] = cand

            bucket = tipo_resumo.setdefault(
                tipo,
                {
                    "tipoDespesa": tipo,
                    "valorTotal": 0.0,
                    "valorLiquido": 0.0,
                    "valorBruto": 0.0,
                    "valorAjustes": 0.0,
                    "qtd": 0
                }
            )
            bucket["valorTotal"] += v_liquido
            bucket["valorLiquido"] += v_liquido
            bucket["valorBruto"] += v_bruto
            bucket["valorAjustes"] += v_ajustes
            bucket["qtd"] += 1
            if cat == cmap.default:
                pendentes_set.add(tipo)

        rows.append({
            "id": dep_id,
            "nome": dep_map[dep_id].get("nome"),
            "uf": dep_map[dep_id].get("siglaUf"),
            "partido": dep_map[dep_id].get("siglaPartido"),
            "urlFoto": dep_map[dep_id].get("urlFoto"),
            "totais": {
                "total": round(total_liquido, 2),
                "totalLiquido": round(total_liquido, 2),
                "totalBruto": round(total_bruto, 2),
                "totalAjustes": round(total_ajustes, 2),
                "qtdLancamentos": qtd,
                "porCategoria": {k: round(v, 2) for k, v in por_cat_liquido.items()},
                "porCategoriaLiquido": {k: round(v, 2) for k, v in por_cat_liquido.items()},
                "porCategoriaBruto": {k: round(v, 2) for k, v in por_cat_bruto.items()},
                "porCategoriaAjustes": {k: round(v, 2) for k, v in por_cat_ajustes.items()}
            }
        })

        if sleep_s > 0:
            time.sleep(sleep_s)

        if i % 50 == 0:
            print(f"[{ano}-{mes:02d}] processados {i}/{len(deputados)} deputados...")

    tipo_lista = list(tipo_resumo.values())
    for item in tipo_lista:
        item["valorTotal"] = round(float(item.get("valorTotal", 0.0)), 2)
        item["valorLiquido"] = round(float(item.get("valorLiquido", 0.0)), 2)
        item["valorBruto"] = round(float(item.get("valorBruto", 0.0)), 2)
        item["valorAjustes"] = round(float(item.get("valorAjustes", 0.0)), 2)
    tipo_lista.sort(key=lambda x: x["valorTotal"], reverse=True)

    insight_diario = None
    if max_dia_por_data:
        data_mais_recente = max(max_dia_por_data.keys())
        insight_diario = dict(max_dia_por_data.get(data_mais_recente) or {})
        total_dia = round(float(total_dia_por_data.get(data_mais_recente, insight_diario.get("valor", 0.0)) or 0.0), 2)
        datas_ordenadas = sorted(total_dia_por_data.keys())
        idx_data = datas_ordenadas.index(data_mais_recente) if data_mais_recente in datas_ordenadas else -1
        data_anterior = datas_ordenadas[idx_data - 1] if idx_data > 0 else None
        total_dia_anterior = round(float(total_dia_por_data.get(data_anterior, 0.0) or 0.0), 2) if data_anterior else 0.0
        delta = round(total_dia - total_dia_anterior, 2)
        pct = round((delta / total_dia_anterior * 100.0), 2) if total_dia_anterior > 0 else 0.0
        direction = "up" if delta > 0 else ("down" if delta < 0 else "flat")
        trend_dates = datas_ordenadas[-7:]
        trend7d = [{"date": d, "amountNet": round(float(total_dia_por_data.get(d, 0.0) or 0.0), 2)} for d in trend_dates]
        insight_diario["totalDia"] = total_dia
        insight_diario["totalDiaAnterior"] = total_dia_anterior
        insight_diario["deltaTotalDia"] = delta
        insight_diario["deltaPercentTotalDia"] = pct
        insight_diario["deltaDirection"] = direction
        insight_diario["trend7d"] = trend7d
        insight_diario["trend7dTotal"] = round(sum(x["amountNet"] for x in trend7d), 2)

    aggregates_obj = {
        "meta": {
            "fonte": "Camara Dados Abertos",
            "escopo": "federal/camara",
            "periodo": {"ano": ano, "mes": mes},
            "geradoEm": now_iso(),
            "versaoSchema": "2.0.0",
            "versaoCategoryMap": cmap.version,
            "insights": {
                "diario": insight_diario,
                "dailyTotals": {k: round(float(v or 0.0), 2) for k, v in sorted(total_dia_por_data.items())}
            }
        },
        "data": rows
    }

    tipo_resumo_obj = {
        "meta": {
            "fonte": "Camara Dados Abertos",
            "escopo": "federal/camara",
            "periodo": {"ano": ano, "mes": mes},
            "geradoEm": now_iso()
        },
        "data": tipo_lista
    }

    pendentes = sorted(p for p in pendentes_set if p not in ("(Sem tipo)",))
    return aggregates_obj, tipo_resumo_obj, pendentes, insight_diario

def mk_ranking(meta_base: dict, itens: List[dict]) -> dict:
    return {"meta": meta_base, "itens": itens}

def cleanup_integridade_rankings(ranking_dir: Path) -> None:
    remove_if_exists(ranking_dir / "integridade_sem_pdf_top10.json")
    remove_if_exists(ranking_dir / "integridade_outros_recibos_top10.json")

def build_rankings_from_rows(rows: List[dict], periodo_meta: dict, cmap: CategoryMap) -> Dict[str, dict]:
    cats = all_categories(cmap)

    base_meta_total = {
        "tipo": "ranking_total",
        "escopo": "federal/camara",
        "periodo": periodo_meta,
        "geradoEm": now_iso(),
        "versaoSchema": "2.0.0",
        "versaoCategoryMap": cmap.version,
        "criterio": {"consideraAtivosNoPeriodo": True}
    }

    ativos = [r for r in rows if (r["totais"]["qtdLancamentos"] > 0 or r["totais"]["total"] > 0)]

    total_sorted = sorted(ativos, key=lambda r: r["totais"]["total"], reverse=True)
    top_total = [{
        "id": r["id"], "nome": r["nome"], "uf": r["uf"], "partido": r["partido"],
        "valor": round(float(r["totais"].get("totalLiquido", r["totais"]["total"])), 2),
        "valorLiquido": round(float(r["totais"].get("totalLiquido", r["totais"]["total"])), 2),
        "valorBruto": round(float(r["totais"].get("totalBruto", max(0.0, float(r["totais"]["total"])))), 2),
        "valorAjustes": round(float(r["totais"].get("totalAjustes", min(0.0, float(r["totais"]["total"])))), 2),
        "qtdLancamentos": r["totais"]["qtdLancamentos"]
    } for r in total_sorted[:10]]

    bottom_total = [{
        "id": r["id"], "nome": r["nome"], "uf": r["uf"], "partido": r["partido"],
        "valor": round(float(r["totais"].get("totalLiquido", r["totais"]["total"])), 2),
        "valorLiquido": round(float(r["totais"].get("totalLiquido", r["totais"]["total"])), 2),
        "valorBruto": round(float(r["totais"].get("totalBruto", max(0.0, float(r["totais"]["total"])))), 2),
        "valorAjustes": round(float(r["totais"].get("totalAjustes", min(0.0, float(r["totais"]["total"])))), 2),
        "qtdLancamentos": r["totais"]["qtdLancamentos"]
    } for r in sorted(ativos, key=lambda r: r["totais"]["total"])[:10]]

    out: Dict[str, dict] = {}
    out["total_top10.json"] = mk_ranking(base_meta_total, top_total)
    out["total_bottom10.json"] = mk_ranking(base_meta_total, bottom_total)

    for cat in cats:
        meta_cat = {
            "tipo": "ranking_categoria",
            "escopo": "federal/camara",
            "categoriaQC": cat,
            "periodo": periodo_meta,
            "geradoEm": now_iso(),
            "versaoSchema": "2.0.0",
            "versaoCategoryMap": cmap.version,
            "criterio": {"consideraAtivosNoPeriodo": True}
        }
        arr = []
        for r in ativos:
            v = float((r["totais"].get("porCategoriaLiquido") or r["totais"].get("porCategoria") or {}).get(cat, 0.0))
            v_bruto = float((r["totais"].get("porCategoriaBruto") or {}).get(cat, max(v, 0.0)))
            v_ajustes = float((r["totais"].get("porCategoriaAjustes") or {}).get(cat, min(v, 0.0)))
            arr.append({
                "id": r["id"], "nome": r["nome"], "uf": r["uf"], "partido": r["partido"],
                "valor": round(v, 2),
                "valorLiquido": round(v, 2),
                "valorBruto": round(v_bruto, 2),
                "valorAjustes": round(v_ajustes, 2),
                "qtdLancamentos": r["totais"]["qtdLancamentos"]
            })
        arr_sorted = sorted(arr, key=lambda x: x["valor"], reverse=True)[:10]
        out[f"categoria_{cat}_top10.json"] = mk_ranking(meta_cat, arr_sorted)

    return out

def build_overview_from_rows(rows: List[dict], periodo_meta: dict, cmap: CategoryMap, daily_insight: Optional[dict] = None) -> dict:
    cats = all_categories(cmap)

    total_geral = 0.0
    total_bruto_geral = 0.0
    total_ajustes_geral = 0.0
    soma_cat_liquido = {c: 0.0 for c in cats}
    soma_cat_bruto = {c: 0.0 for c in cats}
    soma_cat_ajustes = {c: 0.0 for c in cats}
    soma_uf: Dict[str, dict] = {}
    total_lancamentos = 0

    ativos = [r for r in rows if (r["totais"]["qtdLancamentos"] > 0 or r["totais"]["total"] > 0)]
    base_agentes = len(rows)
    com_gasto_agentes = len(ativos)
    sem_gasto_agentes = max(0, base_agentes - com_gasto_agentes)
    for r in ativos:
        total_liq = float(r["totais"].get("totalLiquido", r["totais"]["total"]))
        total_bruto = float(r["totais"].get("totalBruto", max(total_liq, 0.0)))
        total_ajustes = float(r["totais"].get("totalAjustes", min(total_liq, 0.0)))
        total_geral += total_liq
        total_bruto_geral += total_bruto
        total_ajustes_geral += total_ajustes
        total_lancamentos += int(r["totais"]["qtdLancamentos"])
        uf = r.get("uf") or "?"
        if uf not in soma_uf:
            soma_uf[uf] = {"valorLiquido": 0.0, "valorBruto": 0.0, "valorAjustes": 0.0}
        soma_uf[uf]["valorLiquido"] += total_liq
        soma_uf[uf]["valorBruto"] += total_bruto
        soma_uf[uf]["valorAjustes"] += total_ajustes
        for c in cats:
            v_liq = float((r["totais"].get("porCategoriaLiquido") or r["totais"].get("porCategoria") or {}).get(c, 0.0))
            v_bruto = float((r["totais"].get("porCategoriaBruto") or {}).get(c, max(v_liq, 0.0)))
            v_ajustes = float((r["totais"].get("porCategoriaAjustes") or {}).get(c, min(v_liq, 0.0)))
            soma_cat_liquido[c] += v_liq
            soma_cat_bruto[c] += v_bruto
            soma_cat_ajustes[c] += v_ajustes

    top1 = None
    if ativos:
        top1r = max(ativos, key=lambda r: r["totais"]["total"])
        top1_liq = float(top1r["totais"].get("totalLiquido", top1r["totais"]["total"]))
        top1_bruto = float(top1r["totais"].get("totalBruto", max(top1_liq, 0.0)))
        top1_ajustes = float(top1r["totais"].get("totalAjustes", min(top1_liq, 0.0)))
        top1 = {
            "id": top1r["id"],
            "nome": top1r["nome"],
            "uf": top1r["uf"],
            "partido": top1r["partido"],
            "valor": round(top1_liq, 2),
            "valorLiquido": round(top1_liq, 2),
            "valorBruto": round(top1_bruto, 2),
            "valorAjustes": round(top1_ajustes, 2)
        }

    top_cats_raw = sorted(
        [
            {
                "categoriaQC": c,
                "valor": float(soma_cat_liquido[c]),
                "valorLiquido": float(soma_cat_liquido[c]),
                "valorBruto": float(soma_cat_bruto[c]),
                "valorAjustes": float(soma_cat_ajustes[c])
            }
            for c in cats
        ],
        key=lambda x: x["valor"],
        reverse=True
    )
    top_n = 8
    top_n_list = [
        {
            "categoriaQC": x["categoriaQC"],
            "valor": round(float(x["valor"]), 2),
            "valorLiquido": round(float(x["valorLiquido"]), 2),
            "valorBruto": round(float(x["valorBruto"]), 2),
            "valorAjustes": round(float(x["valorAjustes"]), 2)
        }
        for x in top_cats_raw[:top_n]
    ]
    soma_top_n_liq = sum(float(x["valorLiquido"]) for x in top_cats_raw[:top_n])
    soma_top_n_bruto = sum(float(x["valorBruto"]) for x in top_cats_raw[:top_n])
    soma_top_n_ajustes = sum(float(x["valorAjustes"]) for x in top_cats_raw[:top_n])
    resto_liq = float(total_geral) - float(soma_top_n_liq)
    resto_bruto = float(total_bruto_geral) - float(soma_top_n_bruto)
    resto_ajustes = float(total_ajustes_geral) - float(soma_top_n_ajustes)
    top_cats = top_n_list + [{
        "categoriaQC": "DEMAIS CATEGORIAS",
        "valor": round(resto_liq, 2),
        "valorLiquido": round(resto_liq, 2),
        "valorBruto": round(resto_bruto, 2),
        "valorAjustes": round(resto_ajustes, 2)
    }]

    top_ufs = sorted(
        [
            {
                "uf": uf,
                "valor": round(float(v.get("valorLiquido", 0.0)), 2),
                "valorLiquido": round(float(v.get("valorLiquido", 0.0)), 2),
                "valorBruto": round(float(v.get("valorBruto", 0.0)), 2),
                "valorAjustes": round(float(v.get("valorAjustes", 0.0)), 2)
            }
            for uf, v in soma_uf.items()
        ],
        key=lambda x: x["valor"],
        reverse=True
    )[:12]

    top10_total = sum(float(r["totais"].get("totalLiquido", r["totais"]["total"])) for r in sorted(ativos, key=lambda x: float(x["totais"]["total"]), reverse=True)[:10])
    top10_bruto = sum(float(r["totais"].get("totalBruto", max(float(r["totais"]["total"]), 0.0))) for r in sorted(ativos, key=lambda x: float(x["totais"]["total"]), reverse=True)[:10])
    top10_ajustes = sum(float(r["totais"].get("totalAjustes", min(float(r["totais"]["total"]), 0.0))) for r in sorted(ativos, key=lambda x: float(x["totais"]["total"]), reverse=True)[:10])
    top10_pct = (top10_total / total_geral * 100.0) if total_geral > 0 else 0.0
    media_por_agente = (total_geral / com_gasto_agentes) if com_gasto_agentes > 0 else 0.0
    media_por_lancamento = (total_geral / total_lancamentos) if total_lancamentos > 0 else 0.0

    return {
        "meta": {
            "tipo": "overview",
            "escopo": "federal/camara",
            "periodo": periodo_meta,
            "geradoEm": now_iso(),
            "versaoSchema": "2.0.0",
            "versaoCategoryMap": cmap.version
        },
        "kpis": {
            "totalGasto": round(total_geral, 2),
            "totalLiquido": round(total_geral, 2),
            "totalBruto": round(total_bruto_geral, 2),
            "totalAjustes": round(total_ajustes_geral, 2),
            "top1Gasto": top1,
            "topCategorias": top_cats,
            "topUFs": top_ufs,
            "agentesConsiderados": com_gasto_agentes,
            "agentesBase": base_agentes,
            "agentesComGasto": com_gasto_agentes,
            "agentesSemGasto": sem_gasto_agentes,
            "totalLancamentos": int(total_lancamentos)
        },
        "insights": {
            "gasto": {
                "top1Gasto": top1
            },
            "categoria": {
                "top8MaisDemais": top_cats
            },
            "uf": {
                "topUFs": top_ufs
            },
            "concentracao": {
                "top10Total": round(top10_total, 2),
                "top10Liquido": round(top10_total, 2),
                "top10Bruto": round(top10_bruto, 2),
                "top10Ajustes": round(top10_ajustes, 2),
                "top10Percentual": round(top10_pct, 2)
            },
            "medias": {
                "porAgenteComGasto": round(media_por_agente, 2),
                "porLancamento": round(media_por_lancamento, 2)
            },
            "diario": daily_insight,
            "base": {
                "agentesBase": base_agentes,
                "agentesComGasto": com_gasto_agentes,
                "agentesSemGasto": sem_gasto_agentes,
                "totalLancamentos": int(total_lancamentos),
                "totalGasto": round(total_geral, 2),
                "totalLiquido": round(total_geral, 2),
                "totalBruto": round(total_bruto_geral, 2),
                "totalAjustes": round(total_ajustes_geral, 2)
            }
        }
    }

def build_consulta_deputados_from_rows(rows: List[dict], periodo_meta: dict, cmap: CategoryMap) -> dict:
    itens = []
    for r in rows:
        totais = r.get("totais") or {}
        itens.append({
            "id": r.get("id"),
            "nome": r.get("nome"),
            "uf": r.get("uf"),
            "partido": r.get("partido"),
            "urlFoto": r.get("urlFoto"),
            "total": round(float(totais.get("total", 0.0) or 0.0), 2),
            "totalLiquido": round(float(totais.get("totalLiquido", totais.get("total", 0.0)) or 0.0), 2),
            "totalBruto": round(float(totais.get("totalBruto", max(0.0, float(totais.get("total", 0.0) or 0.0))) or 0.0), 2),
            "totalAjustes": round(float(totais.get("totalAjustes", min(0.0, float(totais.get("total", 0.0) or 0.0))) or 0.0), 2),
            "qtdLancamentos": int(totais.get("qtdLancamentos", 0) or 0),
            "porCategoria": {k: round(float(v or 0.0), 2) for k, v in (totais.get("porCategoria") or {}).items()},
            "porCategoriaLiquido": {k: round(float(v or 0.0), 2) for k, v in (totais.get("porCategoriaLiquido") or totais.get("porCategoria") or {}).items()},
            "porCategoriaBruto": {k: round(float(v or 0.0), 2) for k, v in (totais.get("porCategoriaBruto") or {}).items()},
            "porCategoriaAjustes": {k: round(float(v or 0.0), 2) for k, v in (totais.get("porCategoriaAjustes") or {}).items()}
        })

    return {
        "meta": {
            "tipo": "consulta_deputados",
            "escopo": "federal/camara",
            "periodo": periodo_meta,
            "geradoEm": now_iso(),
            "versaoSchema": "2.0.0",
            "versaoCategoryMap": cmap.version,
            "campos": [
                "id", "nome", "uf", "partido", "urlFoto",
                "total", "totalLiquido", "totalBruto", "totalAjustes",
                "qtdLancamentos", "porCategoria", "porCategoriaLiquido", "porCategoriaBruto", "porCategoriaAjustes",
            ]
        },
        "itens": itens
    }

def list_month_entity_periods(out_dir: Path) -> List[Tuple[int, int, Path]]:
    base = out_dir / "federal/camara/entities"
    out: List[Tuple[int, int, Path]] = []
    if not base.exists():
        return out
    for p in sorted(base.glob("month-*/entities.json")):
        try:
            m = re.match(r"^month-(\d{4})-(\d{2})$", p.parent.name)
            if not m:
                continue
            year = int(m.group(1))
            month = int(m.group(2))
            if month < 1 or month > 12:
                continue
            out.append((year, month, p))
        except Exception:
            continue
    return out

def sum_entity_files(entity_files: List[Path]) -> List[dict]:
    """
    Reads monthly entities contract files and sums per deputy.
    Returns rows in the same schema as monthly rows.
    """
    summed: Dict[int, dict] = {}
    for f in entity_files:
        obj = read_json(f) or {}
        for it in (obj.get("items") or []):
            dep_id_raw = it.get("id")
            if dep_id_raw is None:
                continue
            try:
                dep_id = int(dep_id_raw)
            except Exception:
                continue
            if dep_id not in summed:
                summed[dep_id] = {
                    "id": dep_id,
                    "nome": it.get("name"),
                    "uf": it.get("stateCode"),
                    "partido": it.get("party"),
                    "urlFoto": it.get("photoUrl"),
                    "totais": {
                        "total": 0.0,
                        "totalLiquido": 0.0,
                        "totalBruto": 0.0,
                        "totalAjustes": 0.0,
                        "qtdLancamentos": 0,
                        "porCategoria": {},
                        "porCategoriaLiquido": {},
                        "porCategoriaBruto": {},
                        "porCategoriaAjustes": {}
                    }
                }
            srow = summed[dep_id]
            srow["nome"] = srow.get("nome") or it.get("name")
            srow["uf"] = srow.get("uf") or it.get("stateCode")
            srow["partido"] = srow.get("partido") or it.get("party")
            srow["urlFoto"] = srow.get("urlFoto") or it.get("photoUrl")
            s = srow["totais"]

            amount_net = float(it.get("amountNet", 0.0) or 0.0)
            amount_gross = float(it.get("amountGross", max(amount_net, 0.0)) or 0.0)
            amount_adj = float(it.get("amountAdjustments", min(amount_net, 0.0)) or 0.0)
            s["total"] += amount_net
            s["totalLiquido"] += amount_net
            s["totalBruto"] += amount_gross
            s["totalAjustes"] += amount_adj
            s["qtdLancamentos"] += int(it.get("recordsCount", 0) or 0)

            for k, v in (it.get("byCategoryNet") or {}).items():
                s["porCategoria"][k] = float(s["porCategoria"].get(k, 0.0)) + float(v or 0.0)
                s["porCategoriaLiquido"][k] = float(s["porCategoriaLiquido"].get(k, 0.0)) + float(v or 0.0)
            for k, v in (it.get("byCategoryGross") or {}).items():
                s["porCategoriaBruto"][k] = float(s["porCategoriaBruto"].get(k, 0.0)) + float(v or 0.0)
            for k, v in (it.get("byCategoryAdjustments") or {}).items():
                s["porCategoriaAjustes"][k] = float(s["porCategoriaAjustes"].get(k, 0.0)) + float(v or 0.0)

    out = []
    for r in summed.values():
        r["totais"]["total"] = round(r["totais"]["total"], 2)
        r["totais"]["totalLiquido"] = round(r["totais"]["totalLiquido"], 2)
        r["totais"]["totalBruto"] = round(r["totais"]["totalBruto"], 2)
        r["totais"]["totalAjustes"] = round(r["totais"]["totalAjustes"], 2)
        r["totais"]["porCategoria"] = {k: round(float(v), 2) for k, v in r["totais"]["porCategoria"].items()}
        r["totais"]["porCategoriaLiquido"] = {k: round(float(v), 2) for k, v in r["totais"]["porCategoriaLiquido"].items()}
        r["totais"]["porCategoriaBruto"] = {k: round(float(v), 2) for k, v in r["totais"]["porCategoriaBruto"].items()}
        r["totais"]["porCategoriaAjustes"] = {k: round(float(v), 2) for k, v in r["totais"]["porCategoriaAjustes"].items()}
        out.append(r)
    return out

def pick_latest_daily_insight_from_month_overview_files(overview_files: List[Path]) -> Optional[dict]:
    latest = None
    latest_date = None
    for fp in overview_files:
        obj = read_json(fp) or {}
        daily = ((obj.get("highlights") or {}).get("dailyHighlight") or {})
        ref = str(daily.get("referenceDate") or "").strip()
        if not ref:
            continue
        d = normalize_doc_date(ref)
        if not d:
            continue
        score = float(daily.get("topAmountNet", daily.get("amountNet", 0.0)) or 0.0)
        pick = {
            "dataReferencia": d,
            "deputado": {
                "id": (daily.get("entity") or {}).get("id"),
                "nome": (daily.get("entity") or {}).get("name"),
                "uf": (daily.get("entity") or {}).get("stateCode"),
                "partido": (daily.get("entity") or {}).get("party")
            },
            "valor": round(score, 2),
            "totalDia": round(float(daily.get("totalAmountNet", daily.get("amountNet", 0.0)) or 0.0), 2),
            "totalDiaAnterior": round(float(daily.get("previousTotalAmountNet", 0.0) or 0.0), 2),
            "deltaTotalDia": round(float(daily.get("deltaTotalAmountNet", 0.0) or 0.0), 2),
            "deltaPercentTotalDia": round(float(daily.get("deltaPercentTotalAmountNet", 0.0) or 0.0), 2),
            "deltaDirection": str(daily.get("deltaDirection") or "flat"),
            "trend7d": daily.get("trend7d") if isinstance(daily.get("trend7d"), list) else [],
            "categoriaQC": daily.get("category"),
            "tipoDespesa": daily.get("expenseType"),
            "fornecedor": daily.get("supplier")
        }
        if (latest_date is None) or (d > latest_date):
            latest_date = d
            latest = pick
            continue
        if d == latest_date and float(pick.get("valor", 0.0)) > float((latest or {}).get("valor", 0.0)):
            latest = pick
    return latest



def build_resumos_deputados_from_month_entities(
    deputados: List[dict],
    month_entity_files: List[Path],
    mandate_start_year: int
) -> Dict[int, dict]:
    """
    Reads monthly totals_deputados.json files and builds a per-deputy summary:
      - totaisMandato (total, qtdLancamentos, porCategoria + doc stats)
      - totaisAno (year -> total)
      - porMes (YYYY-MM -> total)
    Returns dict keyed by deputy id.
    """
    dep_map: Dict[int, dict] = {}
    for d in deputados:
        try:
            dep_id = int(d.get("id"))
        except Exception:
            continue
        dep_map[dep_id] = {
            "id": dep_id,
            "nome": d.get("nome") or d.get("nomeCivil") or d.get("nomeParlamentar"),
            "uf": d.get("siglaUf") or d.get("uf"),
            "partido": d.get("siglaPartido") or d.get("partido"),
            "urlFoto": d.get("urlFoto")
        }

    acc: Dict[int, dict] = {}

    def ensure(dep_id: int) -> dict:
        if dep_id not in acc:
            base = dep_map.get(dep_id, {"id": dep_id})
            acc[dep_id] = {
                "id": dep_id,
                "nome": base.get("nome"),
                "uf": base.get("uf"),
                "partido": base.get("partido"),
                "urlFoto": base.get("urlFoto"),
                "totaisMandato": {
                    "total": 0.0,
                    "totalLiquido": 0.0,
                    "totalBruto": 0.0,
                    "totalAjustes": 0.0,
                    "qtdLancamentos": 0,
                    "porCategoria": {},
                    "porCategoriaLiquido": {},
                    "porCategoriaBruto": {},
                    "porCategoriaAjustes": {}
                },
                "totaisAno": {},
                "porMes": {}
            }
        return acc[dep_id]

    for fp in month_entity_files:
        try:
            parts = fp.parts
            i = parts.index("entities")
            period_name = str(parts[i+1])
            m = re.match(r"^month-(\d{4})-(\d{2})$", period_name)
            if not m:
                continue
            year = int(m.group(1))
            month = int(m.group(2))
        except Exception:
            year = None
            month = None

        obj = read_json(fp)
        if not obj:
            continue
        rows = obj.get("items") or []
        ym_key = None
        if year and month:
            ym_key = f"{year:04d}-{month:02d}"

        for row in rows:
            dep_id = row.get("id")
            if dep_id is None:
                continue
            try:
                dep_id = int(dep_id)
            except Exception:
                continue

            a = ensure(dep_id)

            a["nome"] = a.get("nome") or row.get("name")
            a["uf"] = a.get("uf") or row.get("stateCode")
            a["partido"] = a.get("partido") or row.get("party")
            a["urlFoto"] = a.get("urlFoto") or row.get("photoUrl")

            total = float(row.get("amountNet", 0.0) or 0.0)
            total_bruto = float(row.get("amountGross", max(total, 0.0)) or 0.0)
            total_ajustes = float(row.get("amountAdjustments", min(total, 0.0)) or 0.0)
            qtd = int(row.get("recordsCount") or 0)

            a["totaisMandato"]["total"] += total
            a["totaisMandato"]["totalLiquido"] += total
            a["totaisMandato"]["totalBruto"] += total_bruto
            a["totaisMandato"]["totalAjustes"] += total_ajustes
            a["totaisMandato"]["qtdLancamentos"] += qtd

            pc = row.get("byCategoryNet") or {}
            pc_bruto = row.get("byCategoryGross") or {}
            pc_ajustes = row.get("byCategoryAdjustments") or {}
            dst_pc = a["totaisMandato"]["porCategoria"]
            dst_pc_liq = a["totaisMandato"]["porCategoriaLiquido"]
            dst_pc_bruto = a["totaisMandato"]["porCategoriaBruto"]
            dst_pc_ajustes = a["totaisMandato"]["porCategoriaAjustes"]
            for k, v in pc.items():
                dst_pc[k] = float(dst_pc.get(k) or 0.0) + float(v or 0.0)
                dst_pc_liq[k] = float(dst_pc_liq.get(k) or 0.0) + float(v or 0.0)
            for k, v in pc_bruto.items():
                dst_pc_bruto[k] = float(dst_pc_bruto.get(k) or 0.0) + float(v or 0.0)
            for k, v in pc_ajustes.items():
                dst_pc_ajustes[k] = float(dst_pc_ajustes.get(k) or 0.0) + float(v or 0.0)

            if year is not None:
                if year >= mandate_start_year:
                    a["totaisAno"][str(year)] = float(a["totaisAno"].get(str(year)) or 0.0) + total

            if ym_key:
                a["porMes"][ym_key] = float(a["porMes"].get(ym_key) or 0.0) + total

    for dep_id, a in acc.items():
        tm = a["totaisMandato"]
        tm["total"] = round(tm["total"], 2)
        tm["totalLiquido"] = round(tm["totalLiquido"], 2)
        tm["totalBruto"] = round(tm["totalBruto"], 2)
        tm["totalAjustes"] = round(tm["totalAjustes"], 2)
        tm["porCategoria"] = {k: round(float(v), 2) for k, v in (tm.get("porCategoria") or {}).items()}
        tm["porCategoriaLiquido"] = {k: round(float(v), 2) for k, v in (tm.get("porCategoriaLiquido") or {}).items()}
        tm["porCategoriaBruto"] = {k: round(float(v), 2) for k, v in (tm.get("porCategoriaBruto") or {}).items()}
        tm["porCategoriaAjustes"] = {k: round(float(v), 2) for k, v in (tm.get("porCategoriaAjustes") or {}).items()}

        a["totaisAno"] = {k: round(float(v), 2) for k, v in (a.get("totaisAno") or {}).items()}
        a["porMes"] = {k: round(float(v), 2) for k, v in (a.get("porMes") or {}).items()}

        pc = tm.get("porCategoriaLiquido") or tm.get("porCategoria") or {}
        if pc:
            cat, val = max(pc.items(), key=lambda kv: kv[1])
            total = tm.get("total") or 0.0
            pct = (float(val) / float(total) * 100.0) if total else 0.0
            a["maiorCategoria"] = {"categoria": cat, "valor": round(float(val), 2), "pct": round(pct, 2)}
        else:
            a["maiorCategoria"] = None

    return acc


def parse_months(s: str) -> List[int]:
    """
    Accepts:
      "1,2,3"
      "1-12"
      "2"
    """
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
    for m in values:
        if m < 1 or m > 12:
            fail(f"MONTHS has out-of-range value: {m}")
    return sorted(set(values))

def period_key(periodo_meta: dict) -> str:
    t = str((periodo_meta or {}).get("tipo") or "").strip().lower()
    if t == "mes":
        return f"month-{int(periodo_meta.get('ano')):04d}-{int(periodo_meta.get('mes')):02d}"
    if t == "ano":
        return f"year-{int(periodo_meta.get('ano')):04d}"
    return "mandate"

def fresh_until_from(generated_at: str, hours: int = 24) -> str:
    base = datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
    return (base + timedelta(hours=hours)).isoformat().replace("+00:00", "Z")

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
    overview_obj: dict,
    entities_obj: dict,
    ranking_obj: dict,
    expense_types_obj: dict,
    pending_categories_obj: dict,
) -> None:
    paths = period_contract_paths(out_dir, pkey)
    write_json(paths["overview"], overview_obj)
    write_json(paths["entities"], entities_obj)
    write_json(paths["ranking"], ranking_obj)
    write_json(paths["expense_types"], expense_types_obj)
    write_json(paths["pending_categories"], pending_categories_obj)

def map_entity_old_to_new(raw: dict) -> dict:
    src = raw or {}
    amount_net = round(float(src.get("valorLiquido", src.get("valor", 0.0)) or 0.0), 2)
    amount_gross = round(float(src.get("valorBruto", max(amount_net, 0.0)) or 0.0), 2)
    amount_adj = round(float(src.get("valorAjustes", min(amount_net, 0.0)) or 0.0), 2)
    return {
        "id": src.get("id"),
        "name": src.get("nome"),
        "stateCode": src.get("uf"),
        "party": src.get("partido"),
        "photoUrl": src.get("urlFoto"),
        "amountNet": amount_net,
        "amountGross": amount_gross,
        "amountAdjustments": amount_adj
    }

def to_overview_contract(old_overview: dict, pkey: str) -> dict:
    old_meta = (old_overview or {}).get("meta") or {}
    kpis = (old_overview or {}).get("kpis") or {}
    ins = (old_overview or {}).get("insights") or {}
    top_spender = map_entity_old_to_new(((ins.get("gasto") or {}).get("top1Gasto")) or (kpis.get("top1Gasto") or {}))
    top_categories = []
    for c in ((ins.get("categoria") or {}).get("top8MaisDemais") or []):
        top_categories.append({
            "category": c.get("categoriaQC"),
            "amountNet": round(float(c.get("valorLiquido", c.get("valor", 0.0)) or 0.0), 2),
            "amountGross": round(float(c.get("valorBruto", 0.0) or 0.0), 2),
            "amountAdjustments": round(float(c.get("valorAjustes", 0.0) or 0.0), 2)
        })
    top_states = []
    for s in ((ins.get("uf") or {}).get("topUFs") or []):
        top_states.append({
            "stateCode": s.get("uf"),
            "amountNet": round(float(s.get("valorLiquido", s.get("valor", 0.0)) or 0.0), 2),
            "amountGross": round(float(s.get("valorBruto", 0.0) or 0.0), 2),
            "amountAdjustments": round(float(s.get("valorAjustes", 0.0) or 0.0), 2)
        })
    daily = (ins.get("diario") or {}) if isinstance(ins.get("diario"), dict) else {}
    daily_entity = map_entity_old_to_new(daily.get("deputado") or {})
    generated_at = str(old_meta.get("geradoEm") or now_iso())
    conc_src = ins.get("concentracao") or {}
    med_src = ins.get("medias") or {}
    conc_amount = round(float(conc_src.get("top10Liquido", conc_src.get("top10Total", 0.0)) or 0.0), 2)
    conc_percent = round(float(conc_src.get("top10Percentual", 0.0) or 0.0), 2)
    avg_entity = round(float(med_src.get("porAgenteComGasto", 0.0) or 0.0), 2)
    avg_record = round(float(med_src.get("porLancamento", 0.0) or 0.0), 2)
    return {
        "meta": {
            "scope": "federal/camara",
            "type": "overview",
            "period": pkey,
            "generatedAt": generated_at,
            "schemaVersion": "1.0.0"
        },
        "base": {
            "amountNet": round(float(kpis.get("totalLiquido", kpis.get("totalGasto", 0.0)) or 0.0), 2),
            "amountGross": round(float(kpis.get("totalBruto", 0.0) or 0.0), 2),
            "amountAdjustments": round(float(kpis.get("totalAjustes", 0.0) or 0.0), 2),
            "recordsCount": int(kpis.get("totalLancamentos", 0) or 0),
            "entitiesCount": int(kpis.get("agentesBase", 0) or 0),
            "entitiesWithSpending": int(kpis.get("agentesComGasto", 0) or 0),
            "entitiesWithoutSpending": int(kpis.get("agentesSemGasto", 0) or 0)
        },
        "highlights": {
            "topSpender": top_spender,
            "topCategories": top_categories,
            "topStates": top_states,
            "concentrationTop10": {
                "amountNet": conc_amount,
                "percent": conc_percent
            },
            "averages": {
                "perEntityWithSpending": avg_entity,
                "perRecord": avg_record
            },
            "dailyHighlight": {
                "referenceDate": daily.get("dataReferencia"),
                "entity": daily_entity,
                "amountNet": round(float(daily.get("valor", 0.0) or 0.0), 2),
                "topAmountNet": round(float(daily.get("valor", 0.0) or 0.0), 2),
                "totalAmountNet": round(float(daily.get("totalDia", daily.get("valor", 0.0)) or 0.0), 2),
                "previousTotalAmountNet": round(float(daily.get("totalDiaAnterior", 0.0) or 0.0), 2),
                "deltaTotalAmountNet": round(float(daily.get("deltaTotalDia", 0.0) or 0.0), 2),
                "deltaPercentTotalAmountNet": round(float(daily.get("deltaPercentTotalDia", 0.0) or 0.0), 2),
                "deltaDirection": str(daily.get("deltaDirection") or "flat"),
                "trend7d": daily.get("trend7d") if isinstance(daily.get("trend7d"), list) else [],
                "category": daily.get("categoriaQC"),
                "expenseType": daily.get("tipoDespesa"),
                "supplier": daily.get("fornecedor")
            }
        }
    }

def to_entities_contract(old_consulta: dict, pkey: str) -> dict:
    old_meta = (old_consulta or {}).get("meta") or {}
    items = []
    for it in (old_consulta or {}).get("itens") or []:
        items.append({
            "id": it.get("id"),
            "name": it.get("nome"),
            "stateCode": it.get("uf"),
            "party": it.get("partido"),
            "photoUrl": it.get("urlFoto"),
            "amountNet": round(float(it.get("totalLiquido", it.get("total", 0.0)) or 0.0), 2),
            "amountGross": round(float(it.get("totalBruto", 0.0) or 0.0), 2),
            "amountAdjustments": round(float(it.get("totalAjustes", 0.0) or 0.0), 2),
            "recordsCount": int(it.get("qtdLancamentos", 0) or 0),
            "byCategoryNet": {k: round(float(v or 0.0), 2) for k, v in (it.get("porCategoriaLiquido") or it.get("porCategoria") or {}).items()},
            "byCategoryGross": {k: round(float(v or 0.0), 2) for k, v in (it.get("porCategoriaBruto") or {}).items()},
            "byCategoryAdjustments": {k: round(float(v or 0.0), 2) for k, v in (it.get("porCategoriaAjustes") or {}).items()}
        })
    return {
        "meta": {
            "scope": "federal/camara",
            "type": "entities",
            "period": pkey,
            "generatedAt": str(old_meta.get("geradoEm") or now_iso()),
            "schemaVersion": "1.0.0"
        },
        "items": items
    }

def to_expense_types_contract(tipo_resumo_obj: dict, pkey: str) -> dict:
    old_meta = (tipo_resumo_obj or {}).get("meta") or {}
    items = []
    for x in (tipo_resumo_obj or {}).get("data") or []:
        items.append({
            "expenseType": x.get("tipoDespesa"),
            "amountNet": round(float(x.get("valorLiquido", x.get("valorTotal", 0.0)) or 0.0), 2),
            "amountGross": round(float(x.get("valorBruto", 0.0) or 0.0), 2),
            "amountAdjustments": round(float(x.get("valorAjustes", 0.0) or 0.0), 2),
            "recordsCount": int(x.get("qtd", 0) or 0)
        })
    return {
        "meta": {
            "scope": "federal/camara",
            "type": "analytics-expense-types",
            "period": pkey,
            "generatedAt": str(old_meta.get("geradoEm") or now_iso()),
            "schemaVersion": "1.0.0"
        },
        "items": items
    }

def to_pending_categories_contract(tipo_resumo_obj: dict, pendentes: List[str], pkey: str) -> dict:
    old_meta = (tipo_resumo_obj or {}).get("meta") or {}
    pend_set = set(pendentes or [])
    items = []
    for x in (tipo_resumo_obj or {}).get("data") or []:
        t = x.get("tipoDespesa")
        if t not in pend_set:
            continue
        items.append({
            "expenseType": t,
            "amountNet": round(float(x.get("valorLiquido", x.get("valorTotal", 0.0)) or 0.0), 2),
            "recordsCount": int(x.get("qtd", 0) or 0)
        })
    return {
        "meta": {
            "scope": "federal/camara",
            "type": "analytics-pending-categories",
            "period": pkey,
            "generatedAt": str(old_meta.get("geradoEm") or now_iso()),
            "schemaVersion": "1.0.0"
        },
        "items": items
    }

def aggregate_analytics_from_month_periods(out_dir: Path, month_pkeys: List[str]) -> Tuple[dict, List[str]]:
    merged: Dict[str, dict] = {}
    pend_set = set()
    latest_generated_at: Optional[str] = None

    for mp in month_pkeys:
        exp_obj = read_json(out_dir / f"federal/camara/analytics/{mp}/expense-types.json") or {}
        pen_obj = read_json(out_dir / f"federal/camara/analytics/{mp}/pending-categories.json") or {}

        g_exp = parse_iso_utc((exp_obj.get("meta") or {}).get("generatedAt"))
        if g_exp and (latest_generated_at is None or g_exp > parse_iso_utc(latest_generated_at)):
            latest_generated_at = g_exp.isoformat().replace("+00:00", "Z")
        g_pen = parse_iso_utc((pen_obj.get("meta") or {}).get("generatedAt"))
        if g_pen and (latest_generated_at is None or g_pen > parse_iso_utc(latest_generated_at)):
            latest_generated_at = g_pen.isoformat().replace("+00:00", "Z")

        for it in exp_obj.get("items", []) or []:
            t = str(it.get("expenseType") or "").strip()
            if not t:
                continue
            cur = merged.setdefault(t, {
                "tipoDespesa": t,
                "valorTotal": 0.0,
                "valorLiquido": 0.0,
                "valorBruto": 0.0,
                "valorAjustes": 0.0,
                "qtd": 0
            })
            amt_net = float(it.get("amountNet", 0.0) or 0.0)
            cur["valorTotal"] += amt_net
            cur["valorLiquido"] += amt_net
            cur["valorBruto"] += float(it.get("amountGross", 0.0) or 0.0)
            cur["valorAjustes"] += float(it.get("amountAdjustments", 0.0) or 0.0)
            cur["qtd"] += int(it.get("recordsCount", 0) or 0)

        for it in pen_obj.get("items", []) or []:
            t = str(it.get("expenseType") or "").strip()
            if t:
                pend_set.add(t)

    rows = list(merged.values())
    for x in rows:
        x["valorTotal"] = round(float(x.get("valorTotal", 0.0)), 2)
        x["valorLiquido"] = round(float(x.get("valorLiquido", 0.0)), 2)
        x["valorBruto"] = round(float(x.get("valorBruto", 0.0)), 2)
        x["valorAjustes"] = round(float(x.get("valorAjustes", 0.0)), 2)
    rows.sort(key=lambda x: float(x.get("valorTotal", 0.0)), reverse=True)

    return {
        "meta": {
            "geradoEm": latest_generated_at or now_iso()
        },
        "data": rows
    }, sorted(pend_set)

def to_ranking_total_contract(rankings_old: Dict[str, dict], pkey: str) -> dict:
    top_old = ((rankings_old or {}).get("total_top10.json") or {}).get("itens") or []
    bottom_old = ((rankings_old or {}).get("total_bottom10.json") or {}).get("itens") or []
    def map_item(x: dict) -> dict:
        return {
            "id": x.get("id"),
            "name": x.get("nome"),
            "stateCode": x.get("uf"),
            "party": x.get("partido"),
            "amountNet": round(float(x.get("valorLiquido", x.get("valor", 0.0)) or 0.0), 2),
            "amountGross": round(float(x.get("valorBruto", 0.0) or 0.0), 2),
            "amountAdjustments": round(float(x.get("valorAjustes", 0.0) or 0.0), 2),
            "recordsCount": int(x.get("qtdLancamentos", 0) or 0)
        }
    return {
        "meta": {
            "scope": "federal/camara",
            "type": "ranking-total",
            "period": pkey,
            "generatedAt": now_iso(),
            "schemaVersion": "1.0.0"
        },
        "top": [map_item(x) for x in top_old],
        "bottom": [map_item(x) for x in bottom_old]
    }

def build_home_insights(
    overview_mandate: dict,
    entity_photo_by_id: Optional[Dict[str, str]] = None,
    insights_text_catalog: Optional[dict] = None
) -> dict:
    def to_ptbr_date(value: str) -> str:
        s = str(value or "").strip()
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
        if m:
            return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
        return s

    def fmt_brl(amount: float) -> str:
        n = round(float(amount or 0.0), 2)
        s = f"{n:,.2f}"
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"

    generated_at = str(((overview_mandate or {}).get("meta") or {}).get("generatedAt") or now_iso())
    fresh_until = fresh_until_from(generated_at, 24)
    base = (overview_mandate or {}).get("base") or {}
    h = (overview_mandate or {}).get("highlights") or {}
    top_spender = h.get("topSpender") or {}
    top_categories = h.get("topCategories") or []
    top_states = h.get("topStates") or []
    concentration = h.get("concentrationTop10") or {}
    averages = h.get("averages") or {}
    entities_count = int(base.get("entitiesCount", 0) or 0)
    entities_with_spending = int(base.get("entitiesWithSpending", 0) or 0)
    entities_without_spending = int(base.get("entitiesWithoutSpending", max(0, entities_count - entities_with_spending)) or 0)
    daily = h.get("dailyHighlight") or {}
    daily_entity = daily.get("entity") or {}
    daily_amount = round(float(daily.get("topAmountNet", daily.get("amountNet", 0.0)) or 0.0), 2)
    daily_total = round(float(daily.get("totalAmountNet", daily_amount) or 0.0), 2)
    previous_total = round(float(daily.get("previousTotalAmountNet", 0.0) or 0.0), 2)
    delta_total = round(float(daily.get("deltaTotalAmountNet", daily_total - previous_total) or 0.0), 2)
    delta_percent = round(float(daily.get("deltaPercentTotalAmountNet", ((delta_total / previous_total) * 100.0 if previous_total > 0 else 0.0)) or 0.0), 2)
    delta_direction = str(daily.get("deltaDirection") or ("up" if delta_total > 0 else ("down" if delta_total < 0 else "flat")))
    trend7d = daily.get("trend7d") if isinstance(daily.get("trend7d"), list) and daily.get("trend7d") else None
    top_cat = top_categories[0] if top_categories else {}
    top_state = top_states[0] if top_states else {}
    share = (daily_amount / daily_total * 100.0) if daily_total > 0 else 0.0
    daily_supplier = str(daily.get("supplier") or "").strip()
    entity_photo_by_id = entity_photo_by_id or {}
    text_catalog = insights_text_catalog or {}
    defaults_cfg = text_catalog.get("defaults") if isinstance(text_catalog.get("defaults"), dict) else {}
    templates_cfg = text_catalog.get("templates") if isinstance(text_catalog.get("templates"), dict) else {}
    insights_cfg = text_catalog.get("insights") if isinstance(text_catalog.get("insights"), dict) else {}
    default_tag = str(defaults_cfg.get("tag") or "INSIGHT DO DIA").strip()
    source_by_orgao = defaults_cfg.get("sourceByOrgao") if isinstance(defaults_cfg.get("sourceByOrgao"), dict) else {}
    default_source = str(source_by_orgao.get("camara") or "Câmara dos Deputados").strip()

    def render_template(tpl: str, values: Dict[str, str]) -> str:
        out = str(tpl or "")
        for key, val in (values or {}).items():
            out = out.replace("{" + str(key) + "}", str(val))
        return out

    def resolve_text(iid: str, fallback_title: str, fallback_context: str) -> dict:
        base = {"tag": default_tag, "source": default_source, "title": fallback_title, "context": fallback_context}
        entry = insights_cfg.get(iid) if isinstance(insights_cfg.get(iid), dict) else {}
        tpl_name = str(entry.get("useTemplate") or "").strip()
        tpl_obj = templates_cfg.get(tpl_name) if tpl_name and isinstance(templates_cfg.get(tpl_name), dict) else {}
        merged = dict(base)
        for src in [tpl_obj, entry]:
            for k in ["tag", "source", "title", "context", "contextTemplate", "contextFallback", "variationTemplate"]:
                v = src.get(k)
                if isinstance(v, str) and v.strip():
                    merged[k] = v.strip()
        if not str(merged.get("context") or "").strip() and str(merged.get("contextFallback") or "").strip():
            merged["context"] = str(merged.get("contextFallback")).strip()
        return merged

    def with_photo_fallback(entity_obj: dict) -> dict:
        ent = dict(entity_obj or {})
        pid = str(ent.get("id") or "").strip()
        direct = str(ent.get("photoUrl") or ent.get("urlFoto") or "").strip()
        if direct:
            ent["photoUrl"] = direct
            return ent
        if pid:
            mapped = str(entity_photo_by_id.get(pid) or "").strip()
            if mapped:
                ent["photoUrl"] = mapped
        return ent
    def calc_quality(item: dict) -> float:
        now_dt = datetime.now(timezone.utc)
        g = parse_iso_utc(item.get("generatedAt"))
        f = parse_iso_utc(item.get("freshUntil"))
        if not g or not f:
            freshness = 0.0
        else:
            delta_hours = max(0.0, (f - g).total_seconds() / 3600.0)
            if delta_hours <= 24:
                freshness = 1.0
            elif delta_hours <= 72:
                freshness = 0.8
            elif delta_hours <= 168:
                freshness = 0.6
            else:
                freshness = 0.4
            if f < now_dt:
                freshness = min(freshness, 0.5)
        required = ["id", "type", "level", "period", "tag", "title", "context", "source", "enabled", "priority", "weight", "generatedAt", "freshUntil"]
        itype = str(item.get("type") or "")
        if itype == "person":
            required += ["entity", "value"]
        elif itype == "aggregate":
            required += ["value"]
        elif itype == "comparison":
            required += ["left", "right", "delta"]
        elif itype == "timeline":
            required += ["series"]
        present = 0
        for k in required:
            v = item.get(k)
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            present += 1
        completeness = (present / len(required)) if required else 0.0
        enum_ok = str(item.get("type") or "") in {"person", "aggregate", "comparison", "alert", "timeline"} and str(item.get("period") or "") in {"day", "month", "year", "mandate", "rolling7d"}
        p = float(item.get("priority") or -1.0)
        w = float(item.get("weight") or -1.0)
        range_ok = 0.0 <= p <= 100.0 and 0.0 <= w <= 1.0
        consistency = ((1.0 if enum_ok else 0.0) + (1.0 if range_ok else 0.0)) / 2.0
        q = (0.45 * freshness) + (0.35 * completeness) + (0.20 * consistency)
        return round(max(0.0, min(1.0, q)), 4)
    def mk_base(iid: str, itype: str, period: str, title: str, context: str, priority: int, weight: float) -> dict:
        text_cfg = resolve_text(iid, title, context)
        obj = {
            "id": iid,
            "type": itype,
            "level": "federal",
            "period": period,
            "tag": str(text_cfg.get("tag") or default_tag),
            "title": str(text_cfg.get("title") or title),
            "context": str(text_cfg.get("context") or context),
            "source": str(text_cfg.get("source") or default_source),
            "enabled": True,
            "priority": priority,
            "weight": weight,
            "generatedAt": generated_at,
            "freshUntil": fresh_until
        }
        obj["qualityScore"] = 0.0
        return obj
    def deputy_subtitle(entity: dict) -> str:
        party = str(entity.get("party") or "").strip()
        state_code = str(entity.get("stateCode") or "").strip()
        suffix = f"{party}{'/' if party and state_code else ''}{state_code}".strip()
        return f"Deputado federal • {suffix}".strip(" •")

    def pct_of_base(amount: float) -> float:
        base_amount = float(base.get("amountNet", 0.0) or 0.0)
        if base_amount <= 0:
            return 0.0
        return (float(amount or 0.0) / base_amount) * 100.0

    def set_brl_value(item: dict, amount: float) -> None:
        item["value"] = {"amount": round(float(amount or 0.0), 2), "currency": "BRL"}

    items = []

    x = mk_base(
        "federal-deputies-mandate-total-amount",
        "aggregate",
        "mandate",
        "Total acumulado no mandato",
        "Valor líquido total acumulado no mandato.",
        100,
        1.0
    )
    set_brl_value(x, float(base.get("amountNet", 0.0) or 0.0))
    x["tags"] = ["Dados federais"]
    x["referenceDate"] = f"{int(base.get('recordsCount', 0) or 0)} lançamentos no período"
    x["variationText"] = f"Base: {entities_count} • Com gasto: {entities_with_spending} • Sem gasto: {entities_without_spending}"
    items.append(x)

    x = mk_base(
        "federal-deputies-mandate-top-spender",
        "person",
        "mandate",
        "Maior gasto no mandato",
        "Parlamentar com maior valor líquido acumulado no mandato.",
        95,
        0.95
    )
    x["entity"] = with_photo_fallback({
        "id": top_spender.get("id"),
        "name": top_spender.get("name"),
        "party": top_spender.get("party"),
        "stateCode": top_spender.get("stateCode"),
        "photoUrl": (top_spender.get("photoUrl") or top_spender.get("urlFoto")),
        "subtitle": deputy_subtitle(top_spender)
    })
    set_brl_value(x, float(top_spender.get("amountNet", 0.0) or 0.0))
    x["referenceDate"] = "Deputado com maior gasto no mandato"
    x["variationText"] = f"Base: {entities_count} • Com gasto: {entities_with_spending}"
    items.append(x)

    concentration_percent = round(float(concentration.get("percent", 0.0) or 0.0), 2)
    x = mk_base(
        "federal-deputies-mandate-top10-concentration",
        "comparison",
        "mandate",
        "Concentração dos 10 maiores no mandato",
        "Participação dos 10 maiores no total líquido acumulado do mandato.",
        92,
        0.90
    )
    concentration_amount = float(concentration.get("amountNet", 0.0) or 0.0)
    x["left"] = {"amount": round(concentration_amount, 2), "currency": "BRL"}
    x["right"] = {"amount": round(float(base.get("amountNet", 0.0) or 0.0), 2), "currency": "BRL"}
    x["delta"] = {
        "amount": round(concentration_amount - float(base.get("amountNet", 0.0) or 0.0), 2),
        "percent": concentration_percent,
        "direction": "up"
    }
    x["tags"] = ["Top 10 deputados"]
    x["referenceDate"] = f"{entities_with_spending} deputados com gasto no período"
    x["variationText"] = f"Montante Top 10: {fmt_brl(concentration_amount)}"
    items.append(x)

    x = mk_base(
        "federal-deputies-day-total-amount",
        "aggregate",
        "day",
        "Movimento do dia",
        "Valor líquido total no dia mais recente disponível.",
        90,
        0.88
    )
    set_brl_value(x, daily_total)
    x["tags"] = ["Movimento do dia"]
    daily_ref = to_ptbr_date(str(daily.get("referenceDate") or ""))
    daily_expense_type = str(daily.get("expenseType") or "").strip()
    if daily_ref and daily_expense_type:
        x["referenceDate"] = f"{daily_ref} • {daily_expense_type}"
    else:
        x["referenceDate"] = daily.get("referenceDate")
    x["variationText"] = f"Base: {entities_count} • Com gasto: {entities_with_spending}"
    items.append(x)

    x = mk_base(
        "federal-deputies-day-top-spender",
        "person",
        "day",
        "Despesa mais alta do dia",
        "Parlamentar com maior valor líquido no dia mais recente disponível.",
        89,
        0.86
    )
    x["entity"] = with_photo_fallback({
        "id": daily_entity.get("id"),
        "name": daily_entity.get("name"),
        "party": daily_entity.get("party"),
        "stateCode": daily_entity.get("stateCode"),
        "photoUrl": (daily_entity.get("photoUrl") or daily_entity.get("urlFoto")),
        "subtitle": deputy_subtitle(daily_entity)
    })
    set_brl_value(x, daily_amount)
    x["referenceDate"] = daily.get("referenceDate")
    daily_text_cfg = resolve_text(
        "federal-deputies-day-top-spender",
        "Despesa mais alta do dia",
        "Parlamentar com maior valor líquido no dia mais recente disponível."
    )
    variation_tpl = str(daily_text_cfg.get("variationTemplate") or "").strip()
    context_tpl = str(daily_text_cfg.get("contextTemplate") or "").strip()
    if daily_supplier and variation_tpl:
        x["variationText"] = render_template(variation_tpl, {"supplier": daily_supplier})
    elif daily_supplier:
        x["variationText"] = f"Fornecedor: {daily_supplier}"
    if daily_supplier and context_tpl:
        x["context"] = render_template(context_tpl, {"supplier": daily_supplier})
    else:
        ref_br = to_ptbr_date(str(daily.get("referenceDate") or ""))
        if ref_br:
            x["context"] = f"Maior despesa líquida no dia {ref_br}."
        else:
            x["context"] = "Maior despesa líquida no dia mais recente disponível."
    items.append(x)

    x = mk_base(
        "federal-deputies-day-vs-previous-day",
        "comparison",
        "day",
        "Oscilação diária",
        "Variação diária comparando o último dia disponível com o dia anterior.",
        85,
        0.82
    )
    x["left"] = {"amount": daily_total, "currency": "BRL"}
    x["right"] = {"amount": previous_total, "currency": "BRL"}
    x["delta"] = {"amount": delta_total, "percent": delta_percent, "direction": delta_direction}
    x["tags"] = ["Variação diária"]
    x["referenceDate"] = daily.get("referenceDate")
    x["variationText"] = f"Dia anterior: {fmt_brl(previous_total)}"
    items.append(x)

    x = mk_base(
        "federal-deputies-mandate-top-category",
        "aggregate",
        "mandate",
        "Categoria líder no mandato",
        "Categoria com maior valor líquido acumulado no mandato.",
        82,
        0.78
    )
    top_cat_amount = float(top_cat.get("amountNet", 0.0) or 0.0)
    set_brl_value(x, top_cat_amount)
    x["tags"] = [str(top_cat.get("category") or "")]
    x["referenceDate"] = "Participação no total do mandato"
    x["variationText"] = f"Participação: {pct_of_base(top_cat_amount):.1f}%".replace(".", ",")
    items.append(x)

    x = mk_base(
        "federal-deputies-mandate-top-state",
        "aggregate",
        "mandate",
        "UF líder no mandato",
        "UF com maior valor líquido acumulado no mandato.",
        80,
        0.76
    )
    top_state_amount = float(top_state.get("amountNet", 0.0) or 0.0)
    set_brl_value(x, top_state_amount)
    x["tags"] = [str(top_state.get("stateCode") or "")]
    x["referenceDate"] = f"{entities_with_spending} deputados com gasto no período"
    x["variationText"] = f"Participação: {pct_of_base(top_state_amount):.1f}%".replace(".", ",")
    items.append(x)

    x = mk_base(
        "federal-deputies-mandate-average-per-entity",
        "aggregate",
        "mandate",
        "Média por parlamentar com gasto no mandato",
        "Média de valor líquido por parlamentar com gasto no mandato.",
        78,
        0.72
    )
    set_brl_value(x, float(averages.get("perEntityWithSpending", 0.0) or 0.0))
    x["tags"] = ["Média por agente"]
    x["referenceDate"] = f"{entities_with_spending} deputados com gasto no período"
    x["variationText"] = f"Montante: {fmt_brl(float(base.get('amountNet', 0.0) or 0.0))}"
    items.append(x)

    x = mk_base(
        "federal-deputies-day-top-share",
        "comparison",
        "day",
        "Peso do líder no dia",
        "Participação percentual do maior gasto diário sobre o total do dia.",
        74,
        0.68
    )
    x["left"] = {"amount": daily_amount, "currency": "BRL"}
    x["right"] = {"amount": daily_total, "currency": "BRL"}
    x["delta"] = {
        "amount": round(daily_amount - daily_total, 2),
        "percent": round(share, 2),
        "direction": ("up" if daily_amount > daily_total else ("down" if daily_amount < daily_total else "flat"))
    }
    x["tags"] = ["Peso do maior gasto no dia"]
    x["referenceDate"] = daily.get("referenceDate")
    x["variationText"] = f"Valor líder: {fmt_brl(daily_amount)}"
    items.append(x)

    x = mk_base(
        "federal-deputies-rolling7d-trend",
        "aggregate",
        "mandate",
        "Tendência dos últimos 7 dias",
        "Tendência dos totais diários nos últimos 7 dias disponíveis.",
        72,
        0.66
    )
    set_brl_value(x, float(averages.get("perRecord", 0.0) or 0.0))
    x["tags"] = ["Média por item"]
    x["referenceDate"] = f"{int(base.get('recordsCount', 0) or 0)} lançamentos no período"
    x["variationText"] = f"Montante: {fmt_brl(float(base.get('amountNet', 0.0) or 0.0))}"
    items.append(x)
    for it in items:
        it["qualityScore"] = calc_quality(it)
    return {
        "meta": {"scope": "federal/camara", "type": "home-insights", "generatedAt": generated_at, "schemaVersion": "1.0.0"},
        "items": items
    }

def cleanup_legacy_output(out_dir: Path) -> None:
    for rel in [
        "federal/camara/aggregates",
        "federal/camara/consultas",
        "federal/camara/dicionarios",
        "federal/camara/pendencias"
    ]:
        p = out_dir / rel
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.exists():
            p.unlink()
    resumos_root = out_dir / "federal/camara/resumos"
    if resumos_root.exists():
        for child in resumos_root.iterdir():
            if child.is_file() and child.name == "overview.json":
                continue
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            elif child.is_file():
                child.unlink()
    rankings_root = out_dir / "federal/camara/rankings"
    if rankings_root.exists():
        for child in rankings_root.iterdir():
            n = child.name
            if n == "mandate" or n.startswith("month-") or n.startswith("year-"):
                continue
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            elif child.is_file():
                child.unlink()

def write_profiles_and_index(out_dir: Path, resumos: Dict[int, dict]) -> None:
    profiles_dir = out_dir / "federal/camara/entities/profiles"
    ensure_dir(profiles_dir)
    index_items = []
    for dep_id, a in sorted(resumos.items(), key=lambda kv: safe_slug((kv[1].get("nome") or "").lower())):
        tmand = a.get("totaisMandato") or {}
        mandate_amount_net = round(float(tmand.get("totalLiquido", tmand.get("total", 0.0)) or 0.0), 2)
        mandate_amount_gross = round(float(tmand.get("totalBruto", 0.0) or 0.0), 2)
        mandate_amount_adjust = round(float(tmand.get("totalAjustes", 0.0) or 0.0), 2)
        mandate_records = int(tmand.get("qtdLancamentos", 0) or 0)
        mandate_by_cat_net = {k: round(float(v or 0.0), 2) for k, v in (tmand.get("porCategoriaLiquido") or tmand.get("porCategoria") or {}).items()}
        mandate_by_cat_gross = {k: round(float(v or 0.0), 2) for k, v in (tmand.get("porCategoriaBruto") or {}).items()}
        mandate_by_cat_adjust = {k: round(float(v or 0.0), 2) for k, v in (tmand.get("porCategoriaAjustes") or {}).items()}
        profile = {
            "meta": {
                "scope": "federal/camara",
                "type": "entity-profile",
                "generatedAt": now_iso(),
                "schemaVersion": "1.0.0"
            },
            "entity": {
                "id": dep_id,
                "name": a.get("nome"),
                "stateCode": a.get("uf"),
                "party": a.get("partido"),
                "photoUrl": a.get("urlFoto")
            },
            "mandateTotals": {
                "amountNet": mandate_amount_net,
                "amountGross": mandate_amount_gross,
                "amountAdjustments": mandate_amount_adjust,
                "recordsCount": mandate_records,
                "byCategoryNet": mandate_by_cat_net,
                "byCategoryGross": mandate_by_cat_gross,
                "byCategoryAdjustments": mandate_by_cat_adjust
            },
            "yearTotals": {str(k): round(float(v or 0.0), 2) for k, v in (a.get("totaisAno") or {}).items()},
            "monthTotals": {str(k): round(float(v or 0.0), 2) for k, v in (a.get("porMes") or {}).items()}
        }
        write_json(profiles_dir / f"{dep_id}.json", profile)
        index_items.append({
            "id": dep_id,
            "name": a.get("nome"),
            "stateCode": a.get("uf"),
            "party": a.get("partido"),
            "photoUrl": a.get("urlFoto"),
            "amountNet": mandate_amount_net,
            "recordsCount": mandate_records,
            "yearTotals": {str(k): round(float(v or 0.0), 2) for k, v in (a.get("totaisAno") or {}).items()}
        })
    write_json(out_dir / "federal/camara/entities/index.json", {
        "meta": {
            "scope": "federal/camara",
            "type": "entities-index",
            "generatedAt": now_iso(),
            "schemaVersion": "1.0.0",
            "itemsCount": len(index_items)
        },
        "items": index_items
    })


def _missing(v) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())

def _assert_ok(cond: bool, msg: str) -> None:
    if not cond:
        raise RuntimeError(msg)

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
        "federal-deputies-rolling7d-trend"
    }

def _get_path(obj: dict, path: str):
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur.get(part)
    return cur

def _validate_insight_item(it: dict, idx: int) -> None:
    required_common = ["id", "type", "level", "period", "tag", "title", "context", "source", "enabled", "priority", "weight", "qualityScore", "generatedAt", "freshUntil"]
    for k in required_common:
        _assert_ok(not _missing(it.get(k)), f"home-insights item[{idx}] missing field: {k}")
    _assert_ok(str(it.get("type")) in {"person", "aggregate", "comparison", "alert", "timeline"}, f"home-insights item[{idx}] invalid type")
    _assert_ok(str(it.get("period")) in {"day", "month", "year", "mandate", "rolling7d"}, f"home-insights item[{idx}] invalid period")
    p = float(it.get("priority"))
    w = float(it.get("weight"))
    q = float(it.get("qualityScore"))
    _assert_ok(0.0 <= p <= 100.0, f"home-insights item[{idx}] invalid priority range")
    _assert_ok(0.0 <= w <= 1.0, f"home-insights item[{idx}] invalid weight range")
    _assert_ok(0.0 <= q <= 1.0, f"home-insights item[{idx}] invalid qualityScore range")
    _assert_ok(q >= 0.6, f"home-insights item[{idx}] blocked by qualityScore < 0.60")
    t = str(it.get("type"))
    if t == "person":
        for k in ["entity.id", "entity.name", "value.amount", "value.currency"]:
            _assert_ok(not _missing(_get_path(it, k)), f"home-insights item[{idx}] missing required person field: {k}")
    elif t == "aggregate":
        for k in ["value.amount", "value.currency"]:
            _assert_ok(not _missing(_get_path(it, k)), f"home-insights item[{idx}] missing required aggregate field: {k}")
    elif t == "comparison":
        for k in ["left.amount", "right.amount", "delta.amount", "delta.percent", "delta.direction"]:
            _assert_ok(not _missing(_get_path(it, k)), f"home-insights item[{idx}] missing required comparison field: {k}")
    elif t == "alert":
        for k in ["severity", "baseline.amount"]:
            _assert_ok(not _missing(_get_path(it, k)), f"home-insights item[{idx}] missing required alert field: {k}")
    elif t == "timeline":
        series = it.get("series")
        _assert_ok(isinstance(series, list) and len(series) > 0, f"home-insights item[{idx}] missing series")
        for j, pnt in enumerate(series):
            _assert_ok(not _missing((pnt or {}).get("date")), f"home-insights item[{idx}] series[{j}] missing date")
            _assert_ok(not _missing((pnt or {}).get("amount")), f"home-insights item[{idx}] series[{j}] missing amount")

def _validate_overview_schema(path: Path, obj: dict) -> None:
    _assert_ok(isinstance(obj, dict), f"invalid JSON object: {path}")
    meta = obj.get("meta") or {}
    base = obj.get("base") or {}
    highlights = obj.get("highlights") or {}
    for k in ["scope", "type", "period", "generatedAt", "schemaVersion"]:
        _assert_ok(not _missing(meta.get(k)), f"{path} missing meta.{k}")
    for k in ["amountNet", "amountGross", "amountAdjustments", "recordsCount", "entitiesCount", "entitiesWithSpending", "entitiesWithoutSpending"]:
        _assert_ok(not _missing(base.get(k)), f"{path} missing base.{k}")
    for k in ["topSpender", "topCategories", "topStates", "concentrationTop10", "averages", "dailyHighlight"]:
        _assert_ok(k in highlights, f"{path} missing highlights.{k}")

def _validate_entities_schema(path: Path, obj: dict) -> None:
    _assert_ok(isinstance(obj, dict), f"invalid JSON object: {path}")
    meta = obj.get("meta") or {}
    items = obj.get("items")
    for k in ["scope", "type", "period", "generatedAt", "schemaVersion"]:
        _assert_ok(not _missing(meta.get(k)), f"{path} missing meta.{k}")
    _assert_ok(isinstance(items, list), f"{path} missing items array")
    for i, it in enumerate(items):
        for k in ["id", "name", "stateCode", "party", "amountNet", "amountGross", "amountAdjustments", "recordsCount"]:
            _assert_ok(not _missing((it or {}).get(k)), f"{path} item[{i}] missing {k}")

def _validate_ranking_schema(path: Path, obj: dict) -> None:
    _assert_ok(isinstance(obj, dict), f"invalid JSON object: {path}")
    meta = obj.get("meta") or {}
    for k in ["scope", "type", "period", "generatedAt", "schemaVersion"]:
        _assert_ok(not _missing(meta.get(k)), f"{path} missing meta.{k}")
    _assert_ok(isinstance(obj.get("top"), list), f"{path} missing top array")
    _assert_ok(isinstance(obj.get("bottom"), list), f"{path} missing bottom array")

def _validate_analytics_schema(path: Path, obj: dict) -> None:
    _assert_ok(isinstance(obj, dict), f"invalid JSON object: {path}")
    meta = obj.get("meta") or {}
    for k in ["scope", "type", "period", "generatedAt", "schemaVersion"]:
        _assert_ok(not _missing(meta.get(k)), f"{path} missing meta.{k}")
    _assert_ok(isinstance(obj.get("items"), list), f"{path} missing items array")

def _scan_legacy_keys(obj, legacy: set, out: set) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in legacy:
                out.add(k)
            _scan_legacy_keys(v, legacy, out)
    elif isinstance(obj, list):
        for x in obj:
            _scan_legacy_keys(x, legacy, out)

def _validate_artifact_governance(out_dir: Path) -> None:
    """Validate artifact classes used for UI, scale/contract and compatibility."""
    policy = {
        "core_ui": [
            "catalog.json",
            "home-insights-index.json",
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
        "compatibility": [
            "federal/camara/resumos/overview.json",
        ]
    }
    rel_files = [
        fp.relative_to(out_dir).as_posix()
        for fp in sorted(out_dir.rglob("*.json"))
    ]

    for cls_name, patterns in policy.items():
        for patt in patterns:
            has_match = any(fnmatch.fnmatch(path, patt) for path in rel_files)
            _assert_ok(has_match, f"artifact governance missing [{cls_name}] pattern: {patt}")

def validate_contract_outputs(out_dir: Path) -> None:
    top_files = [
        out_dir / "catalog.json",
        out_dir / "home-insights-index.json",
        out_dir / "federal/camara/insights/home-insights.json",
        out_dir / "federal/camara/resumos/overview.json",
        out_dir / "federal/camara/entities/index.json"
    ]
    for p in top_files:
        _assert_ok(p.exists(), f"missing required artifact: {p}")

    insights_obj = read_json(out_dir / "federal/camara/insights/home-insights.json") or {}
    items = insights_obj.get("items")
    _assert_ok(isinstance(items, list), "home-insights missing items array")
    ids = [str((x or {}).get("id") or "") for x in items]
    _assert_ok(len(ids) == len(set(ids)), "home-insights has duplicated id")
    official_ids = official_home_insight_ids()
    _assert_ok(set(ids) == official_ids, "home-insights IDs are not the official 11 set")
    for i, it in enumerate(items):
        _validate_insight_item(it or {}, i)

    index_obj = read_json(out_dir / "home-insights-index.json") or {}
    idx_items = index_obj.get("items")
    _assert_ok(isinstance(idx_items, list) and len(idx_items) > 0, "home-insights-index missing items")
    idx_ids = [str((x or {}).get("id") or "") for x in idx_items]
    _assert_ok(len(idx_ids) == len(set(idx_ids)), "home-insights-index has duplicated id")
    for i, it in enumerate(idx_items):
        for k in ["id", "level", "scope", "orgao", "path", "enabled"]:
            _assert_ok(not _missing((it or {}).get(k)), f"home-insights-index item[{i}] missing {k}")
        _assert_ok((out_dir / str(it.get("path")).strip("/")).exists(), f"home-insights-index item[{i}] path does not exist")

    catalog_obj = read_json(out_dir / "catalog.json") or {}
    datasets = catalog_obj.get("datasets")
    _assert_ok(isinstance(datasets, list) and len(datasets) > 0, "catalog missing datasets")
    ds_ids = [str((d or {}).get("id") or "") for d in datasets]
    _assert_ok(len(ds_ids) == len(set(ds_ids)), "catalog has duplicated dataset id")
    for i, d in enumerate(datasets):
        for k in ["id", "pathBaseData", "pathInsightsHome", "pathOverviewRoot", "pathEntitiesRoot", "pathRankingsRoot", "pathAnalyticsRoot"]:
            _assert_ok(not _missing((d or {}).get(k)), f"catalog dataset[{i}] missing {k}")
        _assert_ok((out_dir / str((d or {}).get("pathInsightsHome")).strip("/")).exists(), f"catalog dataset[{i}] pathInsightsHome does not exist")

    overview_root = out_dir / "federal/camara/overview"
    _assert_ok(overview_root.exists(), "missing overview root")
    pkeys = sorted([p.name for p in overview_root.iterdir() if p.is_dir()])
    _assert_ok(len(pkeys) > 0, "missing overview periods")
    for pkey in pkeys:
        ov_path = out_dir / f"federal/camara/overview/{pkey}/overview.json"
        en_path = out_dir / f"federal/camara/entities/{pkey}/entities.json"
        rk_path = out_dir / f"federal/camara/rankings/{pkey}/ranking-total.json"
        an_exp = out_dir / f"federal/camara/analytics/{pkey}/expense-types.json"
        an_pen = out_dir / f"federal/camara/analytics/{pkey}/pending-categories.json"
        for p in [ov_path, en_path, rk_path, an_exp, an_pen]:
            _assert_ok(p.exists(), f"missing period artifact: {p}")
        _validate_overview_schema(ov_path, read_json(ov_path) or {})
        _validate_entities_schema(en_path, read_json(en_path) or {})
        _validate_ranking_schema(rk_path, read_json(rk_path) or {})
        _validate_analytics_schema(an_exp, read_json(an_exp) or {})
        _validate_analytics_schema(an_pen, read_json(an_pen) or {})

    legacy_keys = {
        "geradoEm", "periodo", "top1Gasto", "topCategorias", "topUFs",
        "agentesComGasto", "agentesSemGasto", "totalLancamentos", "valorLiquido",
        "valorBruto", "valorAjustes", "uf", "partido", "nome", "urlFoto"
    }
    contract_roots = [
        out_dir / "catalog.json",
        out_dir / "home-insights-index.json",
        out_dir / "federal/camara/insights/home-insights.json",
        out_dir / "federal/camara/overview",
        out_dir / "federal/camara/entities",
        out_dir / "federal/camara/rankings",
        out_dir / "federal/camara/analytics",
        out_dir / "federal/camara/resumos/overview.json"
    ]
    for root in contract_roots:
        files = [root] if root.is_file() else sorted(root.rglob("*.json"))
        for fp in files:
            obj = read_json(fp) or {}
            found = set()
            _scan_legacy_keys(obj, legacy_keys, found)
            _assert_ok(not found, f"legacy keys found in {fp}: {sorted(found)}")

    _validate_artifact_governance(out_dir)


def validate_category_map_file(mapping_path: Path) -> None:
    _assert_ok(mapping_path.exists(), f"CATEGORY_MAP not found: {mapping_path}")
    cfg = json.loads(mapping_path.read_text(encoding="utf-8"))
    _assert_ok(isinstance(cfg, dict), "CATEGORY_MAP must be a JSON object")
    _assert_ok(isinstance(cfg.get("default"), str) and cfg.get("default", "").strip(), "CATEGORY_MAP default must be a non-empty string")
    rules = cfg.get("rules")
    _assert_ok(isinstance(rules, list), "CATEGORY_MAP rules must be an array")
    for i, rule in enumerate(rules):
        _assert_ok(isinstance(rule, dict), f"CATEGORY_MAP rule[{i}] must be an object")
        match = rule.get("match")
        category = rule.get("category")
        _assert_ok(isinstance(match, str) and match.strip(), f"CATEGORY_MAP rule[{i}] missing match")
        _assert_ok(isinstance(category, str) and category.strip(), f"CATEGORY_MAP rule[{i}] missing category")
        re.compile(match, re.IGNORECASE)
    load_category_map(mapping_path)

def main():
    mode = os.environ.get("MODE", "single").strip().lower()
    _assert_ok(mode in {"single", "validate", "validate-mapping"}, f"Unsupported MODE: {mode!r}")
    year = parse_int_env("YEAR", 2026, 2000, 2100)
    months_s = os.environ.get("MONTHS", "1")
    months = parse_months(months_s)
    mandate_start_year = parse_int_env("MANDATE_START_YEAR", 2023, 2000, 2100)
    _assert_ok(mandate_start_year <= year, f"MANDATE_START_YEAR ({mandate_start_year}) cannot be greater than YEAR ({year})")

    out_dir = Path(os.environ.get("OUT_DIR", "data")).resolve()
    validate_out_dir(out_dir)
    repo_root = Path(__file__).resolve().parents[3]
    mapping_env = os.environ.get("CATEGORY_MAP", "").strip()
    if mapping_env:
        mapping_path = Path(mapping_env).resolve()
    else:
        mapping_path = (repo_root / "mapping/federal/deputados/category_map.json").resolve()
    insights_catalog_env = os.environ.get("INSIGHTS_TEXT_CATALOG", "").strip()
    if insights_catalog_env:
        insights_catalog_path = Path(insights_catalog_env).resolve()
    else:
        insights_catalog_path = (repo_root / "mapping/insights/texts_ptbr.json").resolve()

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

    for m in months:
        aggregates_obj, tipo_resumo_obj, pendentes, insight_diario_mes = build_month_aggregates(deputados, year, m, cmap)

        rows = aggregates_obj["data"]
        periodo_mes = {"tipo": "mes", "ano": year, "mes": m}
        rankings = build_rankings_from_rows(rows, periodo_mes, cmap)
        overview = build_overview_from_rows(rows, periodo_mes, cmap, daily_insight=insight_diario_mes)
        consulta_mes = build_consulta_deputados_from_rows(rows, periodo_mes, cmap)
        pkey = period_key(periodo_mes)
        ov_contract = to_overview_contract(overview, pkey)
        en_contract = to_entities_contract(consulta_mes, pkey)
        rk_contract = to_ranking_total_contract(rankings, pkey)
        period_overviews[pkey] = ov_contract
        period_entities[pkey] = en_contract
        period_rankings[pkey] = rk_contract
        write_period_contracts(
            out_dir=out_dir,
            pkey=pkey,
            overview_obj=ov_contract,
            entities_obj=en_contract,
            ranking_obj=rk_contract,
            expense_types_obj=to_expense_types_contract(tipo_resumo_obj, pkey),
            pending_categories_obj=to_pending_categories_contract(tipo_resumo_obj, pendentes, pkey),
        )

    month_periods = list_month_entity_periods(out_dir)
    year_month_periods = [(y, mm, p) for (y, mm, p) in month_periods if y == year]
    if year_month_periods:
        year_entity_files = [p for (_, _, p) in year_month_periods]
        rows_year = sum_entity_files(year_entity_files)
        periodo_ano = {"tipo": "ano", "ano": year, "mesesIncluidos": [mm for (_, mm, _) in year_month_periods]}
        year_month_overview_files = [out_dir / f"federal/camara/overview/month-{y:04d}-{mm:02d}/overview.json" for (y, mm, _) in year_month_periods]
        daily_year = pick_latest_daily_insight_from_month_overview_files(year_month_overview_files)
        rankings_year = build_rankings_from_rows(rows_year, periodo_ano, cmap)
        overview_year = build_overview_from_rows(rows_year, periodo_ano, cmap, daily_insight=daily_year)
        consulta_year = build_consulta_deputados_from_rows(rows_year, periodo_ano, cmap)
        pkey = period_key(periodo_ano)
        period_overviews[pkey] = to_overview_contract(overview_year, pkey)
        period_entities[pkey] = to_entities_contract(consulta_year, pkey)
        period_rankings[pkey] = to_ranking_total_contract(rankings_year, pkey)
        month_pkeys_year = [f"month-{y:04d}-{mm:02d}" for (y, mm, _) in year_month_periods]
        analytics_year, pendentes_year = aggregate_analytics_from_month_periods(out_dir, month_pkeys_year)
        write_period_contracts(
            out_dir=out_dir,
            pkey=pkey,
            overview_obj=period_overviews[pkey],
            entities_obj=period_entities[pkey],
            ranking_obj=period_rankings[pkey],
            expense_types_obj=to_expense_types_contract(analytics_year, pkey),
            pending_categories_obj=to_pending_categories_contract(analytics_year, pendentes_year, pkey),
        )

    mandate_periods = [(y, mm, p) for (y, mm, p) in month_periods if mandate_start_year <= y <= year]
    overview_mandate_contract = None
    if mandate_periods:
        mandate_entity_files = [p for (_, _, p) in mandate_periods]
        rows_mandato = sum_entity_files(mandate_entity_files)
        mandate_month_overview_files = [out_dir / f"federal/camara/overview/month-{y:04d}-{mm:02d}/overview.json" for (y, mm, _) in mandate_periods]
        daily_mandato = pick_latest_daily_insight_from_month_overview_files(mandate_month_overview_files)
        periodo_mandato = {
            "tipo": "mandato",
            "inicioAno": mandate_start_year,
            "fimAno": year,
            "totalMesesIncluidos": len(mandate_periods)
        }
        rankings_mandato = build_rankings_from_rows(rows_mandato, periodo_mandato, cmap)
        overview_mandato = build_overview_from_rows(rows_mandato, periodo_mandato, cmap, daily_insight=daily_mandato)
        consulta_mandato = build_consulta_deputados_from_rows(rows_mandato, periodo_mandato, cmap)
        pkey = period_key(periodo_mandato)
        period_overviews[pkey] = to_overview_contract(overview_mandato, pkey)
        period_entities[pkey] = to_entities_contract(consulta_mandato, pkey)
        period_rankings[pkey] = to_ranking_total_contract(rankings_mandato, pkey)
        overview_mandate_contract = period_overviews[pkey]
        month_pkeys_mandato = [f"month-{y:04d}-{mm:02d}" for (y, mm, _) in mandate_periods]
        analytics_mandato, pendentes_mandato = aggregate_analytics_from_month_periods(out_dir, month_pkeys_mandato)
        write_period_contracts(
            out_dir=out_dir,
            pkey=pkey,
            overview_obj=period_overviews[pkey],
            entities_obj=period_entities[pkey],
            ranking_obj=period_rankings[pkey],
            expense_types_obj=to_expense_types_contract(analytics_mandato, pkey),
            pending_categories_obj=to_pending_categories_contract(analytics_mandato, pendentes_mandato, pkey),
        )

    write_json(out_dir / "federal/camara/resumos/overview.json", overview_mandate_contract or {
        "meta": {"scope": "federal/camara", "type": "overview", "period": "mandate", "generatedAt": now_iso(), "schemaVersion": "1.0.0"},
        "base": {"amountNet": 0.0, "amountGross": 0.0, "amountAdjustments": 0.0, "recordsCount": 0, "entitiesCount": 0, "entitiesWithSpending": 0, "entitiesWithoutSpending": 0},
        "highlights": {"topSpender": {}, "topCategories": [], "topStates": [], "concentrationTop10": {"amountNet": 0.0, "percent": 0.0}, "averages": {"perEntityWithSpending": 0.0, "perRecord": 0.0}, "dailyHighlight": {}}
    })

    entity_photo_by_id: Dict[str, str] = {}
    if mandate_periods:
        mandate_entity_files = [p for (_, _, p) in mandate_periods]
        resumos = build_resumos_deputados_from_month_entities(deputados, mandate_entity_files, mandate_start_year)
        write_profiles_and_index(out_dir, resumos)
    entities_index_obj = read_json(out_dir / "federal/camara/entities/index.json") or {}
    for it in (entities_index_obj.get("items") or []):
        if not isinstance(it, dict):
            continue
        pid = str(it.get("id") or "").strip()
        photo = str(it.get("photoUrl") or it.get("urlFoto") or "").strip()
        if pid and photo and pid not in entity_photo_by_id:
            entity_photo_by_id[pid] = photo

    home_insights = build_home_insights(
        overview_mandate_contract or {},
        entity_photo_by_id=entity_photo_by_id,
        insights_text_catalog=insights_text_catalog
    )
    write_json(out_dir / "federal/camara/insights/home-insights.json", home_insights)
    write_json(out_dir / "home-insights-index.json", {
        "meta": {"generatedAt": now_iso(), "schemaVersion": "1.0.0"},
        "items": [{"id": "federal/camara", "level": "federal", "scope": "federal", "orgao": "camara", "path": "federal/camara/insights/home-insights.json", "enabled": True}]
    })

    catalog_local = {
        "meta": {"generatedAt": now_iso(), "schemaVersion": "1.0.0"},
        "datasets": [{
            "id": "federal/camara",
            "description": "Federal chamber ETL outputs",
            "pathBaseData": "federal/camara",
            "pathInsightsHome": "federal/camara/insights/home-insights.json",
            "pathOverviewRoot": "federal/camara/overview",
            "pathEntitiesRoot": "federal/camara/entities",
            "pathRankingsRoot": "federal/camara/rankings",
            "pathAnalyticsRoot": "federal/camara/analytics",
            "pathCategoryMap": "federal/camara/mapping/categoria/category_map.json"
        }]
    }
    write_json(out_dir / "catalog.json", catalog_local)
    cleanup_legacy_output(out_dir)

    print("OK: generation complete.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

