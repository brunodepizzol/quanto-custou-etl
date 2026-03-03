import os
import json
import re
import time
import hashlib
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests

CAMARA_BASE = "https://dadosabertos.camara.leg.br/api/v2"

# ----------------------------
# Helpers
# ----------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_json(path: Path, obj) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def read_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def safe_slug(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "_"

def pct(num: float, den: float) -> float:
    if den <= 0:
        return 0.0
    return float(num) / float(den)

def compute_concentration_from_map(m: Dict[str, float], total: float) -> dict:
    if total <= 0 or not m:
        return {"top1Pct": 0.0, "top3Pct": 0.0, "hhi": 0.0, "topFornecedores": []}
    items = sorted(((k, float(v)) for k, v in m.items() if float(v) > 0), key=lambda x: x[1], reverse=True)
    top = items[:5]
    shares = [v / total for _, v in items]
    hhi = sum(s * s for s in shares)
    top1 = (items[0][1] / total) if items else 0.0
    top3 = (sum(v for _, v in items[:3]) / total) if items else 0.0
    return {
        "top1Pct": round(top1, 6),
        "top3Pct": round(top3, 6),
        "hhi": round(hhi, 6),
        "topFornecedores": [{"nome": k, "valor": round(v, 2)} for k, v in top]
    }

def compute_month_stats(series: Dict[str, float]) -> dict:
    vals = [float(v) for v in series.values() if float(v) > 0]
    if len(vals) == 0:
        return {"media": 0.0, "cv": 0.0, "picoMes": None, "picoValor": 0.0}
    media = sum(vals) / len(vals)
    desvio = 0.0
    if len(vals) >= 2 and media > 0:
        desvio = statistics.pstdev(vals)
    cv = (desvio / media) if media > 0 else 0.0
    pico_mes = max(series.items(), key=lambda kv: float(kv[1]))[0]
    pico_val = float(series[pico_mes])
    return {
        "media": round(media, 2),
        "cv": round(cv, 6),
        "picoMes": pico_mes,
        "picoValor": round(pico_val, 2)
    }

def http_get_json(url: str, params: dict = None, retries: int = 3) -> dict:
    headers = {"Accept": "application/json"}
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(1.5 * attempt)
    raise last_err  # type: ignore

def paginate(endpoint: str, params: dict) -> List[dict]:
    itens = int(params.get("itens", 100))
    pagina = 1
    out: List[dict] = []
    while True:
        p = dict(params)
        p["itens"] = itens
        p["pagina"] = pagina
        data = http_get_json(endpoint, params=p)
        dados = data.get("dados", [])
        out.extend(dados)
        links = data.get("links", [])
        has_next = any(l.get("rel") == "next" for l in links)
        if not has_next or not dados:
            break
        pagina += 1
    return out

# ----------------------------
# Category mapping
# ----------------------------
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

# ----------------------------
# Câmara fetchers
# ----------------------------
def fetch_deputados() -> List[dict]:
    # deputados atuais
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

# ----------------------------
# Aggregation (monthly)
# ----------------------------
def build_month_aggregates(
    deputados: List[dict],
    ano: int,
    mes: int,
    cmap: CategoryMap,
    sleep_s: float = 0.05
) -> Tuple[dict, dict, List[str]]:
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

    rows = []
    for i, dep in enumerate(deputados, start=1):
        dep_id = int(dep["id"])
        despesas = fetch_despesas(dep_id, ano, mes)

        total = 0.0
        por_cat = {c: 0.0 for c in cats}
        por_forn: Dict[str, float] = {}
        qtd = 0
        qtd_sem_documento = 0
        valor_sem_documento = 0.0
        qtd_recibos_outros = 0
        valor_recibos_outros = 0.0

        for x in despesas:
            qtd += 1
            v = float(x.get("valorLiquido") or x.get("valorDocumento") or 0.0)
            total += v

            tipo = (x.get("tipoDespesa") or "").strip() or "(Sem tipo)"
            cat = categorize(tipo, cmap)
            por_cat.setdefault(cat, 0.0)
            por_cat[cat] += v

            fornecedor = (x.get("nomeFornecedor") or x.get("fornecedor") or "").strip() or "(Sem fornecedor)"
            por_forn[fornecedor] = float(por_forn.get(fornecedor, 0.0)) + v

            # doc indicators (objetivo, sem acusação)
            url = (x.get("urlDocumento") or "").strip()
            tipo_doc = (x.get("tipoDocumento") or "").strip()
            tem_pdf_publico = bool(url.lower().endswith(".pdf")) and url.startswith("http")
            if not tem_pdf_publico:
                qtd_sem_documento += 1
                valor_sem_documento += v

            if tipo_doc.lower() == "recibos/outros":
                qtd_recibos_outros += 1
                valor_recibos_outros += v

            # tipoDespesa resumo + pendências
            bucket = tipo_resumo.setdefault(tipo, {"tipoDespesa": tipo, "valorTotal": 0.0, "qtd": 0})
            bucket["valorTotal"] += v
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
                "total": round(total, 2),
                "qtdLancamentos": qtd,
                "porCategoria": {k: round(v, 2) for k, v in por_cat.items()},
                "qtdSemDocumentoPdf": qtd_sem_documento,
                "valorSemDocumentoPdf": round(valor_sem_documento, 2),
                "qtdRecibosOutros": qtd_recibos_outros,
                "valorRecibosOutros": round(valor_recibos_outros, 2),
                "fornecedoresUnicos": int(len(por_forn)),
                "porFornecedor": {k: round(v, 2) for k, v in por_forn.items()}
            }
        })

        # aliviar um pouco o ritmo
        if sleep_s > 0:
            time.sleep(sleep_s)

        # (opcional) print progress no log do Actions
        if i % 50 == 0:
            print(f"[{ano}-{mes:02d}] processados {i}/{len(deputados)} deputados...")

    tipo_lista = list(tipo_resumo.values())
    tipo_lista.sort(key=lambda x: x["valorTotal"], reverse=True)

    aggregates_obj = {
        "meta": {
            "fonte": "Camara Dados Abertos",
            "escopo": "federal/camara",
            "periodo": {"ano": ano, "mes": mes},
            "geradoEm": now_iso(),
            "versaoSchema": "1.1.0",
            "versaoCategoryMap": cmap.version
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
    return aggregates_obj, tipo_resumo_obj, pendentes

# ----------------------------
# Build rankings + overview from aggregates
# ----------------------------
def mk_ranking(meta_base: dict, itens: List[dict]) -> dict:
    return {"meta": meta_base, "itens": itens}

def build_rankings_from_rows(rows: List[dict], periodo_meta: dict, cmap: CategoryMap) -> Dict[str, dict]:
    cats = all_categories(cmap)

    base_meta_total = {
        "tipo": "ranking_total",
        "escopo": "federal/camara",
        "periodo": periodo_meta,
        "geradoEm": now_iso(),
        "versaoSchema": "1.1.0",
        "versaoCategoryMap": cmap.version,
        "criterio": {"consideraAtivosNoPeriodo": True}
    }

    # considera "ativos" = teve ao menos 1 lançamento ou total > 0
    ativos = [r for r in rows if (r["totais"]["qtdLancamentos"] > 0 or r["totais"]["total"] > 0)]

    total_sorted = sorted(ativos, key=lambda r: r["totais"]["total"], reverse=True)
    top_total = [{
        "id": r["id"], "nome": r["nome"], "uf": r["uf"], "partido": r["partido"],
        "valor": r["totais"]["total"], "qtdLancamentos": r["totais"]["qtdLancamentos"]
    } for r in total_sorted[:10]]

    bottom_total = [{
        "id": r["id"], "nome": r["nome"], "uf": r["uf"], "partido": r["partido"],
        "valor": r["totais"]["total"], "qtdLancamentos": r["totais"]["qtdLancamentos"]
    } for r in sorted(ativos, key=lambda r: r["totais"]["total"])[:10]]

    out: Dict[str, dict] = {}
    out["total_top10.json"] = mk_ranking(base_meta_total, top_total)
    out["total_bottom10.json"] = mk_ranking(base_meta_total, bottom_total)

    # por categoria (top100)
    for cat in cats:
        meta_cat = {
            "tipo": "ranking_categoria",
            "escopo": "federal/camara",
            "categoriaQC": cat,
            "periodo": periodo_meta,
            "geradoEm": now_iso(),
            "versaoSchema": "1.1.0",
            "versaoCategoryMap": cmap.version,
            "criterio": {"consideraAtivosNoPeriodo": True}
        }
        arr = []
        for r in ativos:
            v = float(r["totais"]["porCategoria"].get(cat, 0.0))
            arr.append({
                "id": r["id"], "nome": r["nome"], "uf": r["uf"], "partido": r["partido"],
                "valor": round(v, 2),
                "qtdLancamentos": r["totais"]["qtdLancamentos"]
            })
        arr_sorted = sorted(arr, key=lambda x: x["valor"], reverse=True)[:10]
        out[f"categoria_{cat}_top10.json"] = mk_ranking(meta_cat, arr_sorted)

    return out

def build_overview_from_rows(rows: List[dict], periodo_meta: dict, cmap: CategoryMap) -> dict:
    cats = all_categories(cmap)

    total_geral = 0.0
    soma_cat = {c: 0.0 for c in cats}
    soma_uf: Dict[str, float] = {}

    ativos = [r for r in rows if (r["totais"]["qtdLancamentos"] > 0 or r["totais"]["total"] > 0)]
    for r in ativos:
        total_geral += float(r["totais"]["total"])
        uf = r.get("uf") or "?"
        soma_uf[uf] = soma_uf.get(uf, 0.0) + float(r["totais"]["total"])
        for c in cats:
            soma_cat[c] += float(r["totais"]["porCategoria"].get(c, 0.0))

    top1 = None
    if ativos:
        top1r = max(ativos, key=lambda r: r["totais"]["total"])
        top1 = {
            "id": top1r["id"],
            "nome": top1r["nome"],
            "uf": top1r["uf"],
            "partido": top1r["partido"],
            "valor": round(float(top1r["totais"]["total"]), 2)
        }

    # ordenar top categorias e UFs
    top_cats_raw = sorted(
        [{"categoriaQC": c, "valor": float(v)} for c, v in soma_cat.items()],
        key=lambda x: x["valor"],
        reverse=True
    )
    top_n = 4
    top_n_list = [{"categoriaQC": x["categoriaQC"], "valor": round(x["valor"], 2)} for x in top_cats_raw[:top_n]]
    soma_top_n = sum(float(x["valor"]) for x in top_cats_raw[:top_n])
    resto = max(0.0, float(total_geral) - float(soma_top_n))
    top_cats = top_n_list + [{"categoriaQC": "DEMAIS CATEGORIAS", "valor": round(resto, 2)}]

    top_ufs = sorted(
        [{"uf": uf, "valor": round(v, 2)} for uf, v in soma_uf.items()],
        key=lambda x: x["valor"],
        reverse=True
    )[:12]

    return {
        "meta": {
            "tipo": "overview",
            "escopo": "federal/camara",
            "periodo": periodo_meta,
            "geradoEm": now_iso(),
            "versaoSchema": "1.1.0",
            "versaoCategoryMap": cmap.version
        },
        "kpis": {
            "totalGasto": round(total_geral, 2),
            "top1Gasto": top1,
            "topCategorias": top_cats,
            "topUFs": top_ufs,
            "agentesConsiderados": len(ativos)
        }
    }

# ----------------------------
# Rankings by UF + checksums + manifest/methodology
# ----------------------------
def period_path(periodo: dict) -> str:
    t = periodo.get("tipo")
    if t == "mes":
        return f"{int(periodo['ano']):04d}/{int(periodo['mes']):02d}"
    if t == "ano":
        return f"{int(periodo['ano']):04d}/ano"
    if t == "mandato":
        return "mandato"
    return safe_slug(str(t or "periodo"))

def write_checksums(out_dir: Path, escopo: str, periodo: dict, included_files: List[Path], rows: List[dict]) -> None:
    total = round(sum(float(r['totais']['total']) for r in rows if (r['totais']['qtdLancamentos']>0 or r['totais']['total']>0)), 2)
    agentes = len([r for r in rows if (r['totais']['qtdLancamentos']>0 or r['totais']['total']>0)])
    file_hashes = []
    for f in included_files:
        if f.exists():
            file_hashes.append({"path": str(f).replace(str(out_dir), "").lstrip("/"), "sha256": sha256_file(f)})
    combined = sha256_bytes(json.dumps(file_hashes, ensure_ascii=False, sort_keys=True).encode("utf-8"))
    obj = {
        "meta": {
            "escopo": escopo,
            "tipo": "checksums",
            "periodo": periodo,
            "geradoEm": now_iso(),
            "versaoSchema": "1.1.0"
        },
        "data": {
            "totalGasto": total,
            "agentesConsiderados": agentes,
            "files": file_hashes,
            "combinedSha256": combined
        }
    }
    key = period_path(periodo).replace("/", "_")
    write_json(out_dir / f"{escopo}/checksums/{key}.json", obj)

def write_methodology(out_dir: Path, cmap: CategoryMap) -> None:
    obj = {
        "meta": {
            "escopo": "federal/camara",
            "tipo": "methodology",
            "versao": "v1",
            "geradoEm": now_iso(),
            "versaoSchema": "1.1.0",
            "versaoCategoryMap": cmap.version
        },
        "metricas": {
            "total": "Soma do valorLiquido/valorDocumento no período.",
            "variacaoMensalCV": "Coeficiente de variação (desvio padrão populacional / média) dos totais mensais (>0).",
            "picoMensal": "Maior total mensal do período (mês e valor).",
            "fornecedorTop1Pct": "Participação do fornecedor #1 no total do período.",
            "fornecedorTop3Pct": "Participação acumulada dos 3 maiores fornecedores.",
            "fornecedorHHI": "Índice HHI baseado nas participações de fornecedores (0..1).",
            "categoriaDominantePct": "Maior categoriaQC / total do período.",
            "semDocumentoPct": "valorSemDocumentoPdf / total (indicador neutro de documentação pública em PDF).",
            "recibosOutrosPct": "valorRecibosOutros / total (indicador neutro)."
        },
        "flags": {
            "high_supplier_concentration": {"regra": "fornecedorTop1Pct >= 0.60"},
            "spiky_spend": {"regra": "picoValor >= 4x média mensal e >=3 meses com gasto"},
            "category_dominant": {"regra": "categoriaDominantePct >= 0.70"},
            "high_without_pdf": {"regra": "semDocumentoPct >= 0.30 e total >= 20000"},
            "high_receipts_other": {"regra": "recibosOutrosPct >= 0.30 e total >= 20000"}
        },
        "observacoes": [
            "Flags não indicam irregularidade; apenas padrões para investigação.",
            "Alguns períodos podem ser parciais (ano em andamento). Ver manifest.json."
        ]
    }
    write_json(out_dir / "federal/camara/methodology/v1.json", obj)

def write_manifest(out_dir: Path, mandate_start_year: int, year: int, mandate_files: List[Path], cmap: CategoryMap) -> None:
    # range
    min_ym = None
    max_ym = None
    for f in mandate_files:
        try:
            y = int(f.parent.parent.name)
            m = int(f.parent.name)
            ym = f"{y:04d}-{m:02d}"
            if min_ym is None or ym < min_ym:
                min_ym = ym
            if max_ym is None or ym > max_ym:
                max_ym = ym
        except Exception:
            pass
    sha = os.environ.get("GITHUB_SHA") or ""
    obj = {
        "meta": {
            "escopo": "federal/camara",
            "tipo": "manifest",
            "geradoEm": now_iso(),
            "versaoSchema": "1.1.0",
            "versaoCategoryMap": cmap.version,
            "methodologyVersion": "v1",
            "source": {"githubSha": sha}
        },
        "dataRange": {
            "mandatoInicioAno": mandate_start_year,
            "mandatoFimAno": year,
            "minYearMonth": min_ym,
            "maxYearMonth": max_ym,
            "totalMesesIncluidos": len(mandate_files)
        },
        "support": {
            "rankings": ["total_top10", "total_bottom10", "categoria_*_top10", "uf/*"],
            "resumos": ["overview", "deputados/{id}", "index shards by_uf/by_letter"],
            "metrics": [
                "total",
                "variacaoMensalCV",
                "picoMensal",
                "fornecedorTop1Pct",
                "fornecedorTop3Pct",
                "fornecedorHHI",
                "categoriaDominantePct",
                "semDocumentoPct",
                "recibosOutrosPct"
            ],
            "flags": ["high_supplier_concentration", "spiky_spend", "category_dominant", "high_without_pdf", "high_receipts_other"]
        }
    }
    write_json(out_dir / "federal/camara/manifest.json", obj)

def write_rankings_by_uf(out_dir: Path, rows: List[dict], periodo_meta: dict, cmap: CategoryMap) -> None:
    # agrupar ativos por UF e gerar os mesmos rankings (top10/bottom10/categorias)
    ativos = [r for r in rows if (r["totais"]["qtdLancamentos"] > 0 or r["totais"]["total"] > 0)]
    groups: Dict[str, List[dict]] = {}
    for r in ativos:
        uf = (r.get("uf") or "?").strip() or "?"
        groups.setdefault(uf, []).append(r)

    base = out_dir / "federal/camara/rankings" / period_path(periodo_meta) / "uf"
    for uf, g in groups.items():
        rankings = build_rankings_from_rows(g, {**periodo_meta, "recorte": {"tipo": "uf", "valor": uf}}, cmap)
        for fname, obj in rankings.items():
            write_json(base / safe_slug(uf) / fname, obj)


# ----------------------------
# Summation helpers (year / mandate) using stored aggregates
# ----------------------------
def sum_aggregate_files(aggregate_files: List[Path]) -> List[dict]:
    """
    Reads monthly aggregates and sums per deputy.
    Returns rows in the same schema as monthly rows.
    """
    summed: Dict[int, dict] = {}

    for f in aggregate_files:
        obj = read_json(f)
        if not obj:
            continue
        for r in obj.get("data", []):
            dep_id = int(r["id"])
            if dep_id not in summed:
                summed[dep_id] = {
                    "id": dep_id,
                    "nome": r.get("nome"),
                    "uf": r.get("uf"),
                    "partido": r.get("partido"),
                    "urlFoto": r.get("urlFoto"),
                    "totais": {
                        "total": 0.0,
                        "qtdLancamentos": 0,
                        "porCategoria": dict(r["totais"]["porCategoria"]),
                        "qtdSemDocumentoPdf": 0,
                        "valorSemDocumentoPdf": 0.0,
                        "qtdRecibosOutros": 0,
                        "valorRecibosOutros": 0.0,
                        "porFornecedor": {}
                    }
                }
                # zerar porCategoria (mantendo keys)
                for k in summed[dep_id]["totais"]["porCategoria"].keys():
                    summed[dep_id]["totais"]["porCategoria"][k] = 0.0

            s = summed[dep_id]["totais"]
            s["total"] += float(r["totais"]["total"])
            s["qtdLancamentos"] += int(r["totais"]["qtdLancamentos"])
            s["qtdSemDocumentoPdf"] += int(r["totais"].get("qtdSemDocumentoPdf", 0))
            s["valorSemDocumentoPdf"] += float(r["totais"].get("valorSemDocumentoPdf", 0.0))
            s["qtdRecibosOutros"] += int(r["totais"].get("qtdRecibosOutros", 0))
            s["valorRecibosOutros"] += float(r["totais"].get("valorRecibosOutros", 0.0))

            for k, v in (r["totais"].get("porFornecedor") or {}).items():
                s["porFornecedor"][k] = float(s["porFornecedor"].get(k, 0.0)) + float(v)

            for k, v in r["totais"]["porCategoria"].items():
                s["porCategoria"][k] = float(s["porCategoria"].get(k, 0.0)) + float(v)

    # arredondar
    out = []
    for r in summed.values():
        r["totais"]["total"] = round(r["totais"]["total"], 2)
        r["totais"]["valorSemDocumentoPdf"] = round(r["totais"]["valorSemDocumentoPdf"], 2)
        r["totais"]["valorRecibosOutros"] = round(r["totais"]["valorRecibosOutros"], 2)
        r["totais"]["porCategoria"] = {k: round(float(v), 2) for k, v in r["totais"]["porCategoria"].items()}
        if "porFornecedor" in r["totais"] and isinstance(r["totais"]["porFornecedor"], dict):
            r["totais"]["porFornecedor"] = {k: round(float(v), 2) for k, v in r["totais"]["porFornecedor"].items()}
        out.append(r)
    return out



def build_resumos_deputados(out_dir: Path, mandate_start_year: int, year: int, rows_mandato: List[dict], cmap: CategoryMap) -> None:
    """
    Gera:
      - federal/camara/resumos/_index.json (compat)
      - federal/camara/resumos/index/by_uf/{UF}.json
      - federal/camara/resumos/index/by_letter/{a..z,#}.json
      - federal/camara/resumos/deputados/{id}.json (compacto, com métricas e flags)
    """
    # Totais por ano e por mês (para CV/picos)
    totals_ano: Dict[int, Dict[str, float]] = {}
    totals_mes: Dict[int, Dict[str, float]] = {}

    mandate_files: List[Path] = []
    for y in range(mandate_start_year, year + 1):
        year_dir = out_dir / f"federal/camara/aggregates/{y:04d}"
        month_files = sorted(year_dir.glob("*/totais_deputados.json"))
        if not month_files:
            continue
        mandate_files.extend(month_files)

        rows_y = sum_aggregate_files(month_files)
        for r in rows_y:
            dep_id = int(r["id"])
            totals_ano.setdefault(dep_id, {})[str(y)] = float(r["totais"]["total"])

        # por mês
        for mf in month_files:
            mm = int(mf.parent.name)
            ym_key = f"{y:04d}-{mm:02d}"
            obj = read_json(mf) or {}
            for r in obj.get("data", []):
                dep_id = int(r["id"])
                totals_mes.setdefault(dep_id, {})[ym_key] = float(r["totais"]["total"])

    cats = all_categories(cmap)

    # ativos no mandato
    ativos = [r for r in rows_mandato if (r["totais"]["qtdLancamentos"] > 0 or r["totais"]["total"] > 0)]

    # montar index + arquivos por deputado
    index_data = []
    base_dir = out_dir / "federal/camara/resumos/deputados"
    ensure_dir(base_dir)

    for r in ativos:
        dep_id = int(r["id"])
        total = float(r["totais"]["total"])
        por_cat = r["totais"].get("porCategoria", {}) or {}
        # dominante por categoria
        dom_cat = None
        dom_val = 0.0
        for c in cats:
            v = float(por_cat.get(c, 0.0) or 0.0)
            if v > dom_val:
                dom_val = v
                dom_cat = c
        dom_pct = pct(dom_val, total)

        # indicadores de documento
        valor_sem_doc = float(r["totais"].get("valorSemDocumentoPdf", 0.0) or 0.0)
        valor_recibos = float(r["totais"].get("valorRecibosOutros", 0.0) or 0.0)
        sem_doc_pct = pct(valor_sem_doc, total)
        recibos_pct = pct(valor_recibos, total)

        # stats por mês
        series = totals_mes.get(dep_id, {})
        stats_mes = compute_month_stats(series)

        # concentração por fornecedor (mandato) - calculada a partir do agregado (porFornecedor)
        conc = compute_concentration_from_map(r["totais"].get("porFornecedor", {}) or {}, total)

        # flags (neutras; sem acusação)
        flags = []
        if conc["top1Pct"] >= 0.60 and total > 0:
            flags.append("high_supplier_concentration")
        if stats_mes["media"] > 0 and stats_mes["picoValor"] >= 4.0 * stats_mes["media"] and len([v for v in series.values() if float(v) > 0]) >= 3:
            flags.append("spiky_spend")
        if dom_pct >= 0.70 and total > 0:
            flags.append("category_dominant")
        if sem_doc_pct >= 0.30 and total >= 20000:
            flags.append("high_without_pdf")
        if recibos_pct >= 0.30 and total >= 20000:
            flags.append("high_receipts_other")

        # registro no index (leve)
        idx_item = {
            "id": dep_id,
            "nome": r.get("nome"),
            "uf": r.get("uf"),
            "partido": r.get("partido"),
            "urlFoto": r.get("urlFoto"),
            "totalMandato": round(total, 2),
            "totaisAno": totals_ano.get(dep_id, {}),
            "metricas": {
                "variacaoMensalCV": stats_mes["cv"],
                "picoMes": stats_mes["picoMes"],
                "picoValor": stats_mes["picoValor"],
                "fornecedorTop1Pct": conc["top1Pct"],
                "fornecedorTop3Pct": conc["top3Pct"],
                "fornecedorHHI": conc["hhi"],
                "categoriaDominante": dom_cat,
                "categoriaDominantePct": round(dom_pct, 6),
                "semDocumentoPct": round(sem_doc_pct, 6),
                "recibosOutrosPct": round(recibos_pct, 6)
            },
            "flags": flags
        }
        index_data.append(idx_item)

        # arquivo completo por deputado (ainda compacto)
        dep_obj = {
            "meta": {
                "escopo": "federal/camara",
                "tipo": "resumo_deputado",
                "id": dep_id,
                "geradoEm": now_iso(),
                "versaoSchema": "1.1.0",
                "mandatoInicioAno": mandate_start_year,
                "mandatoFimAno": year,
                "versaoCategoryMap": cmap.version
            },
            "data": {
                "id": dep_id,
                "nome": r.get("nome"),
                "uf": r.get("uf"),
                "partido": r.get("partido"),
                "urlFoto": r.get("urlFoto"),
                "totaisMandato": {
                    "total": round(total, 2),
                    "qtdLancamentos": int(r["totais"]["qtdLancamentos"]),
                    "porCategoria": {k: round(float(v), 2) for k, v in por_cat.items()}
                },
                "totaisAno": totals_ano.get(dep_id, {}),
                "porMes": {k: round(float(v), 2) for k, v in sorted(series.items())},
                "metricas": idx_item["metricas"],
                "topFornecedores": conc["topFornecedores"],
                "flags": flags
            }
        }
        write_json(base_dir / f"{dep_id}.json", dep_obj)

    index_obj = {
        "meta": {
            "escopo": "federal/camara",
            "tipo": "resumo_index_deputados",
            "geradoEm": now_iso(),
            "versaoSchema": "1.1.0",
            "mandatoInicioAno": mandate_start_year,
            "mandatoFimAno": year
        },
        "data": sorted(index_data, key=lambda x: (str(x.get("nome") or "").lower(), x["id"]))
    }

    # compat
    write_json(out_dir / "federal/camara/resumos/_index.json", index_obj)

    # shards por UF
    by_uf_dir = out_dir / "federal/camara/resumos/index/by_uf"
    ensure_dir(by_uf_dir)
    ufs: Dict[str, List[dict]] = {}
    for it in index_obj["data"]:
        uf = (it.get("uf") or "?").strip() or "?"
        ufs.setdefault(uf, []).append(it)
    for uf, arr in ufs.items():
        write_json(by_uf_dir / f"{safe_slug(uf)}.json", {
            "meta": {**index_obj["meta"], "shard": {"tipo": "uf", "valor": uf}},
            "data": arr
        })

    # shards por letra
    by_letter_dir = out_dir / "federal/camara/resumos/index/by_letter"
    ensure_dir(by_letter_dir)
    buckets: Dict[str, List[dict]] = {chr(c): [] for c in range(ord("a"), ord("z") + 1)}
    buckets["#"] = []
    for it in index_obj["data"]:
        nm = (it.get("nome") or "").strip().lower()
        first = nm[:1] if nm else "#"
        key = first if first.isalpha() else "#"
        buckets.setdefault(key, []).append(it)
    for k, arr in buckets.items():
        write_json(by_letter_dir / f"{k}.json", {
            "meta": {**index_obj["meta"], "shard": {"tipo": "letra", "valor": k}},
            "data": arr
        })


# ----------------------------
# Main
# ----------------------------
def parse_months(s: str) -> List[int]:
    """
    Accepts:
      "1,2,3"
      "1-12"
      "2"
    """
    s = s.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        return list(range(int(a), int(b) + 1))
    if "," in s:
        return [int(x.strip()) for x in s.split(",") if x.strip()]
    return [int(s)]

def main():
    mode = os.environ.get("MODE", "single").strip().lower()
    year = int(os.environ.get("YEAR", "2026"))
    months_s = os.environ.get("MONTHS", "1")
    months = parse_months(months_s)
    mandate_start_year = int(os.environ.get("MANDATE_START_YEAR", "2023"))

    out_dir = Path(os.environ.get("OUT_DIR", "data")).resolve()
    mapping_path = Path(os.environ.get("CATEGORY_MAP", "mapping/category_map.json")).resolve()

    cmap = load_category_map(mapping_path)

    # garantir que category_map esteja também em data/mapping (para o site/verificação)
    write_json(out_dir / "mapping/category_map.json", json.loads(mapping_path.read_text(encoding="utf-8")))

    deputados = fetch_deputados()

    # gerar mês a mês (agregado + rankings + overview + dicionário/pendências)
    for m in months:
        aggregates_obj, tipo_resumo_obj, pendentes = build_month_aggregates(deputados, year, m, cmap)

        agg_path = out_dir / f"federal/camara/aggregates/{year:04d}/{m:02d}/totais_deputados.json"
        write_json(agg_path, aggregates_obj)

        write_json(out_dir / f"federal/camara/dicionarios/{year:04d}/{m:02d}/tipoDespesa_resumo.json", tipo_resumo_obj)
        write_json(out_dir / f"federal/camara/pendencias/{year:04d}/{m:02d}/tipoDespesa_pendentes.json", {
            "meta": {"escopo": "federal/camara", "periodo": {"ano": year, "mes": m}, "geradoEm": now_iso()},
            "data": pendentes
        })

        rows = aggregates_obj["data"]
        periodo_mes = {"tipo": "mes", "ano": year, "mes": m}

        # rankings do mês
        rankings = build_rankings_from_rows(rows, periodo_mes, cmap)
        for fname, obj in rankings.items():
            write_json(out_dir / f"federal/camara/rankings/{year:04d}/{m:02d}/{fname}", obj)

        # overview do mês
        overview = build_overview_from_rows(rows, periodo_mes, cmap)
        write_json(out_dir / f"federal/camara/resumos/{year:04d}/{m:02d}/overview.json", overview)

        # rankings por UF (mes)
        write_rankings_by_uf(out_dir, rows, periodo_mes, cmap)
        # checksums (mes)
        write_checksums(out_dir, "federal/camara", periodo_mes, [agg_path], rows)

    # rebuild YEAR (ano fechado ou acumulado) e MANDATO usando aggregates armazenados
    # Ano: soma todos os meses que existem em aggregates/{year}/
    year_agg_dir = out_dir / f"federal/camara/aggregates/{year:04d}"
    month_files = sorted(year_agg_dir.glob("*/totais_deputados.json"))
    if month_files:
        rows_year = sum_aggregate_files(month_files)
        periodo_ano = {"tipo": "ano", "ano": year, "mesesIncluidos": [int(p.parent.name) for p in month_files]}
        rankings_year = build_rankings_from_rows(rows_year, periodo_ano, cmap)
        for fname, obj in rankings_year.items():
            write_json(out_dir / f"federal/camara/rankings/{year:04d}/ano/{fname}", obj)
        overview_year = build_overview_from_rows(rows_year, periodo_ano, cmap)
        write_json(out_dir / f"federal/camara/resumos/{year:04d}/ano/overview.json", overview_year)

        write_rankings_by_uf(out_dir, rows_year, periodo_ano, cmap)
        write_checksums(out_dir, "federal/camara", periodo_ano, month_files, rows_year)

    # Mandato: soma todos os aggregates de mandate_start_year..(year) existentes
    mandate_files = []
    for y in range(mandate_start_year, year + 1):
        mandate_files.extend(sorted((out_dir / f"federal/camara/aggregates/{y:04d}").glob("*/totais_deputados.json")))
    if mandate_files:
        rows_mandato = sum_aggregate_files(mandate_files)
        periodo_mandato = {
            "tipo": "mandato",
            "inicioAno": mandate_start_year,
            "fimAno": year,
            "totalMesesIncluidos": len(mandate_files)
        }
        rankings_mandato = build_rankings_from_rows(rows_mandato, periodo_mandato, cmap)
        for fname, obj in rankings_mandato.items():
            write_json(out_dir / "federal/camara/rankings/mandato" / fname, obj)
        overview_mandato = build_overview_from_rows(rows_mandato, periodo_mandato, cmap)
        write_json(out_dir / "federal/camara/resumos/mandato/overview.json", overview_mandato)
        build_resumos_deputados(out_dir, mandate_start_year, year, rows_mandato, cmap)

        write_rankings_by_uf(out_dir, rows_mandato, periodo_mandato, cmap)
        write_checksums(out_dir, "federal/camara", periodo_mandato, mandate_files, rows_mandato)
        write_methodology(out_dir, cmap)
        write_manifest(out_dir, mandate_start_year, year, mandate_files, cmap)

    # catalog
    catalog = read_json(out_dir / "catalog.json") or {"meta": {}, "datasets": []}
    catalog["meta"] = {"geradoEm": now_iso(), "versaoSchema": "1.1.0"}
    # registra o que existe hoje para a Câmara
    catalog["datasets"] = [
        {
            "id": "federal/camara",
            "descricao": "Rankings e resumos (Deputados Federais) gerados a partir da API da Câmara",
            "mandatoInicioAno": mandate_start_year
        }
    ]
    write_json(out_dir / "catalog.json", catalog)

    print("OK: geração concluída.")

if __name__ == "__main__":
    main()