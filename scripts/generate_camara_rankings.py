import os
import json
import re
import time
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

def write_resumos_index(out_dir: Path, escopo: str = "federal/camara") -> None:
    deps_dir = out_dir / "federal/camara/resumos/deputados"
    if not deps_dir.exists():
        print("WARN: resumos/deputados não existe; pulando _index.json")
        return

    itens = []
    ufs = set()
    partidos = set()

    for fp in sorted(deps_dir.glob("*.json")):
        obj = read_json(fp)
        if not obj:
            continue
        d = obj.get("data") or {}
        dep_id = d.get("id") if d.get("id") is not None else (obj.get("meta") or {}).get("id")
        nome = d.get("nome")
        uf = d.get("uf")
        partido = d.get("partido")
        url_foto = d.get("urlFoto")
        totais = d.get("totaisMandato") or {}
        total = totais.get("total")
        qtd = totais.get("qtdLancamentos")
        if dep_id is None or not nome:
            continue

        if uf:
            ufs.add(str(uf))
        if partido:
            partidos.add(str(partido))

        try:
            dep_id_out = int(dep_id)
        except Exception:
            dep_id_out = dep_id

        itens.append({
            "id": dep_id_out,
            "nome": nome,
            "uf": uf,
            "partido": partido,
            "urlFoto": url_foto,
            "totalMandato": total,
            "qtdLancamentosMandato": qtd
        })

    itens.sort(key=lambda x: safe_slug((x.get("nome") or "").lower()))

    obj_out = {
        "meta": {
            "escopo": escopo,
            "tipo": "resumos_index",
            "geradoEm": now_iso(),
            "versaoSchema": "1.1.0",
            "itens": len(itens)
        },
        "data": {
            "deputados": itens,
            "ufs": sorted(ufs),
            "partidos": sorted(partidos)
        }
    }
    write_json(out_dir / "federal/camara/resumos/_index.json", obj_out)


def http_get_json(url: str, params: dict = None, retries: int = 8, timeout_s: int = 120) -> dict:
    """GET JSON with retry/backoff for transient HTTP failures (429/5xx)."""
    headers = {"Accept": "application/json"}
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout_s)
            # handle transient codes with retry
            if r.status_code in (429, 500, 502, 503, 504):
                # respect Retry-After if present (seconds)
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
            # exponential backoff with cap
            time.sleep(min(2 ** attempt, 30))
        except Exception as e:
            last_err = e
            time.sleep(min(2 ** attempt, 30))
    raise last_err  # type: ignore

def paginate(endpoint: str, params: dict) -> List[dict]:
    """Paginate Câmara API. If timeouts happen, auto-reduce page size."""
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
            # if it was a gateway timeout, try reducing page size
            msg = str(e)
            if ("504" in msg or "Gateway Timeout" in msg) and itens > 25:
                itens = 50 if itens > 50 else 25
                print(f"AVISO: 504 no endpoint, reduzindo page-size para {itens} e repetindo (pagina={pagina}).")
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
                "valorRecibosOutros": round(valor_recibos_outros, 2)
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
            "versaoSchema": "1.0.0",
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
        "versaoSchema": "1.0.0",
        "versaoCategoryMap": cmap.version,
        "criterio": {"consideraAtivosNoPeriodo": True}
    }

    # considera "ativos" = teve ao menos 1 lançamento ou total > 0
    ativos = [r for r in rows if (r["totais"]["qtdLancamentos"] > 0 or r["totais"]["total"] > 0)]

    total_sorted = sorted(ativos, key=lambda r: r["totais"]["total"], reverse=True)
    top_total = [{
        "id": r["id"], "nome": r["nome"], "uf": r["uf"], "partido": r["partido"],
        "valor": r["totais"]["total"], "qtdLancamentos": r["totais"]["qtdLancamentos"]
    } for r in total_sorted[:100]]

    bottom_total = [{
        "id": r["id"], "nome": r["nome"], "uf": r["uf"], "partido": r["partido"],
        "valor": r["totais"]["total"], "qtdLancamentos": r["totais"]["qtdLancamentos"]
    } for r in sorted(ativos, key=lambda r: r["totais"]["total"])[:100]]

    out: Dict[str, dict] = {}
    out["total_top100.json"] = mk_ranking(base_meta_total, top_total)
    out["total_bottom100.json"] = mk_ranking(base_meta_total, bottom_total)

    # por categoria (top100)
    for cat in cats:
        meta_cat = {
            "tipo": "ranking_categoria",
            "escopo": "federal/camara",
            "categoriaQC": cat,
            "periodo": periodo_meta,
            "geradoEm": now_iso(),
            "versaoSchema": "1.0.0",
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
        arr_sorted = sorted(arr, key=lambda x: x["valor"], reverse=True)[:100]
        out[f"categoria_{cat}_top100.json"] = mk_ranking(meta_cat, arr_sorted)

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
            "versaoSchema": "1.0.0",
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
                        "valorRecibosOutros": 0.0
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

            for k, v in r["totais"]["porCategoria"].items():
                s["porCategoria"][k] = float(s["porCategoria"].get(k, 0.0)) + float(v)

    # arredondar
    out = []
    for r in summed.values():
        r["totais"]["total"] = round(r["totais"]["total"], 2)
        r["totais"]["valorSemDocumentoPdf"] = round(r["totais"]["valorSemDocumentoPdf"], 2)
        r["totais"]["valorRecibosOutros"] = round(r["totais"]["valorRecibosOutros"], 2)
        r["totais"]["porCategoria"] = {k: round(float(v), 2) for k, v in r["totais"]["porCategoria"].items()}
        out.append(r)
    return out

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

    write_resumos_index(out_dir)

    # catalog
    catalog = read_json(out_dir / "catalog.json") or {"meta": {}, "datasets": []}
    catalog["meta"] = {"geradoEm": now_iso(), "versaoSchema": "1.0.0"}
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
