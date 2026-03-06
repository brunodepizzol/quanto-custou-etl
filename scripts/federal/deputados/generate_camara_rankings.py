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
    # Common API format: YYYY-MM-DD (possibly with time suffix)
    if len(s) >= 10:
        cand = s[:10]
        try:
            datetime.strptime(cand, "%Y-%m-%d")
            return cand
        except Exception:
            pass
    # Fallback for DD/MM/YYYY
    try:
        d = datetime.strptime(s, "%d/%m/%Y")
        return d.strftime("%Y-%m-%d")
    except Exception:
        return None
    
def write_resumos_index(out_dir: Path, escopo: str = "federal/camara") -> None:
    deps_dir = out_dir / "federal/camara/resumos/deputados"
    if not deps_dir.exists():
        print("WARN: resumos/deputados não existe; pulando index.json")
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

        totais_ano = d.get("totaisAno") or {}

        itens.append({
            "id": dep_id_out,
            "nome": nome,
            "uf": uf,
            "partido": partido,
            "urlFoto": url_foto,
            "totalMandato": total,
            "qtdLancamentosMandato": qtd,
            "totaisAno": totais_ano
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
    write_json(out_dir / "federal/camara/resumos/index.json", obj_out)

def build_metodologia_global() -> dict:
    return {
        "meta": {
            "id": "metodologia_global",
            "escopo": "global",
            "geradoEm": now_iso(),
            "versaoSchema": "1.0.0",
            "versaoDocumento": "1.0.0"
        },
        "principios": [
            "Uso exclusivo de bases públicas oficiais.",
            "Não substituição da fonte oficial de origem.",
            "Processamento técnico padronizado e replicável.",
            "Ausência de juízo de valor sobre condutas individuais.",
            "Transparência de fórmulas, versões e critérios."
        ],
        "operacoesBase": [
            {"id": "soma", "descricao": "Soma de valores monetários por período e recortes."},
            {"id": "ordenacao", "descricao": "Ordenação numérica ascendente/descendente por métrica."},
            {"id": "filtro", "descricao": "Aplicação de filtros explícitos definidos pelo usuário."},
            {"id": "agrupamento", "descricao": "Agrupamento técnico por regras de categoria e/ou chaves oficiais."},
            {"id": "arredondamento", "descricao": "Arredondamento monetário para 2 casas decimais."}
        ],
        "governanca": {
            "criterioDivergencia": "Em caso de divergência, prevalece o dado do órgão público responsável.",
            "auditoriaMinima": [
                "Fonte oficial utilizada.",
                "Versão do mapeamento/categorização.",
                "Versão de schema dos artefatos.",
                "Data/hora de geração."
            ]
        }
    }

def build_metodologia_scope_camara(cmap: CategoryMap, mandate_start_year: int) -> dict:
    return {
        "meta": {
            "id": "metodologia_scope_federal_camara",
            "escopo": "federal/camara",
            "geradoEm": now_iso(),
            "versaoSchema": "1.0.0",
            "versaoDocumento": "1.0.0"
        },
        "fonte": {
            "orgao": "Câmara dos Deputados",
            "apiBase": CAMARA_BASE,
            "endpoints": [
                "/deputados",
                "/deputados/{id}/despesas"
            ]
        },
        "periodizacao": {
            "mes": "Consulta por ano/mês da API e geração de agregados mensais.",
            "ano": "Soma dos agregados mensais existentes no ano.",
            "mandato": f"Soma de agregados mensais de {mandate_start_year} em diante."
        },
        "categorizacao": {
            "pathCategoryMap": "federal/camara/mapping/categoria/category_map.json",
            "versaoCategoryMap": cmap.version,
            "default": cmap.default,
            "descricao": "Tipo de despesa oficial agrupado por regras regex versionadas."
        },
        "criterios": {
            "agenteAtivoPeriodo": "Deputado com qtdLancamentos > 0 ou total > 0 no período.",
            "integridadeDocumental": [
                "Sem documento PDF público (urlDocumento não termina com .pdf ou não é URL HTTP).",
                "Tipo de documento 'recibos/outros'."
            ]
        },
        "artefatosSaida": [
            {"path": "federal/camara/aggregates/{ano}/{mes}/totais_deputados.json", "descricao": "Base agregada mensal por deputado."},
            {"path": "federal/camara/consultas/{periodo}/deputados.json", "descricao": "Consulta otimizada por período para consumo do front."},
            {"path": "federal/camara/rankings/{periodo}/*.json", "descricao": "Rankings por total, integridade e categoria."},
            {"path": "federal/camara/resumos/{periodo}/overview.json", "descricao": "KPIs e insights agregados do período."},
            {"path": "federal/camara/dicionarios/{ano}/{mes}/tipoDespesa_resumo.json", "descricao": "Resumo por tipo de despesa oficial."},
            {"path": "federal/camara/pendencias/{ano}/{mes}/tipoDespesa_pendentes.json", "descricao": "Tipos que caíram na categoria default."},
            {"path": "federal/camara/resumos/deputados/{id}.json", "descricao": "Resumo individual de mandato por deputado."},
            {"path": "federal/camara/resumos/index.json", "descricao": "Índice de deputados, UFs e partidos."}
        ],
        "indicadores": [
            {
                "id": "totalGasto",
                "fonteSaida": "resumos/{periodo}/overview.json:kpis.totalGasto",
                "formula": "sum(total por deputado ativo no período)"
            },
            {
                "id": "top1Gasto",
                "fonteSaida": "resumos/{periodo}/overview.json:kpis.top1Gasto",
                "formula": "argmax(total por deputado ativo no período)"
            },
            {
                "id": "top8MaisDemais",
                "fonteSaida": "resumos/{periodo}/overview.json:insights.categoria.top8MaisDemais",
                "formula": "ordenar categorias por valor, selecionar top 8, residual = totalGasto - soma(top8)"
            },
            {
                "id": "topUFs",
                "fonteSaida": "resumos/{periodo}/overview.json:insights.uf.topUFs",
                "formula": "sum(total) por UF, ordenar desc, limitar 12"
            },
            {
                "id": "concentracaoTop10",
                "fonteSaida": "resumos/{periodo}/overview.json:insights.concentracao",
                "formula": "top10Total = soma dos 10 maiores totais; top10Percentual = top10Total/totalGasto*100"
            },
            {
                "id": "medias",
                "fonteSaida": "resumos/{periodo}/overview.json:insights.medias",
                "formula": "porAgenteComGasto = totalGasto/agentesComGasto; porLancamento = totalGasto/totalLancamentos"
            },
            {
                "id": "insightDiario",
                "fonteSaida": "resumos/{periodo}/overview.json:insights.diario",
                "formula": "selecionar a dataDocumento mais recente disponível; dentro dela, escolher o maior valor"
            }
        ]
    }

def write_metodologia_docs(out_dir: Path, cmap: CategoryMap, mandate_start_year: int) -> None:
    write_json(out_dir / "metodologia.json", build_metodologia_global())
    write_json(out_dir / "federal/camara/metodologia_scope.json", build_metodologia_scope_camara(cmap, mandate_start_year))


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

            data_doc = normalize_doc_date(x.get("dataDocumento"))
            if data_doc:
                cand = {
                    "dataReferencia": data_doc,
                    "deputado": {
                        "id": dep_id,
                        "nome": dep_map[dep_id].get("nome"),
                        "uf": dep_map[dep_id].get("siglaUf"),
                        "partido": dep_map[dep_id].get("siglaPartido")
                    },
                    "valor": round(v, 2),
                    "tipoDespesa": tipo,
                    "categoriaQC": cat,
                    "fornecedor": (x.get("nomeFornecedor") or "").strip() or None,
                    "tipoDocumento": (x.get("tipoDocumento") or "").strip() or None,
                    "urlDocumento": (x.get("urlDocumento") or "").strip() or None
                }
                prev = max_dia_por_data.get(data_doc)
                if (not prev) or (float(cand["valor"]) > float(prev.get("valor", 0.0))):
                    max_dia_por_data[data_doc] = cand

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

    insight_diario = None
    if max_dia_por_data:
        data_mais_recente = max(max_dia_por_data.keys())
        insight_diario = max_dia_por_data.get(data_mais_recente)

    aggregates_obj = {
        "meta": {
            "fonte": "Camara Dados Abertos",
            "escopo": "federal/camara",
            "periodo": {"ano": ano, "mes": mes},
            "geradoEm": now_iso(),
            "versaoSchema": "1.1.0",
            "versaoCategoryMap": cmap.version,
            "insights": {
                "diario": insight_diario
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

def pick_latest_daily_insight_from_aggregate_files(aggregate_files: List[Path]) -> Optional[dict]:
    latest = None
    latest_date = None
    for fp in aggregate_files:
        obj = read_json(fp)
        if not obj:
            continue
        daily = (((obj.get("meta") or {}).get("insights") or {}).get("diario")) or None
        if not isinstance(daily, dict):
            continue
        d = normalize_doc_date(daily.get("dataReferencia"))
        if not d:
            continue
        if (latest_date is None) or (d > latest_date):
            latest_date = d
            latest = daily
        elif d == latest_date:
            if float(daily.get("valor") or 0.0) > float((latest or {}).get("valor") or 0.0):
                latest = daily
    return latest

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
    } for r in total_sorted[:10]]

    bottom_total = [{
        "id": r["id"], "nome": r["nome"], "uf": r["uf"], "partido": r["partido"],
        "valor": r["totais"]["total"], "qtdLancamentos": r["totais"]["qtdLancamentos"]
    } for r in sorted(ativos, key=lambda r: r["totais"]["total"])[:10]]

    out: Dict[str, dict] = {}
    out["total_top10.json"] = mk_ranking(base_meta_total, top_total)
    out["total_bottom10.json"] = mk_ranking(base_meta_total, bottom_total)

    # integridade documental (top10)
    meta_int_base = {
        "tipo": "ranking_integridade",
        "escopo": "federal/camara",
        "periodo": periodo_meta,
        "geradoEm": now_iso(),
        "versaoSchema": "1.0.0",
        "versaoCategoryMap": cmap.version,
        "criterio": {"consideraAtivosNoPeriodo": True}
    }

    sem_pdf_sorted = sorted(ativos, key=lambda r: float(r["totais"].get("valorSemDocumentoPdf", 0.0)), reverse=True)
    out["integridade_sem_pdf_top10.json"] = mk_ranking(
        {**meta_int_base, "metrica": "valorSemDocumentoPdf"},
        [{
            "id": r["id"], "nome": r["nome"], "uf": r["uf"], "partido": r["partido"],
            "valor": round(float(r["totais"].get("valorSemDocumentoPdf", 0.0)), 2),
            "qtdLancamentos": r["totais"]["qtdLancamentos"],
            "qtdSemDocumentoPdf": int(r["totais"].get("qtdSemDocumentoPdf", 0))
        } for r in sem_pdf_sorted[:10]]
    )

    outros_sorted = sorted(ativos, key=lambda r: float(r["totais"].get("valorRecibosOutros", 0.0)), reverse=True)
    out["integridade_outros_recibos_top10.json"] = mk_ranking(
        {**meta_int_base, "metrica": "valorRecibosOutros"},
        [{
            "id": r["id"], "nome": r["nome"], "uf": r["uf"], "partido": r["partido"],
            "valor": round(float(r["totais"].get("valorRecibosOutros", 0.0)), 2),
            "qtdLancamentos": r["totais"]["qtdLancamentos"],
            "qtdRecibosOutros": int(r["totais"].get("qtdRecibosOutros", 0))
        } for r in outros_sorted[:10]]
    )

    # por categoria (top10)
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
        arr_sorted = sorted(arr, key=lambda x: x["valor"], reverse=True)[:10]
        out[f"categoria_{cat}_top10.json"] = mk_ranking(meta_cat, arr_sorted)

    return out

def build_overview_from_rows(rows: List[dict], periodo_meta: dict, cmap: CategoryMap, daily_insight: Optional[dict] = None) -> dict:
    cats = all_categories(cmap)

    total_geral = 0.0
    soma_cat = {c: 0.0 for c in cats}
    soma_uf: Dict[str, float] = {}
    total_lancamentos = 0

    ativos = [r for r in rows if (r["totais"]["qtdLancamentos"] > 0 or r["totais"]["total"] > 0)]
    base_agentes = len(rows)
    com_gasto_agentes = len(ativos)
    sem_gasto_agentes = max(0, base_agentes - com_gasto_agentes)
    for r in ativos:
        total_geral += float(r["totais"]["total"])
        total_lancamentos += int(r["totais"]["qtdLancamentos"])
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
    top_n = 8
    top_n_list = [{"categoriaQC": x["categoriaQC"], "valor": round(x["valor"], 2)} for x in top_cats_raw[:top_n]]
    soma_top_n = sum(float(x["valor"]) for x in top_cats_raw[:top_n])
    resto = max(0.0, float(total_geral) - float(soma_top_n))
    top_cats = top_n_list + [{"categoriaQC": "DEMAIS CATEGORIAS", "valor": round(resto, 2)}]

    top_ufs = sorted(
        [{"uf": uf, "valor": round(v, 2)} for uf, v in soma_uf.items()],
        key=lambda x: x["valor"],
        reverse=True
    )[:12]

    # Insight helpers (não quebram contrato antigo de kpis)
    top10_total = sum(float(r["totais"]["total"]) for r in sorted(ativos, key=lambda x: float(x["totais"]["total"]), reverse=True)[:10])
    top10_pct = (top10_total / total_geral * 100.0) if total_geral > 0 else 0.0
    media_por_agente = (total_geral / com_gasto_agentes) if com_gasto_agentes > 0 else 0.0
    media_por_lancamento = (total_geral / total_lancamentos) if total_lancamentos > 0 else 0.0

    return {
        "meta": {
            "tipo": "overview",
            "escopo": "federal/camara",
            "periodo": periodo_meta,
            "geradoEm": now_iso(),
            "versaoSchema": "1.2.0",
            "versaoCategoryMap": cmap.version
        },
        "kpis": {
            "totalGasto": round(total_geral, 2),
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
                "totalGasto": round(total_geral, 2)
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
            "qtdLancamentos": int(totais.get("qtdLancamentos", 0) or 0),
            "porCategoria": {k: round(float(v or 0.0), 2) for k, v in (totais.get("porCategoria") or {}).items()},
            "qtdSemDocumentoPdf": int(totais.get("qtdSemDocumentoPdf", 0) or 0),
            "valorSemDocumentoPdf": round(float(totais.get("valorSemDocumentoPdf", 0.0) or 0.0), 2),
            "qtdRecibosOutros": int(totais.get("qtdRecibosOutros", 0) or 0),
            "valorRecibosOutros": round(float(totais.get("valorRecibosOutros", 0.0) or 0.0), 2)
        })

    return {
        "meta": {
            "tipo": "consulta_deputados",
            "escopo": "federal/camara",
            "periodo": periodo_meta,
            "geradoEm": now_iso(),
            "versaoSchema": "1.0.0",
            "versaoCategoryMap": cmap.version,
            "campos": [
                "id", "nome", "uf", "partido", "urlFoto",
                "total", "qtdLancamentos", "porCategoria",
                "qtdSemDocumentoPdf", "valorSemDocumentoPdf",
                "qtdRecibosOutros", "valorRecibosOutros"
            ]
        },
        "itens": itens
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
# Build per-deputy resumos (mandato) from stored monthly aggregates
# ----------------------------
def build_resumos_deputados_from_monthlies(
    deputados: List[dict],
    aggregate_files: List[Path],
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
                    "qtdLancamentos": 0,
                    "porCategoria": {},
                    "qtdSemDocumentoPdf": 0,
                    "valorSemDocumentoPdf": 0.0,
                    "qtdRecibosOutros": 0,
                    "valorRecibosOutros": 0.0
                },
                "totaisAno": {},
                "porMes": {}
            }
        return acc[dep_id]

    for fp in aggregate_files:
        # infer year/month from path: .../aggregates/YYYY/MM/totais_deputados.json
        try:
            parts = fp.parts
            i = parts.index("aggregates")
            year = int(parts[i+1])
            month = int(parts[i+2])
        except Exception:
            year = None
            month = None

        obj = read_json(fp)
        if not obj:
            continue
        rows = obj.get("data") or []
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

            # refresh identity fields if missing
            a["nome"] = a.get("nome") or row.get("nome")
            a["uf"] = a.get("uf") or row.get("uf")
            a["partido"] = a.get("partido") or row.get("partido")
            a["urlFoto"] = a.get("urlFoto") or row.get("urlFoto")

            t = row.get("totais") or {}
            total = float(t.get("total") or 0.0)
            qtd = int(t.get("qtdLancamentos") or 0)

            a["totaisMandato"]["total"] += total
            a["totaisMandato"]["qtdLancamentos"] += qtd

            # per-category sums
            pc = t.get("porCategoria") or {}
            dst_pc = a["totaisMandato"]["porCategoria"]
            for k, v in pc.items():
                dst_pc[k] = float(dst_pc.get(k) or 0.0) + float(v or 0.0)

            # doc stats
            a["totaisMandato"]["qtdSemDocumentoPdf"] += int(t.get("qtdSemDocumentoPdf") or 0)
            a["totaisMandato"]["valorSemDocumentoPdf"] += float(t.get("valorSemDocumentoPdf") or 0.0)
            a["totaisMandato"]["qtdRecibosOutros"] += int(t.get("qtdRecibosOutros") or 0)
            a["totaisMandato"]["valorRecibosOutros"] += float(t.get("valorRecibosOutros") or 0.0)

            if year is not None:
                if year >= mandate_start_year:
                    a["totaisAno"][str(year)] = float(a["totaisAno"].get(str(year)) or 0.0) + total

            if ym_key:
                a["porMes"][ym_key] = float(a["porMes"].get(ym_key) or 0.0) + total

    # finalize rounding and compute derived fields
    for dep_id, a in acc.items():
        tm = a["totaisMandato"]
        tm["total"] = round(tm["total"], 2)
        tm["valorSemDocumentoPdf"] = round(tm["valorSemDocumentoPdf"], 2)
        tm["valorRecibosOutros"] = round(tm["valorRecibosOutros"], 2)
        tm["porCategoria"] = {k: round(float(v), 2) for k, v in (tm.get("porCategoria") or {}).items()}

        a["totaisAno"] = {k: round(float(v), 2) for k, v in (a.get("totaisAno") or {}).items()}
        a["porMes"] = {k: round(float(v), 2) for k, v in (a.get("porMes") or {}).items()}

        # maiorCategoria
        pc = tm.get("porCategoria") or {}
        if pc:
            cat, val = max(pc.items(), key=lambda kv: kv[1])
            total = tm.get("total") or 0.0
            pct = (float(val) / float(total) * 100.0) if total else 0.0
            a["maiorCategoria"] = {"categoria": cat, "valor": round(float(val), 2), "pct": round(pct, 2)}
        else:
            a["maiorCategoria"] = None

    return acc


def write_resumos_deputados_mandato(
    out_dir: Path,
    deputados: List[dict],
    aggregate_files: List[Path],
    mandate_start_year: int
) -> None:
    deps_dir = out_dir / "federal/camara/resumos/deputados"
    deps_dir.mkdir(parents=True, exist_ok=True)

    resumos = build_resumos_deputados_from_monthlies(deputados, aggregate_files, mandate_start_year)

    for dep_id, a in resumos.items():
        obj = {
            "meta": {
                "escopo": "federal/camara",
                "tipo": "resumo_deputado_mandato",
                "id": dep_id,
                "geradoEm": now_iso(),
                "versaoSchema": "1.1.0"
            },
            "data": a
        }
        write_json(deps_dir / f"{dep_id}.json", obj)


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
    repo_root = Path(__file__).resolve().parents[3]
    mapping_env = os.environ.get("CATEGORY_MAP", "").strip()
    if mapping_env:
        mapping_path = Path(mapping_env).resolve()
    else:
        mapping_path = (repo_root / "mapping/federal/deputados/category_map.json").resolve()
    if not mapping_path.exists():
        raise FileNotFoundError(f"CATEGORY_MAP não encontrado: {mapping_path}")

    cmap = load_category_map(mapping_path)
    # Publica o map usado pela execução como artefato de dados para consumo do front.
    out_mapping_path = out_dir / "federal/camara/mapping/categoria/category_map.json"
    ensure_dir(out_mapping_path.parent)
    out_mapping_path.write_text(mapping_path.read_text(encoding="utf-8"), encoding="utf-8")
    write_metodologia_docs(out_dir, cmap, mandate_start_year)

    deputados = fetch_deputados()

    # gerar mês a mês (agregado + rankings + overview + dicionário/pendências)
    for m in months:
        aggregates_obj, tipo_resumo_obj, pendentes, insight_diario_mes = build_month_aggregates(deputados, year, m, cmap)

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
        overview = build_overview_from_rows(rows, periodo_mes, cmap, daily_insight=insight_diario_mes)
        write_json(out_dir / f"federal/camara/resumos/{year:04d}/{m:02d}/overview.json", overview)
        consulta_mes = build_consulta_deputados_from_rows(rows, periodo_mes, cmap)
        write_json(out_dir / f"federal/camara/consultas/{year:04d}/{m:02d}/deputados.json", consulta_mes)

    # rebuild YEAR (ano fechado ou acumulado) e MANDATO usando aggregates armazenados
    # Ano: soma todos os meses que existem em aggregates/{year}/
    year_agg_dir = out_dir / f"federal/camara/aggregates/{year:04d}"
    month_files = sorted(year_agg_dir.glob("*/totais_deputados.json"))
    if month_files:
        rows_year = sum_aggregate_files(month_files)
        periodo_ano = {"tipo": "ano", "ano": year, "mesesIncluidos": [int(p.parent.name) for p in month_files]}
        daily_year = pick_latest_daily_insight_from_aggregate_files(month_files)
        rankings_year = build_rankings_from_rows(rows_year, periodo_ano, cmap)
        for fname, obj in rankings_year.items():
            write_json(out_dir / f"federal/camara/rankings/{year:04d}/ano/{fname}", obj)
        overview_year = build_overview_from_rows(rows_year, periodo_ano, cmap, daily_insight=daily_year)
        write_json(out_dir / f"federal/camara/resumos/{year:04d}/ano/overview.json", overview_year)
        consulta_year = build_consulta_deputados_from_rows(rows_year, periodo_ano, cmap)
        write_json(out_dir / f"federal/camara/consultas/{year:04d}/ano/deputados.json", consulta_year)

    # Mandato: soma todos os aggregates de mandate_start_year..(year) existentes
    mandate_files = []
    for y in range(mandate_start_year, year + 1):
        mandate_files.extend(sorted((out_dir / f"federal/camara/aggregates/{y:04d}").glob("*/totais_deputados.json")))
    if mandate_files:
        rows_mandato = sum_aggregate_files(mandate_files)
        daily_mandato = pick_latest_daily_insight_from_aggregate_files(mandate_files)
        periodo_mandato = {
            "tipo": "mandato",
            "inicioAno": mandate_start_year,
            "fimAno": year,
            "totalMesesIncluidos": len(mandate_files)
        }
        rankings_mandato = build_rankings_from_rows(rows_mandato, periodo_mandato, cmap)
        for fname, obj in rankings_mandato.items():
            write_json(out_dir / "federal/camara/rankings/mandato" / fname, obj)
        overview_mandato = build_overview_from_rows(rows_mandato, periodo_mandato, cmap, daily_insight=daily_mandato)
        write_json(out_dir / "federal/camara/resumos/mandato/overview.json", overview_mandato)
        consulta_mandato = build_consulta_deputados_from_rows(rows_mandato, periodo_mandato, cmap)
        write_json(out_dir / "federal/camara/consultas/mandato/deputados.json", consulta_mandato)

    # Resumos individuais (mandato) + índice universal
    if mandate_files:
        write_resumos_deputados_mandato(out_dir, deputados, mandate_files, mandate_start_year)

    write_resumos_index(out_dir)

    # catalog do dataset
    catalog = read_json(out_dir / "federal/camara/catalog.json") or {"meta": {}, "datasets": []}
    catalog["meta"] = {"geradoEm": now_iso(), "versaoSchema": "1.0.0"}
    # registra o que existe hoje para a Câmara
    catalog["datasets"] = [
        {
            "id": "federal/camara",
            "descricao": "Rankings e resumos (Deputados Federais) gerados a partir da API da Câmara",
            "mandatoInicioAno": mandate_start_year,
            "pathConsultasRoot": "federal/camara/consultas",
            "pathCategoryMap": "federal/camara/mapping/categoria/category_map.json",
            "pathCategoryMapSource": "mapping/federal/deputados/category_map.json",
            "pathMetodologiaGlobal": "metodologia.json",
            "pathMetodologiaScope": "federal/camara/metodologia_scope.json"
        }
    ]
    write_json(out_dir / "federal/camara/catalog.json", catalog)

    # índice global de datasets (descoberta para front/clientes)
    global_catalog = read_json(out_dir / "catalog.json") or {"meta": {}, "datasets": []}
    global_catalog["meta"] = {"geradoEm": now_iso(), "versaoSchema": "1.0.0", "tipo": "global_catalog"}

    ds_id = "federal/camara"
    ds_entry = {
        "id": ds_id,
        "escopo": "federal",
        "poder": "camara",
        "descricao": "Rankings e resumos (Deputados Federais) gerados a partir da API da Câmara",
        "pathCatalog": "federal/camara/catalog.json",
        "pathBaseData": "federal/camara",
        "pathConsultasRoot": "federal/camara/consultas",
        "pathCategoryMap": "federal/camara/mapping/categoria/category_map.json",
        "pathCategoryMapSource": "mapping/federal/deputados/category_map.json",
        "pathMetodologiaGlobal": "metodologia.json",
        "pathMetodologiaScope": "federal/camara/metodologia_scope.json",
        "mandatoInicioAno": mandate_start_year
    }
    existentes = [x for x in (global_catalog.get("datasets") or []) if (x or {}).get("id") != ds_id]
    existentes.append(ds_entry)
    global_catalog["datasets"] = existentes
    write_json(out_dir / "catalog.json", global_catalog)

    print("OK: geração concluída.")

if __name__ == "__main__":
    main()

