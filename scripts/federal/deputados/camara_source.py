import csv
import io
import time
import zipfile
from typing import Any, Dict, List, Optional, Tuple, cast

import requests

from .camara_common import CAMARA_BASE, JsonDict


CAMARA_CEAP_URL = "https://www.camara.leg.br/cotas/Ano-{year}.csv.zip"
_annual_expenses_cache: Dict[int, Dict[Tuple[int, int], List[JsonDict]]] = {}


def http_get_json(url: str, params: Optional[dict[str, Any]] = None, retries: int = 8, timeout_s: int = 120) -> JsonDict:
    headers = {"Accept": "application/json"}
    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=timeout_s)
            if response.status_code in (429, 500, 502, 503, 504):
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    time.sleep(min(int(retry_after), 30))
                else:
                    time.sleep(min(2 ** attempt, 30))
                last_err = requests.exceptions.HTTPError(f"{response.status_code} for {response.url}")
                continue
            response.raise_for_status()
            return cast(JsonDict, response.json())
        except requests.exceptions.RequestException as exc:
            last_err = exc
            time.sleep(min(2 ** attempt, 30))
        except Exception as exc:
            last_err = exc
            time.sleep(min(2 ** attempt, 30))
    if last_err is None:
        raise RuntimeError(f"Failed to fetch {url}")
    raise RuntimeError(str(last_err)) from last_err


def paginate(endpoint: str, params: dict[str, Any]) -> List[JsonDict]:
    itens = int(params.get("itens", 100))
    pagina = 1
    out: List[JsonDict] = []
    while True:
        paged_params = dict(params)
        paged_params["itens"] = itens
        paged_params["pagina"] = pagina
        try:
            data = http_get_json(endpoint, params=paged_params)
        except Exception as exc:
            message = str(exc)
            if ("504" in message or "Gateway Timeout" in message) and itens > 25:
                itens = 50 if itens > 50 else 25
                print(f"WARN: 504 response, reducing page size to {itens} and retrying (page={pagina}).")
                continue
            raise
        dados = data.get("dados", [])
        out.extend(dados)
        links = data.get("links", [])
        has_next = any(link.get("rel") == "next" for link in links)
        if not has_next or not dados:
            break
        pagina += 1
    return out


def fetch_deputados() -> List[JsonDict]:
    return paginate(
        f"{CAMARA_BASE}/deputados",
        {"ordem": "ASC", "ordenarPor": "nome", "itens": 100},
    )


def _parse_annual_expenses(csv_bytes: bytes, expected_year: int) -> Dict[Tuple[int, int], List[JsonDict]]:
    text = csv_bytes.decode("utf-8-sig")
    grouped: Dict[Tuple[int, int], List[JsonDict]] = {}
    parsed_rows = 0
    for row in csv.DictReader(io.StringIO(text), delimiter=";"):
        dep_id_raw = str(row.get("ideCadastro") or "").strip()
        year_raw = str(row.get("numAno") or "").strip()
        month_raw = str(row.get("numMes") or "").strip()
        if not dep_id_raw or not year_raw or not month_raw:
            continue
        try:
            dep_id = int(dep_id_raw)
            year = int(year_raw)
            month = int(month_raw)
        except ValueError:
            continue
        if year != expected_year or not 1 <= month <= 12:
            continue
        expense: JsonDict = {
            "ano": year,
            "mes": month,
            "tipoDespesa": str(row.get("txtDescricao") or "").strip(),
            "dataDocumento": str(row.get("datEmissao") or "").strip(),
            "nomeFornecedor": str(row.get("txtFornecedor") or "").strip(),
            "cnpjCpfFornecedor": str(row.get("txtCNPJCPF") or "").strip(),
            "tipoDocumento": str(row.get("indTipoDocumento") or "").strip(),
            "numeroDocumento": str(row.get("txtNumero") or "").strip(),
            "valorDocumento": str(row.get("vlrDocumento") or "").strip(),
            "valorGlosa": str(row.get("vlrGlosa") or "").strip(),
            "valorLiquido": str(row.get("vlrLiquido") or "").strip(),
            "urlDocumento": str(row.get("urlDocumento") or "").strip(),
        }
        grouped.setdefault((dep_id, month), []).append(expense)
        parsed_rows += 1
    if parsed_rows == 0:
        raise RuntimeError(f"Camara CEAP annual file for {expected_year} contained no deputy expenses")
    return grouped


def _fetch_annual_expenses(year: int) -> Dict[Tuple[int, int], List[JsonDict]]:
    cached = _annual_expenses_cache.get(year)
    if cached is not None:
        return cached

    url = CAMARA_CEAP_URL.format(year=year)
    response = requests.get(url, headers={"Accept": "application/zip"}, timeout=120)
    response.raise_for_status()
    try:
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if len(csv_names) != 1:
                raise RuntimeError(f"Expected one CSV in {url}, found {len(csv_names)}")
            grouped = _parse_annual_expenses(archive.read(csv_names[0]), year)
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"Invalid ZIP returned by {url}") from exc

    _annual_expenses_cache[year] = grouped
    total = sum(len(items) for items in grouped.values())
    print(f"OK: loaded Camara CEAP annual file for {year} ({total} expenses).")
    return grouped


def fetch_despesas(dep_id: int, ano: int, mes: int) -> List[JsonDict]:
    return list(_fetch_annual_expenses(ano).get((dep_id, mes), []))
