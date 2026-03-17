import time
from typing import Any, List, Optional, cast

import requests

from .camara_common import CAMARA_BASE, JsonDict


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


def fetch_despesas(dep_id: int, ano: int, mes: int) -> List[JsonDict]:
    return paginate(
        f"{CAMARA_BASE}/deputados/{dep_id}/despesas",
        {"ano": ano, "mes": mes, "itens": 100},
    )
