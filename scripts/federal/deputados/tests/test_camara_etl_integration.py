import os
import shutil
from pathlib import Path

from .. import camara_domain
from .. import camara_validate
from .. import process as etl


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    tmp_dir = Path(__file__).resolve().parents[4] / "tmp-test-camara-etl-integration"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    fake_deputados = [
        {
            "id": 101,
            "nome": "Deputado Um",
            "siglaUf": "SP",
            "siglaPartido": "AAA",
            "urlFoto": "https://example.com/101.jpg",
        },
        {
            "id": 202,
            "nome": "Deputado Dois",
            "siglaUf": "RJ",
            "siglaPartido": "BBB",
            "urlFoto": "https://example.com/202.jpg",
        },
    ]
    fake_despesas = {
        101: [
            {
                "valorLiquido": "30.00",
                "tipoDespesa": "COMBUSTÍVEIS E LUBRIFICANTES",
                "dataDocumento": "2026-03-10",
                "nomeFornecedor": "POSTO A",
                "tipoDocumento": "Nota Fiscal",
                "urlDocumento": "https://example.com/doc-101-1",
            },
            {
                "valorLiquido": "120.00",
                "tipoDespesa": "COMBUSTÍVEIS E LUBRIFICANTES",
                "dataDocumento": "2026-03-11",
                "nomeFornecedor": "POSTO B",
                "tipoDocumento": "Nota Fiscal",
                "urlDocumento": "https://example.com/doc-101-2",
            },
        ],
        202: [
            {
                "valorLiquido": "50.00",
                "tipoDespesa": "HOSPEDAGEM ,EXCETO DO PARLAMENTAR NO DISTRITO FEDERAL.",
                "dataDocumento": "2026-03-11",
                "nomeFornecedor": "HOTEL C",
                "tipoDocumento": "Recibo",
                "urlDocumento": "https://example.com/doc-202-1",
            }
        ],
    }

    original_fetch_deputados = etl.fetch_deputados
    original_fetch_despesas = camara_domain.fetch_despesas
    original_env = {key: os.environ.get(key) for key in ["OUT_DIR", "YEAR", "MONTHS", "MODE", "MANDATE_START_YEAR"]}

    etl.fetch_deputados = lambda: fake_deputados
    camara_domain.fetch_despesas = lambda dep_id, ano, mes: list(fake_despesas.get(int(dep_id), []))

    os.environ["OUT_DIR"] = str(tmp_dir)
    os.environ["YEAR"] = "2026"
    os.environ["MONTHS"] = "3"
    os.environ["MANDATE_START_YEAR"] = "2023"
    os.environ["MODE"] = "single"

    try:
        etl.main()
        camara_validate.validate_contract_outputs(tmp_dir)

        overview = etl.read_json(tmp_dir / "federal/camara/overview/month-2026-03/overview.json") or {}
        highlights = overview.get("highlights") or {}
        expect("dailyTotals" in highlights, "overview missing dailyTotals")
        expect("dailyTopExpense" in highlights, "overview missing dailyTopExpense")
        expect(
            set(highlights.keys()) == {
                "topSpender",
                "topCategories",
                "topStates",
                "concentrationTop10",
                "averages",
                "dailyTotals",
                "dailyTopExpense",
            },
            "overview highlights keys mismatch",
        )
        expect(float((highlights.get("dailyTotals") or {}).get("amountNet") or 0.0) == 170.0, "dailyTotals amountNet mismatch")
        expect(float((highlights.get("dailyTopExpense") or {}).get("amountNet") or 0.0) == 120.0, "dailyTopExpense amountNet mismatch")

        home_insights = etl.read_json(tmp_dir / "federal/camara/insights/home-insights.json") or {}
        items = home_insights.get("items") or []
        by_id = {str((item or {}).get("id") or ""): item for item in items}
        expect(len(items) == 11, "home-insights must keep the official 11-item set")
        expect(str((by_id.get("federal-deputies-day-total-amount") or {}).get("type") or "") == "aggregate", "day-total-amount type mismatch")
        expect(str((by_id.get("federal-deputies-day-top-spender") or {}).get("type") or "") == "person", "day-top-spender type mismatch")
        expect(str((by_id.get("federal-deputies-rolling7d-trend") or {}).get("type") or "") == "timeline", "rolling7d type mismatch")
    finally:
        etl.fetch_deputados = original_fetch_deputados
        camara_domain.fetch_despesas = original_fetch_despesas
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)

    print("OK: camara ETL integration")


if __name__ == "__main__":
    main()
