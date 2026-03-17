# ETL Camara Architecture

## Goal

Keep the published JSON contracts stable while letting the Camara ETL evolve with a single canonical model.

Global root artifacts such as `catalog.json`, `home-insights-index.json` and `home-insights-feed.json` are not produced by the Camara scope ETL. They belong to the shared layer in `scripts/global_insights`.

## Execution entrypoint

- Official entrypoint: `scripts/federal/deputados/process.py`
- This file is orchestration only.
- Official execution mode: `PYTHONPATH=scripts python -m federal.deputados.process`

## Module boundaries

- `scripts/federal/deputados/camara_common.py`
  - schema versions
  - JSON I/O
  - dates/env helpers
  - methodology builders
  - category-map loading

- `scripts/federal/deputados/camara_source.py`
  - Camara API pagination and fetch

- `scripts/federal/deputados/camara_domain.py`
  - canonical internal model
  - monthly aggregation
  - period rollups
  - daily summary selection
  - deputy summaries

- `scripts/federal/deputados/camara_contracts.py`
  - overview/entities/ranking/analytics/profile contracts
  - artifact write paths

- `scripts/federal/deputados/camara_insights.py`
  - home insights context
  - 11 insight builders
  - editorial helpers

- `scripts/federal/deputados/camara_validate.py`
  - contract validation
  - semantic validation
  - artifact governance checks

## Internal canonical model

- Entity identity: `id`, `name`, `stateCode`, `party`, `photoUrl`
- Totals bucket:
  - `amountNet`
  - `amountGross`
  - `amountAdjustments`
  - `recordsCount`
  - `byCategoryNet`
  - `byCategoryGross`
  - `byCategoryAdjustments`
- Daily highlights:
  - `dailyTotals`
  - `dailyTopExpense`

Published contracts must expose only canonical keys such as:

- `dailyTotals`
- `dailyTopExpense`
- `amountNet`
- `name`
- `photoUrl`

## Quality gates

- Unit/contract logic:
  - `scripts/federal/deputados/tests/test_camara_contract_logic.py`
- Integration:
  - `scripts/federal/deputados/tests/test_camara_etl_integration.py`
- Global feed logic:
  - `scripts/global_insights/tests/test_aggregate_insights_logic.py`
- CI workflow:
  - `.github/workflows/validate-etl-camara.yml`

## Rule for next levels

When adding another scope/level:

1. Keep the same module split.
2. Reuse only truly global helpers.
3. Preserve published contract shapes.
4. Add scope-specific contract and integration tests before rollout.
