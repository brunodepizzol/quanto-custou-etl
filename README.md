# quanto-custou-etl

## Objective

Generate and publish the JSON files consumed by the site in `data/`, with a stable contract.

## Workflows

- `Build ETL <Agency>`: generates and publishes data for one agency.
- `Aggregate Insights Feed`: consolidates global insights.
- `Validate Category Mappings`: validates mappings.

## Current flow

1. `Build ETL Camara` publishes `data/federal/camara/...`.
2. `Aggregate Insights Feed` updates:
   - `data/catalog.json`
   - `data/home-insights-index.json`
   - `data/home-insights-feed.json`
3. The aggregator runs on:
   - `workflow_run` from build
   - `push` on `data/**/insights/home-insights.json`

## Global feed rollback

When the global feed is invalid in production:

1. Revert the commit that changed:
   - `data/catalog.json`
   - `data/home-insights-index.json`
   - `data/home-insights-feed.json`
2. Run `Aggregate Insights Feed` manually.
3. Confirm in logs:
   - `Validate aggregated feed` passed
   - counters (`datasets`, `candidate insights`, `feed insights`) were emitted
4. Confirm a new commit with the 3 files above.

## Add a new level

Standard process to include a new level (for example: state, municipal):

1. Define output destination: `data/<level>/<agency>/...`.
2. Generate at minimum:
   - `insights/home-insights.json`
   - `overview/...`
   - `rankings/...`
   - `entities/...`
3. Ensure insights contract:
   - `id`
   - `generatedAt`
   - `freshUntil`
4. Create `Build ETL <Agency>` workflow following current standard (concurrency, timeout, commit only when `data` changes).
5. Validate locally:
   - `MODE=validate-mapping`
   - `MODE=validate`
6. Run in Actions and confirm updates of the 3 global files.

## schemaVersion policy

1. `patch`: non-breaking metadata/document changes.
2. `minor`: backward-compatible optional field addition.
3. `major`: breaking change.
4. Aggregator only accepts `schemaVersion` matching `HOME_INSIGHTS_SCHEMA_VERSION`.
5. For `major`, migrate consumer(s) before production rollout.
