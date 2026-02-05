# Dashboard Plan (Executive Overview)

This document captures product/architecture decisions for the 190 Group “Executive Overview” dashboard and links to the authoritative SOPs in `directives/`.

## Current implementation (this repo)

- Ingestion: deterministic scripts in `execution/` land raw files in GCS and load BigQuery fact tables.
- API: Flask app in `api/dashboard_data.py` queries BigQuery and serves JSON + static assets.
- UI: static dashboard in `dashboard/` (Chart.js + custom UI).
- Runner: `run_dashboard.py` starts the API and opens the dashboard in your browser.

## Key decisions

| Choice | Decision | Why | Tradeoff |
| --- | --- | --- | --- |
| Marketplace | US only | Limits complexity and stabilizes faster | Add marketplaces later |
| Grain | SKU-day facts | Works for margin trends, promo days, ad spikes, fee shifts | Requires rolling backfills |
| Warehouse | BigQuery partitioned by date + clustered by SKU | Cheap + fast for time-series by SKU | BI must enforce date filters |
| Build strategy | Economics stable first | Avoids fragile multi-source models early | Requires ongoing QA |

Details live in the directives:

- `directives/amazon_sp_api_economics_pipeline.md`
- `directives/amazon_sp_api_reports_pipeline.md`
- `directives/amazon_sp_api_settlements_pipeline.md`

## Data model (BigQuery)

Defaults:

- Ingestion scripts use `BIGQUERY_DATASET` (default `amazon_econ`)
- The dashboard API uses `AMAZON_ECON_DATASET` or falls back to `BIGQUERY_DATASET` (default `amazon_econ`)

Primary tables:

- `fact_sku_day_us` (economics)
- `fact_business_reports_us` (traffic + conversion)
- `fact_settlements_us` (financial truth)
- `etl_runs` (ops logging)

## Run order (local)

1. Configure `.env` from `.env.example` and ensure GCP credentials are available.
2. Run ingests:
   - `python execution/ingest_amazon_economics.py --backfill-days 7`
   - `python execution/ingest_amazon_business_reports.py --days 7`
   - `python execution/ingest_amazon_settlements.py --limit 1`
3. Start the dashboard:
   - `python run_dashboard.py`

## Quality checks

- Follow the “Economics Stable” checks in `directives/amazon_sp_api_economics_pipeline.md`.
- Smoke tests against the running API:
  - `python execution/verify_net_proceeds.py`
  - `python execution/verify_yoy.py`

## Backlog (high-value improvements)

- Add directives for existing scripts without SOPs (GA4 backfill, wholesale invoice ingestion).
- Add a single orchestrator command to run daily pipelines in order (e.g., `python execution/run_daily.py`).
