# 190 Group Analytics Dashboard

End-to-end analytics pipeline + lightweight CEO dashboard for Bonsai Outlet / 190 Group:

- Ingest Amazon SP-API datasets (Economics, Business Reports, Settlements) into BigQuery
- Serve a local dashboard via Flask (`api/`) + static frontend (`dashboard/`)

See `dashboard_plan.md` for the current architecture decisions and near-term backlog.

## Repo layout

- `directives/` — SOPs (what to do)
- `execution/` — deterministic scripts (do the work)
- `api/` — Flask API (BigQuery → JSON endpoints)
- `dashboard/` — static UI (HTML/CSS/JS)
- `.tmp/` — intermediate files (safe to delete/regenerate)

## Setup

1. Create `.env` from `.env.example` and fill values.
2. Create/activate a Python venv and install deps:
   - `pip install -r requirements.txt`
3. Ensure Google credentials are available for BigQuery/GCS:
   - Set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON, or use Application Default Credentials.
4. Validate core SP-API env vars:
   - `python execution/validate_env.py`

## Run the dashboard

- `python run_dashboard.py`
  - Dashboard: `http://localhost:5000`
  - API: `http://localhost:5000/api/dashboard`

## Pipelines (SOP → script)

- Economics (SP-API Data Kiosk): `directives/amazon_sp_api_economics_pipeline.md`
  - Run: `python execution/ingest_amazon_economics.py --backfill-days 7`
  - Writes to: `${GOOGLE_CLOUD_PROJECT}.${BIGQUERY_DATASET}` (`fact_sku_day_us`, `etl_runs`)
- Business Reports (Sales & Traffic): `directives/amazon_sp_api_reports_pipeline.md`
  - Run: `python execution/ingest_amazon_business_reports.py --days 7`
  - Writes to: `${GOOGLE_CLOUD_PROJECT}.${BIGQUERY_DATASET}` (`fact_business_reports_us`)
- Settlements: `directives/amazon_sp_api_settlements_pipeline.md`
  - Run: `python execution/ingest_amazon_settlements.py --limit 5 --start_date 2026-01-01`
  - Writes to: `${GOOGLE_CLOUD_PROJECT}.${BIGQUERY_DATASET}` (`fact_settlements_us`)

## Business logic

- `directives/business_rules.md` contains category and SKU mappings used for consistent reporting.

## Notes

- Secrets and credentials are gitignored (`.env`, `service-account.json`, `credentials.json`, `token.json`).
- Dataset envs: ingestion uses `BIGQUERY_DATASET`; the API uses `AMAZON_ECON_DATASET` or falls back to `BIGQUERY_DATASET`.
- `AGENTS.md`, `CLAUDE.md`, and `GEMINI.md` are intentionally mirrored; keep them identical when editing.
