# Directive: Amazon SP-API Business Reports Pipeline

## Goal
Ingest Amazon Sales and Traffic reports (Business Reports) to capture SKU-level traffic, conversion, and buy-box metrics. This complements the economics feed by providing the "Top of Funnel" metrics.

## Source Configuration
- **API**: Amazon Selling Partner API (SP-API) - Reports API
- **Report Type**: `GET_SALES_AND_TRAFFIC_REPORT`
- **Granularity**: SKU-Day
- **Marketplace**: US Only

## Implementation (Script)
- **Primary script**: `execution/ingest_amazon_business_reports.py`
- **Run (default last 7 days ending yesterday)**: `python execution/ingest_amazon_business_reports.py --days 7`

### Required environment variables
- **Amazon SP-API**: `SP_API_REFRESH_TOKEN`, `SP_API_CLIENT_ID`, `SP_API_CLIENT_SECRET`, `SP_API_AWS_ACCESS_KEY`, `SP_API_AWS_SECRET_KEY`
- **Optional**: `SP_API_REGION` (default `us-east-1`)
- **Google Cloud**: `GOOGLE_CLOUD_PROJECT`, `BIGQUERY_DATASET` (default `amazon_econ`), `GCS_BUCKET`

### Outputs
- **Bronze (GCS)**: `gs://$GCS_BUCKET/amazon/reports/business/us/run_date=YYYY-MM-DD/report_id=<id>/part-000.json.gz`
- **Silver (BigQuery)**: `${GOOGLE_CLOUD_PROJECT}.${BIGQUERY_DATASET}.fact_business_reports_us`

## Schema/Fields
The `GET_SALES_AND_TRAFFIC_REPORT` provides a broad range of metrics. We will focus on:
- `date`
- `asin`
- `sku`
- `sessions`
- `browserSessions`
- `mobileAppSessions`
- `sessionPercentage`
- `pageViews`
- `browserPageViews`
- `mobileAppPageViews`
- `pageViewsPercentage`
- `buyBoxPercentage`
- `unitSessionPercentage` (Conversion Rate)
- `orderedProductSales`
- `unitsOrdered`
- `totalOrderItems`

## Architecture Overview
1.  **Request**: Submit a report request for a specific date range (default T-1 to T-7).
2.  **Poll**: Check report status until `DONE`.
3.  **Download**: Fetch the document, decompress (usually GZIP if specified or ZIP), and parse JSON content.
4.  **Bronze Layer**: Store compressed JSON in GCS: `amazon/reports/business/us/run_date=YYYY-MM-DD/report_id=<id>/part-000.json.gz`.
5.  **Silver Layer**: Load into BigQuery `fact_business_reports_us`.

## Silver Layer (BigQuery Fact Table)
**Table**: `fact_business_reports_us`
**Partitioning**: By `report_date` (DATE)
**Clustering**: By `msku`, `asin`

| Field Name | Type | Description |
|---|---|---|
| `report_date` | DATE | **Partition Key**. |
| `msku` | STRING | **Cluster Key 1**. |
| `asin` | STRING | **Cluster Key 2**. |
| `sessions` | INT64 | Total sessions. |
| `page_views` | INT64 | Total page views. |
| `buy_box_percentage` | FLOAT64 | |
| `unit_session_percentage` | FLOAT64 | Conversion rate. |
| `units_ordered` | INT64 | |
| `ordered_product_sales` | NUMERIC | |
| `ingested_at` | TIMESTAMP | |

## Orchestration
- **Frequency**: Daily.
- **Backfill**: 7 days rolling window to ensure data stability (Amazon reports can take 24-48h to fully stabilize).

## Smoke test
- Start the API/dashboard: `python run_dashboard.py`
- Verify YoY matching logic against the API output: `python execution/verify_yoy.py`
