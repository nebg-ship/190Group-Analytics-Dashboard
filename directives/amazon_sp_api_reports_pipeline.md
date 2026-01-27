# Directive: Amazon SP-API Business Reports Pipeline

## Goal
Ingest Amazon Sales and Traffic reports (Business Reports) to capture SKU-level traffic, conversion, and buy-box metrics. This complements the economics feed by providing the "Top of Funnel" metrics.

## Source Configuration
- **API**: Amazon Selling Partner API (SP-API) - Reports API
- **Report Type**: `GET_SALES_AND_TRAFFIC_REPORT`
- **Granularity**: SKU-Day
- **Marketplace**: US Only

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
