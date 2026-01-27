# Directive: Amazon SP-API Economics Data Kiosk Pipeline

## Goal
Ingest and analyze Amazon Seller Economics data using the SP-API Data Kiosk to provide granular financial insights. The pipeline ensures data stability through a rolling backfill and strict quality checks, loading data into BigQuery for efficient analysis.

## Source Configuration
- **API**: Amazon Selling Partner API (SP-API) - Data Kiosk
- **Schema/Root Type**: `Analytics_Economics_2024_03_15`
- **Granularity**: SKU-Day
- **Marketplace**: US Only

## Architecture Overview
The pipeline follows a "Bronze -> Silver" pattern to decouple ingestion from analysis and handle schema changes gracefully.

1.  **Ingest (Bronze)**: Fetch raw JSON documents from Data Kiosk and store them immutably in Google Cloud Storage (GCS).
2.  **Load & Transform (Silver)**: Load raw data into BigQuery, transform into a strict schema, and write to a partitioned fact table.
3.  **Audit**: Log every run to an `etl_runs` table for observability.

## 1. Bronze Layer (GCS Storage)
- **Format**: Raw compressed JSON/JSONL (`.json.gz`).
- **Path Structure**: `amazon/economics/us/run_date=YYYY-MM-DD/source_document_id=<id>/part-000.json.gz`
- **Retention**: Permanent (or long-term cold storage) to allow full replay.

## 2. Silver Layer (BigQuery Fact Table)
**Table**: `fact_sku_day_us`
**Partitioning**: By `business_date` (DATE)
**Clustering**: By `msku`, `asin`

| Field Name | Type | Description |
|---|---|---|
| `business_date` | DATE | **Partition Key**. Transaction/summary date. |
| `marketplace` | STRING | Marketplace ID (e.g., ATVPDKIKX0DER for US). |
| `msku` | STRING | **Cluster Key 1**. Merchant SKU. |
| `asin` | STRING | **Cluster Key 2**. Amazon Standard ID. |
| `fnsku` | STRING | Fulfillment Network SKU. |
| `units` | INT64 | Quantity sold/moved. |
| `gross_sales` | NUMERIC | Gross sales amount. |
| `refunds` | NUMERIC | Refund amount (negative). |
| `net_sales` | NUMERIC | Gross + Refunds. |
| `amazon_fees` | NUMERIC | Total fees (Referral, FBA, etc.). |
| `ad_spend` | NUMERIC | PPC/Advertising spend attributed to this SKU-day. |
| `net_proceeds` | NUMERIC | Final payout amount. |
| `source_query_id` | STRING | ID of the Data Kiosk query. |
| `source_document_id` | STRING | ID of the downloaded document. |
| `ingested_at` | TIMESTAMP | ETL execution timestamp. |

## 3. Orchestration & Backfill Strategy
**Trigger**: Daily (Cloud Scheduler -> Cloud Run)
**Scope**:
1.  **Primary**: Fetch data for `yesterday`.
2.  **Rolling Backfill**: Fetch data for the **last 7 days** in every run.
    *   *Why*: catches late fee adjustments, reversals, and attribution updates common in financial data.
    *   *Mechanism*: `DELETE FROM fact_sku_day_us WHERE business_date BETWEEN <T-7> AND <T-1>` followed by `INSERT`.

## 4. Operational Table: `etl_runs`
Tracks the health and history of every ingestion job.

| Field Name | Type | Description |
|---|---|---|
| `run_date` | DATE | Date the job ran. |
| `status` | STRING | `SUCCESS`, `FAILED`, `WARNING`. |
| `query_id` | STRING | Data Kiosk query ID. |
| `document_id` | STRING | Downloaded document ID. |
| `row_count` | INT64 | Number of rows ingested. |
| `checksum` | STRING | Hash of the source file (optional). |
| `started_at` | TIMESTAMP | |
| `finished_at`| TIMESTAMP | |
| `error` | STRING | Error message if failed. |

## 5. Data Quality Checks (The "Economics Stable" Standard)
Pipeline **must** fail or alert if these conditions are not met.

### Hard Checks (Blocker)
- **Partition Check**: partition for `business_date = yesterday` must exist after run.
- **Volume Check**: Row count must be consistent with SKU count (approx 70 rows/day for 70 SKUs).
- **Zero-Sum Check**: `SUM(units)` and `SUM(net_sales)` > 0 (unless known holiday).
- **Key Integrity**: `msku` must not be NULL for >99% of rows.
- **Uniqueness**: Primary key (`business_date`, `msku`) must be unique.

### Drift Checks (Alerting)
- **Sales Trend**: `SUM(net_sales)` vs 14-day median. Alert on significant deviation.
- **Fee Ratio**: `SUM(amazon_fees) / SUM(net_sales)`. Alert if ratio spikes (indicates miscategoriztion or fee hike).
- **Ad Spend**: Compare `SUM(ad_spend)` to daily budget expectations.
