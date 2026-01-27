# Directive: Amazon SP-API Settlement Reports Pipeline

## Goal
Ingest Amazon Settlement Reports (`GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE`) to capture the "Financial Truth" of sales, fees, and payouts. This data is used to calculate net proceeds and reconcile with bank deposits.

## Source Configuration
- **API**: Amazon Selling Partner API (SP-API) - Reports API
- **Report Type**: `GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE`
- **Format**: Tab-separated flat file
- **Marketplace**: US Only

## Key Fields
Settlement reports contain many columns. We will prioritize:
- `settlement-id`
- `settlement-start-date`
- `settlement-end-date`
- `transaction-type`
- `order-id`
- `merchant-order-id`
- `adjustment-id`
- `sku`
- `amount-type` (e.g., ItemPrice, ItemFees, Promotion)
- `amount-description` (e.g., Principal, ReferralFee, FBAPerUnitFulfillmentFee)
- `amount`
- `currency`

## Architecture Overview
1.  **Discovery**: Polling for the most recent settlement reports.
2.  **Download**: Fetch the document and parse the TSV content.
3.  **Bronze Layer**: Store raw TSV in GCS.
4.  **Silver Layer**: Load into BigQuery `fact_settlements_us`.

## Silver Layer (BigQuery Fact Table)
**Table**: `fact_settlements_us`
**Partitioning**: By `settlement_start_date` (TIMESTAMP)
**Clustering**: By `sku`, `order_id`

## Data Processing Rules
- **Attribution**: Fees and sales are attributed to the `settlement_end_date` for reporting purposes, or backdated to the order date if available.
- **Deduplication**: Use `settlement_id` and unique transaction identifiers to avoid double-counting.
