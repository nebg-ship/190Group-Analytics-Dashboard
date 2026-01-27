
| Choice | What we’ll do | Why it’s the right move for 70 SKUs | Key tradeoff |
| ----- | ----- | ----- | ----- |
| Marketplace | **US only** | Keeps the surface area tiny and stabilizes faster | You will add marketplaces later |
| Grain | **SKU-day** fact table | Perfect for margin trending, promo days, ad spikes, fee shifts | Daily backfills still needed for late adjustments |
| Source | **SP-API Data Kiosk Seller Economics** | Amazon’s intended bulk interface for economics style reporting ([Amazon Selling Partner API](https://developer.amazonservices.com/datakiosk?utm_source=chatgpt.com)) | It’s job-based, not instant |
| Warehouse | **BigQuery** partitioned by date, clustered by SKU | Cheapest, fastest pattern for “time-series by SKU” ([Google Cloud Documentation](https://docs.cloud.google.com/bigquery/docs/partitioned-tables?utm_source=chatgpt.com)) | You must enforce date filters in BI |
| Build strategy | **Economics stable first** | Avoids building a fragile multi-source model too early | You will still need QA to trust the numbers |

You’re in an ideal zone here: 70 active SKUs means you can keep the pipeline simple, cheap, and very reliable. My strong recommendation is a daily batch ingest into BigQuery with a small rolling backfill window, plus aggressive data quality checks.

## **1\) Minimal architecture that stays stable**

### **Flow**

1. **Create Data Kiosk query** (economics schema)  
2. **Poll until complete**  
3. **Download the Data Kiosk document** via `getDocument` (this is literally what it’s for) ([developer-docs.amazon.com](https://developer-docs.amazon.com/sp-api/lang-it_IT/reference/getdocument?utm_source=chatgpt.com))  
4. Land raw output in **GCS** (bronze)  
5. Load into **BigQuery** (silver fact table)  
6. Build a couple of thin “gold” views for dashboarding

Why this works: Data Kiosk is designed around generating documents from GraphQL queries, and the API gives you document retrieval primitives plus strict rate limits, so batch is the natural fit. ([Amazon Selling Partner API](https://developer.amazonservices.com/datakiosk?utm_source=chatgpt.com))

### **Rate limit reality**

`getDocument` is extremely low rate by default (tiny RPS, bursty). That is fine because you are doing 1 run per day plus a small backfill. ([developer-docs.amazon.com](https://developer-docs.amazon.com/sp-api/lang-it_IT/reference/getdocument?utm_source=chatgpt.com))

## **2\) BigQuery table design for SKU-day**

You want to keep BigQuery scans tiny and predictable.

### **Partitioning and clustering**

* Partition by `business_date`  
* Cluster by `msku` (and optionally `asin`)

This is exactly how you keep cost down and speed up queries: partition pruning via `WHERE business_date BETWEEN ...` and block skipping via clustering. ([Google Cloud Documentation](https://docs.cloud.google.com/bigquery/docs/partitioned-tables?utm_source=chatgpt.com))

### **Recommended tables**

**A) bronze storage (GCS)**

* Keep raw documents, compressed, immutable.  
* Path convention that makes backfills easy:  
  * `amazon/economics/us/run_date=YYYY-MM-DD/source_document_id=<id>/part-000.json.gz`

**B) silver BigQuery fact table: `fact_sku_day_us`**  
Columns I’d actually store (keep it tight):

* Keys: `business_date`, `msku`, `asin`, `fnsku` (if present), `marketplace`  
* Measures: `units`, `gross_sales`, `net_sales`, `refunds`, `amazon_fees`, `ad_spend`, `net_proceeds`  
* Audit: `source_query_id`, `source_document_id`, `ingested_at`

BigQuery DDL sketch:

CREATE TABLE IF NOT EXISTS \`your\_project.amazon\_econ.fact\_sku\_day\_us\` (  
  business\_date DATE NOT NULL,  
  marketplace STRING NOT NULL,  
  msku STRING,  
  asin STRING,  
  fnsku STRING,

  units INT64,  
  gross\_sales NUMERIC,  
  refunds NUMERIC,  
  net\_sales NUMERIC,  
  amazon\_fees NUMERIC,  
  ad\_spend NUMERIC,  
  net\_proceeds NUMERIC,

  source\_query\_id STRING,  
  source\_document\_id STRING,  
  ingested\_at TIMESTAMP NOT NULL  
)  
PARTITION BY business\_date  
CLUSTER BY msku, asin;

**C) run log table: `etl_runs`**  
You want idempotency and observability:

* `run_date`, `status`, `query_id`, `document_id`, `row_count`, `checksum`, `started_at`, `finished_at`, `error`

## **3\) Data Kiosk query strategy (US, all fulfillment types)**

You said “all Amazon-fulfilled types.” In practice, that means:

* You do not pre-filter by fulfillment channel at ingest time  
* You ingest everything economics provides, then optionally add breakdown fields if the schema supports it

### **Daily schedule with backfill**

* Every day, pull **yesterday**.  
* Also re-pull the **last 7 days** (rolling backfill) to catch late fee adjustments, reversals, or attribution updates.

For 70 SKUs, this is trivial volume and buys you stability.

### **Why I’m pushing a rolling backfill**

Economics style datasets can get revised. Also, there have been reports of schema quirks or unexpected results in the economics dataset from time to time, so your pipeline should assume revisions and anomalies and self-heal by design. ([GitHub](https://github.com/amzn/selling-partner-api-models/issues/4928?utm_source=chatgpt.com))

## **4\) Loading into BigQuery cleanly**

### **File format**

Aim for **newline-delimited JSON (NDJSON)** or a flat JSON array that you convert to NDJSON.

BigQuery loads NDJSON from Cloud Storage cleanly, and it is the common path for bulk JSON ingest. ([Google Cloud](https://cloud.google.com/blog/products/data-analytics/load-your-json-data-into-bigquery-despite-wacky-formatting/?utm_source=chatgpt.com))

### **Load pattern**

* Stage into `stg_econ_raw` with relaxed schema  
* Transform into `fact_sku_day_us` with explicit casts and defaults

This isolates schema drift. If Amazon adds fields, bronze and staging survive, your fact table stays stable.

## **5\) Data quality checks that make “economics stable”**

If you do only one thing beyond ingestion, do this.

### **Hard checks**

* Partition exists for `business_date = yesterday`  
* Row count is within expected band (for 70 SKUs, expect roughly 70 rows per day, plus or minus)  
* `SUM(units) > 0` and `SUM(net_sales) > 0` for non-holiday closures  
* `msku` not null for most rows  
* Duplicate key check: no more than 1 row per `business_date + msku` in your final fact

### **Drift checks (very high value)**

* Compare `SUM(net_sales)` yesterday vs trailing 14-day median, alert if outside threshold  
* Compare `SUM(amazon_fees / net_sales)` ratio, alert on spikes  
* Compare `SUM(ad_spend)` vs your expectation based on campaign state

### **“Trust but verify” check**

Once a week, reconcile high-level totals against another Amazon money source you already trust. You are not building that multi-source model yet, but a weekly top-line check prevents silent failure.

## **6\) Orchestration that is simple and high quality**

Because the API is job-based and rate-limited, avoid complex streaming.

I’d pick:

* **Cloud Run** job that executes the daily pull and backfill  
* **Cloud Scheduler** to trigger it  
* Store secrets in Secret Manager  
* Use a dedicated service account with least privilege

This is a clean, production-grade setup without overbuilding.

## **7\) Pros and cons of this plan**

### **Pros**

* Extremely stable for your scale (70 SKUs)  
* Cheap in BigQuery because partition pruning dominates cost control ([Google Cloud Documentation](https://docs.cloud.google.com/bigquery/docs/partitioned-tables?utm_source=chatgpt.com))  
* Easy to extend later into ads, finance events, QuickBooks once economics is trustworthy  
* Built-in self-healing via rolling backfill and idempotent loads

### **Cons**

* Data Kiosk is more operational than classic report downloads (submit, poll, download) ([developer-docs.amazon.com](https://developer-docs.amazon.com/sp-api/lang-it_IT/reference/getdocument?utm_source=chatgpt.com))  
* Permissions can bite you with 403 if the right role is not attached, and you may need to chase it down ([GitHub](https://github.com/amzn/selling-partner-api-models/issues/567?utm_source=chatgpt.com))  
* Economics data can have occasional anomalies or disputes in specific fee areas, so you must keep QA on ([GitHub](https://github.com/amzn/selling-partner-api-models/issues/4928?utm_source=chatgpt.com))

## **8\) What I would do next, in order**

1. Confirm SP-API app auth works for Data Kiosk economics by running a tiny 1-day query  
2. Stand up GCS bucket \+ BigQuery dataset \+ `etl_runs`  
3. Implement daily job: yesterday \+ last 7 days  
4. Turn on QA checks and alerts  
5. After 2 weeks of stable runs, build the dashboard layer

---

* **Q1:** Do you want the “source of truth SKU key” to be Amazon MSKU, ASIN, or do you already have an internal SKU map you want to standardize on?  
* **Q2:** What is your preferred dashboard tool on top of BigQuery (Looker Studio, Looker, Power BI, Tableau)?  
* **Q3:** Do you want the backfill window to be 7 days or 30 days, given your tolerance for revisions versus extra API calls?  
* **Q4:** Which date do you want to anchor on in SKU-day: order date, ship date, or settlement-related date, assuming the economics dataset offers options?  
* **Q5:** Do you want me to propose a concrete BigQuery “gold” view for contribution margin even before you join in external COGS, using net proceeds as the proxy?
