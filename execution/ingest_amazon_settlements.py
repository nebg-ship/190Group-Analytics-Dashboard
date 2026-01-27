import os
import time
import csv
import gzip
import logging
import argparse
from datetime import datetime, timezone
from io import StringIO, BytesIO

import requests
from requests_aws4auth import AWS4Auth
from dotenv import load_dotenv
from google.cloud import storage
from google.cloud import bigquery

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()

# Configuration
SP_API_REFRESH_TOKEN = os.getenv("SP_API_REFRESH_TOKEN")
SP_API_CLIENT_ID = os.getenv("SP_API_CLIENT_ID")
SP_API_CLIENT_SECRET = os.getenv("SP_API_CLIENT_SECRET")
SP_API_AWS_ACCESS_KEY = os.getenv("SP_API_AWS_ACCESS_KEY")
SP_API_AWS_SECRET_KEY = os.getenv("SP_API_AWS_SECRET_KEY")
SP_API_REGION = os.getenv("SP_API_REGION", "us-east-1")

GCP_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET = os.getenv("BIGQUERY_DATASET", "amazon_econ")
GCS_BUCKET = os.getenv("GCS_BUCKET")

# Constants
SP_API_ENDPOINT = "https://sellingpartnerapi-na.amazon.com"
REPORTS_PATH = "/reports/2021-06-30/reports"
DOCUMENTS_PATH = "/reports/2021-06-30/documents"

MARKETPLACE_ID_US = "ATVPDKIKX0DER"

def get_lwa_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": SP_API_REFRESH_TOKEN,
        "client_id": SP_API_CLIENT_ID,
        "client_secret": SP_API_CLIENT_SECRET
    }
    resp = requests.post(url, data=data)
    if resp.status_code != 200:
        logger.error(f"LWA Token Exchange Failed: {resp.text}")
        resp.raise_for_status()
    return resp.json()["access_token"]

def get_auth_headers(access_token):
    return {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }

def get_aws_auth(service="execute-api"):
    return AWS4Auth(
        SP_API_AWS_ACCESS_KEY,
        SP_API_AWS_SECRET_KEY,
        SP_API_REGION,
        service
    )

def get_settlement_reports(limit=5, start_date=None):
    """List most recent settlement reports."""
    access_token = get_lwa_access_token()
    auth = get_aws_auth()
    
    params = {
        "reportTypes": ["GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE", "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"],
        "processingStatuses": ["DONE"],
        "pageSize": limit
    }
    
    # Note: createdSince is limited to 90 days by SP-API. We filter client-side.

    resp = requests.get(
        f"{SP_API_ENDPOINT}{REPORTS_PATH}",
        params=params,
        auth=auth,
        headers=get_auth_headers(access_token)
    )
    
    if resp.status_code != 200:
        logger.error(f"Failed to fetch reports list: {resp.text}")
        resp.raise_for_status()
        
    return resp.json().get("reports", [])

# ... (rest of file) ...


def download_document(document_id):
    logger.info(f"Retrieving document url for {document_id}")
    access_token = get_lwa_access_token()
    auth = get_aws_auth()
    
    resp = requests.get(
        f"{SP_API_ENDPOINT}{DOCUMENTS_PATH}/{document_id}",
        auth=auth,
        headers=get_auth_headers(access_token)
    )
    resp.raise_for_status()
    doc_info = resp.json()
    download_url = doc_info["url"]
    compression = doc_info.get("compressionAlgorithm")
    
    logger.info(f"Downloading content ({compression})...")
    content_resp = requests.get(download_url)
    content_resp.raise_for_status()
    
    content = content_resp.content
    if compression == "GZIP":
        content = gzip.decompress(content)
        
    return content

def upload_to_gcs(content_bytes, settlement_id):
    client = storage.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCS_BUCKET)
    blob_path = f"amazon/reports/settlements/us/settlement_id={settlement_id}/part-000.tsv.gz"
    blob = bucket.blob(blob_path)
    
    compressed_content = gzip.compress(content_bytes)
    blob.upload_from_string(compressed_content, content_type="text/tab-separated-values")
    
    logger.info(f"Uploaded to gs://{GCS_BUCKET}/{blob_path}")
    return f"gs://{GCS_BUCKET}/{blob_path}"

def get_bigquery_client():
    return bigquery.Client(project=GCP_PROJECT)

def transform_and_load_bq(content_bytes, settlement_id):
    client = get_bigquery_client()
    fact_table_id = f"{GCP_PROJECT}.{BQ_DATASET}.fact_settlements_us"
    
    schema = [
        bigquery.SchemaField("settlement_id", "STRING"),
        bigquery.SchemaField("settlement_start_date", "TIMESTAMP"),
        bigquery.SchemaField("settlement_end_date", "TIMESTAMP"),
        bigquery.SchemaField("deposit_date", "TIMESTAMP"),
        bigquery.SchemaField("total_amount", "FLOAT64"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("transaction_type", "STRING"),
        bigquery.SchemaField("order_id", "STRING"),
        bigquery.SchemaField("merchant_order_id", "STRING"),
        bigquery.SchemaField("adjustment_id", "STRING"),
        bigquery.SchemaField("shipment_id", "STRING"),
        bigquery.SchemaField("marketplace_name", "STRING"),
        bigquery.SchemaField("amount_type", "STRING"),
        bigquery.SchemaField("amount_description", "STRING"),
        bigquery.SchemaField("amount", "FLOAT64"),
        bigquery.SchemaField("fulfillment_id", "STRING"),
        bigquery.SchemaField("posted_date_time", "TIMESTAMP"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("quantity_purchased", "INT64"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]
    
    # Create table if needed
    try:
        client.get_table(fact_table_id)
    except Exception:
        table = bigquery.Table(fact_table_id, schema=schema)
        table.partitioning_column = "settlement_start_date"
        table.clustering_fields = ["sku", "order_id"]
        client.create_table(table)

    # 1. Deduplicate
    logger.info(f"Deduplicating settlement {settlement_id}...")
    delete_query = f"DELETE FROM `{fact_table_id}` WHERE settlement_id = '{settlement_id}'"
    client.query(delete_query).result()
    
    def parse_date(date_str):
        if not date_str:
            return None
        try:
            # Try ISO first
            return datetime.fromisoformat(date_str.replace('Z', '+00:00')).isoformat()
        except ValueError:
            try:
                # Try DD.MM.YYYY
                return datetime.strptime(date_str, "%d.%m.%Y").isoformat()
            except ValueError:
                return date_str

    # 2. Parse TSV
    logger.info("Parsing and Unpivoting TSV content...")
    reader = csv.DictReader(StringIO(content_bytes.decode('utf-8', errors='ignore')), delimiter='\t')
    
    final_rows = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    
    rows_logged = 0
    for row in reader:
        try:
            qty_str = row.get('quantity-purchased')
            qty = int(qty_str or 0)
            
            if qty > 0 and rows_logged < 5:
                logger.info(f"CAPTured Qty > 0 - SKU: {row.get('sku')}, Type: {row.get('transaction-type')}, Qty: {qty}")
                logger.info(f"Amounts: price={row.get('price-amount')}, fee={row.get('item-related-fee-amount')}, promo={row.get('promotion-amount')}, other={row.get('other-amount')}")
                rows_logged += 1
            
            # Helper to create component rows
            def add_component(amt_type, amt_desc, amt_val):
                if amt_val != 0 or (amt_type == "Price" and amt_desc == "Principal" and qty > 0):
                    final_rows.append({
                        "settlement_id": row.get('settlement-id'),
                        "settlement_start_date": parse_date(row.get('settlement-start-date')),
                        "settlement_end_date": parse_date(row.get('settlement-end-date')),
                        "deposit_date": parse_date(row.get('deposit-date')),
                        "total_amount": float(row.get('total-amount') or 0),
                        "currency": row.get('currency'),
                        "transaction_type": row.get('transaction-type'),
                        "order_id": row.get('order-id'),
                        "merchant_order_id": row.get('merchant-order-id'),
                        "adjustment_id": row.get('adjustment-id'),
                        "shipment_id": row.get('shipment-id'),
                        "marketplace_name": row.get('marketplace-name'),
                        "amount_type": amt_type,
                        "amount_description": amt_desc,
                        "amount": float(amt_val),
                        "fulfillment_id": row.get('fulfillment-id'),
                        "posted_date_time": parse_date(row.get('posted-date') or row.get('posted-date-time')),
                        "sku": row.get('sku'),
                        "quantity_purchased": qty,
                        "ingested_at": ingested_at
                    })

            def safe_float(val):
                if not val or str(val).strip() == '':
                    return 0.0
                try:
                    return float(val)
                except ValueError:
                    return 0.0

            # Unpivot components
            add_component("Price", row.get('price-type') or "Principal", safe_float(row.get('price-amount')))
            add_component("Fee", row.get('item-related-fee-type') or row.get('order-fee-type') or row.get('shipment-fee-type') or "Fee", 
                          safe_float(row.get('item-related-fee-amount')) + safe_float(row.get('order-fee-amount')) + safe_float(row.get('shipment-fee-amount')))
            add_component("Promotion", row.get('promotion-type') or "Promotion", safe_float(row.get('promotion-amount')))
            add_component("Other", row.get('other-fee-reason-description') or "Other", safe_float(row.get('other-fee-amount')) + safe_float(row.get('other-amount')) + safe_float(row.get('misc-fee-amount')))

        except Exception as e:
            logger.warning(f"Failed to parse settlement row: {e}")
            continue

    if not final_rows:
        logger.warning(f"No valid rows found in settlement {settlement_id}")
        return

    # 3. Load to BQ
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    logger.info(f"Loading {len(final_rows)} rows to {fact_table_id}...")
    for r in final_rows:
        if r.get('transaction_type') == 'Order' and r.get('quantity_purchased', 0) > 0:
            logger.info(f"SAMPLE FINAL ROW: {r}")
            break

    job = client.load_table_from_json(final_rows, fact_table_id, job_config=job_config)
    job.result()
    logger.info(f"Loaded {job.output_rows} rows to {fact_table_id}.")


def run_pipeline(limit=1, start_date=None):
    reports = get_settlement_reports(limit=limit, start_date=start_date)
    if not reports:
        logger.info("No settlement reports found.")
        return
        
    # Parse start_date for filtering
    filter_date = None
    if start_date:
        if "T" not in start_date:
            start_date = f"{start_date}T00:00:00Z"
        filter_date = datetime.fromisoformat(start_date.replace("Z", "+00:00"))

    for report in reports:
        report_id = report["reportId"]
        doc_id = report["reportDocumentId"]
        created_time_str = report.get("createdTime")
        
        # Client-side filtering
        if filter_date and created_time_str:
            report_time = datetime.fromisoformat(created_time_str.replace("Z", "+00:00"))
            if report_time < filter_date:
                logger.info(f"Skipping report {report_id} from {created_time_str} (older than {start_date})")
                continue
        
        logger.info(f"Processing Settlement Report ID: {report_id} from {created_time_str}")
        content = download_document(doc_id)
        
        # Sneak peek at content to get settlement-id for GCS path
        lines = content.decode('utf-8', errors='ignore').splitlines()
        if len(lines) < 2:
            logger.warning(f"Empty or malformed report {report_id}")
            continue
            
        first_row = lines[1].split('\t')
        settlement_id = first_row[0]
        
        # Upload raw to GCS
        upload_to_gcs(content, settlement_id)
        
        # Transform and Load
        transform_and_load_bq(content, settlement_id)

    logger.info("Settlement Ingestion Completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1, help="Number of recent reports to fetch")
    parser.add_argument("--start_date", type=str, help="Filter reports created since (YYYY-MM-DD)")
    args = parser.parse_args()
    
    run_pipeline(args.limit, args.start_date)
