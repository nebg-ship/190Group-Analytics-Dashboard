import os
import time
import json
import gzip
import uuid
import logging
import argparse
import subprocess
import tempfile
from datetime import datetime, timedelta, date, timezone
from io import BytesIO

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

def create_report(report_type, start_date, end_date):
    access_token = get_lwa_access_token()
    auth = get_aws_auth()
    
    # Reports API expects ISO 8601 with Z or offset
    start_iso = f"{start_date.isoformat()}T00:00:00Z"
    end_iso = f"{end_date.isoformat()}T23:59:59Z"
    
    body = {
        "reportType": report_type,
        "marketplaceIds": [MARKETPLACE_ID_US],
        "dataStartTime": start_iso,
        "dataEndTime": end_iso,
        "reportOptions": {
            "dateGranularity": "DAY",
            "asinGranularity": "CHILD"
        }
    }
    
    resp = requests.post(
        f"{SP_API_ENDPOINT}{REPORTS_PATH}",
        json=body,
        auth=auth,
        headers=get_auth_headers(access_token)
    )
    
    if resp.status_code != 202:
        logger.error(f"Failed to create report: {resp.text}")
        resp.raise_for_status()
        
    report_id = resp.json()["reportId"]
    logger.info(f"Report requested: {report_id} for {start_date} to {end_date}")
    return report_id

def wait_for_report(report_id):
    logger.info(f"Waiting for report {report_id}...")
    auth = get_aws_auth()
    
    while True:
        access_token = get_lwa_access_token()
        resp = requests.get(
            f"{SP_API_ENDPOINT}{REPORTS_PATH}/{report_id}",
            auth=auth,
            headers=get_auth_headers(access_token)
        )
        resp.raise_for_status()
        data = resp.json()
        status = data["processingStatus"]
        
        if status == "DONE":
            return data["reportDocumentId"]
        elif status in ["CANCELLED", "FATAL"]:
            raise Exception(f"Report failed with status: {status}")
        
        logger.info(f"Status: {status}. Sleeping 30s...")
        time.sleep(30)

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

def upload_to_gcs(content_bytes, run_date, report_id):
    client = storage.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCS_BUCKET)
    blob_path = f"amazon/reports/business/us/run_date={run_date}/report_id={report_id}/part-000.json.gz"
    blob = bucket.blob(blob_path)
    
    # Compress before upload
    compressed_content = gzip.compress(content_bytes)
    blob.upload_from_string(compressed_content, content_type="application/json")
    
    logger.info(f"Uploaded to gs://{GCS_BUCKET}/{blob_path}")
    return f"gs://{GCS_BUCKET}/{blob_path}"

def get_bigquery_client():
    return bigquery.Client(project=GCP_PROJECT)

def create_table_if_not_exists(client, table_id):
    schema = [
        bigquery.SchemaField("report_date", "DATE"),
        bigquery.SchemaField("msku", "STRING"),
        bigquery.SchemaField("asin", "STRING"),
        bigquery.SchemaField("sessions", "INT64"),
        bigquery.SchemaField("page_views", "INT64"),
        bigquery.SchemaField("buy_box_percentage", "FLOAT64"),
        bigquery.SchemaField("unit_session_percentage", "FLOAT64"),
        bigquery.SchemaField("units_ordered", "INT64"),
        bigquery.SchemaField("ordered_product_sales", "NUMERIC"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.partitioning_column = "report_date"
    table.clustering_fields = ["msku", "asin"]
    try:
        client.get_table(table_id)
        logger.info(f"Table {table_id} already exists.")
    except Exception:
        logger.info(f"Creating table {table_id}...")
        client.create_table(table)
        logger.info(f"Table {table_id} created.")

def transform_and_load_bq(content_bytes, start_date, end_date):
    client = get_bigquery_client()
    fact_table_id = f"{GCP_PROJECT}.{BQ_DATASET}.fact_business_reports_us"
    
    # Ensure table exists
    create_table_if_not_exists(client, fact_table_id)
    
    logger.info("Processing report data...")
    raw_data = json.loads(content_bytes)
    
    # Fallback date from report spec if not in entries
    spec_start = raw_data.get("reportSpecification", {}).get("dataStartTime")
    
    final_rows = []
    asin_data = raw_data.get("salesAndTrafficByAsin", [])
    for entry in asin_data:
        try:
            # Date can be in entry or fallback to spec_start
            date_str = entry.get("date") or spec_start
            report_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else None
            
            # Map SKU/ASIN
            asin = entry.get("childAsin") or entry.get("parentAsin")
            sku = entry.get("sku") or asin # Fallback to ASIN if SKU missing
            
            row = {
                "report_date": report_date.isoformat() if report_date else None,
                "msku": sku,
                "asin": asin,
                "sessions": int(entry.get("trafficByAsin", {}).get("sessions", 0)),
                "page_views": int(entry.get("trafficByAsin", {}).get("pageViews", 0)),
                "buy_box_percentage": float(entry.get("trafficByAsin", {}).get("buyBoxPercentage", 0)),
                "unit_session_percentage": float(entry.get("trafficByAsin", {}).get("unitSessionPercentage", 0)),
                "units_ordered": int(entry.get("salesByAsin", {}).get("unitsOrdered", 0)),
                "ordered_product_sales": float(entry.get("salesByAsin", {}).get("orderedProductSales", {}).get("amount", 0)),
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }
            final_rows.append(row)
        except Exception as e:
            logger.warning(f"Failed to parse row: {e}")
            continue
    
    if not final_rows:
        logger.warning("No data found in report.")
        return
        
    # 1. Delete existing overlap
    logger.info(f"Deleting existing data in {fact_table_id} between {start_date} and {end_date}")
    delete_query = f"""
        DELETE FROM `{fact_table_id}`
        WHERE report_date BETWEEN '{start_date}' AND '{end_date}'
    """
    client.query(delete_query).result()
    
    # 2. Load to Table
    schema = [
        bigquery.SchemaField("report_date", "DATE"),
        bigquery.SchemaField("msku", "STRING"),
        bigquery.SchemaField("asin", "STRING"),
        bigquery.SchemaField("sessions", "INT64"),
        bigquery.SchemaField("page_views", "INT64"),
        bigquery.SchemaField("buy_box_percentage", "FLOAT64"),
        bigquery.SchemaField("unit_session_percentage", "FLOAT64"),
        bigquery.SchemaField("units_ordered", "INT64"),
        bigquery.SchemaField("ordered_product_sales", "NUMERIC"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    logger.info(f"Loading {len(final_rows)} rows to {fact_table_id}...")
    job = client.load_table_from_json(final_rows, fact_table_id, job_config=job_config)
    job.result()
    logger.info(f"Loaded {job.output_rows} rows to {fact_table_id}.")

def process_day(target_date):
    logger.info(f"Processing Reports for {target_date}")
    
    report_type = "GET_SALES_AND_TRAFFIC_REPORT"
    
    # Request for exactly one day to ensure no aggregation
    report_id = create_report(report_type, target_date, target_date)
    
    try:
        doc_id = wait_for_report(report_id)
        content = download_document(doc_id)
        
        # Upload raw to GCS
        upload_to_gcs(content, target_date, report_id)
        
        # Process and load to BQ
        transform_and_load_bq(content, target_date, target_date)
        
    except Exception as e:
        logger.error(f"Failed to process {target_date}: {e}")
        # We might want to continue for other days or fail? For now, fail.
        raise

def run_pipeline(days_ago=1):
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_ago-1)
    
    logger.info(f"Starting Reports pipeline for range {start_date} to {end_date}")
    
    current_date = start_date
    while current_date <= end_date:
        process_day(current_date)
        current_date += timedelta(days=1)
    
    logger.info("Pipeline Execution Completed Successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    
    run_pipeline(args.days)
