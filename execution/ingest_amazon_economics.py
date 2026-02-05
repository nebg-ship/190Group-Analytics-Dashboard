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
SP_API_ROLE_ARN = os.getenv("SP_API_ROLE_ARN") 

GCP_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET = os.getenv("BIGQUERY_DATASET", "amazon_econ")
GCS_BUCKET = os.getenv("GCS_BUCKET")

# Constants
SP_API_ENDPOINT = "https://sellingpartnerapi-na.amazon.com"
DATA_KIOSK_PATH = "/dataKiosk/2023-11-15/queries"
DOCUMENTS_PATH = "/dataKiosk/2023-11-15/documents"

def is_gzip_bytes(data: bytes) -> bool:
    return len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B

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
    logger.info("LWA Access Token retrieved successfully.")
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

def create_economics_query(start_date, end_date):
    query = f"""
    query {{
      analytics_economics_2024_03_15 {{
        economics(
          startDate: "{start_date.isoformat()}"
          endDate: "{end_date.isoformat()}"
          marketplaceIds: ["ATVPDKIKX0DER"]
        ) {{
            startDate
            endDate
            msku
            childAsin
            fnsku
            marketplaceId
            sales {{
                unitsOrdered
                orderedProductSales {{ amount }}
                refundedProductSales {{ amount }}
                netProductSales {{ amount }}
            }}
            fees {{
                feeTypeName
                charges {{
                    aggregatedDetail {{
                        totalAmount {{ amount }}
                    }}
                }}
            }}
            ads {{
                adTypeName
                charge {{
                    totalAmount {{ amount }}
                }}
            }}
            netProceeds {{
                total {{ amount }}
            }}
        }}
      }}
    }}
    """
    return query

def test_connectivity(use_auth=True):
    logger.info(f"Testing general SP-API connectivity (Marketplace Participations) - SigV4={'ON' if use_auth else 'OFF'}...")
    access_token = get_lwa_access_token()
    auth = get_aws_auth() if use_auth else None
    
    resp = requests.get(
        f"{SP_API_ENDPOINT}/sellers/v1/marketplaceParticipations",
        auth=auth,
        headers=get_auth_headers(access_token)
    )
    if resp.status_code == 200:
        logger.info(f"Connectivity Test Passed (SigV4={'ON' if use_auth else 'OFF'})!")
        return True
    else:
        req_id = resp.headers.get("x-amzn-RequestId")
        logger.warning(f"Connectivity Test Failed (SigV4={'ON' if use_auth else 'OFF'}, RequestId={req_id}): {resp.text}")
        return False

def submit_query(query):
    if not test_connectivity(use_auth=True):
        logger.info("Retrying without SigV4...")
        if not test_connectivity(use_auth=False):
            raise Exception("Connectivity failed with both SigV4 ON and OFF.")
        else:
            use_sigv4 = False
    else:
        use_sigv4 = True

    access_token = get_lwa_access_token()
    auth = get_aws_auth() if use_sigv4 else None
    
    body = {
        "query": query
    }
    
    resp = requests.post(
        f"{SP_API_ENDPOINT}{DATA_KIOSK_PATH}",
        json=body,
        auth=auth,
        headers=get_auth_headers(access_token)
    )

    if resp.status_code != 202:
        logger.error(f"Failed to submit query: {resp.text}")
        resp.raise_for_status()
        
    query_id = resp.json()["queryId"]
    logger.info(f"Query submitted: {query_id}")
    return query_id

def wait_for_query(query_id):
    logger.info(f"Waiting for query {query_id}...")
    auth = get_aws_auth()
    
    while True:
        access_token = get_lwa_access_token() 
        resp = requests.get(
            f"{SP_API_ENDPOINT}{DATA_KIOSK_PATH}/{query_id}",
            auth=auth,
            headers=get_auth_headers(access_token)
        )
        resp.raise_for_status()
        data = resp.json()
        status = data.get("processingStatus")
        
        if status == "DONE":
            doc_id = data.get("dataDocumentId")
            if not doc_id:
                logger.error(f"Query DONE but dataDocumentId missing! Response: {data}")
                raise Exception("dataDocumentId missing in DONE response")
            return doc_id
        elif status in ["CANCELLED", "FATAL", "FAILED"]:
            logger.error(f"Query failed: {data}")
            raise Exception(f"Query failed with status: {status} - {data.get('errors')}")
        
        logger.info(f"Status: {status}. Sleeping 15s...")
        time.sleep(15)

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
    compression = doc_info.get("compressionAlgorithm")
    print(f"DEBUG - Document Info: {doc_info}")
    download_url = doc_info.get("documentUrl") or doc_info.get("url")
    if not download_url:
        logger.error(f"Failed to find download URL in doc_info: {doc_info}")
        raise Exception("Download URL missing")
    
    logger.info(f"Downloading content... (compression={compression or 'NONE'})")
    content_resp = requests.get(download_url)
    content_resp.raise_for_status()
    content = content_resp.content

    if compression == "GZIP" or is_gzip_bytes(content):
        logger.info("Decompressing GZIP document content.")
        try:
            content = gzip.decompress(content)
        except OSError as e:
            logger.error(f"Failed to decompress document content: {e}")
            raise

    return content

def upload_to_gcs(content_bytes, run_date, document_id):
    client = storage.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCS_BUCKET)
    blob_path = f"amazon/economics/us/run_date={run_date}/source_document_id={document_id}/part-000.json.gz"
    blob = bucket.blob(blob_path)
    
    if not is_gzip_bytes(content_bytes):
        logger.info("Content is not gzipped. Compressing before upload.")
        content_bytes = gzip.compress(content_bytes)
    else:
        logger.info("Content already gzipped. Uploading as-is.")

    blob.upload_from_string(content_bytes, content_type="application/gzip")
    logger.info(f"Uploaded to gs://{GCS_BUCKET}/{blob_path}")
    return f"gs://{GCS_BUCKET}/{blob_path}"

def transform_and_load_bq(gcs_uri, start_date, end_date, query_id, doc_id):
    """
    Loads data using BigQuery Python SDK.
    """
    client = bigquery.Client(project=GCP_PROJECT)
    staging_table_id = f"{GCP_PROJECT}.{BQ_DATASET}.stg_amazon_economics_raw"
    fact_table_id = f"{GCP_PROJECT}.{BQ_DATASET}.fact_sku_day_us"
    
    logger.info(f"Loading {gcs_uri} into staging table {staging_table_id}")
    
    # 1. Load to Staging
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True
    )
    load_job = client.load_table_from_uri(gcs_uri, staging_table_id, job_config=job_config)
    load_job.result()
    logger.info("Staging load complete.")

    # 2. Cleanup Target Range (DELETE)
    logger.info(f"Deleting existing data in {fact_table_id} between {start_date} and {end_date}")
    delete_query = f"""
        DELETE FROM `{fact_table_id}` 
        WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
    """
    client.query(delete_query).result()

    # 3. Insert Transformed Data (INSERT)
    logger.info(f"Inserting transformed data into {fact_table_id}")
    insert_query = f"""
        INSERT INTO `{fact_table_id}` (
            business_date, 
            marketplace,
            msku, 
            asin, 
            fnsku,
            units, 
            gross_sales, 
            refunds, 
            net_sales, 
            amazon_fees, 
            ad_spend, 
            net_proceeds,
            source_query_id,
            source_document_id,
            ingested_at
        )
        SELECT
            CAST(startDate AS DATE) as business_date,
            marketplaceId as marketplace,
            msku,
            childAsin as asin,
            fnsku,
            CAST(sales.unitsOrdered AS INT64) as units,
            CAST(IFNULL(sales.orderedProductSales.amount, 0.0) AS NUMERIC) as gross_sales,
            CAST(IFNULL(sales.refundedProductSales.amount, 0.0) AS NUMERIC) as refunds,
            CAST(IFNULL(sales.netProductSales.amount, 0.0) AS NUMERIC) as net_sales,
            CAST(IFNULL((SELECT SUM(charge.aggregatedDetail.totalAmount.amount) FROM UNNEST(fees) AS f, UNNEST(f.charges) AS charge), 0.0) AS NUMERIC) as amazon_fees,
            CAST(IFNULL((SELECT SUM(ad.charge.totalAmount.amount) FROM UNNEST(ads) AS ad), 0.0) AS NUMERIC) as ad_spend,
            CAST(IFNULL(netProceeds.total.amount, 0.0) AS NUMERIC) as net_proceeds,
            '{query_id}' as source_query_id,
            '{doc_id}' as source_document_id,
            CURRENT_TIMESTAMP() as ingested_at
        FROM `{staging_table_id}`
    """
    query_job = client.query(insert_query)
    query_job.result()
    
    # Get row count
    return query_job.num_dml_affected_rows or 0


def log_etl_run(run_date, status, query_id, doc_id, row_count, error_msg=None):
    client = bigquery.Client(project=GCP_PROJECT)
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.etl_runs"
    
    row = {
        "run_date": run_date.isoformat(),
        "status": status,
        "query_id": query_id,
        "document_id": doc_id,
        "row_count": row_count,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "error": str(error_msg)[:1000] if error_msg else None
    }
    
    try:
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            logger.error(f"Failed to log etl_run: {errors}")
    except Exception as e:
        logger.error(f"Failed to log etl_run to BQ: {e}")


def run_pipeline(backfill_days=7):
    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    start_date = end_date - timedelta(days=backfill_days - 1)
    
    logger.info(f"Starting pipeline for range {start_date} to {end_date}")
    
    query = create_economics_query(start_date, end_date)
    query_id = submit_query(query)
    
    try:
        doc_id = wait_for_query(query_id)
        content = download_document(doc_id)
        
        gcs_uri = upload_to_gcs(content, end_date, doc_id)
        
        # Load to BQ
        row_count = transform_and_load_bq(gcs_uri, start_date, end_date, query_id, doc_id)
        
        log_etl_run(end_date, "SUCCESS", query_id, doc_id, row_count)
        logger.info("Pipeline Execution Completed Successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        log_etl_run(end_date, "FAILED", query_id if 'query_id' in locals() else None, None, 0, str(e))
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill-days", type=int, default=7)
    args = parser.parse_args()
    
    run_pipeline(args.backfill_days)
