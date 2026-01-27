import os
import time
import json
import gzip
import uuid
import logging
import argparse
import subprocess
import tempfile
from datetime import datetime, timedelta, date
from io import BytesIO

import requests
from requests_aws4auth import AWS4Auth
from dotenv import load_dotenv
from google.cloud import storage

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
      analytics_economics_2024_03_15(
        customerId: "amzn1.ask.skill.1" 
        startDateTime: "{start_date.isoformat()}"
        endDateTime: "{end_date.isoformat()}"
        aggregateBy: [DATE, PRODUCT]
      ) {{
        date
        product {{
            sku
            asin
        }}
        sales {{
            grossSales {{
                amount
                currencyCode
            }}
            unitsOrdered
            refunds {{
                amount
            }}
            netSales {{
                amount
            }}
        }}
        fees {{
            totalFees {{
                amount
            }}
        }}
        advertising {{
            spend {{
                amount
            }}
        }}
        netProceeds {{
            amount
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
        status = data["status"]
        
        if status == "DONE":
            return data["documentId"]
        elif status in ["CANCELLED", "FATAL", "FAILED"]:
            raise Exception(f"Query failed with status: {status} - {data.get('errorList')}")
        
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
    download_url = doc_info["documentUrl"]
    
    logger.info("Downloading content...")
    content_resp = requests.get(download_url)
    content_resp.raise_for_status()
    
    return content_resp.content

def upload_to_gcs(content_bytes, run_date, document_id):
    client = storage.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCS_BUCKET)
    blob_path = f"amazon/economics/us/run_date={run_date}/source_document_id={document_id}/part-000.json.gz"
    blob = bucket.blob(blob_path)
    
    blob.upload_from_string(content_bytes, content_type="application/json")
    logger.info(f"Uploaded to gs://{GCS_BUCKET}/{blob_path}")
    return f"gs://{GCS_BUCKET}/{blob_path}"

def run_bq_command(args, input_data=None):
    """Run a bq command via subprocess."""
    cmd = ["bq", "--headless", "--format=json"] + args
    logger.info(f"Running BQ command: {' '.join(cmd)}")
    result = subprocess.run(
        cmd, 
        input=input_data.encode('utf-8') if input_data else None,
        capture_output=True, 
        text=True
    )
    if result.returncode != 0:
        logger.error(f"BQ Command Failed: {result.stderr}")
        raise Exception(f"BQ Command Failed: {result.stderr}")
    return result.stdout

def transform_and_load_bq(gcs_uri, start_date, end_date):
    """
    Loads data using 'bq load' and 'bq query'.
    """
    staging_table_id = f"{GCP_PROJECT}:{BQ_DATASET}.stg_amazon_economics_raw"
    fact_table_id = f"{GCP_PROJECT}.{BQ_DATASET}.fact_sku_day_us"
    
    logger.info(f"Loading {gcs_uri} into staging table {staging_table_id}")
    
    # 1. Load to Staging
    # bq load --autodetect --source_format=NEWLINE_DELIMITED_JSON --replace <table_id> <uri>
    load_args = [
        "load",
        "--autodetect",
        "--source_format=NEWLINE_DELIMITED_JSON",
        "--replace", # WRITE_TRUNCATE
        "--ignore_unknown_values",
        staging_table_id,
        gcs_uri
    ]
    run_bq_command(load_args)
    logger.info("Staging load complete.")

    # 2. Cleanup Target Range (DELETE)
    logger.info(f"Deleting existing data in {fact_table_id} between {start_date} and {end_date}")
    delete_query = f"""
        DELETE FROM `{fact_table_id}` 
        WHERE business_date BETWEEN '{start_date}' AND '{end_date}'
    """
    run_bq_command(["query", "--use_legacy_sql=false", delete_query])

    # 3. Insert Transformed Data (INSERT)
    logger.info(f"Inserting transformed data into {fact_table_id}")
    insert_query = f"""
        INSERT INTO `{fact_table_id}` (
            business_date, 
            msku, 
            asin, 
            units, 
            gross_sales, 
            refunds, 
            net_sales, 
            amazon_fees, 
            ad_spend, 
            net_proceeds,
            ingested_at
        )
        SELECT
            CAST(date AS DATE) as business_date,
            product.sku as msku,
            product.asin as asin,
            sales.unitsOrdered as units,
            sales.grossSales.amount as gross_sales,
            sales.refunds.amount as refunds,
            sales.netSales.amount as net_sales,
            fees.totalFees.amount as amazon_fees,
            advertising.spend.amount as ad_spend,
            netProceeds.amount as net_proceeds,
            CURRENT_TIMESTAMP() as ingested_at
        FROM `{staging_table_id.replace(':', '.')}`
    """
    run_bq_command(["query", "--use_legacy_sql=false", insert_query])
    logger.info("Merge complete.")

    # Get row count as a bonus (optional)
    return 0 

def log_etl_run(run_date, status, query_id, doc_id, row_count, error_msg=None):
    try:
        table_id = f"{GCP_PROJECT}:{BQ_DATASET}.etl_runs"
        
        row = {
            "run_date": run_date.isoformat(),
            "status": status,
            "query_id": query_id,
            "document_id": doc_id,
            "row_count": row_count,
            "started_at": datetime.utcnow().isoformat(), 
            "finished_at": datetime.utcnow().isoformat(),
            "error": str(error_msg) if error_msg else None
        }
        
        # Write to temp file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
            json.dump(row, tmp)
            tmp_path = tmp.name
            
        # bq insert
        run_bq_command(["insert", table_id, tmp_path])
        os.remove(tmp_path)
            
    except Exception as e:
        logger.error(f"Failed to rewrite etl_runs: {e}")


def run_pipeline(backfill_days=7):
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=backfill_days)
    
    logger.info(f"Starting pipeline for range {start_date} to {end_date}")
    
    query = create_economics_query(start_date, end_date)
    query_id = submit_query(query)
    
    try:
        doc_id = wait_for_query(query_id)
        content = download_document(doc_id)
        
        gcs_uri = upload_to_gcs(content, end_date, doc_id)
        
        # Load to BQ
        row_count = transform_and_load_bq(gcs_uri, start_date, end_date)
        
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
