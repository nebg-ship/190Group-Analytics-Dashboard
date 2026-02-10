import os
import json
import datetime
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
DATASET_ID = os.getenv("GA4_DATASET")
TABLE_ID = os.getenv("GA4_HISTORICAL_TABLE", "ga4_historical_summary")
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")

def get_client():
    """
    Returns a GA4 Data client using Application Default Credentials (ADC).
    To use this locally, run: gcloud auth application-default login
    """
    return BetaAnalyticsDataClient()

def fetch_ga4_data(start_date, end_date):
    print(f"Fetching GA4 metrics from {start_date} to {end_date}...")
    client = get_client()
    
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
        ],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    
    response = client.run_report(request)
    
    rows = []
    for row in response.rows:
        rows.append({
            "date": datetime.datetime.strptime(row.dimension_values[0].value, "%Y%m%d").date(),
            "sessions": int(row.metric_values[0].value),
            "users": int(row.metric_values[1].value),
        })
    
    return pd.DataFrame(rows)

def upload_to_bigquery(df):
    if df.empty:
        print("No data to upload.")
        return

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("sessions", "INTEGER"),
            bigquery.SchemaField("users", "INTEGER"),
        ],
        write_disposition="WRITE_TRUNCATE", # We are backfilling a specific range
    )
    
    print(f"Uploading {len(df)} rows to {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print("Upload complete.")

def main():
    try:
        # Backfill from the start of 2025 to yesterday to ensure all data is current
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Starting backfill from 2025-01-01 to {yesterday}...")
        
        df = fetch_ga4_data("2025-01-01", yesterday)
        upload_to_bigquery(df)
        print("\nBackfill process finished successfully.")
    except Exception as e:
        print(f"\nCRITICAL ERROR during backfill: {e}")

if __name__ == "__main__":
    main()
