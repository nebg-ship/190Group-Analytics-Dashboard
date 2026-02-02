import os
import json
from google.cloud import bigquery

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
DATASET_ID = os.getenv('AMAZON_ECON_DATASET')
TABLE_ID = os.getenv('AMAZON_WEEKLY_ECON_TABLE', 'weekly_sku_economics')
JSONL_FILE = 'temp_ingest/weekly_data.jsonl'

def main():
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    
    with open(JSONL_FILE, 'r') as f:
        # Check if file has data
        if not f.read(1):
             print("JSONL file is empty.")
             return
        f.seek(0)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    print(f"Loading data from {JSONL_FILE} to {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}...")
    with open(JSONL_FILE, 'rb') as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    
    job.result()
    print(f"Loaded {job.output_rows} rows.")

if __name__ == "__main__":
    main()
