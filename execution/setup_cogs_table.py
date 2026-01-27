import os
import logging
from google.cloud import bigquery
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

GCP_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET = os.getenv("BIGQUERY_DATASET", "amazon_econ")

def get_bigquery_client():
    return bigquery.Client(project=GCP_PROJECT)

def setup_cogs_table():
    client = get_bigquery_client()
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.dim_sku_costs_us"
    
    schema = [
        bigquery.SchemaField("msku", "STRING"),
        bigquery.SchemaField("cost_per_unit", "NUMERIC"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("valid_from", "DATE"),
        bigquery.SchemaField("valid_to", "DATE"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    try:
        client.get_table(table_id)
        logger.info(f"Table {table_id} already exists.")
    except Exception:
        logger.info(f"Creating table {table_id}...")
        client.create_table(table)
        logger.info(f"Table {table_id} created.")

    # Seeding with a few placeholder rows if empty
    check_query = f"SELECT count(*) as cnt FROM `{table_id}`"
    results = client.query(check_query).result()
    count = next(results).cnt
    
    if count == 0:
        logger.info("Seeding table with placeholder SKU...")
        # Note: This is just to show the structure
        insert_query = f"""
            INSERT INTO `{table_id}` (msku, cost_per_unit, currency, valid_from, valid_to, updated_at)
            VALUES ('PLACEHOLDER-SKU', 10.00, 'USD', '2020-01-01', '2099-12-31', CURRENT_TIMESTAMP())
        """
        client.query(insert_query).result()
        logger.info("Seeding complete.")

if __name__ == "__main__":
    setup_cogs_table()
