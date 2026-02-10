"""
Load COGS data from inventory CSV into BigQuery dim_sku_costs_us table.
Reads the master inventory CSV, extracts SKU + Cost, and bulk loads into BQ.
"""
import os
import csv
import re
import logging
from datetime import date
from google.cloud import bigquery
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

GCP_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET = os.getenv("BIGQUERY_DATASET", "amazon_econ")

def safe_float(value, default=0.0):
    if not value or not value.strip():
        return default
    try:
        cleaned = re.sub(r'[,$]', '', value.strip())
        return float(cleaned)
    except (ValueError, TypeError):
        return default

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_path = os.path.join(project_root, "amazon_economics", "Master_Updated_web_accounts_v14_1 (1).csv")
    
    client = bigquery.Client(project=GCP_PROJECT)
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.dim_sku_costs_us"
    
    # Read CSV and build rows
    rows = []
    with open(input_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sku = (row.get("Sku") or "").strip()
            if not sku:
                continue
            cost = safe_float(row.get("Cost", "0"))
            rows.append({
                "msku": sku,
                "cost_per_unit": cost,
                "currency": "USD",
                "valid_from": "2020-01-01",
                "valid_to": "2099-12-31",
                "cogs_account": (row.get("COGS Account") or "").strip(),
                "description": (row.get("Description") or "").strip(),
                "price": safe_float(row.get("Price", "0")),
            })
    
    logger.info(f"Loaded {len(rows)} SKUs from CSV")
    
    # Recreate table with expanded schema
    schema = [
        bigquery.SchemaField("msku", "STRING"),
        bigquery.SchemaField("cost_per_unit", "NUMERIC"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("valid_from", "DATE"),
        bigquery.SchemaField("valid_to", "DATE"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("cogs_account", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("price", "NUMERIC"),
    ]
    
    # Delete existing data and reload
    try:
        client.get_table(table_id)
        logger.info(f"Table {table_id} exists. Truncating...")
        client.query(f"TRUNCATE TABLE `{table_id}`").result()
    except Exception:
        logger.info(f"Creating table {table_id}...")
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
    
    # Update schema in case new fields added
    table = client.get_table(table_id)
    existing_fields = {f.name for f in table.schema}
    new_fields = [f for f in schema if f.name not in existing_fields]
    if new_fields:
        table.schema = list(table.schema) + new_fields
        client.update_table(table, ["schema"])
        logger.info(f"Added {len(new_fields)} new fields to schema")
    
    # Batch insert using streaming (fast for < 10k rows)
    BATCH_SIZE = 500
    total_inserted = 0
    
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        # Build VALUES clause
        values_parts = []
        for r in batch:
            desc_escaped = r['description'].replace("'", "\\'")
            cogs_escaped = r['cogs_account'].replace("'", "\\'")
            values_parts.append(
                f"('{r['msku']}', {r['cost_per_unit']}, '{r['currency']}', "
                f"'{r['valid_from']}', '{r['valid_to']}', CURRENT_TIMESTAMP(), "
                f"'{cogs_escaped}', '{desc_escaped}', {r['price']})"
            )
        
        insert_sql = f"INSERT INTO `{table_id}` (msku, cost_per_unit, currency, valid_from, valid_to, updated_at, cogs_account, description, price) VALUES {', '.join(values_parts)}"
        client.query(insert_sql).result()
        total_inserted += len(batch)
        logger.info(f"Inserted {total_inserted}/{len(rows)} rows...")
    
    logger.info(f"Done! Loaded {total_inserted} SKU costs into {table_id}")
    
    # Quick verification
    result = client.query(f"SELECT COUNT(*) as cnt, ROUND(AVG(cost_per_unit), 2) as avg_cost FROM `{table_id}`").result()
    row = next(result)
    logger.info(f"Verification: {row.cnt} rows, avg cost ${row.avg_cost}")

if __name__ == "__main__":
    main()
