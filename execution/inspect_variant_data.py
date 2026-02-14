
import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
SALES_DATASET = os.getenv('SALES_DATASET', 'sales')

client = bigquery.Client(project=PROJECT_ID)

def inspect_li_columns():
    table_id = f"{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items"
    try:
        table = client.get_table(table_id)
        print(f"--- Columns in {table_id} ---")
        for schema_field in table.schema:
            print(f"{schema_field.name}")
            
        print("\n--- Sample Row (Name & SKU?) ---")
        # Try to select just one safe column first
        query = f"SELECT order_id FROM `{table_id}` LIMIT 1"
        df = client.query(query).to_dataframe()
        print(df)
            
    except Exception as e:
        print(e)

if __name__ == "__main__":
    inspect_li_columns()
