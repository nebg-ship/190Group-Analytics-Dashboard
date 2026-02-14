
import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
SALES_DATASET = os.getenv('SALES_DATASET', 'sales')
AMAZON_ECON_DATASET = os.getenv('AMAZON_ECON_DATASET', 'amazon_econ')
WHOLESALE_DATASET = os.getenv('WHOLESALE_DATASET', 'sales_wholesale')

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)


def get_bc_data():
    query = f"""
        SELECT 
            p.sku,
            li.quantity,
            li.total_ex_tax as revenue,
            p.product_name
        FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items` li
        JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_order` o ON li.order_id = o.order_id
        LEFT JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product` p ON li.product_id = p.product_id
        WHERE o.order_status_id IN (2, 10, 11, 3)
          AND DATE(o.order_created_date_time) BETWEEN '2025-01-01' AND '2025-12-31'
          AND LOWER(p.sku) LIKE 'jp%'
    """
    return client.query(query).to_dataframe()

def get_wholesale_data():
    query = f"""
        SELECT 
            p.sku,
            li.quantity,
            li.total_ex_tax as revenue,
            p.product_name
        FROM `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order_line_items` li
        JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order` o ON li.order_id = o.order_id
        LEFT JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_product` p ON li.product_id = p.product_id
        WHERE o.order_status_id IN (2, 10, 11, 3)
          AND DATE(o.order_created_date_time) BETWEEN '2025-01-01' AND '2025-12-31'
          AND LOWER(p.sku) LIKE 'jp%'
    """
    return client.query(query).to_dataframe()

def get_amazon_data():
    query = f"""
        SELECT 
            msku as sku,
            units as quantity,
            gross_sales as revenue,
            msku as product_name
        FROM `{PROJECT_ID}.{AMAZON_ECON_DATASET}.fact_sku_day_us`
        WHERE business_date BETWEEN '2025-01-01' AND '2025-12-31'
          AND LOWER(msku) LIKE 'jp%'
    """
    return client.query(query).to_dataframe()

def analyze():
    print("Fetching data for 'jp' SKUs in 2025 (Revenue & Units)...")
    try:
        df_bc = get_bc_data()
        print(f"BC rows found: {len(df_bc)}")
    except Exception as e:
        print(f"Error fetching BC data: {e}")
        df_bc = pd.DataFrame()

    try:
        df_ws = get_wholesale_data()
        print(f"Wholesale rows found: {len(df_ws)}")
    except Exception as e:
        print(f"Error fetching Wholesale data: {e}")
        df_ws = pd.DataFrame()

    try:
        df_amz = get_amazon_data()
        print(f"Amazon rows found: {len(df_amz)}")
    except Exception as e:
        print(f"Error fetching Amazon data: {e}")
        df_amz = pd.DataFrame()
    
    combined = pd.concat([df_bc, df_ws, df_amz])
    
    if combined.empty:
        print("No data found for 'jp' SKUs in 2025.")
        return

    # Clean up SKU to ensure consistency (lowercase)
    combined['sku'] = combined['sku'].astype(str).str.lower().str.strip()
    combined['revenue'] = pd.to_numeric(combined['revenue'], errors='coerce').fillna(0)
    combined['quantity'] = pd.to_numeric(combined['quantity'], errors='coerce').fillna(0)
    
    # Group by SKU
    grouped = combined.groupby('sku').agg({
        'revenue': 'sum',
        'quantity': 'sum',
        'product_name': 'first' # Just grab first product name found
    }).reset_index()
    
    top_10 = grouped.sort_values('revenue', ascending=False).head(10)
    
    print("\nTop 10 'jp' SKUs by Revenue (2025):")
    # Format revenue as currency
    pd.options.display.float_format = '${:,.2f}'.format
    # Format quantity as integer
    top_10['quantity'] = top_10['quantity'].astype(int)
    
    print(top_10[['sku', 'product_name', 'quantity', 'revenue']].to_string(index=False))

if __name__ == "__main__":
    analyze()
