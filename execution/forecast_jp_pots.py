
import os
import json
import subprocess
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
SALES_DATASET = os.getenv('SALES_DATASET', 'sales')
PROJECT_ROOT = Path(__file__).resolve().parent.parent

client = bigquery.Client(project=PROJECT_ID)

def convex_run(function_name, args_obj):
    cmd = [
        "node",
        str(PROJECT_ROOT / "node_modules" / "convex" / "bin" / "main.js"),
        "run",
        "--push",
        function_name,
        json.dumps(args_obj)
    ]
    
    proc = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )
    
    # Try to find JSON array in payload
    stdout = proc.stdout
    start = stdout.find("[")
    end = stdout.rfind("]")
    if start != -1 and end != -1:
        try:
            return json.loads(stdout[start:end+1])
        except:
            pass
            
    # Fallback
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    for line in reversed(lines):
        if (line.startswith("{") and line.endswith("}")) or (line.startswith("[") and line.endswith("]")):
            try:
                return json.loads(line)
            except:
                continue
    return None

def get_convex_inventory(skus):
    print(f"Fetching inventory for {len(skus)} SKUs from Convex...")
    if not skus:
        return pd.DataFrame()
        
    args = {"skus": skus}
    payload = convex_run("inventory:getPartQuantitiesBySkus", args)
    
    if not payload:
        print("Failed to fetch inventory from Convex.")
        return pd.DataFrame()
    
    data = []
    for r in payload:
        sku = r.get('sku')
        qty = r.get('quantityOnHand2025', 0)
        if sku:
            data.append({'join_sku': sku.strip().lower(), 'inventory_jan_2026': qty})
            
    # Handle duplicates if multiple casing matches same object
    return pd.DataFrame(data).drop_duplicates(subset=['join_sku'])

def get_sales_data():
    query = f"""
        SELECT 
            p.sku as parent_sku,
            p.product_name,
            v.variants_sku,
            li.quantity,
            DATE(o.order_created_date_time) as order_date
        FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items` li
        JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_order` o ON li.order_id = o.order_id
        LEFT JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product` p ON li.product_id = p.product_id
        LEFT JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product_variants` v ON li.variant_id = v.variants_id
        WHERE o.order_status_id IN (2, 10, 11, 3)
          AND DATE(o.order_created_date_time) BETWEEN '2025-01-01' AND '2025-12-31'
          AND LOWER(p.sku) LIKE 'jp%'
    """
    return client.query(query).to_dataframe()

def forecast():
    print("Fetching 2025 sales history...")
    df = get_sales_data()
    
    if df.empty:
        print("No sales data found.")
        return

    # Normalize SKUs
    df['variants_sku'] = df['variants_sku'].fillna('Unknown').astype(str).str.strip()
    df['join_sku'] = df['variants_sku'].str.lower()
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)

    # Define Periods
    # Depletion: Jan 1 - May 31
    mask_depletion = (df['order_date'] >= '2025-01-01') & (df['order_date'] <= '2025-05-31')
    # Need: June 1 - Dec 31
    mask_need = (df['order_date'] >= '2025-06-01') & (df['order_date'] <= '2025-12-31')

    # Aggregate
    # We aggregate by variant
    sales_depletion = df[mask_depletion].groupby(['parent_sku', 'product_name', 'variants_sku', 'join_sku'])['quantity'].sum().reset_index().rename(columns={'quantity': 'sales_jan_may_2025'})
    sales_need = df[mask_need].groupby(['parent_sku', 'product_name', 'variants_sku', 'join_sku'])['quantity'].sum().reset_index().rename(columns={'quantity': 'sales_jun_dec_2025'})
    
    # Merge sales buckets
    forecast_df = pd.merge(sales_depletion, sales_need[['join_sku', 'sales_jun_dec_2025']], on='join_sku', how='outer')
    
    # Fill NA
    # If Outer merge, need to fill parent/variant/product info if it only existed in one period
    # But usually it exists in both or logic is simpler if we group by join_sku first?
    # Let's do a robust merge.
    
    # Better approach: pivot table or double groupby
    # But let's stick to merge, and fillna metadata
    cols_to_fill = ['parent_sku', 'product_name', 'variants_sku']
    for col in cols_to_fill:
        if col + '_x' in forecast_df.columns:
             forecast_df[col] = forecast_df[col + '_x'].fillna(forecast_df[col + '_y'])
    
    forecast_df = forecast_df[['parent_sku', 'product_name', 'variants_sku', 'join_sku', 'sales_jan_may_2025', 'sales_jun_dec_2025']]
    forecast_df = forecast_df.fillna(0)
    

    # --- FETCH INVENTORY ---
    unique_skus = forecast_df['variants_sku'].unique().tolist()
    # Ensure all are strings
    unique_skus = [str(s) for s in unique_skus]
    # Also include uppercase versions for safe lookup
    query_skus = list(set([s.upper() for s in unique_skus] + unique_skus))
    
    inv_df = get_convex_inventory(query_skus)
    
    # Merge Inventory
    final_df = pd.merge(forecast_df, inv_df, on='join_sku', how='left')
    final_df['inventory_jan_2026'] = final_df['inventory_jan_2026'].fillna(0)
    
    # --- CALCULATE FORECAST ---
    
    # Est Inv June 1 = Start Inv - Depletion Period Sales
    # Note: We floor at 0, can't have negative inventory really (unless backordered, but for purchasing we treat as 0 avail)
    final_df['proj_inv_jun_1'] = final_df['inventory_jan_2026'] - final_df['sales_jan_may_2025']
    final_df['proj_inv_jun_1'] = final_df['proj_inv_jun_1'].apply(lambda x: max(x, 0)) # Assuming we don't start with negative
    
    # Net Need = Demand (Jun-Dec) - Available (June 1)
    final_df['buy_recommendation'] = final_df['sales_jun_dec_2025'] - final_df['proj_inv_jun_1']
    final_df['buy_recommendation'] = final_df['buy_recommendation'].apply(lambda x: max(x, 0))
    
    # Sort by Buy Rec
    final_df = final_df.sort_values('buy_recommendation', ascending=False)
    
    # Output
    output_file = 'execution/jp_pot_forecast_2026.csv'
    cols = ['parent_sku', 'variants_sku', 'product_name', 'inventory_jan_2026', 'sales_jan_may_2025', 'proj_inv_jun_1', 'sales_jun_dec_2025', 'buy_recommendation']
    final_df[cols].to_csv(output_file, index=False)
    print(f"Forecast saved to {output_file}")
    
    print("\nTop 10 Buy Recommendations:")
    print(final_df[cols].head(10).to_string(index=False))

if __name__ == "__main__":
    forecast()
