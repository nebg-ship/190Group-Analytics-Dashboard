
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
    # Use the robust node command from seed script

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
        # Removed shell=True
    )
    
    if proc.returncode != 0:
        print(f"Error running convex: {proc.stderr}")
        return None
        


    # Try to find JSON array in payload
    stdout = proc.stdout
    start = stdout.find("[")
    end = stdout.rfind("]")
    if start != -1 and end != -1:
        try:
            return json.loads(stdout[start:end+1])
        except:
            pass
            
    # Fallback for single object or weird format
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
        
    # Convex args
    args = {"skus": skus}
    
    payload = convex_run("inventory:getPartQuantitiesBySkus", args)
    if not payload:
        print("Failed to fetch inventory from Convex or no data returned.")
        return pd.DataFrame()
    
    # Payload should be a list directly from our custom query
    data = []
    for r in payload:
        sku = r.get('sku')
        qty = r.get('quantityOnHand2025', 0)
        if sku:
            data.append({'variants_sku': sku, 'inventory_2026_01_01': qty})
            
    return pd.DataFrame(data)

def get_bc_data_variants():
    query = f"""
        SELECT 
            p.sku as parent_sku,
            p.product_name,
            v.variants_sku,
            li.quantity,
            li.total_ex_tax as revenue
        FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items` li
        JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_order` o ON li.order_id = o.order_id
        LEFT JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product` p ON li.product_id = p.product_id
        LEFT JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product_variants` v ON li.variant_id = v.variants_id
        WHERE o.order_status_id IN (2, 10, 11, 3)
          AND DATE(o.order_created_date_time) BETWEEN '2025-01-01' AND '2025-12-31'
          AND LOWER(p.sku) LIKE 'jp%'
    """
    return client.query(query).to_dataframe()

def analyze_variants():
    print("Fetching variant data for 'jp' SKUs in 2025...")
    try:
        df = get_bc_data_variants()
    except Exception as e:
        print(f"Error fetching BigQuery data: {e}")
        return

    if df.empty:
        print("No BigQuery data found.")
        return

    # Normalize
    df['parent_sku'] = df['parent_sku'].astype(str).str.lower().str.strip()
    df['variants_sku'] = df['variants_sku'].fillna('Unknown').astype(str).str.strip()
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)

    # Filter for top 10 Revenue SKUs (re-calculate top 10 parents from this dataset)
    parent_stats = df.groupby('parent_sku')['revenue'].sum().sort_values(ascending=False).head(10)
    top_10_parents = parent_stats.index.tolist()
    
    print(f"\nTop 10 Parent SKUs: {top_10_parents}")
    
    # Filter DF for these parents
    df_top = df[df['parent_sku'].isin(top_10_parents)].copy()
    
    # Group by Parent SKU -> Variant SKU
    grouped = df_top.groupby(['parent_sku', 'product_name', 'variants_sku']).agg({
        'quantity': 'sum',
        'revenue': 'sum'
    }).reset_index()
    


    # --- MERGE INVENTORY ---
    # Get unique SKUs from dataframe
    unique_skus = grouped['variants_sku'].unique().tolist()
    unique_skus = [s for s in unique_skus if s != 'Unknown']
    
    # Query Convex with UPPERCASE SKUs (assuming inventory is largely uppercase)
    # We'll pass both original and upper to be safe, or just relying on upper might be enough if standard
    # Let's pass upper variants of our list
    query_skus = list(set([s.upper() for s in unique_skus] + unique_skus))
    
    df_inv = get_convex_inventory(query_skus)
    
    if not df_inv.empty:
        # Normalize for merge: Create lowercase join key
        df_inv['join_sku'] = df_inv['variants_sku'].astype(str).str.strip().str.lower()
        grouped['join_sku'] = grouped['variants_sku'].astype(str).str.strip().str.lower()
        
        # Merge on join_sku
        # Note: df_inv might have duplicates if we queried both cases and they resolved to same object (unlikely if unique index)
        # or different objects. We'll drop duplicates on join_sku just in case
        df_inv = df_inv.drop_duplicates(subset=['join_sku'])
        
        grouped = pd.merge(grouped, df_inv[['join_sku', 'inventory_2026_01_01']], on='join_sku', how='left')
        grouped['inventory_2026_01_01'] = grouped['inventory_2026_01_01'].fillna(0).astype(int)
        
        # Cleanup
        grouped = grouped.drop(columns=['join_sku'])
    else:
        grouped['inventory_2026_01_01'] = 0

    # Sort by Parent Revenue (desc) then Variant Quantity (desc)
    parent_rev_map = parent_stats.to_dict()
    grouped['parent_rev'] = grouped['parent_sku'].map(parent_rev_map)
    
    grouped = grouped.sort_values(['parent_rev', 'parent_sku', 'quantity'], ascending=[False, True, False])
    
    print("\nVariant Breakdown for Top 10 'jp' SKUs (2025) with Inventory:")
    pd.options.display.float_format = '${:,.2f}'.format
    
    output_cols = ['parent_sku', 'variants_sku', 'inventory_2026_01_01', 'quantity', 'revenue', 'product_name']
    print(grouped[output_cols].to_string(index=False))
    

    # Save to CSV
    output_file = 'execution/jp_variant_breakdown_2025_with_inventory.csv'
    grouped[output_cols].to_csv(output_file, index=False)
    print(f"\nSaved detailed breakdown to {output_file}")

if __name__ == "__main__":
    analyze_variants()
