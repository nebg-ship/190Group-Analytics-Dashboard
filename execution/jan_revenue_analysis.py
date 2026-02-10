
import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
SALES_DATASET = os.getenv('SALES_DATASET', 'sales')
WHOLESALE_DATASET = os.getenv('WHOLESALE_DATASET', 'sales_wholesale')
AMAZON_ECON_DATASET = os.getenv('AMAZON_ECON_DATASET', 'amazon_econ')
GA4_DATASET = os.getenv('GA4_DATASET')

client = bigquery.Client(project=PROJECT_ID)

def get_bc_data():
    query = f"""
        SELECT 
            o.order_id,
            DATE(o.order_created_date_time) as date,
            o.order_source,
            li.product_id,
            p.sku,
            p.product_name,
            li.quantity,
            li.total_ex_tax as revenue,
            p.cost_price,
            p.product_type,
            vc.category_name
        FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items` li
        JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_order` o ON li.order_id = o.order_id
        LEFT JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product` p ON li.product_id = p.product_id
        LEFT JOIN (
            SELECT product_id, STRING_AGG(category_name, ', ') as category_name
            FROM `{PROJECT_ID}.{SALES_DATASET}.bc_product_category`
            GROUP BY 1
        ) vc ON li.product_id = vc.product_id
        WHERE o.order_status_id IN (2, 10, 11, 3)
          AND (
              (DATE(o.order_created_date_time) BETWEEN '2025-01-01' AND '2025-01-31') OR
              (DATE(o.order_created_date_time) BETWEEN '2026-01-01' AND '2026-01-31')
          )
    """
    return client.query(query).to_dataframe()

def get_wholesale_data():
    query = f"""
        SELECT 
            o.order_id,
            DATE(o.order_created_date_time) as date,
            'Wholesale' as channel,
            p.sku,
            p.product_name,
            li.quantity,
            li.total_ex_tax as revenue,
            NULL as cost_price -- We might not have cost easily for wholesale if product IDs differ, or assume same cost?
        FROM `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order_line_items` li
        JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order` o ON li.order_id = o.order_id
        LEFT JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_product` p ON li.product_id = p.product_id
        WHERE o.order_status_id IN (2, 10, 11, 3)
          AND (
              (DATE(o.order_created_date_time) BETWEEN '2025-01-01' AND '2025-01-31') OR
              (DATE(o.order_created_date_time) BETWEEN '2026-01-01' AND '2026-01-31')
          )
    """
    return client.query(query).to_dataframe()

def get_amazon_data():
    query = f"""
        SELECT 
            DATE(business_date) as date,
            msku as sku,
            msku as product_name, -- product_title missing, use msku
            units as quantity,
            gross_sales as revenue,
            net_proceeds, -- Use as margin proxy if needed, or we link to BC cost
            ad_spend
        FROM `{PROJECT_ID}.{AMAZON_ECON_DATASET}.fact_sku_day_us`
        WHERE (
            (business_date BETWEEN '2025-01-01' AND '2025-01-31') OR
            (business_date BETWEEN '2026-01-01' AND '2026-01-31')
        )
    """
    return client.query(query).to_dataframe()

# Helper to classify BC rows
def classify_bc_row(row):
    name = str(row['product_name']).lower()
    cats = str(row['category_name']).lower()
    source = str(row['order_source']).lower()
    
    # Workshops / Classes
    # Ensure we don't capture "classic" when looking for "class"
    if 'workshop' in name or 'seminar' in name or ('class' in name and 'classic' not in name):
        if 'book' not in name: # Exclude books about workshops
             return 'Workshops'
            
    # Services
    if 'boarding' in name or 'repotting service' in name or 'consult' in name:
        return 'Services'
    
    # Retail
    if source in ['manual', 'pos']: # Assuming 'manual' is Retail/Phone
        return 'Retail'
        
    # .Com (Default)
    return '.Com'

def analyze():
    print("Fetching data...")
    df_bc = get_bc_data()
    df_ws = get_wholesale_data()
    df_amz = get_amazon_data()
    
    print(f"BC Rows: {len(df_bc)}")
    print(f"Wholesale Rows: {len(df_ws)}")
    print(f"Amazon Rows: {len(df_amz)}")
    
    # 1. Classify BC Data
    df_bc['channel'] = df_bc.apply(classify_bc_row, axis=1)
    
    # 3. Process Wholesale
    # Fetch Master Product Data for better mapping (Cost & Category)
    print("Fetching master product data...")
    product_query = f"""
        SELECT 
            p.sku, 
            p.cost_price, 
            vc.category_name 
        FROM `{PROJECT_ID}.{SALES_DATASET}.bc_product` p
        LEFT JOIN (
            SELECT product_id, STRING_AGG(category_name, ', ' ORDER BY category_id LIMIT 1) as category_name
            FROM `{PROJECT_ID}.{SALES_DATASET}.bc_product_category`
            GROUP BY 1
        ) vc ON p.product_id = vc.product_id
    """
    df_products = client.query(product_query).to_dataframe()
    
    # Normalize to lower case for mapping
    df_products['sku'] = df_products['sku'].str.lower()
    sku_costs = df_products.set_index('sku')['cost_price'].to_dict()
    sku_cats = df_products.set_index('sku')['category_name'].fillna('Uncategorized').to_dict()
    
    # 2. Process BC Data 
    df_bc['cost_price'] = pd.to_numeric(df_bc['sku'].str.lower().map(sku_costs), errors='coerce').fillna(0)
    df_bc['category_name'] = df_bc['sku'].str.lower().map(sku_cats).fillna('Uncategorized') 
    df_bc['revenue'] = pd.to_numeric(df_bc['revenue'], errors='coerce').fillna(0)
    df_bc['quantity'] = pd.to_numeric(df_bc['quantity'], errors='coerce').fillna(0)
    df_bc['cogs'] = df_bc['quantity'] * df_bc['cost_price']
    df_bc['margin'] = df_bc['revenue'] - df_bc['cogs']

    # 3. Process Wholesale
    df_ws['cost_price'] = pd.to_numeric(df_ws['sku'].str.lower().map(sku_costs), errors='coerce').fillna(0)
    df_ws['category_name'] = df_ws['sku'].str.lower().map(sku_cats).fillna('Uncategorized')
    df_ws['revenue'] = pd.to_numeric(df_ws['revenue'], errors='coerce').fillna(0)
    df_ws['quantity'] = pd.to_numeric(df_ws['quantity'], errors='coerce').fillna(0)
    df_ws['cogs'] = df_ws['quantity'] * df_ws['cost_price']
    df_ws['margin'] = df_ws['revenue'] - df_ws['cogs']
    
    # 4. Process Amazon
    # Amazon Margin = Net Proceeds? Or Revenue - COGS - Ad Spend?
    # "Net Proceeds" in Amazon reports usually accounts for Amazon fees but not COGS or Ad Spend (sometimes).
    # Let's check the schema definition if I could, but usually `net_proceeds` = Sales - Fees.
    # Margin = Net Proceeds - COGS.
    # Ad Spend is separate.
    # We will map COGS using SKU.
    df_amz['cost_price'] = pd.to_numeric(df_amz['sku'].str.lower().map(sku_costs), errors='coerce').fillna(0)
    df_amz['category_name'] = df_amz['sku'].str.lower().map(sku_cats).fillna('Uncategorized')
    df_amz['revenue'] = pd.to_numeric(df_amz['revenue'], errors='coerce').fillna(0)
    df_amz['quantity'] = pd.to_numeric(df_amz['quantity'], errors='coerce').fillna(0)
    df_amz['net_proceeds'] = pd.to_numeric(df_amz['net_proceeds'], errors='coerce').fillna(0)
    df_amz['ad_spend'] = pd.to_numeric(df_amz['ad_spend'], errors='coerce').fillna(0)
    
    # Estimate Amazon Margin
    # Margin $ = Net Proceeds - (Qty * Cost) - Ad Spend (if we want contribution margin) 
    # Or just Gross Margin = Revenue - Cost.
    # Prompt asks "drove margin dollars". Usually this means contribution margin (money in pocket).
    # For Amazon: Net Proceeds - COGS is a good proxy for "Contribution before Ads".
    # I'll use: Amazon Margin = Net Proceeds - (Quantity * Cost)
    df_amz['cogs'] = df_amz['quantity'] * df_amz['cost_price']
    df_amz['margin'] = df_amz['net_proceeds'] - df_amz['cogs'] 
    
    df_amz['channel'] = 'Amazon'
    
    # Combine all into one summary DF for ease
    # Cols: date, channel, product_name, sku, revenue, quantity, margin, ad_spend (only amz), category_name
    
    cols = ['date', 'channel', 'product_name', 'sku', 'revenue', 'quantity', 'margin', 'category_name']
    
    combined = pd.concat([
        df_bc[cols],
        df_ws[cols],
        df_amz[cols] # Ad spend ignored for now in this stack, handled separately
    ])
    
    combined['year'] = pd.to_datetime(combined['date']).dt.year
    combined['month'] = pd.to_datetime(combined['date']).dt.month
    
    # Filter for January only (though query already did, just safety)
    combined = combined[combined['month'] == 1]
    
    # --- REPORT GENERATION ---
    print("\n\n=== JANUARY REVENUE RECAP ===")
    
    # 1. Revenue Recap by Channel
    total_rev_2026 = combined[combined['year'] == 2026].groupby('channel')['revenue'].sum()
    total_rev_2025 = combined[combined['year'] == 2025].groupby('channel')['revenue'].sum()
    
    summary = pd.DataFrame({
        'Jan 2026': total_rev_2026,
        'Jan 2025': total_rev_2025
    }).fillna(0)
    summary['YoY %'] = ((summary['Jan 2026'] - summary['Jan 2025']) / summary['Jan 2025'] * 100).round(1)
    
    print(summary)
    
    # 2. What moved the needle? (Absolute $ Change)
    summary['Delta $'] = summary['Jan 2026'] - summary['Jan 2025']
    print("\nMovers (Delta $):")
    print(summary.sort_values('Delta $', ascending=False))
    
    # 3. Top Products Driving Margin (2026)
    print("\nTop 5 Margin Drivers (Products) (Jan 2026):")
    top_margin_products = combined[combined['year'] == 2026].groupby(['product_name', 'category_name'])['margin'].sum().reset_index()
    print(top_margin_products.sort_values('margin', ascending=False).head(5))

    # 3b. Top 5 Margin Drivers (Categories) (Jan 2026)
    print("\nTop 5 Margin Drivers (Categories) (Jan 2026):")
    top_margin_cats = combined[combined['year'] == 2026].groupby('category_name')['margin'].sum().reset_index()
    print(top_margin_cats.sort_values('margin', ascending=False).head(5))

    # 3c. Bottom 5 Margin Drivers (Categories) (Jan 2026) -> "Created Noise" or just lost money/lowest
    print("\nBottom 5 Margin Drivers (Categories) (Jan 2026):")
    # Filter out Uncategorized for cleaner list if needed, or keep to show data gaps. 
    # Let's keep all.
    print(top_margin_cats.sort_values('margin', ascending=True).head(5))
    
    # 4. Top "Noise" (High Revenue, Low/Neg Margin)
    print("\nTop 5 Noise Generators (Products with High Rev, Low Margin) (Jan 2026):")
    product_stats = combined[combined['year'] == 2026].groupby(['product_name', 'channel']).agg({
        'revenue': 'sum',
        'margin': 'sum',
        'quantity': 'sum'
    }).reset_index()
    # Define "Noise" as high revenue but margin % < 10% or negative margin
    product_stats['margin_pct'] = (product_stats['margin'] / product_stats['revenue']).fillna(0)
    noise_candidates = product_stats[product_stats['revenue'] > 500] # Min threshold
    print(noise_candidates.sort_values('margin_pct').head(5))
    
    # 5. Ad Efficiency (Quick check if possible)
    print("\nAd Spend (Jan 2026 vs 2025):")
    ad_spend_26 = df_amz[pd.to_datetime(df_amz['date']).dt.year == 2026]['ad_spend'].sum()
    ad_spend_25 = df_amz[pd.to_datetime(df_amz['date']).dt.year == 2025]['ad_spend'].sum()
    print(f"Amazon Ad Spend: 2026=${ad_spend_26:,.2f}, 2025=${ad_spend_25:,.2f}")
    
    # Store results to file if needed or just print
    # I'll rely on the output trace.

if __name__ == "__main__":
    analyze()
