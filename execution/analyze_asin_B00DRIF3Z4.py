
import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
AMAZON_ECON_DATASET = os.getenv('AMAZON_ECON_DATASET', 'amazon_econ')

def analyze_asin(asin):
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
        SELECT 
            EXTRACT(YEAR FROM business_date) as year,
            SUM(gross_sales) as revenue,
            SUM(units) as units,
            SUM(amazon_fees) as fees,
            SUM(ad_spend) as ad_spend,
            SUM(net_proceeds) as net_proceeds
        FROM `{PROJECT_ID}.{AMAZON_ECON_DATASET}.fact_sku_day_us`
        WHERE asin = '{asin}'
        GROUP BY 1
        ORDER BY 1 DESC
    """
    
    print(f"Querying BigQuery for ASIN: {asin}...")
    df = client.query(query).to_dataframe()
    
    if df.empty:
        print("No data found for this ASIN.")
        return

    # Calculate Totals
    total_rev = df['revenue'].sum()
    total_units = df['units'].sum()
    total_fees = df['fees'].sum()
    total_ads = df['ad_spend'].sum()
    total_net = df['net_proceeds'].sum()
    
    # Add a 'Total' row
    total_row = pd.DataFrame([{
        'year': 'Total',
        'revenue': total_rev,
        'units': total_units,
        'fees': total_fees,
        'ad_spend': total_ads,
        'net_proceeds': total_net
    }])
    
    df = pd.concat([df, total_row], ignore_index=True)
    
    # Metrics
    # Profit (Amazon View) = Net Proceeds
    # Net Proceeds / Unit = Net Proceeds / Units
    # (Optional) Contribution Margin = Net Proceeds - COGS (We don't have COGS here yet, just Amazon data)
    
    df['net_proceeds_per_unit'] = df.apply(lambda x: x['net_proceeds'] / x['units'] if x['units'] > 0 else 0, axis=1)
    df['revenue_per_unit'] = df.apply(lambda x: x['revenue'] / x['units'] if x['units'] > 0 else 0, axis=1)
    
    # Formatting
    pd.options.display.float_format = '${:,.2f}'.format
    
    print("\n=== Amazon Financials for ASIN: " + asin + " ===\n")
    print(df[['year', 'units', 'revenue', 'net_proceeds', 'net_proceeds_per_unit', 'revenue_per_unit']].to_string(index=False))

    print("\nNOTE: 'Net Proceeds' is roughly (Revenue - Amazon Fees - Ads). Verify if Ads are included in your Net Proceeds column definition.")
    print("Based on ingest script: net_proceeds comes from Amazon 'netProceeds' field.")
    
if __name__ == "__main__":
    analyze_asin('B00DRIF3Z4')
