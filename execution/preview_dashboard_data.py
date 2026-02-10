
import requests
import json
import pandas as pd

def preview_dashboard():
    url = "http://localhost:5000/api/dashboard"
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            return
        
        result = response.json()
        if not result.get('success'):
            print(f"API Error: {result.get('error')}")
            return
            
        data = result.get('data', [])
        if not data:
            print("No data returned from API.")
            return
            
        # Select key metrics for preview
        preview_cols = [
            'week_start', 'bonsai_revenue', 'amazon_revenue', 
            'bonsai_sessions', 'amazon_sessions', 'total_company_revenue'
        ]
        
        df = pd.DataFrame(data)
        # Take the top 5 rows (most recent weeks)
        preview_df = df[preview_cols].head(5).copy()
        
        # Formatting for display
        preview_df['bonsai_revenue'] = preview_df['bonsai_revenue'].apply(lambda x: f"${x:,.2f}")
        preview_df['amazon_revenue'] = preview_df['amazon_revenue'].apply(lambda x: f"${x:,.2f}")
        preview_df['total_company_revenue'] = preview_df['total_company_revenue'].apply(lambda x: f"${x:,.2f}")
        preview_df['bonsai_sessions'] = preview_df['bonsai_sessions'].apply(lambda x: f"{int(x):,}" if x else "0")
        preview_df['amazon_sessions'] = preview_df['amazon_sessions'].apply(lambda x: f"{int(x):,}" if x else "0")
        
        # Custom Basic Formatting
        header = f"{'Week Start':<12} | {'Bonsai Rev':<12} | {'Amazon Rev':<12} | {'Bonsai Sess':<12} | {'Amz Sess':<10} | {'Total Rev':<12}"
        print("\n" + "="*85)
        print("DASHBOARD DATA PREVIEW (LAST 5 WEEKS)")
        print("="*85)
        print(header)
        print("-" * 85)
        for _, row in preview_df.iterrows():
            print(f"{row['week_start']:<12} | {row['bonsai_revenue']:<12} | {row['amazon_revenue']:<12} | {row['bonsai_sessions']:<12} | {row['amazon_sessions']:<10} | {row['total_company_revenue']:<12}")
        print("="*85 + "\n")
        
    except Exception as e:
        print(f"Connection Error: {e}")

if __name__ == "__main__":
    preview_dashboard()
