import requests
import json

URL = "http://localhost:5000/api/dashboard"

try:
    print(f"Fetching data from {URL}...")
    response = requests.get(URL)
    response.raise_for_status()
    result = response.json()
    
    if result.get('success'):
        data = result.get('data', [])
        if data:
            latest = data[0]
            print("\nLatest Week Metrics (First Row):")
            print(f"Week Start:     {latest.get('week_start')}")
            print(f"Amazon Spend:   ${latest.get('amazon_ad_spend', 0):.2f}")
            print(f"Google Spend:   ${latest.get('google_ad_spend', 0):.2f}")
            print(f"Total Ad Spend: ${latest.get('total_ad_spend', 0):.2f}")
            
            # Check if total sums up
            expected_total = latest.get('amazon_ad_spend', 0) + latest.get('google_ad_spend', 0)
            print(f"Calculation Check: {latest.get('total_ad_spend', 0):.2f} vs {expected_total:.2f}")
            
            if abs(latest.get('total_ad_spend', 0) - expected_total) < 0.01:
                print("\nSUCCESS: API is returning correctly aggregated ad spend.")
            else:
                print("\nWARNING: Total ad spend does not match sum of parts in API.")
        else:
            print("\nERROR: No data rows returned in API response.")
    else:
        print(f"\nAPI Error: {result.get('error')}")

except Exception as e:
    print(f"\nFailed to connect to API: {e}")
    print("Ensure the dashboard API is running (python api/dashboard_data.py)")
