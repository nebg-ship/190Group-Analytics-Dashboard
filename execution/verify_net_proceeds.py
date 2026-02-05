import requests
import json

API_URL = "http://127.0.0.1:5000/api/dashboard"

try:
    response = requests.get(API_URL)
    data = response.json().get('data', [])
    
    if not data:
        print("No data found")
        exit(1)
        
    latest = data[0]
    print(f"Latest Week: {latest.get('week_start')}")
    print(f"Amazon Revenue:      ${latest.get('amazon_revenue', 0):,.2f}")
    print(f"Amazon Net Proceeds: ${latest.get('amazon_net_proceeds', 0):,.2f}")
    
    ytd_data = [d for d in data if d.get('year') == 2026]
    ytd_rev = sum(d.get('amazon_revenue', 0) for d in ytd_data)
    ytd_net = sum(d.get('amazon_net_proceeds', 0) for d in ytd_data)
    
    print(f"\n2026 YTD Amazon Revenue: ${ytd_rev:,.2f}")
    print(f"2026 YTD Amazon Net:     ${ytd_net:,.2f}")
    
    if ytd_net > 0:
        print("\n[OK] Verification Successful: Amazon Net Proceeds data is present and greater than zero.")
    else:
        print("\n[FAIL] Verification Failed: Amazon Net Proceeds data is missing or zero.")
        
except Exception as e:
    print(f"Error: {e}")
