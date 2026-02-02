import requests
import json

API_URL = "http://127.0.0.1:5000/api/dashboard"

try:
    response = requests.get(API_URL)
    result = response.json()
    
    if not result.get('success'):
        print(f"API Error: {result.get('error')}")
        exit(1)
    
    data = result.get('data', [])
    if not data:
        print("No data found")
        exit(1)
        
    latest = data[0]
    print(f"Latest Week: {latest.get('week_start')}")
    print("\n--- Bonsai (GA4) Metrics ---")
    print(f"Bonsai Sessions: {latest.get('bonsai_sessions') or 0:,}")
    print(f"Bonsai Users:    {latest.get('bonsai_users') or 0:,}")
    print(f"Bonsai CVR:      {latest.get('bonsai_cvr') or 0:.2f}%")
    
    print("\n--- Amazon Metrics ---")
    print(f"Amazon Sessions: {latest.get('amazon_sessions') or 0:,}")
    print(f"Amazon CVR:      {latest.get('amazon_cvr') or 0:.2f}%")
    
    # YTD totals for 2026
    ytd_data = [d for d in data if d.get('year') == 2026]
    ytd_bonsai_sessions = sum(d.get('bonsai_sessions', 0) for d in ytd_data)
    ytd_amazon_sessions = sum(d.get('amazon_sessions', 0) for d in ytd_data)
    
    print("\n--- 2026 YTD ---")
    print(f"Bonsai Sessions YTD: {ytd_bonsai_sessions:,}")
    print(f"Amazon Sessions YTD: {ytd_amazon_sessions:,}")
    
    # Check 2025 (Historical)
    ytd_2025 = [d for d in data if d.get('year') == 2025]
    sessions_2025 = sum(d.get('bonsai_sessions', 0) for d in ytd_2025)
    print("\n--- 2025 Data ---")
    print(f"Bonsai Sessions 2025 Total: {sessions_2025:,}")
    
    if sessions_2025 > 0:
        print("\nVerification Successful: 2025 GA4 data is now present.")
    else:
        print("\nVerification Warning: 2025 GA4 session data is zero or missing.")
        
except Exception as e:
    print(f"Error: {e}")
