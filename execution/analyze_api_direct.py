import requests
import json

try:
    resp = requests.get("http://127.0.0.1:5000/api/dashboard")
    resp.raise_for_status()
    content = resp.json()
    
    print(f"Debug Version: {content.get('debug_version', 'NOT FOUND')}")
    data = content['data']

    ytd_2026 = [d for d in data if d.get('year') == 2026]
    print(f"2026 Rows: {len(ytd_2026)}")
    if ytd_2026:
        total_rev = sum(d.get('amazon_revenue', 0) for d in ytd_2026)
        print(f"2026 Total Amazon Revenue: {total_rev}")
        print(f"Latest Week ({ytd_2026[0]['week_start']}) Rev: {ytd_2026[0]['amazon_revenue']}")

except Exception as e:
    print(f"Error: {e}")
