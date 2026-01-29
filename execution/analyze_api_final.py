import requests
import json

try:
    resp = requests.get("http://127.0.0.1:5000/api/dashboard")
    resp.raise_for_status()
    content = resp.json()
    data = content['data']

    ytd_2026 = [d for d in data if d.get('year') == 2026]
    print(f"2026 Rows: {len(ytd_2026)}")
    if ytd_2026:
        total_amazon_rev = sum(d.get('amazon_revenue', 0) for d in ytd_2026)
        total_bonsai_rev = sum(d.get('bonsai_revenue', 0) for d in ytd_2026)
        total_bonsai_orders = sum(d.get('bonsai_orders', 0) for d in ytd_2026)
        
        print(f"2026 Total Amazon Revenue (Gross): {total_amazon_rev}")
        print(f"2026 Total Bonsai Revenue: {total_bonsai_rev}")
        print(f"2026 Total Bonsai Orders: {total_bonsai_orders}")

except Exception as e:
    print(f"Error: {e}")
