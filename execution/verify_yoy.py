
import requests
import json
from datetime import datetime, timedelta

url = 'http://localhost:5000/api/dashboard'
response = requests.get(url)
data = response.json()['data']

current_year = 2026
ytd_2026 = [d for d in data if d['year'] == current_year]

print(f"2026 YTD Weeks: {len(ytd_2026)}")
total_bonsai_rev_2026 = sum(d['bonsai_revenue'] for d in ytd_2026)
print(f"2026 YTD Bonsai Revenue: ${total_bonsai_rev_2026:,.2f}")

# Simulate YoY logic from dashboard.js (Fuzzy Matching)
prev_period_data = []
for d in ytd_2026:
    current_date = datetime.strptime(d['week_start'], '%Y-%m-%d')
    target_date = current_date - timedelta(days=364)
    
    # Fuzzy search: find match within +/- 3 days
    match = None
    for w in data:
        w_date = datetime.strptime(w['week_start'], '%Y-%m-%d')
        if abs((w_date - target_date).days) <= 3:
            match = w
            break
            
    if match:
        prev_period_data.append(match)
        print(f"Match: {d['week_start']} -> {match['week_start']}")
    else:
        print(f"No match for {d['week_start']} -> target ~{target_date.strftime('%Y-%m-%d')}")

print(f"2025 YTD Weeks Found: {len(prev_period_data)}")
total_bonsai_rev_2025 = sum(d['bonsai_revenue'] for d in prev_period_data)
total_bonsai_orders_2025 = sum(d['bonsai_orders'] for d in prev_period_data)
print(f"2025 YTD Bonsai Revenue (Fuzzy): ${total_bonsai_rev_2025:,.2f}")
print(f"2025 YTD Bonsai Orders (Fuzzy): {total_bonsai_orders_2025}")

