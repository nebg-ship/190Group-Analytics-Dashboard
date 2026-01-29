
import requests
import json
from datetime import datetime

url = 'http://localhost:5000/api/dashboard'
response = requests.get(url)
data = response.json()['data']

current_year = 2026
ytd_2026 = [d for d in data if d['year'] == current_year]

print(f"2026 YTD Weeks: {len(ytd_2026)}")
total_bonsai_rev_2026 = sum(d['bonsai_revenue'] for d in ytd_2026)
print(f"2026 YTD Bonsai Revenue: ${total_bonsai_rev_2026:,.2f}")

# Simulate YoY logic from dashboard.js
prev_period_data = []
for d in ytd_2026:
    target_date = datetime.strptime(d['week_start'], '%Y-%m-%d')
    # Subtract 364 days
    from datetime import timedelta
    target_str = (target_date - timedelta(days=364)).strftime('%Y-%m-%d')
    
    match = next((w for w in data if w['week_start'] == target_str), None)
    if match:
        prev_period_data.append(match)
    else:
        print(f"No match for {d['week_start']} -> {target_str}")

print(f"2025 YTD Weeks Found: {len(prev_period_data)}")
total_bonsai_rev_2025 = sum(d['bonsai_revenue'] for d in prev_period_data)
print(f"2025 YTD Bonsai Revenue (YoY Shift): ${total_bonsai_rev_2025:,.2f}")
