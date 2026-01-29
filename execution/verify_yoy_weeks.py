
import requests
import json
from datetime import datetime, timedelta

url = 'http://localhost:5000/api/dashboard'
response = requests.get(url)
data = response.json()['data']

current_year = 2026
ytd_2026 = [d for d in data if d['year'] == current_year]

print("2026 Weeks vs 2025 Matches:")
for d in ytd_2026:
    target_date = datetime.strptime(d['week_start'], '%Y-%m-%d')
    target_str = (target_date - timedelta(days=364)).strftime('%Y-%m-%d')
    match = next((w for w in data if w['week_start'] == target_str), None)
    match_rev = match['bonsai_revenue'] if match else "MISSING"
    print(f"2026: {d['week_start']} (${d['bonsai_revenue']:,.2f}) -> 2025: {target_str} (${match_rev})")
