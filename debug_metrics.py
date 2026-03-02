import requests
from datetime import datetime, timedelta

def find_prior_year_week(data, week_start_str):
    # JS logic: target = current - 364 days
    current_date = datetime.strptime(week_start_str, "%Y-%m-%d")
    target_date = current_date - timedelta(days=364)
    
    for item in data:
        item_date = datetime.strptime(item['week_start'], "%Y-%m-%d")
        if abs((item_date - target_date).total_seconds()) <= 3 * 24 * 3600:
            return item
    return None

def main():
    r = requests.get('http://localhost:5000/api/dashboard')
    data = r.json()['data']
    
    # Filter for YTD 2026 (current logic in dashboard.js)
    current_year = 2026
    selected_data = [row for row in data if row['year'] == current_year]
    
    # Get comparison data (prior year mode)
    comparison_data = []
    for item in selected_data:
        py_week = find_prior_year_week(data, item['week_start'])
        if py_week:
            comparison_data.append(py_week)
            
    # Calculate head-line metrics for Online Storefront (bonsai_revenue)
    current_bonsai = sum(row.get('bonsai_revenue', 0) for row in selected_data)
    previous_bonsai = sum(row.get('bonsai_revenue', 0) for row in comparison_data)
    delta = current_bonsai - previous_bonsai
    
    print(f"Current Year (2026) Bonsai YTD: ${current_bonsai:,.2f}")
    print(f"Prior Year (2025 matched) Bonsai YTD: ${previous_bonsai:,.2f}")
    print(f"Delta: ${delta:,.2f}")
    
    # Just in case they are looking at specific weeks
    print("\nWeekly Detail (2026 -> 2025):")
    for item in selected_data:
        py_week = find_prior_year_week(data, item['week_start'])
        py_val = py_week.get('bonsai_revenue', 0) if py_week else 0
        cur_val = item.get('bonsai_revenue', 0)
        print(f"Week {item['week_start']}: ${cur_val:,.2f} | PY {py_week['week_start'] if py_week else 'N/A'}: ${py_val:,.2f} | Diff: ${(cur_val-py_val):,.2f}")

if __name__ == "__main__":
    main()
