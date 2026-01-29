import json

with open('.tmp/api_response.json', 'r') as f:
    resp = json.load(f)

# The Invoke-RestMethod output seems to have been wrapped or something
# Let's try to find the 'data' key
if isinstance(resp, dict) and 'data' in resp:
    data = resp['data']
elif isinstance(resp, dict) and 'Content' in resp:
    # This happens when Invoke-RestMethod is used without -AsString or when redirected
    content_str = resp['Content']
    content = json.loads(content_str)
    data = content['data']
else:
    print(f"Unexpected JSON structure: {type(resp)}")
    exit(1)

ytd_2026 = [d for d in data if d.get('year') == 2026]
print(f"2026 Rows: {len(ytd_2026)}")
if ytd_2026:
    total_rev = sum(d.get('amazon_revenue', 0) for d in ytd_2026)
    print(f"2026 Total Amazon Revenue: {total_rev}")
    print(f"First Week: {ytd_2026[-1]['week_start']}")
    print(f"Latest Week: {ytd_2026[0]['week_start']}")

ytd_2025 = [d for d in data if d.get('year') == 2025]
print(f"2025 Rows: {len(ytd_2025)}")
if ytd_2025:
    total_rev_2025 = sum(d.get('amazon_revenue', 0) for d in ytd_2025)
    print(f"2025 Total Amazon Revenue: {total_rev_2025}")
