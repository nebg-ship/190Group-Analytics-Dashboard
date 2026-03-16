
import os
from pathlib import Path

PROJECT_ROOT = Path(r"f:\Vibe Code Projects\190Group Analytics Dashboard")
file_path = PROJECT_ROOT / 'api' / 'dashboard_data.py'

content = file_path.read_text(encoding='utf-8')
# Find the query starting at line 111 (approx)
# It's inside get_dashboard_data
start_marker = "-- CEO Dashboard: Combined Amazon + Bonsai Outlet Metrics"
start_idx = content.find(start_marker)
if start_idx != -1:
    query_content = content[start_idx:]
    # Find the end of the query string (the next """)
    end_marker = '"""'
    end_idx = query_content.find(end_marker)
    if end_idx != -1:
        query = query_content[:end_idx]
        lines = query.split('\n')
        for i, line in enumerate(lines, 1):
            print(f"{i:3}: {line}")
else:
    print("Could not find query start marker")
