
import os
import json
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def convex_run(function_name, args_obj):
    cmd = [
        "node",
        str(PROJECT_ROOT / "node_modules" / "convex" / "bin" / "main.js"),
        "run",
        function_name,
        json.dumps(args_obj)
    ]
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, shell=True)
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except:
                continue
    return None

def debug():
    print("Fetching inventory subset...")
    # Fetch a chunk and look for our SKUs
    # inventory:listPartQuantities supports paging but we used a high limit.
    # Let's verify what we get.
    payload = convex_run("inventory:listPartQuantities", {"limit": 50000})
    if not payload:
        print("No payload")
        return

    rows = payload.get('rows', [])
    print(f"Fetched {len(rows)} rows.")
    
    target = 'jp1529-gr-la'
    found = False
    for r in rows:
        sku = r.get('sku', '')
        if target.lower() in sku.lower():
            print(f"FOUND MATCH: {sku} -> Qty2025: {r.get('quantityOnHand2025')}")
            found = True
            
    if not found:
        print(f"Target {target} NOT FOUND in {len(rows)} rows.")
        # Print a few samples
        print("Samples:", [r.get('sku') for r in rows[:5]])

if __name__ == "__main__":
    debug()
