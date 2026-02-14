
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
        "--push", # Ensure code is pushed
        function_name,
        json.dumps(args_obj)
    ]
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True) # Removed shell=True
    try:
        lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]

        for line in reversed(lines):
            if (line.startswith("{") and line.endswith("}")) or (line.startswith("[") and line.endswith("]")):
                return json.loads(line)

    except:
        pass
    
    print("STDOUT:", proc.stdout)
    print("STDERR:", proc.stderr)
    return None

def debug():
    # Test with a few mixed case SKUs
    test_skus = ["jp1529-gr-la", "jp1529-GR-LA", "JP1529-GR-LA"]
    print(f"Testing SKUs: {test_skus}")
    
    args = {"skus": test_skus}
    payload = convex_run("inventory:getPartQuantitiesBySkus", args)
    
    print("Result:", json.dumps(payload, indent=2))

if __name__ == "__main__":
    debug()
