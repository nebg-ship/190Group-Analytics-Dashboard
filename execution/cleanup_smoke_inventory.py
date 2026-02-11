"""
Clean up smoke-test inventory data from Convex.

By default this removes records created by the smoke harness:
- SKU prefix: SMOKE-SKU-
- Location code prefix: SMOKE_
- createdBy: smoke-test

Usage:
  python execution/cleanup_smoke_inventory.py --dry-run
  python execution/cleanup_smoke_inventory.py
  python execution/cleanup_smoke_inventory.py --env-file .env.local
"""

from __future__ import annotations

import argparse
import json
import subprocess
from typing import Any, Dict


def parse_last_json(stdout: str) -> Dict[str, Any] | None:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue

    start = stdout.find("{")
    end = stdout.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        return json.loads(stdout[start : end + 1])
    except json.JSONDecodeError:
        return None


def run_cleanup(payload: Dict[str, Any], env_file: str | None) -> Dict[str, Any]:
    cmd = [
        "cmd",
        "/c",
        "npx",
        "convex",
        "run",
        "--typecheck",
        "disable",
        "--codegen",
        "disable",
    ]
    if env_file:
        cmd.extend(["--env-file", env_file])
    cmd.extend(["inventory:cleanupSmokeData", json.dumps(payload)])

    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )
    parsed = parse_last_json(proc.stdout)
    if parsed is None:
        raise RuntimeError(
            "Failed to parse cleanup response from Convex.\n"
            f"Command: {' '.join(cmd)}\n"
            f"Return code: {proc.returncode}\n"
            f"STDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}"
        )

    return {
        "result": parsed,
        "returncode": proc.returncode,
        "stderr": proc.stderr.strip(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete smoke-test inventory records from Convex.")
    parser.add_argument("--sku-prefix", default="SMOKE-SKU-", help="SKU prefix to target.")
    parser.add_argument(
        "--location-code-prefix",
        default="SMOKE_",
        help="Location code prefix to target.",
    )
    parser.add_argument(
        "--created-by",
        default="smoke-test",
        help="createdBy value on events to target.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional env file for Convex CLI deployment selection.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report what would be deleted.",
    )
    args = parser.parse_args()

    payload = {
        "skuPrefix": args.sku_prefix,
        "locationCodePrefix": args.location_code_prefix,
        "createdBy": args.created_by,
        "dryRun": args.dry_run,
    }

    response = run_cleanup(payload, args.env_file)
    print(json.dumps(response["result"], indent=2))

    # Convex CLI currently exits non-zero in this environment because of a
    # Windows assertion after successful output. Surface as warning, not failure.
    if response["returncode"] != 0 and response["stderr"]:
        print(
            "\nWARNING: Convex CLI returned a non-zero exit code after returning JSON.\n"
            f"CLI stderr:\n{response['stderr']}"
        )


if __name__ == "__main__":
    main()
