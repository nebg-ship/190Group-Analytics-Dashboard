"""
Continuously poll Convex queue summary counts and append snapshots to a JSONL log.

Usage:
  python execution/watch_qb_queue_counts.py --prod
  python execution/watch_qb_queue_counts.py --prod --interval-seconds 20 --output .tmp/qb_queue_counts_live.jsonl
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def get_queue_summary(run_prod: bool) -> dict[str, Any]:
    cmd = [
        "node",
        str(PROJECT_ROOT / "node_modules" / "convex" / "bin" / "main.js"),
        "run",
        "--typecheck",
        "disable",
        "--codegen",
        "disable",
    ]
    if run_prod:
        cmd.append("--prod")
    cmd.extend(["inventory:getQueueSummary", json.dumps({"recentLimit": 5})])

    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    if proc.returncode != 0:
        return {
            "ok": False,
            "error": proc.stderr.strip() or f"convex run failed rc={proc.returncode}",
        }

    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {"ok": False, "error": "convex run returned non-JSON stdout"}

    return {
        "ok": True,
        "counts": payload.get("counts", {}),
        "totalEvents": payload.get("totalEvents"),
        "generatedAt": payload.get("generatedAt"),
    }


def append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as outfile:
        outfile.write(json.dumps(obj, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch QB queue summary counts and log snapshots.")
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Query production deployment (`convex run --prod`).",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=20,
        help="Polling interval in seconds.",
    )
    parser.add_argument(
        "--output",
        default=".tmp/qb_queue_counts_live.jsonl",
        help="JSONL output path.",
    )
    parser.add_argument(
        "--max-polls",
        type=int,
        default=0,
        help="Optional max polls; 0 means run forever.",
    )
    args = parser.parse_args()

    if args.interval_seconds < 1:
        raise RuntimeError("--interval-seconds must be >= 1")
    if args.max_polls < 0:
        raise RuntimeError("--max-polls must be >= 0")

    out_path = Path(args.output)
    poll_count = 0

    while True:
        now = dt.datetime.now(dt.UTC).isoformat()
        summary = get_queue_summary(run_prod=args.prod)
        row = {"ts": now, **summary}
        append_jsonl(out_path, row)
        print(json.dumps(row))

        poll_count += 1
        if args.max_polls > 0 and poll_count >= args.max_polls:
            return
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
