"""
Void duplicate qb-item-account-sync events by memo, keeping the newest per memo.

Usage:
  python execution/void_duplicate_item_account_sync.py --prod
  python execution/void_duplicate_item_account_sync.py --prod --dry-run
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def parse_last_json(stdout: str) -> Any:
    text = (stdout or "").strip()
    if not text:
        raise RuntimeError("Convex command returned empty stdout.")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    for open_char, close_char in (("{", "}"), ("[", "]")):
        start = text.find(open_char)
        end = text.rfind(close_char)
        if start != -1 and end > start:
            return json.loads(text[start : end + 1])
    raise RuntimeError("Convex command did not return parseable JSON.")


def convex_run(
    function_name: str,
    args_obj: dict[str, Any],
    *,
    env_file: str | None,
    run_prod: bool,
) -> Any:
    cmd = [
        "node",
        str(PROJECT_ROOT / "node_modules" / "convex" / "bin" / "main.js"),
        "run",
        "--typecheck",
        "disable",
        "--codegen",
        "disable",
    ]
    if env_file:
        cmd.extend(["--env-file", env_file])
    if run_prod:
        cmd.append("--prod")
    cmd.extend([function_name, json.dumps(args_obj)])

    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    parsed = parse_last_json(proc.stdout)
    if proc.returncode != 0 and proc.stderr.strip():
        print(
            "WARNING: Convex CLI returned non-zero after JSON output.\n"
            f"CLI stderr:\n{proc.stderr.strip()}\n"
        )
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Void duplicate qb-item-account-sync events by memo.",
    )
    parser.add_argument(
        "--created-by",
        default="qb-item-account-sync",
        help="createdBy value to target.",
    )
    parser.add_argument(
        "--status",
        default="pending",
        help="QB status to target (default: pending).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Max events to inspect.",
    )
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--prod", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.limit < 1:
        raise RuntimeError("--limit must be >= 1")

    payload = convex_run(
        "inventory:listEventsByCreatedBy",
        {
            "createdBy": args.created_by,
            "statuses": [args.status],
            "limit": args.limit,
        },
        env_file=args.env_file,
        run_prod=args.prod,
    )
    rows = payload.get("rows", [])
    if not isinstance(rows, list) or not rows:
        print(json.dumps({"status": "noop", "message": "No matching events found."}, indent=2))
        return

    by_memo: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        memo = str(row.get("memo") or "")
        by_memo.setdefault(memo, []).append(row)

    to_void: list[dict[str, Any]] = []
    for memo, group in by_memo.items():
        if len(group) < 2:
            continue
        group_sorted = sorted(
            group,
            key=lambda r: int(r.get("createdAt") or 0),
            reverse=True,
        )
        # Keep newest; void the rest.
        to_void.extend(group_sorted[1:])

    voided: list[str] = []
    for row in to_void:
        event_id = str(row.get("eventId") or "")
        if not event_id:
            continue
        if args.dry_run:
            voided.append(event_id)
            continue
        convex_run(
            "inventory:voidEvent",
            {"eventId": event_id},
            env_file=args.env_file,
            run_prod=args.prod,
        )
        voided.append(event_id)

    print(
        json.dumps(
            {
                "status": "ok",
                "dryRun": args.dry_run,
                "candidateCount": len(rows),
                "uniqueMemos": len(by_memo),
                "duplicatesFound": len(to_void),
                "voidedEventIds": voided,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
