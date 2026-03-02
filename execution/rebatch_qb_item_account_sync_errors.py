"""
Rebatch qb-item-account-sync error events into smaller chunks.

Usage:
  python execution/rebatch_qb_item_account_sync_errors.py --prod --batch-size 25
  python execution/rebatch_qb_item_account_sync_errors.py --prod --batch-size 25 --dry-run
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any, TypeVar


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
    raise RuntimeError("Convex command did not return parseable JSON object.")


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


T = TypeVar("T")


def chunked(values: list[T], size: int) -> list[list[T]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebatch qb-item-account-sync error events into smaller chunks.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Lines per new event.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Max error events to inspect.",
    )
    parser.add_argument(
        "--created-by",
        default="qb-item-account-sync",
        help="createdBy prefix to filter.",
    )
    parser.add_argument(
        "--error-code",
        default="0x80040400",
        help="QB error code to filter.",
    )
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--prod", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--no-void-original",
        action="store_true",
        help="Do not void original error events.",
    )
    args = parser.parse_args()

    if args.batch_size < 1:
        raise RuntimeError("--batch-size must be >= 1")
    if args.limit < 1:
        raise RuntimeError("--limit must be >= 1")

    payload = convex_run(
        "inventory:listErrorEvents",
        {"limit": args.limit},
        env_file=args.env_file,
        run_prod=args.prod,
    )
    rows = payload.get("rows", [])
    created_by_prefix = args.created_by.strip().casefold()
    error_code_filter = args.error_code.strip()

    candidates = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        created_by = str(row.get("createdBy") or "").casefold()
        if created_by_prefix and not created_by.startswith(created_by_prefix):
            continue
        if error_code_filter and str(row.get("qbErrorCode") or "") != error_code_filter:
            continue
        candidates.append(row)

    if not candidates:
        print(json.dumps({"status": "noop", "message": "No matching error events found."}, indent=2))
        return

    dry_run = args.dry_run
    void_original = not args.no_void_original

    summary_events = []
    created_event_ids: list[str] = []
    voided_event_ids: list[str] = []

    for event in candidates:
        event_id = str(event.get("eventId") or "")
        effective_date = str(event.get("effectiveDate") or "").strip()
        created_by = str(event.get("createdBy") or "").strip()
        lines = event.get("lines", []) if isinstance(event.get("lines"), list) else []
        if not event_id or not lines:
            continue

        # Group by locationId so we don't mix locations in a single adjustment.
        by_location: dict[str, list[dict[str, Any]]] = {}
        for line in lines:
            if not isinstance(line, dict):
                continue
            location_id = str(line.get("locationId") or "").strip()
            if not location_id:
                continue
            sku = str(line.get("sku") or "").strip()
            if not sku:
                continue
            by_location.setdefault(location_id, []).append(
                {
                    "sku": sku,
                    "newQty": line.get("newQty"),
                }
            )

        for location_id, location_lines in by_location.items():
            chunks = chunked(location_lines, args.batch_size)
            summary_events.append(
                {
                    "eventId": event_id,
                    "locationId": location_id,
                    "lineCount": len(location_lines),
                    "chunkCount": len(chunks),
                }
            )

            if dry_run:
                continue

            for index, chunk in enumerate(chunks, start=1):
                result = convex_run(
                    "inventory:createAdjustmentEvent",
                    {
                        "effectiveDate": effective_date,
                        "locationId": location_id,
                        "mode": "set",
                        "memo": f"QB item account sync rebatch {event_id} - {index}/{len(chunks)}",
                        "createdBy": created_by or "qb-item-account-sync",
                        "reasonCode": "qb_account_sync",
                        "lines": chunk,
                    },
                    env_file=args.env_file,
                    run_prod=args.prod,
                )
                created_event_ids.append(str(result.get("eventId") or ""))

        if void_original and not dry_run:
            try:
                convex_run(
                    "inventory:voidEvent",
                    {"eventId": event_id},
                    env_file=args.env_file,
                    run_prod=args.prod,
                )
                voided_event_ids.append(event_id)
            except Exception as exc:
                print(
                    json.dumps(
                        {
                            "status": "warning",
                            "message": f"Failed to void {event_id}: {exc}",
                        }
                    )
                )

    print(
        json.dumps(
            {
                "status": "ok",
                "dryRun": dry_run,
                "rebatchPlan": summary_events,
                "createdEventIds": created_event_ids,
                "voidedEventIds": voided_event_ids,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
