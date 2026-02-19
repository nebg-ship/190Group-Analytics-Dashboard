"""
Queue QB quantity set-sync events from parity mismatch output.

This script reads a parity mismatch CSV and enqueues `set` adjustments for SKUs
with `quantity` mismatches, using Convex `Quantity_On_Hand_2025` values.

Default location assignment matches opening-balance seed rules:
- SKUs starting with WEB -> BELLINGHAM
- all others -> OKEECHOBEE

Usage:
  python execution/queue_qb_quantity_sync_from_parity.py --prod --dry-run
  python execution/queue_qb_quantity_sync_from_parity.py --prod
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
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
    push: bool,
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
    if push:
        cmd.append("--push")
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


def find_latest_mismatch_csv() -> Path:
    candidates = sorted(
        (PROJECT_ROOT / ".tmp").glob("qb_parity_mismatches_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise RuntimeError("No qb_parity_mismatches_*.csv found in .tmp.")
    return candidates[0]


def normalize_code(value: str) -> str:
    return value.strip().upper()


def chunked(values: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def parse_float(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(parsed):
        return 0.0
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description="Queue QB quantity sync events from parity mismatch CSV.")
    parser.add_argument(
        "--mismatch-csv",
        default=None,
        help="Path to qb_parity_mismatches_*.csv. Defaults to latest in .tmp.",
    )
    parser.add_argument(
        "--effective-date",
        default="2026-01-01",
        help="Effective date for queued events (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--created-by",
        default="qb-parity-quantity-sync",
        help="createdBy marker for queued quantity sync events.",
    )
    parser.add_argument(
        "--memo-prefix",
        default="QB parity quantity sync",
        help="Memo prefix for queued events.",
    )
    parser.add_argument(
        "--reason-code",
        default="opening_balance",
        help="Reason code for queued events.",
    )
    parser.add_argument(
        "--web-prefix",
        default="WEB",
        help="Prefix mapped to Bellingham location.",
    )
    parser.add_argument("--bellingham-code", default="BELLINGHAM")
    parser.add_argument("--okeechobee-code", default="OKEECHOBEE")
    parser.add_argument("--chunk-size", type=int, default=100)
    parser.add_argument("--parts-limit", type=int, default=20000)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--prod", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--push-first", action="store_true")
    args = parser.parse_args()

    if args.chunk_size < 1:
        raise RuntimeError("--chunk-size must be >= 1")
    if args.parts_limit < 1:
        raise RuntimeError("--parts-limit must be >= 1")

    mismatch_path = Path(args.mismatch_csv) if args.mismatch_csv else find_latest_mismatch_csv()
    if not mismatch_path.exists():
        raise RuntimeError(f"Mismatch CSV not found: {mismatch_path}")

    with mismatch_path.open("r", encoding="utf-8-sig", newline="") as infile:
        rows = list(csv.DictReader(infile))

    target_skus = sorted(
        {
            str(row.get("sku") or "").strip()
            for row in rows
            if str(row.get("field") or "").strip() == "quantity"
            and str(row.get("sku") or "").strip()
        }
    )
    if not target_skus:
        raise RuntimeError("No SKUs with `quantity` mismatches found in mismatch CSV.")

    push_next = args.push_first
    locations_payload = convex_run(
        "inventory:listLocations",
        {"includeInactive": True},
        env_file=args.env_file,
        push=push_next,
        run_prod=args.prod,
    )
    push_next = False
    if not isinstance(locations_payload, dict):
        raise RuntimeError(f"Unexpected location payload: {type(locations_payload)!r}")
    location_rows = locations_payload.get("rows", [])
    if not isinstance(location_rows, list):
        raise RuntimeError("Unexpected location rows payload.")

    location_by_code: dict[str, dict[str, Any]] = {}
    for row in location_rows:
        if not isinstance(row, dict):
            continue
        code = normalize_code(str(row.get("code") or ""))
        if code:
            location_by_code[code] = row

    bellingham_code = normalize_code(args.bellingham_code)
    okeechobee_code = normalize_code(args.okeechobee_code)
    bellingham = location_by_code.get(bellingham_code)
    okeechobee = location_by_code.get(okeechobee_code)
    if not bellingham:
        raise RuntimeError(f"Location code not found: {bellingham_code}")
    if not okeechobee:
        raise RuntimeError(f"Location code not found: {okeechobee_code}")

    parts_payload = convex_run(
        "inventory:listPartQuantities",
        {"includeInactive": True, "limit": args.parts_limit},
        env_file=args.env_file,
        push=False,
        run_prod=args.prod,
    )
    if not isinstance(parts_payload, dict):
        raise RuntimeError(f"Unexpected part payload: {type(parts_payload)!r}")
    part_rows = parts_payload.get("rows", [])
    if not isinstance(part_rows, list):
        raise RuntimeError("Unexpected part rows payload.")

    target_lookup = set(target_skus)
    quantity_by_sku: dict[str, float] = {}
    negative_skus: list[str] = []
    for row in part_rows:
        if not isinstance(row, dict):
            continue
        sku = str(row.get("sku") or "").strip()
        if not sku or sku not in target_lookup:
            continue
        qty = parse_float(row.get("quantityOnHand2025", 0))
        if qty < 0:
            negative_skus.append(sku)
            continue
        quantity_by_sku[sku] = qty

    missing_in_parts = sorted(sku for sku in target_skus if sku not in quantity_by_sku)
    if negative_skus:
        raise RuntimeError(
            "Negative quantityOnHand2025 detected for one or more SKUs. "
            f"First 10: {', '.join(sorted(negative_skus)[:10])}"
        )
    if missing_in_parts:
        raise RuntimeError(
            "Some quantity-mismatch SKUs were not found in inventory:listPartQuantities. "
            f"First 10: {', '.join(missing_in_parts[:10])}"
        )

    web_prefix = args.web_prefix.strip().upper()
    lines_bellingham: list[dict[str, Any]] = []
    lines_okeechobee: list[dict[str, Any]] = []
    preview_rows: list[dict[str, Any]] = []
    for sku in target_skus:
        qty = quantity_by_sku[sku]
        if sku.upper().startswith(web_prefix):
            lines_bellingham.append({"sku": sku, "newQty": qty})
            preview_rows.append({"sku": sku, "newQty": qty, "locationCode": bellingham_code})
        else:
            lines_okeechobee.append({"sku": sku, "newQty": qty})
            preview_rows.append({"sku": sku, "newQty": qty, "locationCode": okeechobee_code})

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    preview_path = PROJECT_ROOT / ".tmp" / f"qb_quantity_sync_preview_{timestamp}.csv"
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    with preview_path.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["sku", "newQty", "locationCode"])
        writer.writeheader()
        writer.writerows(preview_rows)

    bellingham_chunks = chunked(lines_bellingham, args.chunk_size)
    okeechobee_chunks = chunked(lines_okeechobee, args.chunk_size)
    summary = {
        "mismatchCsv": str(mismatch_path),
        "targetSkuCount": len(target_skus),
        "effectiveDate": args.effective_date,
        "webPrefix": web_prefix,
        "bellinghamCode": bellingham_code,
        "okeechobeeCode": okeechobee_code,
        "bellinghamLineCount": len(lines_bellingham),
        "okeechobeeLineCount": len(lines_okeechobee),
        "chunkSize": args.chunk_size,
        "eventCountPlanned": len(bellingham_chunks) + len(okeechobee_chunks),
        "dryRun": args.dry_run,
        "previewCsv": str(preview_path),
    }
    print(json.dumps(summary, indent=2))

    if args.dry_run:
        return

    created_events: list[str] = []
    for location_name, location_row, chunks in [
        (bellingham_code, bellingham, bellingham_chunks),
        (okeechobee_code, okeechobee, okeechobee_chunks),
    ]:
        if not chunks:
            continue
        location_id = location_row["locationId"]
        for index, chunk in enumerate(chunks, start=1):
            result = convex_run(
                "inventory:createAdjustmentEvent",
                {
                    "effectiveDate": args.effective_date,
                    "locationId": location_id,
                    "mode": "set",
                    "memo": f"{args.memo_prefix} - {location_name} - chunk {index}/{len(chunks)}",
                    "createdBy": args.created_by,
                    "reasonCode": args.reason_code,
                    "lines": chunk,
                },
                env_file=args.env_file,
                push=False,
                run_prod=args.prod,
            )
            event_id = str(result.get("eventId") or "")
            created_events.append(event_id)
            print(
                json.dumps(
                    {
                        "locationCode": location_name,
                        "chunk": index,
                        "chunksTotal": len(chunks),
                        "lineCount": len(chunk),
                        "eventId": event_id,
                        "qbStatus": result.get("qbStatus"),
                    }
                )
            )

    print(
        json.dumps(
            {
                "status": "queued",
                "eventCount": len(created_events),
                "eventIds": created_events,
            }
        )
    )


if __name__ == "__main__":
    main()
