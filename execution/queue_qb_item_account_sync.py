"""
Queue QB item account sync events from parity mismatch output.

This script reads a parity mismatch CSV and enqueues no-op `set` adjustments for
SKUs with `income_account` and/or `cogs_account` mismatches. The QBWC middleware
detects `createdBy=qb-item-account-sync*` events and emits `ItemInventoryModRq`
instead of inventory adjustments, so item-level accounts can be updated in QB.

Usage:
  python execution/queue_qb_item_account_sync.py --prod --dry-run
  python execution/queue_qb_item_account_sync.py --prod
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
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

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        return json.loads(text[start : end + 1])
    raise RuntimeError("Convex command did not return parseable JSON object.")


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


T = TypeVar("T")


def chunked(values: list[T], size: int) -> list[list[T]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def location_onhand_from_item_detail(
    detail: dict[str, Any] | None,
    *,
    location_id: str,
) -> float:
    if not isinstance(detail, dict):
        return 0.0
    balances = detail.get("balances", [])
    if not isinstance(balances, list):
        return 0.0
    for row in balances:
        if not isinstance(row, dict):
            continue
        if str(row.get("locationId") or "") != str(location_id):
            continue
        raw = row.get("onHand")
        try:
            return float(raw or 0)
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Queue QB item account sync events from parity mismatch CSV.")
    parser.add_argument(
        "--mismatch-csv",
        default=None,
        help="Path to qb_parity_mismatches_*.csv. Defaults to latest in .tmp.",
    )
    parser.add_argument(
        "--fields",
        default="income_account,cogs_account",
        help="Comma-separated mismatch fields to include.",
    )
    parser.add_argument(
        "--location-code",
        default="BELLINGHAM",
        help="Location code used for no-op set events.",
    )
    parser.add_argument(
        "--effective-date",
        default="2026-01-01",
        help="Effective date for queued events (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--created-by",
        default="qb-item-account-sync",
        help="createdBy marker (must start with qb-item-account-sync).",
    )
    parser.add_argument(
        "--memo-prefix",
        default="QB item account sync from parity mismatches",
        help="Memo prefix for queued events.",
    )
    parser.add_argument(
        "--reason-code",
        default="qb_account_sync",
        help="Reason code for queued events.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of SKUs per queued event.",
    )
    parser.add_argument(
        "--onhand-chunk-size",
        type=int,
        default=500,
        help="SKU chunk size for inventory:getOnHandBySkuAtLocation calls.",
    )
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--prod", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--push-first", action="store_true")
    args = parser.parse_args()

    if args.batch_size < 1:
        raise RuntimeError("--batch-size must be >= 1")
    if args.onhand_chunk_size < 1:
        raise RuntimeError("--onhand-chunk-size must be >= 1")

    mismatch_path = Path(args.mismatch_csv) if args.mismatch_csv else find_latest_mismatch_csv()
    if not mismatch_path.exists():
        raise RuntimeError(f"Mismatch CSV not found: {mismatch_path}")

    include_fields = {
        token.strip()
        for token in args.fields.split(",")
        if token.strip()
    }
    if not include_fields:
        raise RuntimeError("--fields must include at least one field name.")

    with mismatch_path.open("r", encoding="utf-8-sig", newline="") as infile:
        rows = list(csv.DictReader(infile))
    target_skus = sorted(
        {
            (row.get("sku") or "").strip()
            for row in rows
            if (row.get("field") or "").strip() in include_fields
            and (row.get("sku") or "").strip()
        }
    )
    if not target_skus:
        raise RuntimeError("No SKUs matched the requested mismatch fields.")

    push_next = args.push_first
    locations_payload = convex_run(
        "inventory:listLocations",
        {"includeInactive": True},
        env_file=args.env_file,
        push=push_next,
        run_prod=args.prod,
    )
    if not isinstance(locations_payload, dict):
        raise RuntimeError(f"inventory:listLocations returned unexpected payload: {type(locations_payload)!r}")
    push_next = False
    locations = locations_payload.get("rows", [])
    location_code = args.location_code.strip().upper()
    location_row = None
    for row in locations:
        if not isinstance(row, dict):
            continue
        if str(row.get("code") or "").strip().upper() == location_code:
            location_row = row
            break
    if location_row is None:
        raise RuntimeError(f"Location code not found: {location_code}")
    location_id = location_row["locationId"]

    onhand_by_sku: dict[str, float] = {}
    sku_chunks = chunked(target_skus, args.onhand_chunk_size)
    used_onhand_source = "inventory:getOnHandBySkuAtLocation"
    try:
        for sku_chunk in sku_chunks:
            onhand_payload = convex_run(
                "inventory:getOnHandBySkuAtLocation",
                {
                    "locationId": location_id,
                    "skus": sku_chunk,
                },
                env_file=args.env_file,
                push=push_next,
                run_prod=args.prod,
            )
            push_next = False
            if not isinstance(onhand_payload, dict):
                raise RuntimeError(
                    f"inventory:getOnHandBySkuAtLocation returned unexpected payload: {type(onhand_payload)!r}"
                )
            onhand_rows = onhand_payload.get("rows", [])
            for row in onhand_rows:
                if not isinstance(row, dict):
                    continue
                sku = str(row.get("sku") or "").strip()
                if not sku:
                    continue
                raw = row.get("onHand")
                try:
                    on_hand = float(raw or 0)
                except (TypeError, ValueError):
                    on_hand = 0.0
                onhand_by_sku[sku] = on_hand
    except RuntimeError as exc:
        used_onhand_source = "inventory:getItemDetail (fallback)"
        print(f"INFO: Falling back to per-SKU on-hand lookup via getItemDetail. Cause: {exc}")
        for sku in target_skus:
            detail_payload = convex_run(
                "inventory:getItemDetail",
                {"sku": sku, "eventLimit": 1},
                env_file=args.env_file,
                push=push_next,
                run_prod=args.prod,
            )
            push_next = False
            onhand_by_sku[sku] = location_onhand_from_item_detail(
                detail_payload if isinstance(detail_payload, dict) else None,
                location_id=str(location_id),
            )

    lines = [{"sku": sku, "newQty": onhand_by_sku.get(sku, 0.0)} for sku in target_skus]
    chunks = chunked(lines, args.batch_size)

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    preview_path = PROJECT_ROOT / ".tmp" / f"qb_item_account_sync_preview_{timestamp}.csv"
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    with preview_path.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["sku", "newQty"])
        writer.writeheader()
        writer.writerows(lines)

    summary = {
        "mismatchCsv": str(mismatch_path),
        "fieldsIncluded": sorted(include_fields),
        "targetSkuCount": len(target_skus),
        "locationCode": location_code,
        "locationId": str(location_id),
        "batchSize": args.batch_size,
        "onHandChunkSize": args.onhand_chunk_size,
        "onHandResolvedCount": len(onhand_by_sku),
        "onHandSource": used_onhand_source,
        "eventCountPlanned": len(chunks),
        "dryRun": args.dry_run,
        "previewCsv": str(preview_path),
    }
    print(json.dumps(summary, indent=2))

    if args.dry_run:
        return

    created_event_ids: list[str] = []
    for index, chunk in enumerate(chunks, start=1):
        result = convex_run(
            "inventory:createAdjustmentEvent",
            {
                "effectiveDate": args.effective_date,
                "locationId": location_id,
                "mode": "set",
                "memo": f"{args.memo_prefix} - chunk {index}/{len(chunks)}",
                "createdBy": args.created_by,
                "reasonCode": args.reason_code,
                "lines": chunk,
            },
            env_file=args.env_file,
            push=push_next,
            run_prod=args.prod,
        )
        push_next = False
        event_id = str(result.get("eventId") or "")
        created_event_ids.append(event_id)
        print(
            json.dumps(
                {
                    "chunk": index,
                    "chunksTotal": len(chunks),
                    "eventId": event_id,
                    "qbStatus": result.get("qbStatus"),
                    "lineCount": len(chunk),
                }
            )
        )

    print(
        json.dumps(
            {
                "status": "queued",
                "eventCount": len(created_event_ids),
                "eventIds": created_event_ids,
            }
        )
    )


if __name__ == "__main__":
    main()
