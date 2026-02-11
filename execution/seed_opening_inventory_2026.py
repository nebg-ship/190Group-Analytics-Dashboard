"""
Seed opening inventory balances as of 2026-01-01 using Quantity_On_Hand_2025.

Rules:
- SKUs starting with WEB -> Bellingham
- All other SKUs -> Okeechobee

This script creates `set` adjustment events so on-hand matches the source quantity.

Usage:
  python execution/seed_opening_inventory_2026.py --push-first
  python execution/seed_opening_inventory_2026.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class SeedConfig:
    effective_date: str
    bellingham_code: str
    okeechobee_code: str
    web_prefix: str
    chunk_size: int
    include_inactive: bool
    created_by: str
    reason_code: str
    memo_prefix: str
    dry_run: bool
    push_first: bool
    env_file: str | None


def parse_last_json(output: str) -> dict[str, Any] | None:
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("{") and line.endswith("}"):
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed

    start = output.find("{")
    end = output.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        parsed = json.loads(output[start : end + 1])
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def convex_run(function_name: str, args_obj: dict[str, Any], *, env_file: str | None, push: bool) -> dict[str, Any]:
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
    if push:
        cmd.append("--push")
    if env_file:
        cmd.extend(["--env-file", env_file])
    cmd.extend([function_name, json.dumps(args_obj)])

    proc = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    parsed = parse_last_json(proc.stdout)
    if parsed is None:
        raise RuntimeError(
            "Convex command did not return parseable JSON.\n"
            f"Command: {' '.join(cmd)}\n"
            f"Return code: {proc.returncode}\n"
            f"STDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}"
        )
    return parsed


def chunked(items: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    return [items[index : index + chunk_size] for index in range(0, len(items), chunk_size)]


def normalize_code(value: str) -> str:
    return value.strip().upper()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed opening balances for 2026-01-01.")
    parser.add_argument("--effective-date", default="2026-01-01")
    parser.add_argument("--bellingham-code", default="BELLINGHAM")
    parser.add_argument("--okeechobee-code", default="OKEECHOBEE")
    parser.add_argument("--web-prefix", default="WEB")
    parser.add_argument("--chunk-size", type=int, default=100)
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only seed active SKUs. Default seeds all SKUs in inventory_parts.",
    )
    parser.add_argument("--created-by", default="seed-2026-01-01")
    parser.add_argument("--reason-code", default="opening_balance")
    parser.add_argument("--memo-prefix", default="Opening balance seed 2026-01-01")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--push-first", action="store_true")
    parser.add_argument("--env-file", default=None)
    args = parser.parse_args()

    if args.chunk_size < 1:
        raise ValueError("--chunk-size must be >= 1")

    config = SeedConfig(
        effective_date=args.effective_date,
        bellingham_code=normalize_code(args.bellingham_code),
        okeechobee_code=normalize_code(args.okeechobee_code),
        web_prefix=args.web_prefix.strip().upper(),
        chunk_size=args.chunk_size,
        include_inactive=not args.active_only,
        created_by=args.created_by.strip() or "seed-2026-01-01",
        reason_code=args.reason_code.strip() or "opening_balance",
        memo_prefix=args.memo_prefix.strip() or "Opening balance seed",
        dry_run=args.dry_run,
        push_first=args.push_first,
        env_file=args.env_file,
    )

    push_next = config.push_first
    locations_payload = convex_run(
        "inventory:listLocations",
        {"includeInactive": True},
        env_file=config.env_file,
        push=push_next,
    )
    push_next = False

    location_rows = locations_payload.get("rows", [])
    if not isinstance(location_rows, list):
        raise RuntimeError("Unexpected locations response shape.")

    location_by_code: dict[str, dict[str, Any]] = {}
    for row in location_rows:
        if isinstance(row, dict):
            location_by_code[normalize_code(str(row.get("code", "")))] = row

    bellingham = location_by_code.get(config.bellingham_code)
    okeechobee = location_by_code.get(config.okeechobee_code)
    if not bellingham:
        raise RuntimeError(f"Location not found: {config.bellingham_code}")
    if not okeechobee:
        raise RuntimeError(f"Location not found: {config.okeechobee_code}")

    parts_payload = convex_run(
        "inventory:listPartQuantities",
        {"includeInactive": config.include_inactive, "limit": 20000},
        env_file=config.env_file,
        push=False,
    )
    part_rows = parts_payload.get("rows", [])
    if not isinstance(part_rows, list):
        raise RuntimeError("Unexpected part quantity response shape.")

    lines_bellingham: list[dict[str, Any]] = []
    lines_okeechobee: list[dict[str, Any]] = []
    skipped_negative: list[str] = []

    for row in part_rows:
        if not isinstance(row, dict):
            continue
        sku = str(row.get("sku", "")).strip()
        if not sku:
            continue

        quantity_raw = row.get("quantityOnHand2025", 0)
        try:
            quantity = float(quantity_raw or 0)
        except (TypeError, ValueError):
            quantity = 0.0
        if not math.isfinite(quantity):
            quantity = 0.0

        if quantity < 0:
            skipped_negative.append(sku)
            continue

        line = {
            "sku": sku,
            "newQty": quantity,
        }
        if sku.upper().startswith(config.web_prefix):
            lines_bellingham.append(line)
        else:
            lines_okeechobee.append(line)

    if skipped_negative:
        raise RuntimeError(
            "Negative opening quantity found for one or more SKUs. "
            f"First 10: {', '.join(skipped_negative[:10])}"
        )

    summary = {
        "effectiveDate": config.effective_date,
        "bellinghamCode": config.bellingham_code,
        "okeechobeeCode": config.okeechobee_code,
        "webPrefix": config.web_prefix,
        "totalParts": len(part_rows),
        "bellinghamLines": len(lines_bellingham),
        "okeechobeeLines": len(lines_okeechobee),
        "chunkSize": config.chunk_size,
        "dryRun": config.dry_run,
    }
    print(json.dumps(summary, indent=2))

    if config.dry_run:
        return

    events_created = 0
    for location_name, location_row, location_lines in [
        ("Bellingham", bellingham, lines_bellingham),
        ("Okeechobee", okeechobee, lines_okeechobee),
    ]:
        location_id = location_row["locationId"]
        chunks = chunked(location_lines, config.chunk_size)
        if not chunks:
            continue

        for index, lines_chunk in enumerate(chunks, start=1):
            result = convex_run(
                "inventory:createAdjustmentEvent",
                {
                    "effectiveDate": config.effective_date,
                    "locationId": location_id,
                    "mode": "set",
                    "memo": f"{config.memo_prefix} - {location_name} - chunk {index}/{len(chunks)}",
                    "createdBy": config.created_by,
                    "reasonCode": config.reason_code,
                    "lines": lines_chunk,
                },
                env_file=config.env_file,
                push=False,
            )
            events_created += 1
            print(
                json.dumps(
                    {
                        "location": location_name,
                        "chunk": index,
                        "chunksTotal": len(chunks),
                        "lines": len(lines_chunk),
                        "eventId": result.get("eventId"),
                        "qbStatus": result.get("qbStatus"),
                    }
                )
            )

    print(json.dumps({"eventsCreated": events_created, "status": "done"}))


if __name__ == "__main__":
    main()
