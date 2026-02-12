"""
Import the master inventory CSV into Convex `inventory_parts` via idempotent SKU upserts.

Usage examples:
  python execution/inventory_import_items.py
  python execution/inventory_import_items.py --env-file .env.local --push-first
  python execution/inventory_import_items.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import subprocess
import time
from typing import Any, Dict, Iterable, List

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


COLUMN_MAP = {
    "Active Status": "Active_Status",
    "Type": "Type",
    "Sku": "Sku",
    "Description": "Description",
    "Sales Tax Code": "Sales_Tax_Code",
    "Account": "Account",
    "COGS Account": "COGS_Account",
    "Asset Account": "Asset_Account",
    "Accumulated Depreciation": "Accumulated_Depreciation",
    "Purchase Description": "Purchase_Description",
    "Quantity On Hand (2025)": "Quantity_On_Hand_2025",
    "U/M": "U_M",
    "U/M Set": "U_M_Set",
    "Cost": "Cost",
    "Preferred Vendor": "Preferred_Vendor",
    "Tax Agency": "Tax_Agency",
    "Price": "Price",
    "Reorder Pt (Min)": "Reorder_Pt_Min",
    "MPN": "MPN",
    "Category": "Category",
}

NUMERIC_FIELDS = {
    "Accumulated_Depreciation",
    "Quantity_On_Hand_2025",
    "Cost",
    "Price",
    "Reorder_Pt_Min",
}

STRING_FIELDS = {
    "Account",
    "Active_Status",
    "Asset_Account",
    "COGS_Account",
    "Category",
    "Description",
    "MPN",
    "Preferred_Vendor",
    "Purchase_Description",
    "Sales_Tax_Code",
    "Sku",
    "Tax_Agency",
    "Type",
    "U_M",
    "U_M_Set",
}


def safe_float(value: str | None, default: float = 0.0) -> float:
    if not value:
        return default
    cleaned = re.sub(r"[,$]", "", value.strip())
    if not cleaned:
        return default
    try:
        parsed = float(cleaned)
        if math.isnan(parsed) or math.isinf(parsed):
            return default
        return parsed
    except ValueError:
        return default


def build_part(row: Dict[str, str]) -> Dict[str, Any]:
    item: Dict[str, Any] = {}
    for source_col, target_col in COLUMN_MAP.items():
        raw = row.get(source_col, "")
        if target_col in NUMERIC_FIELDS:
            item[target_col] = safe_float(raw)
        else:
            item[target_col] = str(raw or "").strip()

    # Normalize required fields to avoid schema violations.
    for key in STRING_FIELDS:
        item[key] = str(item.get(key, "") or "").strip()

    # Optional convenience fields for downstream QuickBooks mapping.
    item["isActive"] = item["Active_Status"].lower() == "active"
    return item


def load_parts(input_path: str) -> List[Dict[str, Any]]:
    rows_by_sku: Dict[str, Dict[str, Any]] = {}
    with open(input_path, "r", encoding="utf-8-sig", newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            part = build_part(row)
            sku = part["Sku"]
            if not sku:
                continue
            rows_by_sku[sku] = part
    return list(rows_by_sku.values())


def chunked(items: List[Dict[str, Any]], chunk_size: int) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def parse_last_json_line(output: str) -> Dict[str, Any] | None:
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


def run_convex_upsert_batch(
    batch: List[Dict[str, Any]],
    env_file: str | None,
    push: bool,
    run_prod: bool,
) -> Dict[str, Any]:
    cmd = [
        "node",
        os.path.join(PROJECT_ROOT, "node_modules", "convex", "bin", "main.js"),
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
    cmd.extend(
        [
            "inventory:upsertInventoryPartsBatch",
            json.dumps({"parts": batch}),
        ]
    )

    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
        parsed = parse_last_json_line(result.stdout)
        stderr_text = result.stderr.strip()
        is_internal_error = "InternalServerError" in stderr_text

        if result.returncode == 0 or parsed is not None:
            if result.returncode != 0 and stderr_text:
                print(
                    "WARNING: Convex CLI returned non-zero after returning JSON.\n"
                    f"CLI stderr:\n{stderr_text}"
                )
            if parsed is None:
                return {"processed": len(batch), "inserted": None, "updated": None}
            return parsed

        if is_internal_error and attempt < max_attempts:
            time.sleep(1.5 * attempt)
            continue

        raise RuntimeError(
            f"Convex upsert command failed.\nCommand: {' '.join(cmd)}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    return {"processed": len(batch), "inserted": None, "updated": None}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Idempotently import inventory parts into Convex by SKU."
    )
    parser.add_argument(
        "--input",
        default=os.path.join(
            PROJECT_ROOT,
            "amazon_economics",
            "Master_Updated_web_accounts_v14_1 (1).csv",
        ),
        help="Path to source inventory CSV.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Number of records per Convex mutation call.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional env file passed to `npx convex run --env-file`.",
    )
    parser.add_argument(
        "--push-first",
        action="store_true",
        help="Pass `--push` on the first batch so updated Convex code is deployed.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and summarize input without calling Convex.",
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Run against production deployment (equivalent to `convex run --prod`).",
    )
    args = parser.parse_args()

    if args.batch_size < 1:
        raise ValueError("--batch-size must be >= 1")

    parts = load_parts(args.input)
    if not parts:
        raise RuntimeError("No valid SKU rows found in input file.")

    print(f"Loaded {len(parts)} unique SKU records from {args.input}")
    if args.dry_run:
        print("Dry run enabled. No Convex mutations were executed.")
        return

    total_processed = 0
    total_inserted = 0
    total_updated = 0

    for index, batch in enumerate(chunked(parts, args.batch_size), start=1):
        should_push = args.push_first and index == 1
        result = run_convex_upsert_batch(
            batch=batch,
            env_file=args.env_file,
            push=should_push,
            run_prod=args.prod,
        )
        processed = int(result.get("processed") or len(batch))
        inserted = result.get("inserted")
        updated = result.get("updated")
        total_processed += processed
        if isinstance(inserted, int):
            total_inserted += inserted
        if isinstance(updated, int):
            total_updated += updated

        print(
            f"Batch {index}: processed={processed}, "
            f"inserted={inserted}, updated={updated}"
        )

    print(
        "Done. "
        f"processed={total_processed}, inserted={total_inserted}, updated={total_updated}"
    )


if __name__ == "__main__":
    main()
