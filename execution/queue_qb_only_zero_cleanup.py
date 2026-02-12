"""
Queue pre-sync QuickBooks cleanup adjustments for QB-only inventory parts.

This script compares a QuickBooks item export CSV against Convex inventory parts.
For SKUs that exist in QuickBooks as `Inventory Part` but do not exist in Convex,
it enqueues `newQty = 0` adjustment events for each mapped location.

Inventory assemblies are always ignored.

Usage:
  python execution/queue_qb_only_zero_cleanup.py --qb-items-csv path\\to\\qb_items.csv --dry-run
  python execution/queue_qb_only_zero_cleanup.py --qb-items-csv path\\to\\qb_items.csv --push-first
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import subprocess
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent


TYPE_COLUMN_CANDIDATES = (
    "Type",
    "Item Type",
)

SKU_COLUMN_CANDIDATES = (
    "Sku",
    "SKU",
    "Item",
    "Item Name/Number",
    "Item Name",
    "Full Name",
    "Name",
)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def parse_last_json(stdout: str) -> dict[str, Any] | None:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("{") and line.endswith("}"):
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed

    start = stdout.find("{")
    end = stdout.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        parsed = json.loads(stdout[start : end + 1])
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def convex_run(
    function_name: str,
    args_obj: dict[str, Any],
    *,
    env_file: str | None,
    push: bool,
    run_prod: bool,
) -> dict[str, Any]:
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
    if parsed is None:
        raise RuntimeError(
            "Convex command did not return parseable JSON.\n"
            f"Command: {' '.join(cmd)}\n"
            f"Return code: {proc.returncode}\n"
            f"STDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}"
        )
    if proc.returncode != 0 and proc.stderr.strip():
        print(
            "WARNING: Convex CLI returned non-zero after JSON output.\n"
            f"CLI stderr:\n{proc.stderr.strip()}\n"
        )
    return parsed


def resolve_column(
    headers: list[str],
    explicit_name: str | None,
    candidates: tuple[str, ...],
    label: str,
) -> str:
    header_map = {normalize_header(header): header for header in headers}

    if explicit_name:
        key = normalize_header(explicit_name)
        resolved = header_map.get(key)
        if not resolved:
            raise RuntimeError(
                f"{label} column '{explicit_name}' was not found in CSV headers: {headers}"
            )
        return resolved

    for candidate in candidates:
        key = normalize_header(candidate)
        resolved = header_map.get(key)
        if resolved:
            return resolved

    raise RuntimeError(
        f"Unable to auto-detect {label} column. "
        f"CSV headers: {headers}. "
        f"Use --{label}-column to set it explicitly."
    )


def type_key(value: str) -> str:
    return normalize_header(value.strip())


def is_inventory_part(type_value: str) -> bool:
    key = type_key(type_value)
    if "inventoryassembly" in key:
        return False
    return "inventorypart" in key


def load_qb_inventory_part_skus(
    csv_path: Path,
    type_column: str | None,
    sku_column: str | None,
) -> tuple[set[str], str, str]:
    if not csv_path.exists():
        raise RuntimeError(f"QB CSV not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as infile:
        reader = csv.DictReader(infile)
        headers = list(reader.fieldnames or [])
        if not headers:
            raise RuntimeError(f"QB CSV appears empty or missing headers: {csv_path}")

        resolved_type_col = resolve_column(headers, type_column, TYPE_COLUMN_CANDIDATES, "type")
        resolved_sku_col = resolve_column(headers, sku_column, SKU_COLUMN_CANDIDATES, "sku")

        skus: set[str] = set()
        for row in reader:
            item_type = str(row.get(resolved_type_col, "") or "").strip()
            if not item_type:
                continue
            if not is_inventory_part(item_type):
                continue

            sku = str(row.get(resolved_sku_col, "") or "").strip()
            if not sku:
                continue
            skus.add(sku)

    return skus, resolved_type_col, resolved_sku_col


def chunked(values: list[str], chunk_size: int) -> list[list[str]]:
    return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]


def parse_location_codes(raw_values: list[str]) -> set[str]:
    parsed: set[str] = set()
    for value in raw_values:
        for token in value.split(","):
            clean = token.strip()
            if clean:
                parsed.add(clean)
    return parsed


def write_missing_skus(path: Path, skus: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(skus)
    if body:
        body += "\n"
    path.write_text(body, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Queue QuickBooks zero-out adjustments for inventory parts that exist in QB "
            "but not in Convex."
        )
    )
    parser.add_argument(
        "--qb-items-csv",
        required=True,
        help="Path to QuickBooks items export CSV.",
    )
    parser.add_argument(
        "--type-column",
        default=None,
        help="Optional explicit CSV column name for item type.",
    )
    parser.add_argument(
        "--sku-column",
        default=None,
        help="Optional explicit CSV column name for SKU/item name.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional env file for Convex CLI deployment selection.",
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Run against production deployment (equivalent to `convex run --prod`).",
    )
    parser.add_argument(
        "--effective-date",
        default=dt.date.today().isoformat(),
        help="Effective date for queued adjustments (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of SKUs per queued event per location.",
    )
    parser.add_argument(
        "--location-code",
        action="append",
        default=[],
        help="Optional location code filter. Repeatable or comma-separated.",
    )
    parser.add_argument(
        "--include-inactive-locations",
        action="store_true",
        help="Include inactive Convex locations (still requires QB site mapping).",
    )
    parser.add_argument(
        "--memo",
        default="Pre-sync QB cleanup: zero QB-only inventory parts",
        help="Memo applied to each queued QB cleanup event.",
    )
    parser.add_argument(
        "--created-by",
        default="qb-pre-sync-cleanup",
        help="createdBy value on queued events.",
    )
    parser.add_argument(
        "--reason-code",
        default="qb_cleanup",
        help="Optional reasonCode for reason-account mapping.",
    )
    parser.add_argument(
        "--write-missing-skus",
        default=".tmp/qb_only_inventory_parts.txt",
        help="Path to write missing SKU list for audit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report differences; do not queue events.",
    )
    parser.add_argument(
        "--push-first",
        action="store_true",
        help="Pass --push on the first queue mutation.",
    )
    args = parser.parse_args()

    if not DATE_RE.match(args.effective_date):
        raise RuntimeError("--effective-date must be YYYY-MM-DD")
    if args.batch_size < 1:
        raise RuntimeError("--batch-size must be >= 1")

    qb_csv_path = Path(args.qb_items_csv)
    qb_skus, type_col, sku_col = load_qb_inventory_part_skus(
        qb_csv_path,
        args.type_column,
        args.sku_column,
    )

    push_next = args.push_first

    def convex_run_with_optional_push(
        function_name: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        nonlocal push_next
        result = convex_run(
            function_name,
            payload,
            env_file=args.env_file,
            push=push_next,
            run_prod=args.prod,
        )
        if push_next:
            push_next = False
        return result

    convex_parts_payload = convex_run_with_optional_push(
        "inventory:listPartQuantities",
        {
            "includeInactive": True,
            "limit": 20000,
        },
    )
    convex_rows = convex_parts_payload.get("rows", [])
    convex_skus = {
        str(row.get("sku", "") or "").strip()
        for row in convex_rows
        if str(row.get("sku", "") or "").strip()
    }

    missing_skus = sorted(sku for sku in qb_skus if sku not in convex_skus)

    locations_payload = convex_run_with_optional_push(
        "inventory:listLocations",
        {"includeInactive": args.include_inactive_locations},
    )
    raw_locations = locations_payload.get("rows", [])
    eligible_locations = [
        loc
        for loc in raw_locations
        if (args.include_inactive_locations or bool(loc.get("active")))
        and str(loc.get("qbSiteFullName", "") or "").strip()
    ]

    requested_codes = parse_location_codes(args.location_code)
    if requested_codes:
        by_code = {str(loc.get("code")): loc for loc in eligible_locations}
        missing_codes = sorted(code for code in requested_codes if code not in by_code)
        if missing_codes:
            raise RuntimeError(
                "Some --location-code values were not found among eligible mapped locations: "
                + ", ".join(missing_codes)
            )
        target_locations = [by_code[code] for code in sorted(requested_codes)]
    else:
        target_locations = sorted(
            eligible_locations,
            key=lambda loc: str(loc.get("code", "")),
        )

    location_ids = [loc.get("locationId") for loc in target_locations if loc.get("locationId")]
    already_queued_by_location: dict[str, set[str]] = {}
    coverage_event_count = 0
    if location_ids:
        coverage_payload = convex_run_with_optional_push(
            "inventory:getQbCleanupCoverage",
            {
                "createdBy": args.created_by,
                "effectiveDate": args.effective_date,
                "locationIds": location_ids,
            },
        )
        coverage_event_count = int(coverage_payload.get("eventCount", 0) or 0)
        for row in coverage_payload.get("rows", []):
            location_id = str(row.get("locationId", "") or "").strip()
            if not location_id:
                continue
            skus = {
                str(sku).strip()
                for sku in row.get("skus", [])
                if str(sku).strip()
            }
            already_queued_by_location[location_id] = skus

    missing_path = Path(args.write_missing_skus)
    write_missing_skus(missing_path, missing_skus)

    remaining_by_location: dict[str, list[str]] = {}
    per_location_counts: list[dict[str, Any]] = []
    for location in target_locations:
        location_id = str(location.get("locationId", "") or "").strip()
        if not location_id:
            continue
        queued_skus = already_queued_by_location.get(location_id, set())
        remaining_skus = [sku for sku in missing_skus if sku not in queued_skus]
        remaining_by_location[location_id] = remaining_skus
        per_location_counts.append(
            {
                "locationCode": str(location.get("code", "")),
                "alreadyQueuedSkus": len(queued_skus),
                "remainingSkus": len(remaining_skus),
            }
        )

    total_sku_location_pairs = len(missing_skus) * len(target_locations)
    already_queued_pairs = sum(len(skus) for skus in already_queued_by_location.values())
    remaining_pairs = sum(len(skus) for skus in remaining_by_location.values())

    summary = {
        "qbCsv": str(qb_csv_path),
        "resolvedColumns": {
            "type": type_col,
            "sku": sku_col,
        },
        "counts": {
            "qbInventoryPartSkus": len(qb_skus),
            "convexSkus": len(convex_skus),
            "qbOnlySkus": len(missing_skus),
            "targetLocations": len(target_locations),
            "skuLocationPairs": total_sku_location_pairs,
            "alreadyQueuedPairs": already_queued_pairs,
            "remainingPairs": remaining_pairs,
        },
        "existingCleanupEvents": coverage_event_count,
        "perLocation": per_location_counts,
        "missingSkuOutput": str(missing_path),
        "effectiveDate": args.effective_date,
    }
    print(json.dumps(summary, indent=2))

    if not missing_skus:
        print("No QB-only inventory-part SKUs found. Nothing to queue.")
        return

    if not target_locations:
        raise RuntimeError(
            "No eligible locations found. Ensure locations are active and have qbSiteFullName mapping."
        )

    if remaining_pairs <= 0:
        print("All target SKU/location pairs are already queued. Nothing to add.")
        return

    if args.dry_run:
        print("Dry run enabled. No events were queued.")
        return

    queued_events: list[dict[str, Any]] = []

    for location in target_locations:
        location_id = location.get("locationId")
        if not location_id:
            raise RuntimeError(f"Location missing locationId: {location}")
        location_code = str(location.get("code", ""))
        remaining_skus = remaining_by_location.get(str(location_id), [])
        for sku_batch in chunked(remaining_skus, args.batch_size):
            payload = {
                "effectiveDate": args.effective_date,
                "locationId": location_id,
                "skus": sku_batch,
                "memo": args.memo,
                "createdBy": args.created_by,
                "reasonCode": args.reason_code,
            }
            result = convex_run_with_optional_push(
                "inventory:enqueueQbCleanupZeroOutEvent",
                payload,
            )
            queued_events.append(
                {
                    "eventId": result.get("eventId"),
                    "locationCode": location_code,
                    "lineCount": result.get("lineCount"),
                }
            )

    queued_line_count = sum(
        int(event.get("lineCount") or 0)
        for event in queued_events
    )
    outcome = {
        "queuedEventCount": len(queued_events),
        "queuedLineCount": queued_line_count,
        "remainingPairsBeforeQueue": remaining_pairs,
        "events": queued_events,
    }
    print(json.dumps(outcome, indent=2))


if __name__ == "__main__":
    main()
