"""
Queue opening-balance zero-fill adjustments for locations without assigned quantity.

Default primary-location rule matches the opening seed:
- SKUs starting with WEB -> BELLINGHAM
- all other SKUs -> OKEECHOBEE

For each target location, this script queues `newQty=0` for SKUs whose primary
location is different, ensuring explicit zeroes are sent to QuickBooks.

Usage:
  python execution/queue_opening_zero_fill_2026.py --dry-run --env-file .tmp/convex_prod.env
  python execution/queue_opening_zero_fill_2026.py --env-file .tmp/convex_prod.env --prod
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent


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
    return parsed


def chunked(values: list[str], chunk_size: int) -> list[list[str]]:
    return [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]


def normalize_code(value: str) -> str:
    return value.strip().upper()


def parse_location_codes(raw_values: list[str]) -> set[str]:
    parsed: set[str] = set()
    for value in raw_values:
        for token in value.split(","):
            clean = normalize_code(token)
            if clean:
                parsed.add(clean)
    return parsed


def primary_location_code_for_sku(sku: str, web_prefix: str, bellingham_code: str, okeechobee_code: str) -> str:
    if sku.upper().startswith(web_prefix):
        return bellingham_code
    return okeechobee_code


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Queue 2026-01-01 zero-fill adjustments for non-primary locations."
    )
    parser.add_argument("--effective-date", default="2026-01-01")
    parser.add_argument("--created-by", default="seed-2026-01-01-zero-fill")
    parser.add_argument("--reason-code", default="opening_balance")
    parser.add_argument("--memo-prefix", default="Opening balance zero-fill 2026-01-01")
    parser.add_argument("--web-prefix", default="WEB")
    parser.add_argument("--bellingham-code", default="BELLINGHAM")
    parser.add_argument("--okeechobee-code", default="OKEECHOBEE")
    parser.add_argument("--location-code", action="append", default=[])
    parser.add_argument("--chunk-size", type=int, default=100)
    parser.add_argument("--include-inactive-locations", action="store_true")
    parser.add_argument("--active-only-parts", action="store_true")
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--prod", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--push-first", action="store_true")
    args = parser.parse_args()

    if args.chunk_size < 1:
        raise RuntimeError("--chunk-size must be >= 1")

    web_prefix = args.web_prefix.strip().upper()
    bellingham_code = normalize_code(args.bellingham_code)
    okeechobee_code = normalize_code(args.okeechobee_code)
    requested_codes = parse_location_codes(args.location_code)

    push_next = args.push_first

    def run(function_name: str, payload: dict[str, Any]) -> dict[str, Any]:
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

    locations_payload = run(
        "inventory:listLocations",
        {"includeInactive": args.include_inactive_locations},
    )
    location_rows = locations_payload.get("rows", [])
    if not isinstance(location_rows, list):
        raise RuntimeError("Unexpected locations response shape.")

    eligible_locations = [
        row
        for row in location_rows
        if isinstance(row, dict)
        and bool(row.get("qbSiteFullName"))
        and (args.include_inactive_locations or bool(row.get("active")))
    ]
    if not eligible_locations:
        raise RuntimeError("No eligible locations with qbSiteFullName were found.")

    location_by_code = {
        normalize_code(str(row.get("code", ""))): row
        for row in eligible_locations
        if normalize_code(str(row.get("code", "")))
    }

    if requested_codes:
        missing = sorted(code for code in requested_codes if code not in location_by_code)
        if missing:
            raise RuntimeError(
                "Some --location-code values are not eligible mapped locations: "
                + ", ".join(missing)
            )
        target_codes = sorted(requested_codes)
    else:
        target_codes = sorted(location_by_code.keys())

    if bellingham_code not in location_by_code:
        raise RuntimeError(f"Bellingham source location code not found among eligible locations: {bellingham_code}")
    if okeechobee_code not in location_by_code:
        raise RuntimeError(f"Okeechobee source location code not found among eligible locations: {okeechobee_code}")

    parts_payload = run(
        "inventory:listPartQuantities",
        {"includeInactive": not args.active_only_parts, "limit": 20000},
    )
    part_rows = parts_payload.get("rows", [])
    if not isinstance(part_rows, list):
        raise RuntimeError("Unexpected parts response shape.")

    skus = sorted(
        {
            str(row.get("sku", "")).strip()
            for row in part_rows
            if isinstance(row, dict) and str(row.get("sku", "")).strip()
        }
    )
    if not skus:
        raise RuntimeError("No SKUs found in inventory:listPartQuantities.")

    desired_zero_by_location: dict[str, list[str]] = {}
    for location_code in target_codes:
        location_zero_skus: list[str] = []
        for sku in skus:
            primary = primary_location_code_for_sku(
                sku=sku,
                web_prefix=web_prefix,
                bellingham_code=bellingham_code,
                okeechobee_code=okeechobee_code,
            )
            if primary != location_code:
                location_zero_skus.append(sku)
        desired_zero_by_location[location_code] = location_zero_skus

    coverage = run(
        "inventory:getQbCleanupCoverage",
        {
            "createdBy": args.created_by,
            "effectiveDate": args.effective_date,
            "locationIds": [location_by_code[code]["locationId"] for code in target_codes],
        },
    )
    coverage_rows = coverage.get("rows", [])
    if not isinstance(coverage_rows, list):
        raise RuntimeError("Unexpected coverage response shape.")

    already_queued_by_location_id: dict[str, set[str]] = {}
    for row in coverage_rows:
        if not isinstance(row, dict):
            continue
        location_id = str(row.get("locationId", ""))
        if not location_id:
            continue
        row_skus = row.get("skus", [])
        if not isinstance(row_skus, list):
            continue
        already_queued_by_location_id[location_id] = {
            str(sku).strip()
            for sku in row_skus
            if str(sku).strip()
        }

    remaining_by_location: dict[str, list[str]] = {}
    for location_code in target_codes:
        location_id = str(location_by_code[location_code]["locationId"])
        already = already_queued_by_location_id.get(location_id, set())
        remaining = [sku for sku in desired_zero_by_location[location_code] if sku not in already]
        remaining_by_location[location_code] = remaining

    summary = {
        "effectiveDate": args.effective_date,
        "createdBy": args.created_by,
        "reasonCode": args.reason_code,
        "memoPrefix": args.memo_prefix,
        "webPrefix": web_prefix,
        "sourceRule": {
            "webTo": bellingham_code,
            "nonWebTo": okeechobee_code,
        },
        "targetLocationCodes": target_codes,
        "totalSkus": len(skus),
        "byLocation": [
            {
                "locationCode": code,
                "desiredZeroSkus": len(desired_zero_by_location[code]),
                "remainingToQueue": len(remaining_by_location[code]),
            }
            for code in target_codes
        ],
        "dryRun": args.dry_run,
    }
    print(json.dumps(summary, indent=2))

    total_remaining = sum(len(values) for values in remaining_by_location.values())
    if total_remaining == 0:
        print(json.dumps({"status": "nothing_to_queue"}))
        return

    if args.dry_run:
        print(json.dumps({"status": "dry_run", "remainingPairs": total_remaining}))
        return

    queued_events: list[dict[str, Any]] = []
    for location_code in target_codes:
        remaining_skus = remaining_by_location[location_code]
        if not remaining_skus:
            continue

        location = location_by_code[location_code]
        location_id = location["locationId"]
        chunks = chunked(remaining_skus, args.chunk_size)
        for index, sku_chunk in enumerate(chunks, start=1):
            payload = run(
                "inventory:enqueueQbCleanupZeroOutEvent",
                {
                    "effectiveDate": args.effective_date,
                    "locationId": location_id,
                    "skus": sku_chunk,
                    "memo": f"{args.memo_prefix} - {location_code} - chunk {index}/{len(chunks)}",
                    "createdBy": args.created_by,
                    "reasonCode": args.reason_code,
                },
            )
            queued_events.append(
                {
                    "locationCode": location_code,
                    "chunk": index,
                    "chunksTotal": len(chunks),
                    "eventId": payload.get("eventId"),
                    "lineCount": payload.get("lineCount"),
                    "qbStatus": payload.get("qbStatus"),
                }
            )

    print(
        json.dumps(
            {
                "status": "queued",
                "queuedEventCount": len(queued_events),
                "queuedLineCount": sum(int(event.get("lineCount") or 0) for event in queued_events),
                "events": queued_events,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

