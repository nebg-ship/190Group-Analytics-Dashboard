"""
Smoke test for Batch 3 inventory dashboard APIs.

Flow:
1) Seed smoke locations and SKU in Convex.
2) Exercise inventory API endpoints through Flask test client.
3) Validate transfer/adjustment creation + queue summary response.
4) Cleanup smoke records.

Usage:
  python execution/smoke_inventory_api.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "api"))
from dashboard_data import app  # noqa: E402


def _extract_json(stdout: str) -> dict[str, Any] | None:
    first = stdout.find("{")
    last = stdout.rfind("}")
    if first == -1 or last == -1 or last <= first:
        return None
    try:
        parsed = json.loads(stdout[first : last + 1])
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def convex_run(function_name: str, args_obj: dict[str, Any]) -> dict[str, Any]:
    import subprocess

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
        function_name,
        json.dumps(args_obj),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)
    parsed = _extract_json(proc.stdout)
    if parsed is None:
        raise RuntimeError(
            "Convex command did not return parseable JSON.\n"
            f"Command: {' '.join(cmd)}\n"
            f"Return code: {proc.returncode}\n"
            f"STDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}"
        )
    return parsed


def parse_success(response, expected_status: int = 200) -> dict[str, Any]:
    if response.status_code != expected_status:
        raise RuntimeError(f"Unexpected status {response.status_code}: {response.data.decode('utf-8')}")
    payload = response.get_json()
    if not payload or not payload.get("success"):
        raise RuntimeError(f"Endpoint failure: {payload}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(f"Expected object data payload, got: {type(data)}")
    return data


def main() -> None:
    run_id = int(time.time())
    location_a_code = f"SMOKE_A_{run_id}"
    location_b_code = f"SMOKE_B_{run_id}"
    sku = f"SMOKE-SKU-{run_id}"
    created_by = "smoke-test"
    today = "2026-02-11"

    location_a_id = None
    location_b_id = None

    client = app.test_client()
    try:
        location_a = convex_run(
            "inventory:upsertLocation",
            {
                "code": location_a_code,
                "displayName": f"Smoke A {run_id}",
                "active": True,
                "isVirtual": False,
                "qbSiteFullName": f"Smoke A {run_id}",
            },
        )
        location_a_id = location_a["locationId"]
        location_b = convex_run(
            "inventory:upsertLocation",
            {
                "code": location_b_code,
                "displayName": f"Smoke B {run_id}",
                "active": True,
                "isVirtual": False,
                "qbSiteFullName": f"Smoke B {run_id}",
            },
        )
        location_b_id = location_b["locationId"]

        convex_run(
            "inventory:upsertInventoryPartsBatch",
            {
                "parts": [
                    {
                        "Account": "Supplies:Smoke",
                        "Accumulated_Depreciation": 0,
                        "Active_Status": "Active",
                        "Asset_Account": "12100 - Inventory Asset",
                        "COGS_Account": "COGS:Supplies",
                        "Category": "Smoke",
                        "Cost": 5,
                        "Description": f"Smoke API part {run_id}",
                        "MPN": "",
                        "Preferred_Vendor": "",
                        "Price": 10,
                        "Purchase_Description": f"Smoke API part {run_id}",
                        "Quantity_On_Hand_2025": 0,
                        "Reorder_Pt_Min": 1,
                        "Sales_Tax_Code": "Non",
                        "Sku": sku,
                        "Tax_Agency": "",
                        "Type": "Inventory Part",
                        "U_M": "each (ea)",
                        "U_M_Set": "Count in each",
                        "isActive": True,
                    }
                ]
            },
        )

        health = parse_success(client.get("/api/inventory/health"))
        if not health.get("ok"):
            raise RuntimeError(f"Unexpected health payload: {health}")

        adjustment = parse_success(
            client.post(
                "/api/inventory/adjustment",
                json={
                    "effectiveDate": today,
                    "locationId": location_a_id,
                    "mode": "delta",
                    "memo": f"Smoke adjustment {run_id}",
                    "createdBy": created_by,
                    "reasonCode": "cycle_count",
                    "lines": [{"sku": sku, "qty": 5}],
                },
            ),
            expected_status=201,
        )

        transfer = parse_success(
            client.post(
                "/api/inventory/transfer",
                json={
                    "effectiveDate": today,
                    "memo": f"Smoke transfer {run_id}",
                    "createdBy": created_by,
                    "lines": [
                        {
                            "sku": sku,
                            "qty": 2,
                            "fromLocationId": location_a_id,
                            "toLocationId": location_b_id,
                        }
                    ],
                },
            ),
            expected_status=201,
        )

        overview = parse_success(client.get(f"/api/inventory/overview?search={sku}&limit=25"))
        rows = overview.get("rows", [])
        if not rows:
            raise RuntimeError("Inventory overview did not return seeded SKU.")

        locations = parse_success(client.get("/api/inventory/locations?include_inactive=true"))
        if not locations.get("rows"):
            raise RuntimeError("Location list returned empty rows.")

        item_detail = parse_success(client.get(f"/api/inventory/item/{sku}?event_limit=20"))
        if item_detail.get("sku") != sku:
            raise RuntimeError("Item detail did not return expected SKU.")

        queue_summary = parse_success(client.get("/api/inventory/queue-summary?recent_limit=20"))
        pending_count = int(queue_summary.get("counts", {}).get("pending", 0))
        if pending_count < 1:
            raise RuntimeError(f"Queue summary pending count unexpectedly low: {pending_count}")

        events = parse_success(client.get("/api/inventory/events?limit=50"))
        event_rows = events.get("rows", [])
        if not event_rows:
            raise RuntimeError("Recent events endpoint returned no rows.")

        void_result = parse_success(client.post(f"/api/inventory/events/{transfer['eventId']}/void", json={}))
        if void_result.get("status") != "voided":
            raise RuntimeError(f"Expected voided status, got: {void_result}")

        print("INVENTORY_API_SMOKE_PASS")
        print(
            json.dumps(
                {
                    "runId": run_id,
                    "sku": sku,
                    "adjustmentEventId": adjustment["eventId"],
                    "transferEventId": transfer["eventId"],
                    "voidedEventId": void_result["eventId"],
                    "pendingCount": pending_count,
                },
                indent=2,
            )
        )
    finally:
        try:
            convex_run(
                "inventory:cleanupSmokeData",
                {
                    "skuPrefix": "SMOKE-SKU-",
                    "locationCodePrefix": "SMOKE_",
                    "createdBy": "smoke-test",
                    "dryRun": False,
                },
            )
        except Exception:
            pass


if __name__ == "__main__":
    main()
