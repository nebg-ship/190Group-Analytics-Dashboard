"""
Batch 4 smoke test: access control + approval workflow + audit visibility.

Flow:
1) Enable security/approval env vars for this process.
2) Seed smoke SKU + locations in Convex.
3) Verify unauthorized write is blocked.
4) Verify authorized write becomes pending approval.
5) Approve request and confirm execution.
6) Verify audit endpoint returns records.
7) Cleanup smoke inventory data.

Usage:
  python execution/smoke_inventory_batch4.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
APPROVALS_PATH = PROJECT_ROOT / ".tmp" / "inventory_approval_requests.json"


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


def response_payload(response, expected_status: int) -> dict[str, Any]:
    if response.status_code != expected_status:
        raise RuntimeError(
            f"Expected HTTP {expected_status}, got {response.status_code}: {response.data.decode('utf-8')}"
        )
    payload = response.get_json()
    if not isinstance(payload, dict):
        raise RuntimeError("Response was not JSON object.")
    return payload


def cleanup_approval_request(request_id: str | None) -> None:
    if not request_id or not APPROVALS_PATH.exists():
        return
    try:
        rows = json.loads(APPROVALS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(rows, list):
        return
    next_rows = [row for row in rows if isinstance(row, dict) and row.get("requestId") != request_id]
    APPROVALS_PATH.write_text(json.dumps(next_rows, indent=2), encoding="utf-8")


def main() -> None:
    run_id = int(time.time())
    write_token = f"batch4-write-{run_id}"
    admin_token = f"batch4-admin-{run_id}"
    actor = f"batch4-smoke-{run_id}"
    sku = f"SMOKE-SKU-{run_id}"
    location_a_code = f"SMOKE_A_{run_id}"
    location_b_code = f"SMOKE_B_{run_id}"
    today = "2026-02-11"

    os.environ["INVENTORY_WRITE_TOKEN"] = write_token
    os.environ["INVENTORY_ADMIN_TOKEN"] = admin_token
    os.environ["INVENTORY_REQUIRE_APPROVAL"] = "true"
    os.environ["INVENTORY_APPROVAL_QTY_THRESHOLD"] = "1"

    sys.path.insert(0, str(PROJECT_ROOT / "api"))
    from dashboard_data import app  # noqa: E402

    client = app.test_client()
    location_a_id = None
    location_b_id = None
    approval_request_id = None
    executed_event_id = None

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
                        "Description": f"Smoke Batch4 part {run_id}",
                        "MPN": "",
                        "Preferred_Vendor": "",
                        "Price": 10,
                        "Purchase_Description": f"Smoke Batch4 part {run_id}",
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

        convex_run(
            "inventory:createAdjustmentEvent",
            {
                "effectiveDate": today,
                "locationId": location_a_id,
                "mode": "delta",
                "memo": f"Batch4 seed balance {run_id}",
                "createdBy": "smoke-test",
                "reasonCode": "cycle_count",
                "lines": [{"sku": sku, "qty": 5}],
            },
        )

        # Unauthorized write attempt should fail with 401.
        unauthorized_transfer = client.post(
            "/api/inventory/transfer",
            json={
                "effectiveDate": today,
                "createdBy": actor,
                "memo": f"Batch4 unauthorized transfer {run_id}",
                "lines": [
                    {
                        "sku": sku,
                        "qty": 2,
                        "fromLocationId": location_a_id,
                        "toLocationId": location_b_id,
                    }
                ],
            },
        )
        unauthorized_payload = response_payload(unauthorized_transfer, 401)
        if unauthorized_payload.get("success") is not False:
            raise RuntimeError("Unauthorized transfer did not fail as expected.")

        # Authorized transfer should be queued for approval.
        pending_transfer = client.post(
            "/api/inventory/transfer",
            headers={
                "X-Inventory-Token": write_token,
                "X-Inventory-User": actor,
            },
            json={
                "effectiveDate": today,
                "createdBy": actor,
                "memo": f"Batch4 pending transfer {run_id}",
                "lines": [
                    {
                        "sku": sku,
                        "qty": 2,
                        "fromLocationId": location_a_id,
                        "toLocationId": location_b_id,
                    }
                ],
            },
        )
        pending_payload = response_payload(pending_transfer, 202)
        if not pending_payload.get("success"):
            raise RuntimeError("Pending transfer did not return success=true.")
        request_data = pending_payload.get("data", {}).get("request", {})
        approval_request_id = request_data.get("requestId")
        if not approval_request_id:
            raise RuntimeError("Pending transfer did not return approval request id.")

        unauthorized_approvals = client.get("/api/inventory/approvals?status=all&limit=20")
        unauthorized_approvals_payload = response_payload(unauthorized_approvals, 401)
        if unauthorized_approvals_payload.get("success") is not False:
            raise RuntimeError("Approvals list without admin token should fail.")

        approvals_response = client.get(
            "/api/inventory/approvals?status=all&limit=20",
            headers={
                "X-Inventory-Admin-Token": admin_token,
                "X-Inventory-User": actor,
            },
        )
        approvals_payload = response_payload(approvals_response, 200)
        rows = approvals_payload.get("data", {}).get("rows", [])
        if not any(row.get("requestId") == approval_request_id for row in rows):
            raise RuntimeError("Approval request not found in admin approvals list.")

        approve_response = client.post(
            f"/api/inventory/approvals/{approval_request_id}/approve",
            headers={
                "X-Inventory-Admin-Token": admin_token,
                "X-Inventory-User": actor,
            },
            json={"note": "batch4 smoke approve"},
        )
        approve_payload = response_payload(approve_response, 200)
        execution_result = approve_payload.get("data", {}).get("executionResult", {})
        executed_event_id = execution_result.get("eventId")
        if not executed_event_id:
            raise RuntimeError("Approve response missing execution eventId.")

        queue_summary_response = client.get("/api/inventory/queue-summary?recent_limit=20")
        queue_summary_payload = response_payload(queue_summary_response, 200)
        pending_count = int(queue_summary_payload.get("data", {}).get("counts", {}).get("pending", 0))
        if pending_count < 1:
            raise RuntimeError("Queue summary pending count should be at least 1 after approval execution.")

        audit_response = client.get(
            "/api/inventory/audit?limit=30",
            headers={
                "X-Inventory-Admin-Token": admin_token,
                "X-Inventory-User": actor,
            },
        )
        audit_payload = response_payload(audit_response, 200)
        audit_rows = audit_payload.get("data", {}).get("rows", [])
        if not audit_rows:
            raise RuntimeError("Audit endpoint returned no rows.")

        print("INVENTORY_BATCH4_SMOKE_PASS")
        print(
            json.dumps(
                {
                    "runId": run_id,
                    "approvalRequestId": approval_request_id,
                    "executedEventId": executed_event_id,
                    "pendingCount": pending_count,
                    "auditRows": len(audit_rows),
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
        cleanup_approval_request(approval_request_id)


if __name__ == "__main__":
    main()
