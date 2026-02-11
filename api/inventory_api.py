from __future__ import annotations

import json
import os
import subprocess
import threading
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from flask import Blueprint, jsonify, request


inventory_api = Blueprint("inventory_api", __name__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
APPROVALS_PATH = PROJECT_ROOT / ".tmp" / "inventory_approval_requests.json"
AUDIT_PATH = PROJECT_ROOT / ".tmp" / "inventory_api_audit.jsonl"
APPROVALS_LOCK = threading.Lock()
AUDIT_LOCK = threading.Lock()

HEADER_WRITE_TOKEN = "X-Inventory-Token"
HEADER_ADMIN_TOKEN = "X-Inventory-Admin-Token"
HEADER_USER = "X-Inventory-User"


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _success(data: Any, status_code: int = 200):
    return (
        jsonify(
            {
                "success": True,
                "data": data,
                "timestamp": _timestamp(),
            }
        ),
        status_code,
    )


def _error(message: str, status_code: int = 400):
    return (
        jsonify(
            {
                "success": False,
                "error": message,
                "timestamp": _timestamp(),
            }
        ),
        status_code,
    )


def _extract_json(stdout: str) -> dict[str, Any] | list[Any] | None:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    for line in reversed(lines):
        if (line.startswith("{") and line.endswith("}")) or (
            line.startswith("[") and line.endswith("]")
        ):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue

    starts = [idx for idx in (stdout.find("{"), stdout.find("[")) if idx >= 0]
    if not starts:
        return None
    start = min(starts)
    end_object = stdout.rfind("}")
    end_array = stdout.rfind("]")
    end = max(end_object, end_array)
    if end < start:
        return None
    try:
        return json.loads(stdout[start : end + 1])
    except json.JSONDecodeError:
        return None


def _convex_command(function_name: str, args_obj: dict[str, Any]) -> list[str]:
    cmd = [
        "npx",
        "convex",
        "run",
        "--typecheck",
        "disable",
        "--codegen",
        "disable",
    ]
    env_file = os.getenv("CONVEX_ENV_FILE", "").strip()
    if env_file:
        cmd.extend(["--env-file", env_file])
    cmd.extend([function_name, json.dumps(args_obj)])

    if os.name == "nt":
        return ["cmd", "/c", *cmd]
    return cmd


def convex_run(function_name: str, args_obj: dict[str, Any]) -> dict[str, Any] | list[Any]:
    cmd = _convex_command(function_name, args_obj)
    proc = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
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


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


def _optional_str(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _actor_from_request(payload: dict[str, Any] | None = None) -> str:
    if payload:
        payload_actor = _optional_str(payload, "createdBy")
        if payload_actor:
            return payload_actor
    header_actor = (request.headers.get(HEADER_USER) or "").strip()
    if header_actor:
        return header_actor
    return "unknown"


def _security_config() -> dict[str, Any]:
    return {
        "writeTokenRequired": bool(os.getenv("INVENTORY_WRITE_TOKEN", "").strip()),
        "adminTokenRequired": bool(os.getenv("INVENTORY_ADMIN_TOKEN", "").strip()),
        "approvalEnabled": _parse_bool(os.getenv("INVENTORY_REQUIRE_APPROVAL"), False),
        "approvalQtyThreshold": _approval_qty_threshold(),
    }


def _write_access_error(actor: str):
    _record_audit(
        action="write_access_denied",
        outcome="denied",
        actor=actor,
        details={"path": request.path},
    )
    return _error("Write access denied: invalid or missing inventory token.", 401)


def _admin_access_error(actor: str):
    _record_audit(
        action="admin_access_denied",
        outcome="denied",
        actor=actor,
        details={"path": request.path},
    )
    return _error("Admin access denied: invalid or missing admin token.", 401)


def _require_write_access(actor: str):
    expected = os.getenv("INVENTORY_WRITE_TOKEN", "").strip()
    if not expected:
        return None
    supplied = (request.headers.get(HEADER_WRITE_TOKEN) or "").strip()
    if supplied != expected:
        return _write_access_error(actor)
    return None


def _require_admin_access(actor: str):
    expected = os.getenv("INVENTORY_ADMIN_TOKEN", "").strip()
    if not expected:
        return None
    supplied = (request.headers.get(HEADER_ADMIN_TOKEN) or "").strip()
    if supplied != expected:
        return _admin_access_error(actor)
    return None


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _record_audit(action: str, outcome: str, actor: str, details: dict[str, Any]) -> None:
    entry = {
        "timestamp": _timestamp(),
        "action": action,
        "outcome": outcome,
        "actor": actor,
        "path": request.path if request else "",
        "method": request.method if request else "",
        "details": details,
    }
    with AUDIT_LOCK:
        _ensure_parent(AUDIT_PATH)
        with AUDIT_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry) + "\n")


def _read_audit(limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    if not AUDIT_PATH.exists():
        return []

    with AUDIT_LOCK:
        lines = AUDIT_PATH.read_text(encoding="utf-8").splitlines()
    rows: list[dict[str, Any]] = []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
        if len(rows) >= limit:
            break
    return rows


def _read_approvals() -> list[dict[str, Any]]:
    if not APPROVALS_PATH.exists():
        return []
    try:
        data = json.loads(APPROVALS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    return []


def _write_approvals(rows: list[dict[str, Any]]) -> None:
    _ensure_parent(APPROVALS_PATH)
    APPROVALS_PATH.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def _approval_qty_threshold() -> float:
    raw = os.getenv("INVENTORY_APPROVAL_QTY_THRESHOLD", "25").strip()
    try:
        value = float(raw)
    except ValueError:
        return 25.0
    return max(value, 0.0)


def _requires_approval_for_transfer(lines: list[dict[str, Any]]) -> tuple[bool, str | None]:
    if not _parse_bool(os.getenv("INVENTORY_REQUIRE_APPROVAL"), False):
        return (False, None)
    threshold = _approval_qty_threshold()
    for line in lines:
        if not isinstance(line, dict):
            continue
        qty = float(line.get("qty", 0) or 0)
        if abs(qty) >= threshold:
            return (True, f"line qty {qty} exceeds threshold {threshold}")
    return (False, None)


def _requires_approval_for_adjustment(
    mode: str,
    lines: list[dict[str, Any]],
) -> tuple[bool, str | None]:
    if not _parse_bool(os.getenv("INVENTORY_REQUIRE_APPROVAL"), False):
        return (False, None)
    if mode == "set":
        return (True, "set-mode adjustments require approval")
    threshold = _approval_qty_threshold()
    for line in lines:
        if not isinstance(line, dict):
            continue
        qty = float(line.get("qty", 0) or 0)
        if abs(qty) >= threshold:
            return (True, f"line qty {qty} exceeds threshold {threshold}")
    return (False, None)


def _create_approval_request(
    action: str,
    payload: dict[str, Any],
    requested_by: str,
    reason: str | None,
) -> dict[str, Any]:
    request_id = str(uuid.uuid4())
    request_row = {
        "requestId": request_id,
        "status": "pending",
        "action": action,
        "payload": payload,
        "reason": reason,
        "requestedBy": requested_by,
        "requestedAt": _now_ms(),
        "decidedBy": None,
        "decidedAt": None,
        "decisionNote": None,
        "executionResult": None,
        "executionError": None,
    }
    with APPROVALS_LOCK:
        rows = _read_approvals()
        rows.append(request_row)
        _write_approvals(rows)
    return request_row


def _list_approval_requests(status: str, limit: int) -> list[dict[str, Any]]:
    with APPROVALS_LOCK:
        rows = _read_approvals()
    if status != "all":
        rows = [row for row in rows if row.get("status") == status]
    rows.sort(key=lambda row: int(row.get("requestedAt", 0)), reverse=True)
    return rows[: max(limit, 1)]


def _update_approval_request(
    request_id: str,
    updater: Any,
) -> dict[str, Any] | None:
    with APPROVALS_LOCK:
        rows = _read_approvals()
        updated_row = None
        for index, row in enumerate(rows):
            if row.get("requestId") == request_id:
                next_row = updater(dict(row))
                rows[index] = next_row
                updated_row = next_row
                break
        if updated_row is None:
            return None
        _write_approvals(rows)
    return updated_row


def _execute_approval_request(row: dict[str, Any]) -> dict[str, Any]:
    action = row.get("action")
    payload = row.get("payload")
    if not isinstance(payload, dict):
        raise RuntimeError("Approval payload is invalid.")

    if action == "create_transfer":
        result = convex_run("inventory:createTransferEvent", payload)
    elif action == "create_adjustment":
        result = convex_run("inventory:createAdjustmentEvent", payload)
    else:
        raise RuntimeError(f"Unsupported approval action: {action}")

    if not isinstance(result, dict):
        raise RuntimeError("Convex mutation returned non-object payload.")
    return result


@inventory_api.route("/api/inventory/security-config", methods=["GET"])
def security_config():
    try:
        return _success(_security_config())
    except Exception as exc:  # noqa: BLE001
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/health", methods=["GET"])
def inventory_health():
    try:
        return _success(convex_run("functions:health", {}))
    except Exception as exc:  # noqa: BLE001
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/locations", methods=["GET"])
def list_locations():
    try:
        include_inactive = _parse_bool(request.args.get("include_inactive"), True)
        return _success(
            convex_run(
                "inventory:listLocations",
                {"includeInactive": include_inactive},
            )
        )
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:  # noqa: BLE001
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/overview", methods=["GET"])
def inventory_overview():
    try:
        args: dict[str, Any] = {
            "includeInactive": _parse_bool(request.args.get("include_inactive"), False),
        }
        search = (request.args.get("search") or "").strip()
        location_id = (request.args.get("location_id") or "").strip()
        if search:
            args["search"] = search
        if location_id:
            args["locationId"] = location_id

        limit_raw = (request.args.get("limit") or "").strip()
        if limit_raw:
            args["limit"] = int(limit_raw)

        return _success(convex_run("inventory:getInventoryOverview", args))
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:  # noqa: BLE001
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/events", methods=["GET"])
def inventory_events():
    try:
        limit_raw = (request.args.get("limit") or "50").strip()
        limit = int(limit_raw)
        return _success(convex_run("inventory:listRecentEvents", {"limit": limit}))
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:  # noqa: BLE001
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/queue-summary", methods=["GET"])
def queue_summary():
    try:
        limit_raw = (request.args.get("recent_limit") or "20").strip()
        recent_limit = int(limit_raw)
        return _success(convex_run("inventory:getQueueSummary", {"recentLimit": recent_limit}))
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:  # noqa: BLE001
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/item/<sku>", methods=["GET"])
def item_detail(sku: str):
    try:
        event_limit_raw = (request.args.get("event_limit") or "20").strip()
        payload = {
            "sku": sku,
            "eventLimit": int(event_limit_raw),
        }
        return _success(convex_run("inventory:getItemDetail", payload))
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:  # noqa: BLE001
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/location", methods=["POST"])
def upsert_location():
    payload = request.get_json(silent=True) or {}
    actor = _actor_from_request(payload)
    denied = _require_admin_access(actor)
    if denied:
        return denied

    try:
        code = _optional_str(payload, "code")
        display_name = _optional_str(payload, "displayName")
        if not code or not display_name:
            return _error("code and displayName are required.", 400)

        args: dict[str, Any] = {
            "code": code,
            "displayName": display_name,
        }
        active = payload.get("active")
        is_virtual = payload.get("isVirtual")
        if isinstance(active, bool):
            args["active"] = active
        if isinstance(is_virtual, bool):
            args["isVirtual"] = is_virtual

        qb_site_full_name = _optional_str(payload, "qbSiteFullName")
        qb_site_list_id = _optional_str(payload, "qbSiteListId")
        if qb_site_full_name is not None:
            args["qbSiteFullName"] = qb_site_full_name
        if qb_site_list_id is not None:
            args["qbSiteListId"] = qb_site_list_id

        result = convex_run("inventory:upsertLocation", args)
        _record_audit(
            action="upsert_location",
            outcome="success",
            actor=actor,
            details={"code": code, "created": result.get("created") if isinstance(result, dict) else None},
        )
        return _success(result, 201)
    except Exception as exc:  # noqa: BLE001
        _record_audit(
            action="upsert_location",
            outcome="error",
            actor=actor,
            details={"error": str(exc)},
        )
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/transfer", methods=["POST"])
def create_transfer():
    payload = request.get_json(silent=True) or {}
    actor = _actor_from_request(payload)
    denied = _require_write_access(actor)
    if denied:
        return denied

    try:
        lines = payload.get("lines")
        if not isinstance(lines, list) or not lines:
            return _error("lines must be a non-empty array.", 400)

        args: dict[str, Any] = {
            "effectiveDate": _optional_str(payload, "effectiveDate") or date.today().isoformat(),
            "lines": lines,
            "createdBy": _optional_str(payload, "createdBy") or actor,
        }
        memo = _optional_str(payload, "memo")
        if memo is not None:
            args["memo"] = memo

        needs_approval, reason = _requires_approval_for_transfer(lines)
        if needs_approval:
            approval = _create_approval_request(
                action="create_transfer",
                payload=args,
                requested_by=actor,
                reason=reason,
            )
            _record_audit(
                action="create_transfer",
                outcome="pending_approval",
                actor=actor,
                details={"requestId": approval["requestId"], "reason": reason},
            )
            return _success({"status": "pending_approval", "request": approval}, 202)

        result = convex_run("inventory:createTransferEvent", args)
        _record_audit(
            action="create_transfer",
            outcome="success",
            actor=actor,
            details={"eventId": result.get("eventId") if isinstance(result, dict) else None},
        )
        return _success(result, 201)
    except Exception as exc:  # noqa: BLE001
        _record_audit(
            action="create_transfer",
            outcome="error",
            actor=actor,
            details={"error": str(exc)},
        )
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/adjustment", methods=["POST"])
def create_adjustment():
    payload = request.get_json(silent=True) or {}
    actor = _actor_from_request(payload)
    denied = _require_write_access(actor)
    if denied:
        return denied

    try:
        lines = payload.get("lines")
        location_id = _optional_str(payload, "locationId")
        mode = _optional_str(payload, "mode")
        if not isinstance(lines, list) or not lines:
            return _error("lines must be a non-empty array.", 400)
        if not location_id:
            return _error("locationId is required.", 400)
        if mode not in {"delta", "set"}:
            return _error("mode must be one of: delta, set.", 400)

        args: dict[str, Any] = {
            "effectiveDate": _optional_str(payload, "effectiveDate") or date.today().isoformat(),
            "locationId": location_id,
            "mode": mode,
            "lines": lines,
            "createdBy": _optional_str(payload, "createdBy") or actor,
        }
        memo = _optional_str(payload, "memo")
        reason_code = _optional_str(payload, "reasonCode")
        if memo is not None:
            args["memo"] = memo
        if reason_code is not None:
            args["reasonCode"] = reason_code

        needs_approval, reason = _requires_approval_for_adjustment(mode, lines)
        if needs_approval:
            approval = _create_approval_request(
                action="create_adjustment",
                payload=args,
                requested_by=actor,
                reason=reason,
            )
            _record_audit(
                action="create_adjustment",
                outcome="pending_approval",
                actor=actor,
                details={"requestId": approval["requestId"], "reason": reason},
            )
            return _success({"status": "pending_approval", "request": approval}, 202)

        result = convex_run("inventory:createAdjustmentEvent", args)
        _record_audit(
            action="create_adjustment",
            outcome="success",
            actor=actor,
            details={"eventId": result.get("eventId") if isinstance(result, dict) else None},
        )
        return _success(result, 201)
    except Exception as exc:  # noqa: BLE001
        _record_audit(
            action="create_adjustment",
            outcome="error",
            actor=actor,
            details={"error": str(exc)},
        )
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/events/<event_id>/void", methods=["POST"])
def void_event(event_id: str):
    actor = _actor_from_request(None)
    denied = _require_admin_access(actor)
    if denied:
        return denied
    try:
        result = convex_run("inventory:voidEvent", {"eventId": event_id})
        _record_audit(
            action="void_event",
            outcome="success",
            actor=actor,
            details={"eventId": event_id},
        )
        return _success(result)
    except Exception as exc:  # noqa: BLE001
        _record_audit(
            action="void_event",
            outcome="error",
            actor=actor,
            details={"eventId": event_id, "error": str(exc)},
        )
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/events/<event_id>/retry", methods=["POST"])
def retry_event(event_id: str):
    actor = _actor_from_request(None)
    denied = _require_admin_access(actor)
    if denied:
        return denied
    try:
        result = convex_run("qb_queue:retryFailedEvent", {"eventId": event_id})
        _record_audit(
            action="retry_event",
            outcome="success",
            actor=actor,
            details={"eventId": event_id},
        )
        return _success(result)
    except Exception as exc:  # noqa: BLE001
        _record_audit(
            action="retry_event",
            outcome="error",
            actor=actor,
            details={"eventId": event_id, "error": str(exc)},
        )
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/approvals", methods=["GET"])
def list_approvals():
    actor = _actor_from_request(None)
    denied = _require_admin_access(actor)
    if denied:
        return denied
    try:
        status = (request.args.get("status") or "pending").strip().lower()
        if status not in {"pending", "approved", "rejected", "error", "all"}:
            return _error("status must be one of: pending, approved, rejected, error, all.", 400)
        limit = int((request.args.get("limit") or "50").strip())
        rows = _list_approval_requests(status=status, limit=limit)
        return _success({"rows": rows, "status": status, "limit": limit})
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:  # noqa: BLE001
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/approvals/<request_id>/approve", methods=["POST"])
def approve_request(request_id: str):
    actor = _actor_from_request(None)
    denied = _require_admin_access(actor)
    if denied:
        return denied

    payload = request.get_json(silent=True) or {}
    decision_note = _optional_str(payload, "note")

    def set_in_progress(row: dict[str, Any]) -> dict[str, Any]:
        if row.get("status") != "pending":
            raise RuntimeError(f"Request is not pending (status={row.get('status')}).")
        row["status"] = "in_progress"
        row["decidedBy"] = actor
        row["decidedAt"] = _now_ms()
        row["decisionNote"] = decision_note
        return row

    try:
        staged = _update_approval_request(request_id, set_in_progress)
        if staged is None:
            return _error("Approval request not found.", 404)

        execution_result = _execute_approval_request(staged)

        def mark_approved(row: dict[str, Any]) -> dict[str, Any]:
            row["status"] = "approved"
            row["decidedBy"] = actor
            row["decidedAt"] = _now_ms()
            row["decisionNote"] = decision_note
            row["executionResult"] = execution_result
            row["executionError"] = None
            return row

        updated = _update_approval_request(request_id, mark_approved)
        _record_audit(
            action="approve_request",
            outcome="success",
            actor=actor,
            details={"requestId": request_id},
        )
        return _success({"request": updated, "executionResult": execution_result})
    except Exception as exc:  # noqa: BLE001
        def mark_error(row: dict[str, Any]) -> dict[str, Any]:
            row["status"] = "error"
            row["decidedBy"] = actor
            row["decidedAt"] = _now_ms()
            row["decisionNote"] = decision_note
            row["executionError"] = str(exc)
            return row

        _update_approval_request(request_id, mark_error)
        _record_audit(
            action="approve_request",
            outcome="error",
            actor=actor,
            details={"requestId": request_id, "error": str(exc)},
        )
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/approvals/<request_id>/reject", methods=["POST"])
def reject_request(request_id: str):
    actor = _actor_from_request(None)
    denied = _require_admin_access(actor)
    if denied:
        return denied

    payload = request.get_json(silent=True) or {}
    decision_note = _optional_str(payload, "note")

    def mark_rejected(row: dict[str, Any]) -> dict[str, Any]:
        if row.get("status") != "pending":
            raise RuntimeError(f"Request is not pending (status={row.get('status')}).")
        row["status"] = "rejected"
        row["decidedBy"] = actor
        row["decidedAt"] = _now_ms()
        row["decisionNote"] = decision_note
        return row

    try:
        updated = _update_approval_request(request_id, mark_rejected)
        if updated is None:
            return _error("Approval request not found.", 404)
        _record_audit(
            action="reject_request",
            outcome="success",
            actor=actor,
            details={"requestId": request_id},
        )
        return _success({"request": updated})
    except Exception as exc:  # noqa: BLE001
        _record_audit(
            action="reject_request",
            outcome="error",
            actor=actor,
            details={"requestId": request_id, "error": str(exc)},
        )
        return _error(str(exc), 500)


@inventory_api.route("/api/inventory/audit", methods=["GET"])
def read_audit():
    actor = _actor_from_request(None)
    denied = _require_admin_access(actor)
    if denied:
        return denied
    try:
        limit = int((request.args.get("limit") or "100").strip())
        rows = _read_audit(limit=max(1, min(limit, 500)))
        return _success({"rows": rows, "limit": limit})
    except Exception as exc:  # noqa: BLE001
        return _error(str(exc), 500)
