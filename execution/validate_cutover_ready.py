"""
Unified cutover readiness validator for Batch 2 + Batch 4.

Checks in one command:
1) Required QBWC and inventory-security environment values.
2) Batch 2 middleware health and SOAP handshake.
3) Batch 4 security mode and inventory API health.

Usage:
  python execution/validate_cutover_ready.py
  python execution/validate_cutover_ready.py --generate-qwc
  python execution/validate_cutover_ready.py --no-start-qbwc-service
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import requests
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = PROJECT_ROOT / ".tmp"
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
QBWC_NS = "http://developer.intuit.com/"


@dataclass
class CheckResult:
    section: str
    name: str
    passed: bool
    details: str


def _bool_from_text(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


def _localname(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def soap_envelope(method_name: str, inner_xml: str) -> str:
    return (
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        f"<soap:Envelope xmlns:soap=\"{SOAP_NS}\">"
        "<soap:Body>"
        f"<{method_name} xmlns=\"{QBWC_NS}\">{inner_xml}</{method_name}>"
        "</soap:Body>"
        "</soap:Envelope>"
    )


def soap_post(url: str, xml_body: str, timeout_seconds: int = 20) -> str:
    response = requests.post(
        url,
        data=xml_body.encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=utf-8"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.text


def parse_auth_strings(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text)
    values: list[str] = []
    for element in root.iter():
        if _localname(element.tag) == "string":
            values.append((element.text or "").strip())
    return values


def parse_first_text(xml_text: str, local_name: str) -> str:
    root = ET.fromstring(xml_text)
    for element in root.iter():
        if _localname(element.tag) == local_name:
            return (element.text or "").strip()
    raise RuntimeError(f"Tag {local_name} not found in response.")


def wait_for_health(url: str, timeout_seconds: int = 45) -> CheckResult:
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=2)
            if response.ok:
                return CheckResult("batch2", "middleware_health", True, f"reachable at {url}")
            last_error = f"HTTP {response.status_code}: {response.text}"
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
        time.sleep(0.5)
    return CheckResult("batch2", "middleware_health", False, f"health check failed: {last_error}")


def start_qbwc_service(env: dict[str, str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "execution/start_qbwc_service.py"],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def generate_qwc_file() -> CheckResult:
    cmd = [sys.executable, "execution/generate_qwc.py"]
    proc = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return CheckResult(
            "batch2",
            "generate_qwc",
            False,
            f"failed rc={proc.returncode}; stderr={proc.stderr.strip()}",
        )
    output_path = os.getenv("QBWC_QWC_OUTPUT", ".tmp/qb_inventory_sync.qwc").strip()
    qwc_path = (PROJECT_ROOT / output_path).resolve()
    if not qwc_path.exists():
        return CheckResult(
            "batch2",
            "generate_qwc",
            False,
            f"file not found at {qwc_path}",
        )
    return CheckResult("batch2", "generate_qwc", True, f"generated {qwc_path}")


def check_env_batch2(results: list[CheckResult], values: dict[str, str]) -> None:
    required = ["QBWC_USERNAME", "QBWC_PASSWORD", "QBWC_APP_URL"]
    optional = ["QBWC_BIND_HOST", "QBWC_BIND_PORT", "QBWC_QWC_OUTPUT", "CONVEX_ENV_FILE"]

    for name in required:
        value = os.getenv(name, "").strip()
        values[name] = value
        results.append(
            CheckResult("env", f"env:{name}", bool(value), "set" if value else "missing"),
        )

    for name in optional:
        value = os.getenv(name, "").strip()
        values[name] = value
        results.append(
            CheckResult("env", f"env:{name}", True, value if value else "(default)"),
        )


def check_env_batch4(
    results: list[CheckResult],
    values: dict[str, str],
    *,
    require_security_tokens: bool,
    require_approval_enabled: bool,
    min_approval_threshold: float,
) -> None:
    write_token = os.getenv("INVENTORY_WRITE_TOKEN", "").strip()
    admin_token = os.getenv("INVENTORY_ADMIN_TOKEN", "").strip()
    require_approval_raw = os.getenv("INVENTORY_REQUIRE_APPROVAL", "").strip()
    threshold_raw = os.getenv("INVENTORY_APPROVAL_QTY_THRESHOLD", "").strip()

    values["INVENTORY_WRITE_TOKEN"] = write_token
    values["INVENTORY_ADMIN_TOKEN"] = admin_token
    values["INVENTORY_REQUIRE_APPROVAL"] = require_approval_raw
    values["INVENTORY_APPROVAL_QTY_THRESHOLD"] = threshold_raw

    if require_security_tokens:
        results.append(
            CheckResult(
                "env",
                "env:INVENTORY_WRITE_TOKEN",
                bool(write_token),
                "set" if write_token else "missing",
            ),
        )
        results.append(
            CheckResult(
                "env",
                "env:INVENTORY_ADMIN_TOKEN",
                bool(admin_token),
                "set" if admin_token else "missing",
            ),
        )
    else:
        results.append(
            CheckResult(
                "env",
                "env:INVENTORY_WRITE_TOKEN",
                True,
                "optional" if not write_token else "set",
            ),
        )
        results.append(
            CheckResult(
                "env",
                "env:INVENTORY_ADMIN_TOKEN",
                True,
                "optional" if not admin_token else "set",
            ),
        )

    try:
        approval_enabled = _bool_from_text(require_approval_raw or "false")
        passed = approval_enabled if require_approval_enabled else True
        details = f"value={approval_enabled}"
        if require_approval_enabled and not approval_enabled:
            details += " (expected true)"
        results.append(CheckResult("env", "env:INVENTORY_REQUIRE_APPROVAL", passed, details))
    except ValueError as exc:
        results.append(CheckResult("env", "env:INVENTORY_REQUIRE_APPROVAL", False, str(exc)))

    try:
        threshold = float(threshold_raw or "25")
        passed = threshold >= min_approval_threshold
        details = f"value={threshold}"
        if not passed:
            details += f" (expected >= {min_approval_threshold})"
        results.append(CheckResult("env", "env:INVENTORY_APPROVAL_QTY_THRESHOLD", passed, details))
    except ValueError:
        results.append(
            CheckResult(
                "env",
                "env:INVENTORY_APPROVAL_QTY_THRESHOLD",
                False,
                f"invalid numeric value: {threshold_raw!r}",
            ),
        )


def run_batch2_checks(
    results: list[CheckResult],
    values: dict[str, str],
    *,
    start_service: bool,
    generate_qwc: bool,
    service_url: str,
) -> dict[str, Any]:
    if generate_qwc:
        results.append(generate_qwc_file())

    bind_host = (values.get("QBWC_BIND_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    if bind_host in {"0.0.0.0", "::"}:
        bind_host = "127.0.0.1"
    bind_port = (values.get("QBWC_BIND_PORT") or "8085").strip() or "8085"
    soap_url = service_url.strip() or f"http://{bind_host}:{bind_port}/qbwc"
    health_url = soap_url.rsplit("/", 1)[0] + "/"

    service_proc: subprocess.Popen[str] | None = None
    try:
        if start_service:
            service_proc = start_qbwc_service(os.environ.copy())
            results.append(wait_for_health(health_url, timeout_seconds=45))
        else:
            results.append(wait_for_health(health_url, timeout_seconds=8))

        username = values.get("QBWC_USERNAME", "")
        password = values.get("QBWC_PASSWORD", "")
        if username and password:
            try:
                auth_xml = soap_envelope(
                    "authenticate",
                    f"<strUserName>{username}</strUserName><strPassword>{password}</strPassword>",
                )
                auth_response = soap_post(soap_url, auth_xml, timeout_seconds=20)
                auth_values = parse_auth_strings(auth_response)
                ticket = auth_values[0] if auth_values else ""
                if ticket in {"", "nvu"}:
                    results.append(
                        CheckResult(
                            "batch2",
                            "soap_authenticate",
                            False,
                            f"authenticate failed, ticket={ticket!r}",
                        ),
                    )
                else:
                    results.append(CheckResult("batch2", "soap_authenticate", True, f"ticket={ticket}"))
                    send_xml = soap_envelope(
                        "sendRequestXML",
                        (
                            f"<ticket>{ticket}</ticket>"
                            "<strHCPResponse></strHCPResponse>"
                            "<strCompanyFileName></strCompanyFileName>"
                            "<qbXMLCountry>US</qbXMLCountry>"
                            "<qbXMLMajorVers>13</qbXMLMajorVers>"
                            "<qbXMLMinorVers>0</qbXMLMinorVers>"
                        ),
                    )
                    send_response = soap_post(soap_url, send_xml, timeout_seconds=20)
                    payload = parse_first_text(send_response, "sendRequestXMLResult")
                    if payload:
                        try:
                            ET.fromstring(payload)
                            details = "non-empty qbXML payload returned"
                        except ET.ParseError:
                            details = "non-empty payload returned (not XML-parseable)"
                    else:
                        details = "empty payload returned (no pending events is valid)"
                    results.append(CheckResult("batch2", "soap_sendRequestXML", True, details))

                    last_error_xml = soap_envelope(
                        "getLastError",
                        f"<ticket>{ticket}</ticket>",
                    )
                    last_error_response = soap_post(soap_url, last_error_xml, timeout_seconds=20)
                    last_error = parse_first_text(last_error_response, "getLastErrorResult")
                    clean_last_error = (last_error or "").strip()
                    if clean_last_error and clean_last_error != "No error recorded.":
                        results.append(
                            CheckResult(
                                "batch2",
                                "soap_getLastError",
                                False,
                                clean_last_error,
                            )
                        )
                    else:
                        results.append(
                            CheckResult(
                                "batch2",
                                "soap_getLastError",
                                True,
                                "No error recorded.",
                            )
                        )
            except Exception as exc:  # noqa: BLE001
                results.append(CheckResult("batch2", "soap_calls", False, str(exc)))
        else:
            results.append(
                CheckResult(
                    "batch2",
                    "soap_calls",
                    False,
                    "QBWC_USERNAME/QBWC_PASSWORD missing, cannot verify SOAP handshake",
                ),
            )
    finally:
        if service_proc is not None:
            service_proc.terminate()
            try:
                service_proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                service_proc.kill()

    return {"soap_url": soap_url, "health_url": health_url}


def _parse_json_response(response) -> tuple[bool, dict[str, Any] | None, str]:
    try:
        payload = response.get_json()
    except Exception as exc:  # noqa: BLE001
        return (False, None, f"invalid JSON response: {exc}")
    if not isinstance(payload, dict):
        return (False, None, f"non-object JSON payload: {type(payload)}")
    return (True, payload, "")


def run_batch4_checks(
    results: list[CheckResult],
    values: dict[str, str],
    *,
    require_security_tokens: bool,
    require_approval_enabled: bool,
    min_approval_threshold: float,
    actor: str,
) -> None:
    try:
        api_root = PROJECT_ROOT / "api"
        if str(api_root) not in sys.path:
            sys.path.insert(0, str(api_root))
        from dashboard_data import app  # type: ignore

        client = app.test_client()
    except Exception as exc:  # noqa: BLE001
        results.append(CheckResult("batch4", "inventory_api_import", False, str(exc)))
        return

    try:
        security_response = client.get("/api/inventory/security-config")
        ok, payload, parse_err = _parse_json_response(security_response)
        if not ok or payload is None:
            results.append(CheckResult("batch4", "security_config_endpoint", False, parse_err))
            return
        if security_response.status_code != 200 or not payload.get("success"):
            results.append(
                CheckResult(
                    "batch4",
                    "security_config_endpoint",
                    False,
                    f"HTTP {security_response.status_code} payload={payload}",
                ),
            )
            return

        config = payload.get("data")
        if not isinstance(config, dict):
            results.append(
                CheckResult(
                    "batch4",
                    "security_config_shape",
                    False,
                    f"unexpected data shape: {type(config)}",
                ),
            )
            return

        results.append(CheckResult("batch4", "security_config_endpoint", True, "reachable"))

        if require_security_tokens:
            write_required = bool(config.get("writeTokenRequired"))
            admin_required = bool(config.get("adminTokenRequired"))
            results.append(
                CheckResult(
                    "batch4",
                    "security_write_token_required",
                    write_required,
                    f"value={write_required}",
                ),
            )
            results.append(
                CheckResult(
                    "batch4",
                    "security_admin_token_required",
                    admin_required,
                    f"value={admin_required}",
                ),
            )

        approval_enabled = bool(config.get("approvalEnabled"))
        threshold = float(config.get("approvalQtyThreshold", 0) or 0)
        approval_passed = approval_enabled if require_approval_enabled else True
        threshold_passed = threshold >= min_approval_threshold
        results.append(
            CheckResult(
                "batch4",
                "security_approval_enabled",
                approval_passed,
                f"value={approval_enabled}",
            ),
        )
        results.append(
            CheckResult(
                "batch4",
                "security_threshold",
                threshold_passed,
                f"value={threshold}; expected>={min_approval_threshold}",
            ),
        )

        health_response = client.get("/api/inventory/health")
        ok, health_payload, parse_err = _parse_json_response(health_response)
        if not ok or health_payload is None:
            results.append(CheckResult("batch4", "inventory_health_endpoint", False, parse_err))
        else:
            passed = (
                health_response.status_code == 200
                and bool(health_payload.get("success"))
                and isinstance(health_payload.get("data"), dict)
                and bool(health_payload["data"].get("ok"))
            )
            details = (
                f"HTTP {health_response.status_code}"
                if passed
                else f"HTTP {health_response.status_code} payload={health_payload}"
            )
            results.append(CheckResult("batch4", "inventory_health_endpoint", passed, details))

        # Verify token gates:
        if bool(config.get("writeTokenRequired")):
            unauth_write = client.post("/api/inventory/transfer", json={"lines": []})
            results.append(
                CheckResult(
                    "batch4",
                    "write_gate_unauthorized",
                    unauth_write.status_code == 401,
                    f"HTTP {unauth_write.status_code}",
                ),
            )

            write_token = values.get("INVENTORY_WRITE_TOKEN", "")
            headers = {
                "X-Inventory-User": actor,
                "X-Inventory-Token": write_token,
            }
            auth_write = client.post("/api/inventory/transfer", headers=headers, json={"lines": []})
            results.append(
                CheckResult(
                    "batch4",
                    "write_gate_authorized_header",
                    auth_write.status_code != 401,
                    f"HTTP {auth_write.status_code}",
                ),
            )

        if bool(config.get("adminTokenRequired")):
            unauth_admin = client.get("/api/inventory/approvals?status=pending&limit=1")
            results.append(
                CheckResult(
                    "batch4",
                    "admin_gate_unauthorized",
                    unauth_admin.status_code == 401,
                    f"HTTP {unauth_admin.status_code}",
                ),
            )

            admin_token = values.get("INVENTORY_ADMIN_TOKEN", "")
            headers = {
                "X-Inventory-User": actor,
                "X-Inventory-Admin-Token": admin_token,
            }
            auth_admin = client.get("/api/inventory/approvals?status=pending&limit=1", headers=headers)
            results.append(
                CheckResult(
                    "batch4",
                    "admin_gate_authorized_header",
                    auth_admin.status_code == 200,
                    f"HTTP {auth_admin.status_code}",
                ),
            )

            audit_admin = client.get("/api/inventory/audit?limit=1", headers=headers)
            results.append(
                CheckResult(
                    "batch4",
                    "audit_endpoint_authorized",
                    audit_admin.status_code == 200,
                    f"HTTP {audit_admin.status_code}",
                ),
            )

    except Exception as exc:  # noqa: BLE001
        results.append(CheckResult("batch4", "inventory_api_health_checks", False, str(exc)))


def write_report(
    results: list[CheckResult],
    report_path: Path,
    *,
    context: dict[str, Any],
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    passed_count = sum(1 for result in results if result.passed)
    failed_count = sum(1 for result in results if not result.passed)
    ready = failed_count == 0

    lines = [
        "# Cutover Readiness Report",
        "",
        f"- Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Batch 2 SOAP URL: `{context.get('soap_url', '')}`",
        f"- Batch 2 Health URL: `{context.get('health_url', '')}`",
        f"- Required security tokens: `{context.get('require_security_tokens')}`",
        f"- Required approval mode: `{context.get('require_approval_enabled')}`",
        "",
        "## Results",
    ]
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        lines.append(
            f"- [{status}] `{result.section}:{result.name}` - {result.details}",
        )

    lines.extend(
        [
            "",
            "## Summary",
            f"- Passed: {passed_count}",
            f"- Failed: {failed_count}",
            f"- Cutover Ready: {'YES' if ready else 'NO'}",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Validate Batch 2 + Batch 4 cutover readiness in one command.",
    )
    parser.add_argument(
        "--start-qbwc-service",
        dest="start_qbwc_service",
        action="store_true",
        default=True,
        help="Start QBWC middleware process for validation (default: true).",
    )
    parser.add_argument(
        "--no-start-qbwc-service",
        dest="start_qbwc_service",
        action="store_false",
        help="Do not start middleware; expect it already running.",
    )
    parser.add_argument(
        "--generate-qwc",
        action="store_true",
        help="Generate QWC file as part of validation.",
    )
    parser.add_argument(
        "--service-url",
        default="",
        help="Override QBWC SOAP URL, e.g. http://127.0.0.1:8085/qbwc",
    )
    parser.add_argument(
        "--report-path",
        default=str(TMP_DIR / "cutover_readiness_report.md"),
        help="Path to write markdown report.",
    )
    parser.add_argument(
        "--allow-open-security",
        action="store_true",
        help="Do not require write/admin tokens to be configured.",
    )
    parser.add_argument(
        "--allow-approval-disabled",
        action="store_true",
        help="Do not require INVENTORY_REQUIRE_APPROVAL=true.",
    )
    parser.add_argument(
        "--min-approval-threshold",
        type=float,
        default=1.0,
        help="Minimum accepted approval quantity threshold.",
    )
    parser.add_argument(
        "--actor",
        default="cutover-validator",
        help="Actor label used for header-based security checks.",
    )
    args = parser.parse_args()

    if args.min_approval_threshold < 0:
        raise SystemExit("--min-approval-threshold must be >= 0")

    require_security_tokens = not args.allow_open_security
    require_approval_enabled = not args.allow_approval_disabled

    results: list[CheckResult] = []
    values: dict[str, str] = {}
    check_env_batch2(results, values)
    check_env_batch4(
        results,
        values,
        require_security_tokens=require_security_tokens,
        require_approval_enabled=require_approval_enabled,
        min_approval_threshold=args.min_approval_threshold,
    )

    batch2_context = run_batch2_checks(
        results,
        values,
        start_service=args.start_qbwc_service,
        generate_qwc=args.generate_qwc,
        service_url=args.service_url,
    )
    run_batch4_checks(
        results,
        values,
        require_security_tokens=require_security_tokens,
        require_approval_enabled=require_approval_enabled,
        min_approval_threshold=args.min_approval_threshold,
        actor=args.actor,
    )

    report_path = Path(args.report_path).resolve()
    write_report(
        results,
        report_path,
        context={
            **batch2_context,
            "require_security_tokens": require_security_tokens,
            "require_approval_enabled": require_approval_enabled,
        },
    )

    print(f"Wrote report: {report_path}")
    print(json.dumps([asdict(result) for result in results], indent=2))

    if any(not result.passed for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
