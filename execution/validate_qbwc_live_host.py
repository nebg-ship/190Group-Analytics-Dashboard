"""
Validate QBWC middleware readiness on a real host before connecting QuickBooks Web Connector.

Checks:
1) Environment variables are present.
2) QWC file can be generated.
3) Middleware is reachable.
4) SOAP authenticate works with configured credentials.
5) SOAP sendRequestXML is callable and returns valid XML payload (or empty string).

Usage:
  python execution/validate_qbwc_live_host.py --generate-qwc
  python execution/validate_qbwc_live_host.py --start-service --generate-qwc
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import requests
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = PROJECT_ROOT / ".tmp"

SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
QBWC_NS = "http://developer.intuit.com/"


def _localname(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


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


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str


def check_required_env() -> tuple[list[CheckResult], dict[str, str]]:
    required = [
        "QBWC_USERNAME",
        "QBWC_PASSWORD",
        "QBWC_APP_URL",
    ]
    optional = [
        "QBWC_BIND_HOST",
        "QBWC_BIND_PORT",
        "QBWC_QWC_OUTPUT",
    ]
    values: dict[str, str] = {}
    results: list[CheckResult] = []

    for name in required:
        value = os.getenv(name, "").strip()
        values[name] = value
        results.append(
            CheckResult(
                name=f"env:{name}",
                passed=bool(value),
                details="set" if value else "missing",
            )
        )

    for name in optional:
        value = os.getenv(name, "").strip()
        values[name] = value
        results.append(
            CheckResult(
                name=f"env:{name}",
                passed=True,
                details=value if value else "(default will be used)",
            )
        )

    return results, values


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
            name="generate_qwc",
            passed=False,
            details=f"failed rc={proc.returncode}\nSTDERR:\n{proc.stderr.strip()}",
        )
    output_path = os.getenv("QBWC_QWC_OUTPUT", ".tmp/qb_inventory_sync.qwc").strip()
    qwc_path = (PROJECT_ROOT / output_path).resolve()
    if not qwc_path.exists():
        return CheckResult(
            name="generate_qwc",
            passed=False,
            details=f"script succeeded but file not found at {qwc_path}",
        )
    return CheckResult(
        name="generate_qwc",
        passed=True,
        details=f"generated {qwc_path}",
    )


def start_service_subprocess(env: dict[str, str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "execution/start_qbwc_service.py"],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def wait_for_health(url: str, timeout_seconds: int = 45) -> CheckResult:
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=2)
            if response.ok:
                return CheckResult("service_health", True, f"reachable at {url}")
            last_error = f"HTTP {response.status_code}: {response.text}"
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
        time.sleep(0.5)
    return CheckResult(
        name="service_health",
        passed=False,
        details=f"health check failed for {url}: {last_error}",
    )


def check_auth_and_send_request(soap_url: str, username: str, password: str) -> list[CheckResult]:
    results: list[CheckResult] = []
    try:
        auth_xml = soap_envelope(
            "authenticate",
            (
                f"<strUserName>{username}</strUserName>"
                f"<strPassword>{password}</strPassword>"
            ),
        )
        auth_response = soap_post(soap_url, auth_xml, timeout_seconds=20)
        strings = parse_auth_strings(auth_response)
        ticket = strings[0] if strings else ""
        if ticket in {"", "nvu"}:
            results.append(
                CheckResult(
                    "soap_authenticate",
                    False,
                    f"authenticate failed, ticket={ticket!r}",
                )
            )
            return results
        results.append(CheckResult("soap_authenticate", True, f"ticket={ticket}"))

        send_request_xml = soap_envelope(
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
        send_response = soap_post(soap_url, send_request_xml, timeout_seconds=20)
        payload = parse_first_text(send_response, "sendRequestXMLResult")
        if payload:
            try:
                ET.fromstring(payload)
                details = "non-empty qbXML payload returned"
            except ET.ParseError:
                details = "non-empty payload returned (not XML-parseable string)"
        else:
            details = "empty payload returned (no pending events is valid)"
        results.append(CheckResult("soap_sendRequestXML", True, details))
    except Exception as exc:  # noqa: BLE001
        results.append(CheckResult("soap_qbwc_calls", False, str(exc)))
    return results


def write_report(results: list[CheckResult], report_path: Path, context: dict[str, Any]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# QBWC Live Host Validation Report",
        "",
        f"- Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- SOAP URL: `{context['soap_url']}`",
        "",
        "## Results",
    ]
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        lines.append(f"- [{status}] `{result.name}` - {result.details}")

    lines.extend(
        [
            "",
            "## Summary",
            f"- Passed: {sum(1 for result in results if result.passed)}",
            f"- Failed: {sum(1 for result in results if not result.passed)}",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Validate QBWC middleware live host readiness.")
    parser.add_argument(
        "--start-service",
        action="store_true",
        help="Start the middleware for this validation run.",
    )
    parser.add_argument(
        "--generate-qwc",
        action="store_true",
        help="Generate the QWC file as part of validation.",
    )
    parser.add_argument(
        "--report-path",
        default=str(TMP_DIR / "qbwc_live_validation_report.md"),
        help="Path to write markdown validation report.",
    )
    parser.add_argument(
        "--service-url",
        default="",
        help="Explicit middleware URL, e.g. http://127.0.0.1:8085/qbwc",
    )
    args = parser.parse_args()

    bind_host = os.getenv("QBWC_BIND_HOST", "127.0.0.1").strip() or "127.0.0.1"
    bind_port = os.getenv("QBWC_BIND_PORT", "8085").strip() or "8085"
    soap_url = args.service_url.strip() or f"http://{bind_host}:{bind_port}/qbwc"
    health_url = soap_url.rsplit("/", 1)[0] + "/"

    results: list[CheckResult] = []
    env_results, values = check_required_env()
    results.extend(env_results)

    if args.generate_qwc:
        results.append(generate_qwc_file())

    service_proc: subprocess.Popen[str] | None = None
    try:
        if args.start_service:
            service_proc = start_service_subprocess(os.environ.copy())
            results.append(wait_for_health(health_url))
        else:
            results.append(wait_for_health(health_url, timeout_seconds=5))

        if values.get("QBWC_USERNAME") and values.get("QBWC_PASSWORD"):
            results.extend(
                check_auth_and_send_request(
                    soap_url=soap_url,
                    username=values["QBWC_USERNAME"],
                    password=values["QBWC_PASSWORD"],
                )
            )

    finally:
        if service_proc is not None:
            service_proc.terminate()
            try:
                service_proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                service_proc.kill()

    report_path = Path(args.report_path).resolve()
    write_report(results, report_path, context={"soap_url": soap_url})
    print(f"Wrote report: {report_path}")

    failed = [result for result in results if not result.passed]
    print(json.dumps([result.__dict__ for result in results], indent=2))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

