"""
End-to-end smoke test for QBWC middleware against the local Convex deployment.

Flow:
1. Seed smoke data in Convex (locations, SKU, adjustment event).
2. Start QBWC middleware service as a subprocess.
3. Execute SOAP calls: authenticate -> sendRequestXML -> receiveResponseXML.
4. Verify the event moves to `applied` in Convex.
5. Cleanup smoke data and stop the middleware.

Usage:
  python execution/smoke_qbwc_roundtrip.py
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

import requests


PROJECT_ROOT = Path(__file__).resolve().parent.parent


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


def soap_envelope(method_name: str, inner_xml: str) -> str:
    return (
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
        "<soap:Body>"
        f"<{method_name} xmlns=\"http://developer.intuit.com/\">"
        f"{inner_xml}"
        f"</{method_name}>"
        "</soap:Body>"
        "</soap:Envelope>"
    )


def post_soap(url: str, xml_body: str) -> str:
    response = requests.post(
        url,
        data=xml_body.encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=utf-8"},
        timeout=20,
    )
    response.raise_for_status()
    return response.text


def parse_first_text(xml_text: str, local_name: str) -> str:
    root = ET.fromstring(xml_text)
    for element in root.iter():
        tag = element.tag
        if "}" in tag:
            tag = tag.split("}", 1)[1]
        if tag == local_name:
            return (element.text or "").strip()
    raise RuntimeError(f"Tag {local_name} not found in SOAP response.\n{xml_text}")


def parse_auth_ticket(xml_text: str) -> str:
    root = ET.fromstring(xml_text)
    values: list[str] = []
    for element in root.iter():
        tag = element.tag
        if "}" in tag:
            tag = tag.split("}", 1)[1]
        if tag == "string":
            values.append((element.text or "").strip())
    if len(values) < 1:
        raise RuntimeError(f"authenticate response missing ticket.\n{xml_text}")
    return values[0]


def extract_qbxml_request_id(qbxml_text: str) -> str:
    root = ET.fromstring(qbxml_text)
    for element in root.iter():
        tag = element.tag
        if "}" in tag:
            tag = tag.split("}", 1)[1]
        if tag.endswith("Rq") and "requestID" in element.attrib:
            return element.attrib["requestID"]
    raise RuntimeError(f"No requestID found in qbXML request.\n{qbxml_text}")


def wait_for_health(url: str, timeout_seconds: int = 30) -> None:
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=2)
            if response.ok:
                return
            last_error = f"HTTP {response.status_code}: {response.text}"
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
        time.sleep(0.5)
    raise RuntimeError(f"Middleware health check failed: {last_error}")


@dataclass
class SmokeContext:
    run_id: int
    location_a_code: str
    location_b_code: str
    sku: str
    event_id: str | None = None


def main() -> None:
    run_id = int(time.time())
    ctx = SmokeContext(
        run_id=run_id,
        location_a_code=f"SMOKE_A_{run_id}",
        location_b_code=f"SMOKE_B_{run_id}",
        sku=f"SMOKE-SKU-{run_id}",
    )

    port = random.randint(18085, 18985)
    soap_url = f"http://127.0.0.1:{port}/qbwc"
    health_url = f"http://127.0.0.1:{port}/"
    qbwc_username = f"qbwc-smoke-{run_id}"
    qbwc_password = f"qbwc-pass-{run_id}"

    service_proc: subprocess.Popen[str] | None = None
    try:
        location_b = convex_run(
            "inventory:upsertLocation",
            {
                "code": ctx.location_b_code,
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
                        "Asset_Account": "12100 · Inventory Asset",
                        "COGS_Account": "COG's:Supplies",
                        "Category": "Smoke",
                        "Cost": 5,
                        "Description": f"Smoke roundtrip part {run_id}",
                        "MPN": "",
                        "Preferred_Vendor": "",
                        "Price": 10,
                        "Purchase_Description": f"Smoke roundtrip part {run_id}",
                        "Quantity_On_Hand_2025": 0,
                        "Reorder_Pt_Min": 1,
                        "Sales_Tax_Code": "Non",
                        "Sku": ctx.sku,
                        "Tax_Agency": "",
                        "Type": "Inventory Part",
                        "U_M": "each (ea)",
                        "U_M_Set": "Count in each",
                        "isActive": True,
                    }
                ]
            },
        )

        event = convex_run(
            "inventory:createAdjustmentEvent",
            {
                "effectiveDate": "2026-02-11",
                "locationId": location_b_id,
                "mode": "delta",
                "memo": f"Smoke qbwc event {run_id}",
                "createdBy": "smoke-test",
                "reasonCode": "cycle_count",
                "lines": [{"sku": ctx.sku, "qty": 2}],
            },
        )
        ctx.event_id = event["eventId"]

        env = os.environ.copy()
        env["QBWC_USERNAME"] = qbwc_username
        env["QBWC_PASSWORD"] = qbwc_password
        env["QBWC_BIND_HOST"] = "127.0.0.1"
        env["QBWC_BIND_PORT"] = str(port)
        env["QB_ADJUSTMENT_ACCOUNT_DEFAULT"] = "Inventory Adjustments"
        env["CONVEX_ENV_FILE"] = ""

        service_proc = subprocess.Popen(
            [sys.executable, "execution/start_qbwc_service.py"],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        wait_for_health(health_url, timeout_seconds=45)

        auth_xml = soap_envelope(
            "authenticate",
            (
                f"<strUserName>{qbwc_username}</strUserName>"
                f"<strPassword>{qbwc_password}</strPassword>"
            ),
        )
        auth_response = post_soap(soap_url, auth_xml)
        ticket = parse_auth_ticket(auth_response)
        if ticket in {"", "nvu"}:
            raise RuntimeError(f"authenticate failed. ticket={ticket}\n{auth_response}")

        send_req_xml = soap_envelope(
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
        send_response = post_soap(soap_url, send_req_xml)
        qbxml_request = parse_first_text(send_response, "sendRequestXMLResult")
        if "InventoryAdjustmentAddRq" not in qbxml_request:
            raise RuntimeError(f"sendRequestXML did not return adjustment qbXML.\n{send_response}")
        processed_event_id = extract_qbxml_request_id(qbxml_request)

        qbxml_success = (
            "<?xml version=\"1.0\"?>"
            "<QBXML><QBXMLMsgsRs>"
            f"<InventoryAdjustmentAddRs requestID=\"{processed_event_id}\" statusCode=\"0\" "
            "statusSeverity=\"Info\" statusMessage=\"Status OK\">"
            "<InventoryAdjustmentRet><TxnID>TXN-SMOKE-ROUNDTRIP</TxnID></InventoryAdjustmentRet>"
            "</InventoryAdjustmentAddRs>"
            "</QBXMLMsgsRs></QBXML>"
        )
        receive_req_xml = soap_envelope(
            "receiveResponseXML",
            (
                f"<ticket>{ticket}</ticket>"
                f"<response>{escape(qbxml_success)}</response>"
                "<hresult></hresult>"
                "<message></message>"
            ),
        )
        receive_response = post_soap(soap_url, receive_req_xml)
        completion = parse_first_text(receive_response, "receiveResponseXMLResult")
        if completion != "100":
            raise RuntimeError(f"receiveResponseXML completion expected 100, got {completion}")

        found_status = None
        for _ in range(10):
            events = convex_run("inventory:listRecentEvents", {"limit": 50}).get("rows", [])
            for row in events:
                if row.get("eventId") == processed_event_id:
                    found_status = row.get("qbStatus")
                    break
            if found_status == "applied":
                break
            time.sleep(0.5)
        if found_status != "applied":
            raise RuntimeError(
                f"Expected event {processed_event_id} to be applied, observed qbStatus={found_status}"
            )

        print("QBWC_ROUNDTRIP_PASS")
        print(
            json.dumps(
                {
                    "runId": run_id,
                    "seededEventId": ctx.event_id,
                    "processedEventId": processed_event_id,
                    "qbStatus": found_status,
                    "soapUrl": soap_url,
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

        if service_proc is not None:
            service_proc.terminate()
            try:
                service_proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                service_proc.kill()


if __name__ == "__main__":
    main()
