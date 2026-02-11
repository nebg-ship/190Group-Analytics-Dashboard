"""
Smoke-test QBWC middleware behavior without touching a real QuickBooks file.

This script validates:
- authenticate -> ticket
- sendRequestXML -> qbXML payload
- receiveResponseXML -> Convex apply success
- getLastError/closeConnection basic behavior
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Any

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from qb_sync_service.config import QbSyncConfig
from qb_sync_service.service import QbwcService


@dataclass
class FakeConvexClient:
    events: list[dict[str, Any]] = field(default_factory=list)
    in_flight_calls: list[dict[str, Any]] = field(default_factory=list)
    apply_calls: list[dict[str, Any]] = field(default_factory=list)

    def get_next_pending_event(self, limit: int = 1) -> dict[str, Any]:
        return {"events": self.events[:limit]}

    def mark_event_in_flight(self, event_id: str, ticket: str) -> dict[str, Any]:
        self.in_flight_calls.append({"eventId": event_id, "ticket": ticket})
        return {"eventId": event_id, "ticket": ticket, "qbStatus": "in_flight"}

    def apply_qb_result(
        self,
        event_id: str,
        ticket: str | None,
        *,
        success: bool,
        qb_txn_id: str | None = None,
        qb_txn_type: str | None = None,
        qb_error_code: str | None = None,
        qb_error_message: str | None = None,
        retryable: bool | None = None,
    ) -> dict[str, Any]:
        payload = {
            "eventId": event_id,
            "ticket": ticket,
            "success": success,
            "qbTxnId": qb_txn_id,
            "qbTxnType": qb_txn_type,
            "qbErrorCode": qb_error_code,
            "qbErrorMessage": qb_error_message,
            "retryable": retryable,
        }
        self.apply_calls.append(payload)
        return payload


def main() -> None:
    fake_event = {
        "eventId": "jh_fake_transfer_1",
        "eventType": "transfer",
        "status": "committed",
        "qbStatus": "pending",
        "qbTxnType": "TransferInventoryAdd",
        "effectiveDate": "2026-02-11",
        "createdBy": "smoke-test",
        "memo": "smoke transfer",
        "idempotencyKey": "jh_fake_transfer_1",
        "lines": [
            {
                "sku": "SMOKE-SKU-TEST",
                "qty": 2,
                "qbItemFullName": "SMOKE-SKU-TEST",
                "fromSiteFullName": "Smoke A",
                "toSiteFullName": "Smoke B",
            }
        ],
    }

    fake_convex = FakeConvexClient(events=[fake_event])
    config = QbSyncConfig(
        qbwc_username="qbwc-user",
        qbwc_password="qbwc-pass",
        qb_company_file="",
        qbxml_version="13.0",
        default_adjustment_account="Inventory Adjustments",
        server_version="190Group-QBWC-0.1.0",
        min_client_version="",
        bind_host="127.0.0.1",
        bind_port=8085,
        convex_env_file="",
    )
    service = QbwcService(config=config, convex_client=fake_convex)

    auth_result = service.authenticate("qbwc-user", "qbwc-pass")
    assert auth_result[0] not in {"nvu", ""}, "authenticate did not return a ticket."
    ticket = auth_result[0]

    qbxml_request = service.send_request_xml(
        ticket=ticket,
        _hcp_response="",
        _company_file_name="",
        _qbxml_country="US",
        _qbxml_major="13",
        _qbxml_minor="0",
    )
    assert "TransferInventoryAddRq" in qbxml_request, "sendRequestXML did not build transfer qbXML."
    assert fake_convex.in_flight_calls, "mark_event_in_flight was not called."

    qbxml_response = (
        "<?xml version=\"1.0\"?>"
        "<QBXML>"
        "<QBXMLMsgsRs>"
        "<TransferInventoryAddRs requestID=\"jh_fake_transfer_1\" "
        "statusCode=\"0\" statusSeverity=\"Info\" statusMessage=\"Status OK\">"
        "<TransferInventoryRet><TxnID>TXN-SMOKE-1</TxnID></TransferInventoryRet>"
        "</TransferInventoryAddRs>"
        "</QBXMLMsgsRs>"
        "</QBXML>"
    )
    completion = service.receive_response_xml(
        ticket=ticket,
        response_xml=qbxml_response,
        hresult="",
        message="",
    )
    assert completion == 100, "receiveResponseXML should return 100."
    assert fake_convex.apply_calls, "apply_qb_result was not called."
    assert fake_convex.apply_calls[-1]["success"] is True, "apply_qb_result should mark success."
    assert fake_convex.apply_calls[-1]["qbTxnId"] == "TXN-SMOKE-1", "TxnID was not parsed."

    assert service.get_last_error(ticket) == "No error recorded.", "Unexpected last error."
    assert service.close_connection(ticket) == "OK", "closeConnection should return OK."

    print("QBWC_SMOKE_PASS")


if __name__ == "__main__":
    main()
