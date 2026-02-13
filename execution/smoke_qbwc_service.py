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
from pathlib import Path
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
    run_csv_mode_smoke()
    run_auto_create_csv_mode_smoke()
    run_qbwc_mode_smoke()
    print("QBWC_SMOKE_PASS")


def run_csv_mode_smoke() -> None:
    tmp_dir = Path(PROJECT_ROOT) / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    qb_items_csv = tmp_dir / "smoke_qb_items.csv"
    qb_items_csv.write_text(
        "Sku,Type\n"
        "SMOKE-SKU-TEST,Inventory Part\n",
        encoding="utf-8",
    )

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
        convex_run_prod=False,
        qb_items_csv=str(qb_items_csv),
        qb_items_source="csv",
        qb_items_refresh_minutes=60,
        qb_items_query_max_returned=1000,
        qb_items_query_mode="auto",
        qb_items_auto_create=True,
        qb_item_income_account_default="",
        qb_item_cogs_account_default="",
        qb_item_asset_account_default="",
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
    assert completion == 0, "receiveResponseXML should return 0 while fake client still has pending events."
    assert fake_convex.apply_calls, "apply_qb_result was not called."
    assert fake_convex.apply_calls[-1]["success"] is True, "apply_qb_result should mark success."
    assert fake_convex.apply_calls[-1]["qbTxnId"] == "TXN-SMOKE-1", "TxnID was not parsed."

    assert service.get_last_error(ticket) == "No error recorded.", "Unexpected last error."
    assert service.close_connection(ticket) == "OK", "closeConnection should return OK."


def run_auto_create_csv_mode_smoke() -> None:
    tmp_dir = Path(PROJECT_ROOT) / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    qb_items_csv = tmp_dir / "smoke_qb_items_auto_create.csv"
    qb_items_csv.write_text(
        "Sku,Type\n"
        "LIVE-SKU-A,Inventory Part\n",
        encoding="utf-8",
    )

    missing_sku = "MISSING-SKU-TEST"
    fake_event = {
        "eventId": "jh_fake_transfer_auto_create_1",
        "eventType": "transfer",
        "status": "committed",
        "qbStatus": "pending",
        "qbTxnType": "TransferInventoryAdd",
        "effectiveDate": "2026-02-11",
        "createdBy": "smoke-test",
        "memo": "smoke transfer auto create",
        "idempotencyKey": "jh_fake_transfer_auto_create_1",
        "lines": [
            {
                "sku": missing_sku,
                "qty": 3,
                "qbItemFullName": missing_sku,
                "fromSiteFullName": "Smoke A",
                "toSiteFullName": "Smoke B",
                "itemIncomeAccountFullName": "Sales:Supplies",
                "itemCogsAccountFullName": "COGS:Supplies",
                "itemAssetAccountFullName": "12100 - Inventory Asset",
                "itemSalesDescription": "Auto-created test SKU",
                "itemPurchaseDescription": "Auto-created test SKU",
                "itemSalesPrice": 10,
                "itemPurchaseCost": 5,
                "itemIsActive": True,
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
        convex_run_prod=False,
        qb_items_csv=str(qb_items_csv),
        qb_items_source="csv",
        qb_items_refresh_minutes=60,
        qb_items_query_max_returned=1000,
        qb_items_query_mode="auto",
        qb_items_auto_create=True,
        qb_item_income_account_default="",
        qb_item_cogs_account_default="",
        qb_item_asset_account_default="",
    )
    service = QbwcService(config=config, convex_client=fake_convex)

    auth_result = service.authenticate("qbwc-user", "qbwc-pass")
    ticket = auth_result[0]
    assert ticket not in {"nvu", ""}, "authenticate did not return ticket for auto-create smoke."

    request_1 = service.send_request_xml(
        ticket=ticket,
        _hcp_response="",
        _company_file_name="",
        _qbxml_country="US",
        _qbxml_major="13",
        _qbxml_minor="0",
    )
    assert "ItemInventoryAddRq" in request_1, "Expected ItemInventoryAddRq for missing SKU."
    assert missing_sku in request_1, "Missing SKU not found in item-add request."

    item_add_response = (
        "<?xml version=\"1.0\"?>"
        "<QBXML><QBXMLMsgsRs>"
        "<ItemInventoryAddRs requestID=\"jh_fake_transfer_auto_create_1\" "
        "statusCode=\"0\" statusSeverity=\"Info\" statusMessage=\"Status OK\">"
        "<ItemInventoryRet><ListID>80000001-123456789</ListID></ItemInventoryRet>"
        "</ItemInventoryAddRs>"
        "</QBXMLMsgsRs></QBXML>"
    )
    completion_1 = service.receive_response_xml(
        ticket=ticket,
        response_xml=item_add_response,
        hresult="",
        message="",
    )
    assert completion_1 == 0, "After item create, QBWC should keep polling for event request."

    request_2 = service.send_request_xml(
        ticket=ticket,
        _hcp_response="",
        _company_file_name="",
        _qbxml_country="US",
        _qbxml_major="13",
        _qbxml_minor="0",
    )
    assert "TransferInventoryAddRq" in request_2, "Expected transfer qbXML after item auto-create."
    assert fake_convex.in_flight_calls, "mark_event_in_flight should run for auto-create flow."

    transfer_response = (
        "<?xml version=\"1.0\"?>"
        "<QBXML><QBXMLMsgsRs>"
        "<TransferInventoryAddRs requestID=\"jh_fake_transfer_auto_create_1\" "
        "statusCode=\"0\" statusSeverity=\"Info\" statusMessage=\"Status OK\">"
        "<TransferInventoryRet><TxnID>TXN-SMOKE-AUTO-CREATE-1</TxnID></TransferInventoryRet>"
        "</TransferInventoryAddRs>"
        "</QBXMLMsgsRs></QBXML>"
    )
    completion_2 = service.receive_response_xml(
        ticket=ticket,
        response_xml=transfer_response,
        hresult="",
        message="",
    )
    assert completion_2 == 0, "Fake pending events should keep progress at 0."
    assert fake_convex.apply_calls[-1]["success"] is True, "Auto-create flow should end in success."
    assert fake_convex.apply_calls[-1]["qbTxnId"] == "TXN-SMOKE-AUTO-CREATE-1", "Transfer TxnID missing."


def run_qbwc_mode_smoke() -> None:
    fake_event = {
        "eventId": "jh_fake_transfer_qbwc_1",
        "eventType": "transfer",
        "status": "committed",
        "qbStatus": "pending",
        "qbTxnType": "TransferInventoryAdd",
        "effectiveDate": "2026-02-11",
        "createdBy": "smoke-test",
        "memo": "smoke transfer qbwc mode",
        "idempotencyKey": "jh_fake_transfer_qbwc_1",
        "lines": [
            {
                "sku": "LIVE-SKU-B",
                "qty": 1,
                "qbItemFullName": "LIVE-SKU-B",
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
        convex_run_prod=False,
        qb_items_csv=".tmp/unused.csv",
        qb_items_source="qbwc",
        qb_items_refresh_minutes=60,
        qb_items_query_max_returned=1000,
        qb_items_query_mode="auto",
        qb_items_auto_create=True,
        qb_item_income_account_default="",
        qb_item_cogs_account_default="",
        qb_item_asset_account_default="",
    )
    service = QbwcService(config=config, convex_client=fake_convex)

    auth_result = service.authenticate("qbwc-user", "qbwc-pass")
    ticket = auth_result[0]
    assert ticket not in {"nvu", ""}, "authenticate did not return ticket in qbwc mode."

    request_1 = service.send_request_xml(
        ticket=ticket,
        _hcp_response="",
        _company_file_name="",
        _qbxml_country="US",
        _qbxml_major="13",
        _qbxml_minor="0",
    )
    assert "ItemInventoryQueryRq" in request_1, "qbwc mode should start by querying ItemInventory."
    assert 'iterator="Start"' in request_1, "First item query request must start iterator cycle."

    query_response_1 = (
        "<?xml version=\"1.0\"?>"
        "<QBXML><QBXMLMsgsRs>"
        "<ItemInventoryQueryRs statusCode=\"0\" statusSeverity=\"Info\" statusMessage=\"Status OK\" "
        "iteratorRemainingCount=\"1\" iteratorID=\"ITER-1\">"
        "<ItemInventoryRet><FullName>LIVE-SKU-A</FullName></ItemInventoryRet>"
        "</ItemInventoryQueryRs>"
        "</QBXMLMsgsRs></QBXML>"
    )
    completion_1 = service.receive_response_xml(
        ticket=ticket,
        response_xml=query_response_1,
        hresult="",
        message="",
    )
    assert completion_1 == 0, "Item query continuation should keep QBWC progress at 0."

    request_2 = service.send_request_xml(
        ticket=ticket,
        _hcp_response="",
        _company_file_name="",
        _qbxml_country="US",
        _qbxml_major="13",
        _qbxml_minor="0",
    )
    assert "ItemInventoryQueryRq" in request_2, "Second request should continue ItemInventory query."
    assert 'iterator="Continue"' in request_2, "Second item query request must continue iterator."
    assert "iteratorID=\"ITER-1\"" in request_2, "IteratorID was not propagated."

    query_response_2 = (
        "<?xml version=\"1.0\"?>"
        "<QBXML><QBXMLMsgsRs>"
        "<ItemInventoryQueryRs statusCode=\"0\" statusSeverity=\"Info\" statusMessage=\"Status OK\" "
        "iteratorRemainingCount=\"0\" iteratorID=\"ITER-1\">"
        "<ItemInventoryRet><FullName>LIVE-SKU-B</FullName></ItemInventoryRet>"
        "</ItemInventoryQueryRs>"
        "</QBXMLMsgsRs></QBXML>"
    )
    completion_2 = service.receive_response_xml(
        ticket=ticket,
        response_xml=query_response_2,
        hresult="",
        message="",
    )
    assert completion_2 == 0, "Completing item query with pending events should keep progress at 0."

    request_3 = service.send_request_xml(
        ticket=ticket,
        _hcp_response="",
        _company_file_name="",
        _qbxml_country="US",
        _qbxml_major="13",
        _qbxml_minor="0",
    )
    assert "TransferInventoryAddRq" in request_3, "Expected transfer qbXML after qbwc item query hydration."
    assert fake_convex.in_flight_calls, "mark_event_in_flight should run after item query completes."

    transfer_response = (
        "<?xml version=\"1.0\"?>"
        "<QBXML><QBXMLMsgsRs>"
        "<TransferInventoryAddRs requestID=\"jh_fake_transfer_qbwc_1\" "
        "statusCode=\"0\" statusSeverity=\"Info\" statusMessage=\"Status OK\">"
        "<TransferInventoryRet><TxnID>TXN-SMOKE-QBWC-1</TxnID></TransferInventoryRet>"
        "</TransferInventoryAddRs>"
        "</QBXMLMsgsRs></QBXML>"
    )
    completion_3 = service.receive_response_xml(
        ticket=ticket,
        response_xml=transfer_response,
        hresult="",
        message="",
    )
    assert completion_3 == 0, "Pending events should keep progress at 0 in fake client."
    assert fake_convex.apply_calls[-1]["success"] is True, "qbwc mode transfer should apply success."
    assert fake_convex.apply_calls[-1]["qbTxnId"] == "TXN-SMOKE-QBWC-1", "Transfer TxnID missing."


if __name__ == "__main__":
    main()
