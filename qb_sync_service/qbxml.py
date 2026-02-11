from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape


def _format_number(value: Any) -> str:
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"Invalid numeric value for qbXML quantity: {value!r}") from None
    normalized = dec.normalize()
    as_text = format(normalized, "f")
    if "." in as_text:
        as_text = as_text.rstrip("0").rstrip(".")
    return as_text or "0"


def _memo_for_event(event: dict[str, Any]) -> str:
    pieces = [
        event.get("eventType", ""),
        str(event.get("eventId", "")),
        event.get("createdBy") or "",
        event.get("memo") or "",
    ]
    joined = " ".join(piece for piece in pieces if piece).strip()
    return joined[:4095]


def _line_item_full_name(line: dict[str, Any]) -> str:
    value = (line.get("qbItemFullName") or line.get("sku") or "").strip()
    if not value:
        raise ValueError("Event line is missing qbItemFullName/sku.")
    return value


def _build_transfer_request(event: dict[str, Any], request_id: str) -> str:
    lines = event.get("lines", [])
    if not lines:
        raise ValueError("Transfer event has no lines.")

    memo = escape(_memo_for_event(event))
    txn_date = escape(event.get("effectiveDate", ""))
    if not txn_date:
        raise ValueError("Transfer event missing effectiveDate.")

    line_xml: list[str] = []
    for line in lines:
        item_full_name = escape(_line_item_full_name(line))
        from_site = (line.get("fromSiteFullName") or "").strip()
        to_site = (line.get("toSiteFullName") or "").strip()
        if not from_site or not to_site:
            raise ValueError(
                f"Transfer line for SKU {line.get('sku')} is missing from/to site mapping.",
            )
        qty = _format_number(line.get("qty"))
        line_xml.append(
            (
                "<TransferInventoryLineAdd>"
                f"<ItemRef><FullName>{item_full_name}</FullName></ItemRef>"
                f"<FromInventorySiteRef><FullName>{escape(from_site)}</FullName></FromInventorySiteRef>"
                f"<ToInventorySiteRef><FullName>{escape(to_site)}</FullName></ToInventorySiteRef>"
                f"<QuantityTransferred>{qty}</QuantityTransferred>"
                "</TransferInventoryLineAdd>"
            )
        )

    return (
        f"<TransferInventoryAddRq requestID=\"{escape(request_id)}\">"
        "<TransferInventoryAdd>"
        f"<TxnDate>{txn_date}</TxnDate>"
        f"<Memo>{memo}</Memo>"
        f"{''.join(line_xml)}"
        "</TransferInventoryAdd>"
        "</TransferInventoryAddRq>"
    )


def _build_adjustment_request(
    event: dict[str, Any],
    request_id: str,
    default_adjustment_account: str,
) -> str:
    lines = event.get("lines", [])
    if not lines:
        raise ValueError("Adjustment event has no lines.")

    memo = escape(_memo_for_event(event))
    txn_date = escape(event.get("effectiveDate", ""))
    if not txn_date:
        raise ValueError("Adjustment event missing effectiveDate.")

    account_name = next(
        (
            (line.get("qbAccountFullName") or "").strip()
            for line in lines
            if (line.get("qbAccountFullName") or "").strip()
        ),
        default_adjustment_account.strip(),
    )
    if not account_name:
        raise ValueError("Adjustment event has no account mapping and no default account configured.")

    line_xml: list[str] = []
    for line in lines:
        item_full_name = escape(_line_item_full_name(line))
        site_name = (line.get("siteFullName") or "").strip()
        if not site_name:
            raise ValueError(
                f"Adjustment line for SKU {line.get('sku')} is missing site mapping.",
            )

        quantity_xml = ""
        if line.get("newQty") is not None:
            quantity_xml = f"<NewQuantity>{_format_number(line.get('newQty'))}</NewQuantity>"
        else:
            quantity_xml = f"<QuantityDifference>{_format_number(line.get('qty'))}</QuantityDifference>"

        line_xml.append(
            (
                "<InventoryAdjustmentLineAdd>"
                f"<ItemRef><FullName>{item_full_name}</FullName></ItemRef>"
                f"<InventorySiteRef><FullName>{escape(site_name)}</FullName></InventorySiteRef>"
                f"<QuantityAdjustment>{quantity_xml}</QuantityAdjustment>"
                "</InventoryAdjustmentLineAdd>"
            )
        )

    return (
        f"<InventoryAdjustmentAddRq requestID=\"{escape(request_id)}\">"
        "<InventoryAdjustmentAdd>"
        f"<AccountRef><FullName>{escape(account_name)}</FullName></AccountRef>"
        f"<TxnDate>{txn_date}</TxnDate>"
        f"<Memo>{memo}</Memo>"
        f"<ExternalGUID>{escape(str(event.get('idempotencyKey') or event.get('eventId')))}</ExternalGUID>"
        f"{''.join(line_xml)}"
        "</InventoryAdjustmentAdd>"
        "</InventoryAdjustmentAddRq>"
    )


def build_qbxml_for_event(
    event: dict[str, Any],
    qbxml_version: str,
    default_adjustment_account: str,
) -> str:
    event_id = str(event.get("eventId") or "")
    if not event_id:
        raise ValueError("Event is missing eventId.")
    event_type = event.get("eventType")

    if event_type == "transfer":
        request_xml = _build_transfer_request(event, event_id)
    elif event_type == "adjustment":
        request_xml = _build_adjustment_request(event, event_id, default_adjustment_account)
    else:
        raise ValueError(f"Unsupported event type for qbXML: {event_type!r}")

    return (
        f"<?qbxml version=\"{escape(qbxml_version)}\"?>"
        "<QBXML>"
        "<QBXMLMsgsRq onError=\"stopOnError\">"
        f"{request_xml}"
        "</QBXMLMsgsRq>"
        "</QBXML>"
    )


def _localname(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


@dataclass
class QbxmlResponseResult:
    success: bool
    status_code: str
    status_severity: str
    status_message: str
    txn_id: str | None
    txn_type: str | None


def parse_qbxml_response(response_xml: str) -> QbxmlResponseResult:
    if not response_xml or not response_xml.strip():
        return QbxmlResponseResult(
            success=False,
            status_code="EMPTY_RESPONSE",
            status_severity="Error",
            status_message="Empty qbXML response.",
            txn_id=None,
            txn_type=None,
        )

    try:
        root = ET.fromstring(response_xml)
    except ET.ParseError as exc:
        return QbxmlResponseResult(
            success=False,
            status_code="PARSE_ERROR",
            status_severity="Error",
            status_message=f"Unable to parse qbXML response: {exc}",
            txn_id=None,
            txn_type=None,
        )

    rs_element: ET.Element | None = None
    for element in root.iter():
        name = _localname(element.tag)
        if name.endswith("Rs") and "statusCode" in element.attrib:
            rs_element = element
            break

    if rs_element is None:
        return QbxmlResponseResult(
            success=False,
            status_code="NO_RS_NODE",
            status_severity="Error",
            status_message="No *Rs node found in qbXML response.",
            txn_id=None,
            txn_type=None,
        )

    status_code = rs_element.attrib.get("statusCode", "UNKNOWN")
    status_severity = rs_element.attrib.get("statusSeverity", "Error")
    status_message = rs_element.attrib.get("statusMessage", "")

    txn_id = None
    for element in rs_element.iter():
        if _localname(element.tag) == "TxnID" and (element.text or "").strip():
            txn_id = element.text.strip()
            break

    node_name = _localname(rs_element.tag)
    txn_type = None
    if node_name.endswith("Rs"):
        txn_type = node_name[:-2]

    success = status_code in {"0", "1"} and status_severity.lower() != "error"
    return QbxmlResponseResult(
        success=success,
        status_code=status_code,
        status_severity=status_severity,
        status_message=status_message,
        txn_id=txn_id,
        txn_type=txn_type,
    )
