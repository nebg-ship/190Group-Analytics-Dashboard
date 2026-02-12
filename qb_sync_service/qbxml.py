from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any
import uuid
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


def _normalize_account_full_name(value: str) -> str:
    account = (value or "").strip()
    if not account:
        return account
    # Normalize common artifacts from CSV/imported account names.
    account = account.replace("Â·", "·")
    account = account.replace("’", "'")
    # QB inventory adjustments can reject deep COGS subaccount paths; prefer root account.
    if ":" in account:
        root = account.split(":", 1)[0].strip()
        if root.lower().startswith("cog"):
            account = root
    return account


def _line_item_full_name(line: dict[str, Any]) -> str:
    value = (line.get("qbItemFullName") or line.get("sku") or "").strip()
    if not value:
        raise ValueError("Event line is missing qbItemFullName/sku.")
    return value


def _qbxml_version_major(version: str) -> int:
    token = (version or "").strip().split(".", 1)[0]
    if token.isdigit():
        return int(token)
    return 13


def _external_guid_for_event(event: dict[str, Any]) -> str:
    source = str(event.get("idempotencyKey") or event.get("eventId") or "").strip()
    if not source:
        source = str(uuid.uuid4())
    # QuickBooks expects ExternalGUID to be a valid GUID string.
    deterministic = uuid.uuid5(uuid.NAMESPACE_URL, f"190group:{source}")
    return "{" + str(deterministic).upper() + "}"


def _single_site_name(
    lines: list[dict[str, Any]],
    field_name: str,
    description: str,
    *,
    required: bool,
) -> str:
    names: set[str] = set()
    for line in lines:
        value = (line.get(field_name) or "").strip()
        if value:
            names.add(value)
    if not names:
        if required:
            raise ValueError(f"{description} is missing from all lines.")
        return ""
    if len(names) > 1:
        raise ValueError(f"{description} must be the same for all lines in a single event.")
    return next(iter(names))


def _build_transfer_request(event: dict[str, Any], request_id: str) -> str:
    lines = event.get("lines", [])
    if not lines:
        raise ValueError("Transfer event has no lines.")

    memo = escape(_memo_for_event(event))
    txn_date = escape(event.get("effectiveDate", ""))
    if not txn_date:
        raise ValueError("Transfer event missing effectiveDate.")

    from_site = _single_site_name(
        lines,
        "fromSiteFullName",
        "Transfer from site mapping",
        required=True,
    )
    to_site = _single_site_name(
        lines,
        "toSiteFullName",
        "Transfer to site mapping",
        required=True,
    )

    line_xml: list[str] = []
    for line in lines:
        item_full_name = escape(_line_item_full_name(line))
        qty = _format_number(line.get("qty"))
        line_xml.append(
            (
                "<TransferInventoryLineAdd>"
                f"<ItemRef><FullName>{item_full_name}</FullName></ItemRef>"
                f"<QuantityToTransfer>{qty}</QuantityToTransfer>"
                "</TransferInventoryLineAdd>"
            )
        )

    return (
        f"<TransferInventoryAddRq requestID=\"{escape(request_id)}\">"
        "<TransferInventoryAdd>"
        f"<TxnDate>{txn_date}</TxnDate>"
        f"<FromInventorySiteRef><FullName>{escape(from_site)}</FullName></FromInventorySiteRef>"
        f"<ToInventorySiteRef><FullName>{escape(to_site)}</FullName></ToInventorySiteRef>"
        f"<Memo>{memo}</Memo>"
        f"{''.join(line_xml)}"
        "</TransferInventoryAdd>"
        "</TransferInventoryAddRq>"
    )


def _build_adjustment_request(
    event: dict[str, Any],
    request_id: str,
    default_adjustment_account: str,
    qbxml_version: str,
) -> str:
    lines = event.get("lines", [])
    if not lines:
        raise ValueError("Adjustment event has no lines.")

    qbxml_major = _qbxml_version_major(qbxml_version)
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
    account_name = _normalize_account_full_name(account_name)
    if not account_name:
        raise ValueError("Adjustment event has no account mapping and no default account configured.")

    site_name = _single_site_name(
        lines,
        "siteFullName",
        "Adjustment site mapping",
        required=qbxml_major >= 10,
    )
    site_xml = (
        f"<InventorySiteRef><FullName>{escape(site_name)}</FullName></InventorySiteRef>"
        if site_name and qbxml_major >= 10
        else ""
    )

    line_xml: list[str] = []
    for line in lines:
        item_full_name = escape(_line_item_full_name(line))

        quantity_xml = ""
        if line.get("newQty") is not None:
            quantity_xml = f"<NewQuantity>{_format_number(line.get('newQty'))}</NewQuantity>"
        else:
            quantity_xml = f"<QuantityDifference>{_format_number(line.get('qty'))}</QuantityDifference>"

        line_xml.append(
            (
                "<InventoryAdjustmentLineAdd>"
                f"<ItemRef><FullName>{item_full_name}</FullName></ItemRef>"
                f"<QuantityAdjustment>{quantity_xml}</QuantityAdjustment>"
                "</InventoryAdjustmentLineAdd>"
            )
        )

    external_guid_xml = ""
    if _qbxml_version_major(qbxml_version) >= 9:
        external_guid_xml = f"<ExternalGUID>{escape(_external_guid_for_event(event))}</ExternalGUID>"

    return (
        f"<InventoryAdjustmentAddRq requestID=\"{escape(request_id)}\">"
        "<InventoryAdjustmentAdd>"
        f"<AccountRef><FullName>{escape(account_name)}</FullName></AccountRef>"
        f"<TxnDate>{txn_date}</TxnDate>"
        f"{site_xml}"
        f"<Memo>{memo}</Memo>"
        f"{external_guid_xml}"
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
    qbxml_major = _qbxml_version_major(qbxml_version)

    if event_type == "transfer":
        if qbxml_major < 10:
            raise ValueError("Transfer events require qbXML version 10.0 or higher.")
        request_xml = _build_transfer_request(event, event_id)
    elif event_type == "adjustment":
        request_xml = _build_adjustment_request(
            event,
            event_id,
            default_adjustment_account,
            qbxml_version,
        )
    else:
        raise ValueError(f"Unsupported event type for qbXML: {event_type!r}")

    return (
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
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
