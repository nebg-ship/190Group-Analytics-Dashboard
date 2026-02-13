from __future__ import annotations

import csv
import json
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

from qb_sync_service.config import QbSyncConfig
from qb_sync_service.convex_cli import ConvexCliClient
from qb_sync_service.qbxml import (
    build_item_inventory_add_qbxml,
    build_qbxml_for_event,
    parse_qbxml_response,
)


@dataclass
class SessionState:
    ticket: str
    last_error: str = ""
    in_flight_event_id: str | None = None
    in_flight_txn_type: str | None = None
    in_flight_request_kind: str = ""
    last_request_xml: str = ""
    pending_event: dict[str, Any] | None = None
    pending_event_original_line_count: int = 0
    pending_event_dropped_line_count: int = 0
    pending_item_create_queue: list[dict[str, Any]] = field(default_factory=list)
    in_flight_item_create: dict[str, Any] | None = None


def _parse_version(value: str) -> tuple[int, ...]:
    parts: list[int] = []
    for token in (value or "").split("."):
        token = token.strip()
        if token == "":
            continue
        if not token.isdigit():
            return tuple()
        parts.append(int(token))
    return tuple(parts)


def _parse_int(value: str) -> int | None:
    token = (value or "").strip()
    if token.isdigit():
        return int(token)
    return None


def _parse_qbxml_version(value: str) -> tuple[int, int]:
    parsed = _parse_version(value)
    if not parsed:
        return (13, 0)
    major = parsed[0]
    minor = parsed[1] if len(parsed) > 1 else 0
    return (major, minor)


def _resolve_qbxml_version(
    configured_version: str,
    requested_major: str,
    requested_minor: str,
) -> str:
    cfg_major, cfg_minor = _parse_qbxml_version(configured_version)
    req_major = _parse_int(requested_major)
    req_minor = _parse_int(requested_minor)

    if req_major is None:
        return f"{cfg_major}.{cfg_minor}"

    if req_minor is None:
        req_minor = 0

    if req_major < cfg_major:
        return f"{req_major}.{req_minor}"
    if req_major > cfg_major:
        return f"{cfg_major}.{cfg_minor}"
    return f"{cfg_major}.{min(cfg_minor, req_minor)}"


_TYPE_COLUMN_CANDIDATES = (
    "Type",
    "Item Type",
)

_SKU_COLUMN_CANDIDATES = (
    "Sku",
    "SKU",
    "Item",
    "Item Name/Number",
    "Item Name",
    "Full Name",
    "Name",
)


_QB_ITEMS_SOURCE_CSV = "csv"
_QB_ITEMS_SOURCE_QBWC = "qbwc"
_QB_ITEMS_QUERY_MODE_INVENTORY = "item_inventory_query"
_QB_ITEMS_QUERY_MODE_FALLBACK = "item_query_fallback"


def _normalize_header(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _normalize_item_key(value: str) -> str:
    return (value or "").strip().casefold()


def _add_item_key_variants(keys: set[str], raw_value: str) -> None:
    value = (raw_value or "").strip()
    if not value:
        return
    normalized = _normalize_item_key(value)
    if normalized:
        keys.add(normalized)
    if ":" in value:
        leaf = value.rsplit(":", 1)[1].strip()
        if leaf:
            keys.add(_normalize_item_key(leaf))


def _localname(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _is_inventory_part(type_value: str) -> bool:
    key = _normalize_header(type_value.strip())
    if "inventoryassembly" in key:
        return False
    return "inventorypart" in key


def _resolve_csv_column(
    headers: list[str],
    candidates: tuple[str, ...],
    label: str,
) -> str:
    header_map = {_normalize_header(header): header for header in headers}
    for candidate in candidates:
        resolved = header_map.get(_normalize_header(candidate))
        if resolved:
            return resolved
    raise ValueError(
        f"Unable to find {label} column in QB export CSV. "
        f"Headers: {headers}"
    )


def _line_item_candidates(line: dict[str, Any]) -> set[str]:
    raw_values = [
        str(line.get("qbItemFullName") or ""),
        str(line.get("sku") or ""),
    ]
    candidates: set[str] = set()
    for raw in raw_values:
        value = raw.strip()
        if not value:
            continue
        candidates.add(_normalize_item_key(value))
        if ":" in value:
            leaf = value.rsplit(":", 1)[1].strip()
            if leaf:
                candidates.add(_normalize_item_key(leaf))
    return {item for item in candidates if item}


def _line_item_full_name(line: dict[str, Any]) -> str:
    return str(line.get("qbItemFullName") or line.get("sku") or "").strip()


def _optional_text(value: Any) -> str:
    return str(value or "").strip()


def _is_duplicate_item_name_conflict(status_code: str) -> bool:
    return (status_code or "").strip() == "3100"


def _is_qb_success_response(status_code: str, status_severity: str) -> bool:
    return status_code in {"0", "1"} and status_severity.lower() != "error"


class QbwcService:
    def __init__(self, config: QbSyncConfig, convex_client: ConvexCliClient):
        self.config = config
        self.convex = convex_client
        self.sessions: dict[str, SessionState] = {}
        self._cached_qb_items_path: str = ""
        self._cached_qb_items_mtime_ns: int = -1
        self._cached_qb_inventory_part_keys: set[str] = set()
        self._cached_qb_inventory_part_names: set[str] = set()
        self._cached_qb_items_loaded_at_monotonic: float = 0.0
        self._cached_qb_items_loaded_at_epoch_ms: int = 0
        self._qb_items_query_in_progress: bool = False
        self._qb_items_query_iterator_id: str = ""
        self._qb_items_query_accumulator: set[str] = set()
        self._qb_items_query_name_accumulator: set[str] = set()
        configured_mode = (self.config.qb_items_query_mode or "").strip().casefold()
        if configured_mode in {
            "itemquery",
            "item_query",
            "itemqueryfallback",
            "item_query_fallback",
            "fallback",
            "compat",
            "compatibility",
        }:
            self._qb_items_query_request_mode = _QB_ITEMS_QUERY_MODE_FALLBACK
        else:
            self._qb_items_query_request_mode = _QB_ITEMS_QUERY_MODE_INVENTORY

    def _session(self, ticket: str) -> SessionState:
        clean_ticket = (ticket or "").strip()
        if not clean_ticket:
            clean_ticket = str(uuid.uuid4())
        existing = self.sessions.get(clean_ticket)
        if existing is not None:
            return existing
        created = SessionState(ticket=clean_ticket)
        self.sessions[clean_ticket] = created
        return created

    def _normalized_items_source(self) -> str:
        source = (self.config.qb_items_source or "").strip().casefold()
        if source in {"qb", "qbwc", "quickbooks", "live"}:
            return _QB_ITEMS_SOURCE_QBWC
        return _QB_ITEMS_SOURCE_CSV

    def _qbwc_items_mode_enabled(self) -> bool:
        return self._normalized_items_source() == _QB_ITEMS_SOURCE_QBWC

    def _qb_items_cache_is_fresh(self) -> bool:
        if not self._cached_qb_inventory_part_keys:
            return False
        refresh_minutes = self.config.qb_items_refresh_minutes
        if refresh_minutes <= 0:
            return True
        age_seconds = max(time.monotonic() - self._cached_qb_items_loaded_at_monotonic, 0.0)
        return age_seconds < refresh_minutes * 60

    def _reset_qb_items_query_state(self) -> None:
        self._qb_items_query_in_progress = False
        self._qb_items_query_iterator_id = ""
        self._qb_items_query_accumulator = set()
        self._qb_items_query_name_accumulator = set()

    def _begin_qb_items_query_cycle(self) -> None:
        self._qb_items_query_in_progress = True
        self._qb_items_query_iterator_id = ""
        self._qb_items_query_accumulator = set()
        self._qb_items_query_name_accumulator = set()

    def _build_qb_items_query_xml(
        self,
        *,
        qbxml_version: str,
        continue_iterator_id: str | None,
    ) -> str:
        max_returned = max(1, int(self.config.qb_items_query_max_returned))
        iterator_attr = ""
        if continue_iterator_id:
            iterator_attr = (
                ' iterator="Continue"'
                f' iteratorID="{escape(continue_iterator_id)}"'
            )
            if self._qb_items_query_request_mode == _QB_ITEMS_QUERY_MODE_FALLBACK:
                body = (
                    f"<ItemQueryRq{iterator_attr}>"
                    f"<MaxReturned>{max_returned}</MaxReturned>"
                    "</ItemQueryRq>"
                )
            else:
                body = (
                    f"<ItemInventoryQueryRq{iterator_attr}>"
                    f"<MaxReturned>{max_returned}</MaxReturned>"
                    "</ItemInventoryQueryRq>"
                )
        else:
            iterator_attr = ' iterator="Start"'
            if self._qb_items_query_request_mode == _QB_ITEMS_QUERY_MODE_FALLBACK:
                body = (
                    f"<ItemQueryRq{iterator_attr}>"
                    f"<MaxReturned>{max_returned}</MaxReturned>"
                    "</ItemQueryRq>"
                )
            else:
                body = (
                    f"<ItemInventoryQueryRq{iterator_attr}>"
                    "<ActiveStatus>All</ActiveStatus>"
                    f"<MaxReturned>{max_returned}</MaxReturned>"
                    "</ItemInventoryQueryRq>"
                )

        return (
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
            f"<?qbxml version=\"{escape(qbxml_version)}\"?>"
            "<QBXML>"
            "<QBXMLMsgsRq onError=\"stopOnError\">"
            f"{body}"
            "</QBXMLMsgsRq>"
            "</QBXML>"
        )

    def _next_qb_items_query_request(self, qbxml_version: str) -> str | None:
        if not self._qbwc_items_mode_enabled():
            return None
        if self._qb_items_cache_is_fresh() and not self._qb_items_query_in_progress:
            return None
        if not self._qb_items_query_in_progress:
            self._begin_qb_items_query_cycle()

        iterator_id = self._qb_items_query_iterator_id or None
        return self._build_qb_items_query_xml(
            qbxml_version=qbxml_version,
            continue_iterator_id=iterator_id,
        )

    def _parse_item_inventory_query_response(
        self,
        response_xml: str,
    ) -> tuple[set[str], set[str], str, int]:
        if not (response_xml or "").strip():
            raise ValueError("Empty ItemInventoryQueryRs response from QuickBooks.")

        try:
            root = ET.fromstring(response_xml)
        except ET.ParseError as exc:
            raise ValueError(f"Unable to parse ItemInventoryQueryRs XML: {exc}") from exc

        query_rs: ET.Element | None = None
        for element in root.iter():
            local_name = _localname(element.tag)
            if local_name in {"ItemInventoryQueryRs", "ItemQueryRs"}:
                query_rs = element
                break

        if query_rs is None:
            raise ValueError("No ItemInventoryQueryRs/ItemQueryRs node found in QuickBooks response.")

        status_code = query_rs.attrib.get("statusCode", "UNKNOWN")
        status_severity = query_rs.attrib.get("statusSeverity", "Error")
        status_message = query_rs.attrib.get("statusMessage", "").strip()
        if not _is_qb_success_response(status_code, status_severity):
            raise ValueError(
                "QuickBooks ItemInventoryQuery failed "
                f"(statusCode={status_code}, statusSeverity={status_severity}): "
                f"{status_message or 'Unknown status message.'}"
            )

        iterator_id = (query_rs.attrib.get("iteratorID") or "").strip()
        iterator_remaining = _parse_int(query_rs.attrib.get("iteratorRemainingCount", "") or "")
        remaining_count = iterator_remaining if iterator_remaining is not None else 0

        keys: set[str] = set()
        item_names: set[str] = set()
        for element in query_rs.iter():
            local_name = _localname(element.tag)
            # In fallback mode, ItemQueryRs may include many item types.
            if local_name != "ItemInventoryRet":
                continue
            full_name = ""
            name = ""
            for child in element:
                local = _localname(child.tag)
                text = (child.text or "").strip()
                if local == "FullName" and text:
                    full_name = text
                elif local == "Name" and text:
                    name = text

            if full_name:
                _add_item_key_variants(keys, full_name)
                item_names.add(full_name)
            elif name:
                _add_item_key_variants(keys, name)
                item_names.add(name)

        return keys, item_names, iterator_id, remaining_count

    def _persist_qb_items_cache_file(self) -> None:
        """
        Persist the latest QB item cache for downstream scripts.
        """
        if not self._cached_qb_inventory_part_names:
            return
        try:
            tmp_dir = Path(".tmp")
            tmp_dir.mkdir(parents=True, exist_ok=True)
            csv_path = tmp_dir / "qb_items_live_from_qbwc.csv"
            names = sorted(self._cached_qb_inventory_part_names)
            with csv_path.open("w", encoding="utf-8", newline="") as outfile:
                writer = csv.writer(outfile)
                writer.writerow(["Sku", "Type"])
                for name in names:
                    writer.writerow([name, "Inventory Part"])
        except Exception:
            # Never fail sync flow because cache debug files cannot be written.
            return

    def qb_items_snapshot(self) -> dict[str, Any]:
        names = sorted(self._cached_qb_inventory_part_names)
        return {
            "source": self._normalized_items_source(),
            "cacheReady": bool(self._cached_qb_inventory_part_keys),
            "cacheFresh": self._qb_items_cache_is_fresh(),
            "itemCount": len(names),
            "loadedAtEpochMs": self._cached_qb_items_loaded_at_epoch_ms or None,
            "queryInProgress": self._qb_items_query_in_progress,
            "queryRequestMode": self._qb_items_query_request_mode,
            "autoCreateMissingItems": self.config.qb_items_auto_create,
            "items": names,
        }

    def _reset_in_flight_request_state(self, session: SessionState) -> None:
        session.in_flight_event_id = None
        session.in_flight_txn_type = None
        session.in_flight_request_kind = ""
        session.last_request_xml = ""
        session.in_flight_item_create = None

    def _clear_pending_event_state(self, session: SessionState) -> None:
        session.pending_event = None
        session.pending_event_original_line_count = 0
        session.pending_event_dropped_line_count = 0
        session.pending_item_create_queue = []
        session.in_flight_item_create = None

    def _cache_created_item_name(self, item_full_name: str) -> None:
        clean_name = item_full_name.strip()
        if not clean_name:
            return
        _add_item_key_variants(self._cached_qb_inventory_part_keys, clean_name)
        self._cached_qb_inventory_part_names.add(clean_name)
        self._cached_qb_items_loaded_at_monotonic = time.monotonic()
        self._cached_qb_items_loaded_at_epoch_ms = int(time.time() * 1000)
        self._persist_qb_items_cache_file()

    def _build_missing_item_create_spec(
        self,
        *,
        event: dict[str, Any],
        line: dict[str, Any],
        ordinal: int,
    ) -> dict[str, Any]:
        item_full_name = _line_item_full_name(line)
        if not item_full_name:
            raise ValueError("Missing qbItemFullName/sku for auto-create candidate.")

        income_account = (
            _optional_text(line.get("itemIncomeAccountFullName"))
            or self.config.qb_item_income_account_default
        )
        cogs_account = (
            _optional_text(line.get("itemCogsAccountFullName"))
            or self.config.qb_item_cogs_account_default
        )
        asset_account = (
            _optional_text(line.get("itemAssetAccountFullName"))
            or self.config.qb_item_asset_account_default
        )

        missing_fields: list[str] = []
        if not income_account:
            missing_fields.append("income account")
        if not cogs_account:
            missing_fields.append("COGS account")
        if not asset_account:
            missing_fields.append("asset account")
        if missing_fields:
            sku_value = _optional_text(line.get("sku")) or item_full_name
            raise ValueError(
                f"Cannot auto-create item {sku_value}: missing {', '.join(missing_fields)} mapping."
            )

        event_id = _optional_text(event.get("eventId"))
        if not event_id:
            raise ValueError("Cannot auto-create missing item for an event without eventId.")
        request_seed = f"{event_id}|item_add|{item_full_name.casefold()}|{ordinal}"
        request_id = str(uuid.uuid5(uuid.NAMESPACE_URL, request_seed))
        sales_description = (
            _optional_text(line.get("itemSalesDescription"))
            or _optional_text(line.get("sku"))
            or item_full_name
        )
        purchase_description = (
            _optional_text(line.get("itemPurchaseDescription"))
            or sales_description
        )

        return {
            "eventId": event_id,
            "requestId": request_id,
            "itemFullName": item_full_name,
            "incomeAccountFullName": income_account,
            "cogsAccountFullName": cogs_account,
            "assetAccountFullName": asset_account,
            "salesDesc": sales_description,
            "purchaseDesc": purchase_description,
            "salesPrice": line.get("itemSalesPrice"),
            "purchaseCost": line.get("itemPurchaseCost"),
            "isActive": line.get("itemIsActive"),
        }

    def _send_next_item_create_request(
        self,
        session: SessionState,
        *,
        qbxml_version: str,
    ) -> str:
        if not session.pending_item_create_queue:
            raise ValueError("No pending item create requests for this session.")
        if not session.pending_event:
            raise ValueError("Pending event is required before creating missing items.")

        create_spec = session.pending_item_create_queue.pop(0)
        event_id = _optional_text(session.pending_event.get("eventId"))
        if not event_id:
            raise ValueError("Pending event is missing eventId.")
        qbxml_request = build_item_inventory_add_qbxml(
            item_full_name=_optional_text(create_spec.get("itemFullName")),
            request_id=_optional_text(create_spec.get("requestId")),
            qbxml_version=qbxml_version,
            income_account_full_name=_optional_text(create_spec.get("incomeAccountFullName")),
            cogs_account_full_name=_optional_text(create_spec.get("cogsAccountFullName")),
            asset_account_full_name=_optional_text(create_spec.get("assetAccountFullName")),
            sales_desc=_optional_text(create_spec.get("salesDesc")),
            purchase_desc=_optional_text(create_spec.get("purchaseDesc")),
            sales_price=create_spec.get("salesPrice"),
            purchase_cost=create_spec.get("purchaseCost"),
            is_active=create_spec.get("isActive"),
        )
        session.in_flight_event_id = event_id
        session.in_flight_txn_type = _optional_text(session.pending_event.get("qbTxnType")) or None
        session.in_flight_request_kind = "item_create"
        session.last_request_xml = qbxml_request
        session.in_flight_item_create = create_spec
        session.last_error = ""
        return qbxml_request

    def _send_event_request(
        self,
        session: SessionState,
        *,
        event: dict[str, Any],
        qbxml_version: str,
        requested_major: str,
        requested_minor: str,
        original_line_count: int,
        dropped_line_count: int,
    ) -> str:
        event_id = _optional_text(event.get("eventId"))
        if not event_id:
            raise ValueError("Cannot build event request without eventId.")
        lines = event.get("lines", [])
        if not isinstance(lines, list) or not lines:
            raise ValueError(f"Event {event_id} has no lines to send.")

        qbxml_request = build_qbxml_for_event(
            event=event,
            qbxml_version=qbxml_version,
            default_adjustment_account=self.config.default_adjustment_account,
        )
        session.in_flight_event_id = event_id
        session.in_flight_txn_type = event.get("qbTxnType")
        session.in_flight_request_kind = "event"
        session.last_request_xml = qbxml_request
        session.in_flight_item_create = None
        self._persist_last_request_debug(
            ticket=session.ticket,
            event_id=event_id,
            qbxml_version=qbxml_version,
            requested_major=requested_major,
            requested_minor=requested_minor,
            payload=qbxml_request,
            original_line_count=original_line_count,
            sent_line_count=len(lines),
            dropped_line_count=dropped_line_count,
        )
        session.last_error = ""
        return qbxml_request

    def _persist_last_request_debug(
        self,
        *,
        ticket: str,
        event_id: str,
        qbxml_version: str,
        requested_major: str,
        requested_minor: str,
        payload: str,
        original_line_count: int,
        sent_line_count: int,
        dropped_line_count: int,
    ) -> None:
        try:
            tmp_dir = Path(".tmp")
            tmp_dir.mkdir(parents=True, exist_ok=True)
            (tmp_dir / "last_send_request_payload.xml").write_text(payload, encoding="utf-8")
            (tmp_dir / "last_send_request_meta.json").write_text(
                json.dumps(
                    {
                        "ticket": ticket,
                        "eventId": event_id,
                        "resolvedQbxmlVersion": qbxml_version,
                        "requestedMajor": requested_major,
                        "requestedMinor": requested_minor,
                        "originalLineCount": original_line_count,
                        "sentLineCount": sent_line_count,
                        "droppedLineCount": dropped_line_count,
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
        except Exception:
            # Never fail sync flow because local debug files cannot be written.
            return

    def _load_qb_inventory_part_keys(self) -> set[str]:
        if self._qbwc_items_mode_enabled():
            if self._cached_qb_inventory_part_keys:
                return self._cached_qb_inventory_part_keys
            raise ValueError(
                "QB item cache is empty in qbwc mode. Wait for ItemInventoryQuery to complete."
            )

        csv_path = Path(self.config.qb_items_csv).expanduser()
        try:
            resolved_path = str(csv_path.resolve())
            stat = csv_path.stat()
        except OSError as exc:
            raise ValueError(
                f"QB items CSV is not readable: {csv_path}"
            ) from exc

        if (
            self._cached_qb_items_path == resolved_path
            and self._cached_qb_items_mtime_ns == stat.st_mtime_ns
            and self._cached_qb_inventory_part_keys
        ):
            return self._cached_qb_inventory_part_keys

        with csv_path.open("r", encoding="utf-8-sig", newline="") as infile:
            reader = csv.DictReader(infile)
            headers = list(reader.fieldnames or [])
            if not headers:
                raise ValueError(f"QB items CSV has no headers: {csv_path}")

            type_col = _resolve_csv_column(headers, _TYPE_COLUMN_CANDIDATES, "item type")
            sku_col = _resolve_csv_column(headers, _SKU_COLUMN_CANDIDATES, "sku")

            keys: set[str] = set()
            names: set[str] = set()
            for row in reader:
                item_type = str(row.get(type_col) or "").strip()
                if not _is_inventory_part(item_type):
                    continue

                sku = str(row.get(sku_col) or "").strip()
                if not sku:
                    continue

                _add_item_key_variants(keys, sku)
                names.add(sku)

        if not keys:
            raise ValueError(
                f"QB items CSV contains no Inventory Part SKUs: {csv_path}"
            )

        self._cached_qb_items_path = resolved_path
        self._cached_qb_items_mtime_ns = stat.st_mtime_ns
        self._cached_qb_inventory_part_keys = keys
        self._cached_qb_inventory_part_names = names
        self._cached_qb_items_loaded_at_monotonic = time.monotonic()
        self._cached_qb_items_loaded_at_epoch_ms = int(time.time() * 1000)
        return keys

    def _filter_event_lines_to_qb_items(
        self,
        event: dict[str, Any],
    ) -> tuple[dict[str, Any], int, int, list[dict[str, Any]]]:
        inventory_part_keys = self._load_qb_inventory_part_keys()
        lines = event.get("lines", [])
        if not isinstance(lines, list):
            return dict(event), 0, 0, []

        filtered_lines: list[dict[str, Any]] = []
        missing_lines_for_auto_create: list[dict[str, Any]] = []
        auto_create = self.config.qb_items_auto_create
        for line in lines:
            if not isinstance(line, dict):
                continue
            candidates = _line_item_candidates(line)
            if not candidates:
                continue
            if any(candidate in inventory_part_keys for candidate in candidates):
                filtered_lines.append(line)
                continue
            if auto_create:
                filtered_lines.append(line)
                missing_lines_for_auto_create.append(line)

        missing_item_creates: list[dict[str, Any]] = []
        if auto_create and missing_lines_for_auto_create:
            seen_item_keys: set[str] = set()
            ordinal = 0
            for line in missing_lines_for_auto_create:
                full_name = _line_item_full_name(line)
                normalized_key = _normalize_item_key(full_name)
                if not normalized_key or normalized_key in seen_item_keys:
                    continue
                seen_item_keys.add(normalized_key)
                missing_item_creates.append(
                    self._build_missing_item_create_spec(
                        event=event,
                        line=line,
                        ordinal=ordinal,
                    )
                )
                ordinal += 1

        filtered_event = dict(event)
        filtered_event["lines"] = filtered_lines
        original_count = len(lines)
        sent_count = len(filtered_lines)
        dropped_count = max(original_count - sent_count, 0)
        return filtered_event, original_count, dropped_count, missing_item_creates

    def _qbwc_progress_percent(self) -> int:
        """
        Return QBWC progress for receiveResponseXML.
        QBWC continues polling sendRequestXML while this is < 100.
        """
        try:
            payload = self.convex.get_next_pending_event(limit=1)
            events = payload.get("events", [])
            return 0 if events else 100
        except Exception:
            return 100

    def server_version(self) -> str:
        return self.config.server_version

    def client_version(self, version: str) -> str:
        min_version = self.config.min_client_version
        if not min_version:
            return ""
        current = _parse_version(version)
        minimum = _parse_version(min_version)
        if not current or not minimum:
            return ""
        if current < minimum:
            return f"W:Please upgrade QuickBooks Web Connector to at least {min_version}."
        return ""

    def authenticate(self, username: str, password: str) -> list[str]:
        expected_user = self.config.qbwc_username
        expected_pass = self.config.qbwc_password
        if not expected_user or not expected_pass:
            return ["nvu", ""]
        if username.strip() != expected_user or password.strip() != expected_pass:
            return ["nvu", ""]

        ticket = str(uuid.uuid4())
        self.sessions[ticket] = SessionState(ticket=ticket)
        return [ticket, self.config.qb_company_file or ""]

    def send_request_xml(
        self,
        ticket: str,
        _hcp_response: str,
        _company_file_name: str,
        _qbxml_country: str,
        _qbxml_major: str,
        _qbxml_minor: str,
    ) -> str:
        session = self._session(ticket)
        try:
            qbxml_version = _resolve_qbxml_version(
                configured_version=self.config.qbxml_version,
                requested_major=_qbxml_major,
                requested_minor=_qbxml_minor,
            )
            qb_items_query_request = self._next_qb_items_query_request(qbxml_version)
            if qb_items_query_request:
                session.in_flight_event_id = None
                session.in_flight_txn_type = None
                session.in_flight_request_kind = "item_query"
                session.last_request_xml = qb_items_query_request
                session.in_flight_item_create = None
                session.last_error = ""
                return qb_items_query_request

            if session.pending_item_create_queue or session.pending_event:
                pending_event_id = _optional_text(
                    (session.pending_event or {}).get("eventId")
                )
                try:
                    if session.pending_item_create_queue:
                        return self._send_next_item_create_request(
                            session,
                            qbxml_version=qbxml_version,
                        )
                    if session.pending_event:
                        return self._send_event_request(
                            session,
                            event=session.pending_event,
                            qbxml_version=qbxml_version,
                            requested_major=_qbxml_major,
                            requested_minor=_qbxml_minor,
                            original_line_count=session.pending_event_original_line_count,
                            dropped_line_count=session.pending_event_dropped_line_count,
                        )
                except Exception as exc:
                    if pending_event_id:
                        try:
                            self.convex.apply_qb_result(
                                event_id=pending_event_id,
                                ticket=session.ticket,
                                success=False,
                                qb_txn_type=session.in_flight_txn_type
                                or _optional_text(
                                    (session.pending_event or {}).get("qbTxnType")
                                )
                                or None,
                                qb_error_code="BUILD_ERROR",
                                qb_error_message=f"sendRequestXML build error: {exc}",
                                retryable=False,
                            )
                        except Exception:
                            pass
                    self._clear_pending_event_state(session)
                    self._reset_in_flight_request_state(session)
                    session.last_error = f"sendRequestXML build error for event {pending_event_id}: {exc}"
                    return ""

            payload = self.convex.get_next_pending_event(limit=10)
            events = payload.get("events", [])
            if not events:
                self._reset_in_flight_request_state(session)
                return ""

            for event in events:
                event_id = str(event.get("eventId") or "")
                if not event_id:
                    continue
                try:
                    self.convex.mark_event_in_flight(event_id, session.ticket)
                    (
                        filtered_event,
                        original_line_count,
                        dropped_line_count,
                        missing_item_creates,
                    ) = (
                        self._filter_event_lines_to_qb_items(event)
                    )
                    filtered_lines = filtered_event.get("lines", [])
                    if not filtered_lines:
                        self.convex.apply_qb_result(
                            event_id=event_id,
                            ticket=session.ticket,
                            success=True,
                            qb_txn_type=event.get("qbTxnType"),
                        )
                        session.last_error = ""
                        continue

                    if missing_item_creates:
                        session.pending_event = filtered_event
                        session.pending_event_original_line_count = original_line_count
                        session.pending_event_dropped_line_count = dropped_line_count
                        session.pending_item_create_queue = missing_item_creates
                        return self._send_next_item_create_request(
                            session,
                            qbxml_version=qbxml_version,
                        )

                    return self._send_event_request(
                        session,
                        event=filtered_event,
                        qbxml_version=qbxml_version,
                        requested_major=_qbxml_major,
                        requested_minor=_qbxml_minor,
                        original_line_count=original_line_count,
                        dropped_line_count=dropped_line_count,
                    )
                except Exception as exc:
                    try:
                        self.convex.apply_qb_result(
                            event_id=event_id,
                            ticket=session.ticket,
                            success=False,
                            qb_txn_type=event.get("qbTxnType"),
                            qb_error_code="BUILD_ERROR",
                            qb_error_message=f"sendRequestXML build error: {exc}",
                            retryable=False,
                        )
                    except Exception:
                        pass
                    self._clear_pending_event_state(session)
                    session.last_error = f"sendRequestXML build error for event {event_id}: {exc}"
                    continue

            self._reset_in_flight_request_state(session)
            return ""
        except Exception as exc:
            session.last_error = f"sendRequestXML error: {exc}"
            self._reset_in_flight_request_state(session)
            return ""

    def receive_response_xml(
        self,
        ticket: str,
        response_xml: str,
        hresult: str,
        message: str,
    ) -> int:
        session = self._session(ticket)
        if session.in_flight_request_kind == "item_query":
            try:
                clean_hresult = (hresult or "").strip()
                if clean_hresult:
                    error_message = (message or "").strip() or "QuickBooks returned HResult failure."
                    switched_to_fallback = False
                    if (
                        clean_hresult.casefold() == "0x80040400"
                        and self._qb_items_query_request_mode != _QB_ITEMS_QUERY_MODE_FALLBACK
                    ):
                        self._qb_items_query_request_mode = _QB_ITEMS_QUERY_MODE_FALLBACK
                        switched_to_fallback = True
                        error_message = (
                            f"{error_message} "
                            "[Auto-fallback enabled: switching QB item pull to ItemQueryRq compatibility mode.]"
                        )
                    self._reset_qb_items_query_state()
                    session.last_error = error_message
                    if switched_to_fallback:
                        # Keep QBWC polling in this cycle so we can immediately retry with fallback qbXML.
                        return 0
                    return self._qbwc_progress_percent()

                page_keys, page_names, iterator_id, remaining_count = self._parse_item_inventory_query_response(
                    response_xml or ""
                )
                self._qb_items_query_accumulator.update(page_keys)
                self._qb_items_query_name_accumulator.update(page_names)

                if remaining_count > 0:
                    if not iterator_id:
                        raise ValueError(
                            "QuickBooks ItemInventoryQueryRs missing iteratorID while iteratorRemainingCount > 0."
                        )
                    self._qb_items_query_in_progress = True
                    self._qb_items_query_iterator_id = iterator_id
                    session.last_error = ""
                    return 0

                if not self._qb_items_query_accumulator:
                    raise ValueError("QuickBooks ItemInventoryQuery returned zero inventory-part items.")

                self._cached_qb_inventory_part_keys = set(self._qb_items_query_accumulator)
                self._cached_qb_inventory_part_names = set(self._qb_items_query_name_accumulator)
                self._cached_qb_items_loaded_at_monotonic = time.monotonic()
                self._cached_qb_items_loaded_at_epoch_ms = int(time.time() * 1000)
                self._persist_qb_items_cache_file()
                self._reset_qb_items_query_state()
                session.last_error = ""
                return self._qbwc_progress_percent()
            except Exception as exc:
                self._reset_qb_items_query_state()
                session.last_error = f"receiveResponseXML item query error: {exc}"
                return self._qbwc_progress_percent()
            finally:
                self._reset_in_flight_request_state(session)

        if session.in_flight_request_kind == "item_create":
            event_id = (
                session.in_flight_event_id
                or _optional_text((session.pending_event or {}).get("eventId"))
            )
            item_full_name = _optional_text(
                (session.in_flight_item_create or {}).get("itemFullName")
            )
            try:
                if (hresult or "").strip():
                    error_message = (message or "").strip() or "QuickBooks returned HResult failure."
                    if event_id:
                        self.convex.apply_qb_result(
                            event_id=event_id,
                            ticket=session.ticket,
                            success=False,
                            qb_txn_type=session.in_flight_txn_type,
                            qb_error_code=(hresult or "HRESULT_ERROR").strip(),
                            qb_error_message=error_message,
                            retryable=True,
                        )
                    self._clear_pending_event_state(session)
                    session.last_error = error_message
                    return self._qbwc_progress_percent()

                parsed = parse_qbxml_response(response_xml or "")
                if parsed.success or _is_duplicate_item_name_conflict(parsed.status_code):
                    if item_full_name:
                        self._cache_created_item_name(item_full_name)
                    session.last_error = ""
                    if session.pending_item_create_queue or session.pending_event:
                        return 0
                    return self._qbwc_progress_percent()

                if event_id:
                    self.convex.apply_qb_result(
                        event_id=event_id,
                        ticket=session.ticket,
                        success=False,
                        qb_txn_type=session.in_flight_txn_type,
                        qb_error_code=parsed.status_code,
                        qb_error_message=parsed.status_message or "QuickBooks reported an error.",
                        retryable=True,
                    )
                self._clear_pending_event_state(session)
                session.last_error = parsed.status_message or "QuickBooks reported an error."
                return self._qbwc_progress_percent()
            except Exception as exc:
                if event_id:
                    try:
                        self.convex.apply_qb_result(
                            event_id=event_id,
                            ticket=session.ticket,
                            success=False,
                            qb_txn_type=session.in_flight_txn_type,
                            qb_error_code="ITEM_CREATE_ERROR",
                            qb_error_message=f"receiveResponseXML item create error: {exc}",
                            retryable=True,
                        )
                    except Exception:
                        pass
                self._clear_pending_event_state(session)
                session.last_error = f"receiveResponseXML item create error: {exc}"
                return self._qbwc_progress_percent()
            finally:
                self._reset_in_flight_request_state(session)

        event_id = session.in_flight_event_id
        if not event_id:
            return 100

        try:
            if (hresult or "").strip():
                error_message = (message or "").strip() or "QuickBooks returned HResult failure."
                self.convex.apply_qb_result(
                    event_id=event_id,
                    ticket=session.ticket,
                    success=False,
                    qb_txn_type=session.in_flight_txn_type,
                    qb_error_code=(hresult or "HRESULT_ERROR").strip(),
                    qb_error_message=error_message,
                    retryable=True,
                )
                session.last_error = error_message
                return self._qbwc_progress_percent()

            parsed = parse_qbxml_response(response_xml or "")
            if parsed.success:
                self.convex.apply_qb_result(
                    event_id=event_id,
                    ticket=session.ticket,
                    success=True,
                    qb_txn_id=parsed.txn_id,
                    qb_txn_type=parsed.txn_type or session.in_flight_txn_type,
                )
                session.last_error = ""
                return self._qbwc_progress_percent()

            self.convex.apply_qb_result(
                event_id=event_id,
                ticket=session.ticket,
                success=False,
                qb_txn_type=parsed.txn_type or session.in_flight_txn_type,
                qb_error_code=parsed.status_code,
                qb_error_message=parsed.status_message or "QuickBooks reported an error.",
                retryable=True,
            )
            session.last_error = parsed.status_message or "QuickBooks reported an error."
            return self._qbwc_progress_percent()
        except Exception as exc:
            session.last_error = f"receiveResponseXML error: {exc}"
            return self._qbwc_progress_percent()
        finally:
            self._reset_in_flight_request_state(session)
            pending_event_id = _optional_text((session.pending_event or {}).get("eventId"))
            if pending_event_id and pending_event_id == event_id:
                self._clear_pending_event_state(session)

    def get_last_error(self, ticket: str) -> str:
        session = self._session(ticket)
        return session.last_error or "No error recorded."

    def close_connection(self, ticket: str) -> str:
        clean_ticket = (ticket or "").strip()
        if clean_ticket:
            session = self.sessions.pop(clean_ticket, None)
            if session and session.in_flight_request_kind == "item_query":
                self._reset_qb_items_query_state()
            if session:
                self._clear_pending_event_state(session)
                self._reset_in_flight_request_state(session)
        return "OK"

    def connection_error(self, ticket: str, hresult: str, message: str) -> str:
        session = self._session(ticket)
        if session.in_flight_request_kind == "item_query":
            self._reset_qb_items_query_state()
            self._reset_in_flight_request_state(session)
            session.last_error = (message or "QuickBooks connection error.").strip()
            return "done"

        event_id = session.in_flight_event_id
        if event_id:
            try:
                self.convex.apply_qb_result(
                    event_id=event_id,
                    ticket=session.ticket,
                    success=False,
                    qb_txn_type=session.in_flight_txn_type,
                    qb_error_code=(hresult or "CONNECTION_ERROR").strip(),
                    qb_error_message=(message or "QuickBooks connection error.").strip(),
                    retryable=True,
                )
            finally:
                self._reset_in_flight_request_state(session)
                pending_event_id = _optional_text((session.pending_event or {}).get("eventId"))
                if pending_event_id and pending_event_id == event_id:
                    self._clear_pending_event_state(session)

        session.last_error = (message or "QuickBooks connection error.").strip()
        return "done"

    def get_interactive_url(self) -> str:
        return ""

    def interactive_rejected(self, _ticket: str) -> str:
        return "done"
