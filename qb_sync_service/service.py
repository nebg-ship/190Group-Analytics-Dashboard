from __future__ import annotations

import csv
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from qb_sync_service.config import QbSyncConfig
from qb_sync_service.convex_cli import ConvexCliClient
from qb_sync_service.qbxml import build_qbxml_for_event, parse_qbxml_response


@dataclass
class SessionState:
    ticket: str
    last_error: str = ""
    in_flight_event_id: str | None = None
    in_flight_txn_type: str | None = None
    last_request_xml: str = ""


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


def _normalize_header(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _normalize_item_key(value: str) -> str:
    return (value or "").strip().casefold()


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


class QbwcService:
    def __init__(self, config: QbSyncConfig, convex_client: ConvexCliClient):
        self.config = config
        self.convex = convex_client
        self.sessions: dict[str, SessionState] = {}
        self._cached_qb_items_path: str = ""
        self._cached_qb_items_mtime_ns: int = -1
        self._cached_qb_inventory_part_keys: set[str] = set()

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
            for row in reader:
                item_type = str(row.get(type_col) or "").strip()
                if not _is_inventory_part(item_type):
                    continue

                sku = str(row.get(sku_col) or "").strip()
                if not sku:
                    continue

                normalized = _normalize_item_key(sku)
                if normalized:
                    keys.add(normalized)

                if ":" in sku:
                    leaf = sku.rsplit(":", 1)[1].strip()
                    if leaf:
                        keys.add(_normalize_item_key(leaf))

        if not keys:
            raise ValueError(
                f"QB items CSV contains no Inventory Part SKUs: {csv_path}"
            )

        self._cached_qb_items_path = resolved_path
        self._cached_qb_items_mtime_ns = stat.st_mtime_ns
        self._cached_qb_inventory_part_keys = keys
        return keys

    def _filter_event_lines_to_qb_items(
        self,
        event: dict[str, Any],
    ) -> tuple[dict[str, Any], int, int]:
        inventory_part_keys = self._load_qb_inventory_part_keys()
        lines = event.get("lines", [])
        if not isinstance(lines, list):
            return dict(event), 0, 0

        filtered_lines: list[dict[str, Any]] = []
        for line in lines:
            if not isinstance(line, dict):
                continue
            candidates = _line_item_candidates(line)
            if not candidates:
                continue
            if any(candidate in inventory_part_keys for candidate in candidates):
                filtered_lines.append(line)

        filtered_event = dict(event)
        filtered_event["lines"] = filtered_lines
        original_count = len(lines)
        sent_count = len(filtered_lines)
        dropped_count = max(original_count - sent_count, 0)
        return filtered_event, original_count, dropped_count

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
            payload = self.convex.get_next_pending_event(limit=10)
            events = payload.get("events", [])
            if not events:
                session.in_flight_event_id = None
                session.in_flight_txn_type = None
                session.last_request_xml = ""
                return ""

            for event in events:
                event_id = str(event.get("eventId") or "")
                if not event_id:
                    continue
                try:
                    self.convex.mark_event_in_flight(event_id, session.ticket)
                    filtered_event, original_line_count, dropped_line_count = (
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

                    qbxml_version = _resolve_qbxml_version(
                        configured_version=self.config.qbxml_version,
                        requested_major=_qbxml_major,
                        requested_minor=_qbxml_minor,
                    )
                    qbxml_request = build_qbxml_for_event(
                        event=filtered_event,
                        qbxml_version=qbxml_version,
                        default_adjustment_account=self.config.default_adjustment_account,
                    )
                    session.in_flight_event_id = event_id
                    session.in_flight_txn_type = filtered_event.get("qbTxnType")
                    session.last_request_xml = qbxml_request
                    self._persist_last_request_debug(
                        ticket=session.ticket,
                        event_id=event_id,
                        qbxml_version=qbxml_version,
                        requested_major=_qbxml_major,
                        requested_minor=_qbxml_minor,
                        payload=qbxml_request,
                        original_line_count=original_line_count,
                        sent_line_count=len(filtered_lines),
                        dropped_line_count=dropped_line_count,
                    )
                    session.last_error = ""
                    return qbxml_request
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
                    session.last_error = f"sendRequestXML build error for event {event_id}: {exc}"
                    continue

            session.in_flight_event_id = None
            session.in_flight_txn_type = None
            session.last_request_xml = ""
            return ""
        except Exception as exc:
            session.last_error = f"sendRequestXML error: {exc}"
            session.in_flight_event_id = None
            session.in_flight_txn_type = None
            session.last_request_xml = ""
            return ""

    def receive_response_xml(
        self,
        ticket: str,
        response_xml: str,
        hresult: str,
        message: str,
    ) -> int:
        session = self._session(ticket)
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
            session.in_flight_event_id = None
            session.in_flight_txn_type = None
            session.last_request_xml = ""

    def get_last_error(self, ticket: str) -> str:
        session = self._session(ticket)
        return session.last_error or "No error recorded."

    def close_connection(self, ticket: str) -> str:
        clean_ticket = (ticket or "").strip()
        if clean_ticket:
            self.sessions.pop(clean_ticket, None)
        return "OK"

    def connection_error(self, ticket: str, hresult: str, message: str) -> str:
        session = self._session(ticket)
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
                session.in_flight_event_id = None
                session.in_flight_txn_type = None
                session.last_request_xml = ""

        session.last_error = (message or "QuickBooks connection error.").strip()
        return "done"

    def get_interactive_url(self) -> str:
        return ""

    def interactive_rejected(self, _ticket: str) -> str:
        return "done"
