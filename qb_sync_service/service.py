from __future__ import annotations

import uuid
from dataclasses import dataclass
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


class QbwcService:
    def __init__(self, config: QbSyncConfig, convex_client: ConvexCliClient):
        self.config = config
        self.convex = convex_client
        self.sessions: dict[str, SessionState] = {}

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
                    qbxml_request = build_qbxml_for_event(
                        event=event,
                        qbxml_version=self.config.qbxml_version,
                        default_adjustment_account=self.config.default_adjustment_account,
                    )
                    session.in_flight_event_id = event_id
                    session.in_flight_txn_type = event.get("qbTxnType")
                    session.last_request_xml = qbxml_request
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
                return 100

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
                return 100

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
            return 100
        except Exception as exc:
            session.last_error = f"receiveResponseXML error: {exc}"
            return 100
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
