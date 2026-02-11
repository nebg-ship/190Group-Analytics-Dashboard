from __future__ import annotations

from typing import Any
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, request

from qb_sync_service.config import QbSyncConfig
from qb_sync_service.convex_cli import ConvexCliClient
from qb_sync_service.service import QbwcService


SOAP_ENV_NS = "http://schemas.xmlsoap.org/soap/envelope/"
QBWC_NS = "http://developer.intuit.com/"


def _localname(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _xml_text(element: ET.Element | None) -> str:
    if element is None or element.text is None:
        return ""
    return element.text


def _soap_envelope(method_name: str, payload_xml: str) -> str:
    return (
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        "<soap:Envelope "
        f"xmlns:soap=\"{SOAP_ENV_NS}\" "
        "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
        "xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">"
        "<soap:Body>"
        f"<{method_name}Response xmlns=\"{QBWC_NS}\">"
        f"{payload_xml}"
        f"</{method_name}Response>"
        "</soap:Body>"
        "</soap:Envelope>"
    )


def _soap_fault(message: str) -> str:
    escaped = escape(message)
    return (
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        f"<soap:Envelope xmlns:soap=\"{SOAP_ENV_NS}\">"
        "<soap:Body>"
        "<soap:Fault>"
        "<faultcode>soap:Client</faultcode>"
        f"<faultstring>{escaped}</faultstring>"
        "</soap:Fault>"
        "</soap:Body>"
        "</soap:Envelope>"
    )


def _parse_soap_call(raw_xml: str) -> tuple[str, dict[str, str]]:
    root = ET.fromstring(raw_xml)
    body = None
    for child in root:
        if _localname(child.tag) == "Body":
            body = child
            break
    if body is None:
        raise ValueError("SOAP envelope missing Body.")
    if len(body) == 0:
        raise ValueError("SOAP body is empty.")
    method_element = body[0]
    method_name = _localname(method_element.tag)
    params: dict[str, str] = {}
    for child in method_element:
        params[_localname(child.tag)] = _xml_text(child)
    return method_name, params


def create_app(service: QbwcService) -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def health() -> Any:
        return jsonify(
            {
                "ok": True,
                "service": "qb_sync_service",
                "endpoint": "/qbwc",
                "sessions": len(service.sessions),
            }
        )

    @app.post("/qbwc")
    def qbwc_endpoint() -> Response:
        try:
            method_name, params = _parse_soap_call(request.data.decode("utf-8"))

            if method_name == "serverVersion":
                result = service.server_version()
                body_xml = f"<serverVersionResult>{escape(result)}</serverVersionResult>"
            elif method_name == "clientVersion":
                result = service.client_version(params.get("strVersion", ""))
                body_xml = f"<clientVersionResult>{escape(result)}</clientVersionResult>"
            elif method_name == "authenticate":
                values = service.authenticate(
                    username=params.get("strUserName", ""),
                    password=params.get("strPassword", ""),
                )
                body_xml = (
                    "<authenticateResult>"
                    f"<string>{escape(values[0])}</string>"
                    f"<string>{escape(values[1])}</string>"
                    "</authenticateResult>"
                )
            elif method_name == "sendRequestXML":
                result = service.send_request_xml(
                    ticket=params.get("ticket", ""),
                    _hcp_response=params.get("strHCPResponse", ""),
                    _company_file_name=params.get("strCompanyFileName", ""),
                    _qbxml_country=params.get("qbXMLCountry", ""),
                    _qbxml_major=params.get("qbXMLMajorVers", ""),
                    _qbxml_minor=params.get("qbXMLMinorVers", ""),
                )
                body_xml = f"<sendRequestXMLResult>{escape(result)}</sendRequestXMLResult>"
            elif method_name == "receiveResponseXML":
                result = service.receive_response_xml(
                    ticket=params.get("ticket", ""),
                    response_xml=params.get("response", ""),
                    hresult=params.get("hresult", ""),
                    message=params.get("message", ""),
                )
                body_xml = f"<receiveResponseXMLResult>{result}</receiveResponseXMLResult>"
            elif method_name == "getLastError":
                result = service.get_last_error(params.get("ticket", ""))
                body_xml = f"<getLastErrorResult>{escape(result)}</getLastErrorResult>"
            elif method_name == "closeConnection":
                result = service.close_connection(params.get("ticket", ""))
                body_xml = f"<closeConnectionResult>{escape(result)}</closeConnectionResult>"
            elif method_name == "connectionError":
                result = service.connection_error(
                    params.get("ticket", ""),
                    params.get("hresult", ""),
                    params.get("message", ""),
                )
                body_xml = f"<connectionErrorResult>{escape(result)}</connectionErrorResult>"
            elif method_name == "getInteractiveURL":
                result = service.get_interactive_url()
                body_xml = f"<getInteractiveURLResult>{escape(result)}</getInteractiveURLResult>"
            elif method_name == "interactiveRejected":
                result = service.interactive_rejected(params.get("ticket", ""))
                body_xml = f"<interactiveRejectedResult>{escape(result)}</interactiveRejectedResult>"
            else:
                raise ValueError(f"Unsupported SOAP method: {method_name}")

            xml_response = _soap_envelope(method_name, body_xml)
            return Response(xml_response, mimetype="text/xml")
        except Exception as exc:
            fault = _soap_fault(str(exc))
            return Response(fault, status=500, mimetype="text/xml")

    return app


def run() -> None:
    load_dotenv()
    config = QbSyncConfig.from_env()
    convex = ConvexCliClient(env_file=config.convex_env_file)
    service = QbwcService(config=config, convex_client=convex)
    app = create_app(service)

    print("=" * 60)
    print("QBWC middleware is running")
    print(f"SOAP endpoint: http://{config.bind_host}:{config.bind_port}/qbwc")
    print("=" * 60)
    app.run(host=config.bind_host, port=config.bind_port, debug=False)


if __name__ == "__main__":
    run()

