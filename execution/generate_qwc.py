"""
Generate a QuickBooks Web Connector (.qwc) file from environment variables.

Usage:
  python execution/generate_qwc.py
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from urllib.parse import urlsplit
from xml.sax.saxutils import escape

from dotenv import load_dotenv


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> None:
    load_dotenv()

    app_url = required("QBWC_APP_URL")
    parsed = urlsplit(app_url)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError("QBWC_APP_URL must be a valid absolute URL.")
    default_cert_url = f"{parsed.scheme}://{parsed.netloc}"
    cert_url = os.getenv("QBWC_CERT_URL", default_cert_url).strip()
    app_name = os.getenv("QBWC_APP_NAME", "190 Group QB Sync").strip()
    app_description = os.getenv(
        "QBWC_APP_DESCRIPTION",
        "Sync inventory events from Convex into QuickBooks Desktop Enterprise.",
    ).strip()
    username = required("QBWC_USERNAME")
    owner_id = os.getenv("QBWC_OWNER_ID", "{57F3B9B1-86F1-4fcc-B1EE-566DE1813D20}").strip()
    file_id = os.getenv("QBWC_FILE_ID", "").strip() or f"{{{uuid.uuid4()}}}"
    app_support = os.getenv("QBWC_APP_SUPPORT_URL", app_url).strip()
    run_every = int(os.getenv("QBWC_RUN_EVERY_N_MINUTES", "15").strip())
    read_only = os.getenv("QBWC_IS_READ_ONLY", "false").strip().lower() in {"true", "1", "yes"}
    output_path = Path(
        os.getenv("QBWC_QWC_OUTPUT", ".tmp/qb_inventory_sync.qwc").strip()
    )

    xml = (
        "<?xml version=\"1.0\"?>\n"
        "<QBWCXML>\n"
        f"  <AppName>{escape(app_name)}</AppName>\n"
        f"  <AppID></AppID>\n"
        f"  <AppURL>{escape(app_url)}</AppURL>\n"
        f"  <CertURL>{escape(cert_url)}</CertURL>\n"
        f"  <AppDescription>{escape(app_description)}</AppDescription>\n"
        f"  <AppSupport>{escape(app_support)}</AppSupport>\n"
        f"  <UserName>{escape(username)}</UserName>\n"
        f"  <OwnerID>{escape(owner_id)}</OwnerID>\n"
        f"  <FileID>{escape(file_id)}</FileID>\n"
        f"  <QBType>QBFS</QBType>\n"
        f"  <Scheduler>\n"
        f"    <RunEveryNMinutes>{run_every}</RunEveryNMinutes>\n"
        f"  </Scheduler>\n"
        f"  <IsReadOnly>{str(read_only).lower()}</IsReadOnly>\n"
        "</QBWCXML>\n"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(xml, encoding="utf-8")
    print(f"Wrote QWC file to {output_path}")
    print(f"Use password from QBWC_PASSWORD when importing into Web Connector.")


if __name__ == "__main__":
    main()
