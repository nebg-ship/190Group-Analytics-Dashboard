from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class QbSyncConfig:
    qbwc_username: str
    qbwc_password: str
    qb_company_file: str
    qbxml_version: str
    default_adjustment_account: str
    server_version: str
    min_client_version: str
    bind_host: str
    bind_port: int
    convex_env_file: str
    convex_run_prod: bool
    qb_items_csv: str

    @staticmethod
    def from_env() -> "QbSyncConfig":
        return QbSyncConfig(
            qbwc_username=os.getenv("QBWC_USERNAME", "").strip(),
            qbwc_password=os.getenv("QBWC_PASSWORD", "").strip(),
            qb_company_file=os.getenv("QB_COMPANY_FILE", "").strip(),
            qbxml_version=os.getenv("QBXML_VERSION", "13.0").strip(),
            default_adjustment_account=os.getenv(
                "QB_ADJUSTMENT_ACCOUNT_DEFAULT",
                "Inventory Adjustments",
            ).strip(),
            server_version=os.getenv("QBWC_SERVER_VERSION", "190Group-QBWC-0.1.0").strip(),
            min_client_version=os.getenv("QBWC_MIN_CLIENT_VERSION", "").strip(),
            bind_host=os.getenv("QBWC_BIND_HOST", "0.0.0.0").strip(),
            bind_port=int(os.getenv("QBWC_BIND_PORT", "8085").strip()),
            convex_env_file=os.getenv("CONVEX_ENV_FILE", "").strip(),
            convex_run_prod=os.getenv("CONVEX_RUN_PROD", "").strip().lower()
            in {"1", "true", "yes", "y", "on"},
            qb_items_csv=os.getenv("QB_ITEMS_CSV", ".tmp/qb_items_export.csv").strip(),
        )
