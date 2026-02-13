from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip()
    if raw == "":
        return default
    return raw.lower() in {"1", "true", "yes", "y", "on"}


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
    qb_items_source: str
    qb_items_refresh_minutes: int
    qb_items_query_max_returned: int
    qb_items_query_mode: str
    qb_items_auto_create: bool
    qb_item_income_account_default: str
    qb_item_cogs_account_default: str
    qb_item_asset_account_default: str

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
            qb_items_source=os.getenv("QB_ITEMS_SOURCE", "csv").strip(),
            qb_items_refresh_minutes=_env_int("QB_ITEMS_REFRESH_MINUTES", 60),
            qb_items_query_max_returned=max(
                1,
                _env_int("QB_ITEMS_QUERY_MAX_RETURNED", 1000),
            ),
            qb_items_query_mode=os.getenv("QB_ITEMS_QUERY_MODE", "auto").strip(),
            qb_items_auto_create=_env_bool("QB_ITEMS_AUTO_CREATE", True),
            qb_item_income_account_default=os.getenv(
                "QB_ITEM_INCOME_ACCOUNT_DEFAULT",
                "",
            ).strip(),
            qb_item_cogs_account_default=os.getenv(
                "QB_ITEM_COGS_ACCOUNT_DEFAULT",
                "",
            ).strip(),
            qb_item_asset_account_default=os.getenv(
                "QB_ITEM_ASSET_ACCOUNT_DEFAULT",
                "",
            ).strip(),
        )
