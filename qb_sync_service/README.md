# QB Sync Service

QuickBooks Web Connector middleware that:
- receives SOAP calls from QBWC
- fetches pending inventory events from Convex
- returns qbXML requests (`TransferInventoryAddRq`, `InventoryAdjustmentAddRq`)
- parses qbXML responses and applies success/failure back to Convex

## Run
```bash
python execution/start_qbwc_service.py
```

## No-admin autostart (Windows user logon)
Install per-user startup launcher:
```bash
python execution/install_no_admin_autostart.py
```

Manual run (same action as startup launcher):
```bash
python execution/start_qbwc_stack.py
```

Remove launcher:
```bash
python execution/install_no_admin_autostart.py --remove
```

## Validate live host
Run a full preflight (env, optional QWC generation, service health, QBWC SOAP calls):
```bash
python execution/validate_qbwc_live_host.py --start-service --generate-qwc
```

Outputs:
- markdown report: `.tmp/qbwc_live_validation_report.md`
- process exit code `0` only when all checks pass

## Endpoints
- `GET /` health JSON
- `POST /qbwc` QBWC SOAP endpoint
- `GET /qbwc/items-cache` live QB item-cache snapshot (add `?includeItems=0` to omit full item list)

## Required environment
- `QBWC_USERNAME`
- `QBWC_PASSWORD`
- `QBWC_APP_URL` (for `.qwc` generation)
- `QBWC_CERT_URL` (optional; defaults to scheme+host from `QBWC_APP_URL`)

Recommended:
- `CONVEX_ENV_FILE` only when you need explicit deployment selection
- `CONVEX_RUN_PROD=true` on production QB host
- `QBWC_BIND_PORT=8085`
- `QB_ADJUSTMENT_ACCOUNT_DEFAULT=Inventory Adjustments`

## QB item source mode
Default behavior uses a CSV export to decide which items are valid inventory parts:
- `QB_ITEMS_SOURCE=csv`
- `QB_ITEMS_CSV=.tmp/qb_items_export.csv`

Direct QuickBooks pull mode (no CSV export required):
- `QB_ITEMS_SOURCE=qbwc`
- Service sends `ItemInventoryQueryRq` via QBWC before processing events.
- Retrieved item keys are cached in-memory and refreshed by:
  - `QB_ITEMS_REFRESH_MINUTES` (default `60`; `0` means refresh only when cache is empty)
  - `QB_ITEMS_QUERY_MAX_RETURNED` (default `1000`, per iterator page)
  - `QB_ITEMS_QUERY_MODE`:
    - `auto` (default): start with `ItemInventoryQueryRq`, auto-fallback on `0x80040400`
    - `item_query_fallback`: force compatibility mode (`ItemQueryRq`) from the first request
- Latest pulled items are also written to `.tmp/qb_items_live_from_qbwc.csv` for downstream scripts.

Missing-item auto-create:
- `QB_ITEMS_AUTO_CREATE=true` (default) enables automatic `ItemInventoryAddRq` when an event SKU is missing in QB.
- Required mappings for auto-create can come from Convex `inventory_parts` fields, or fall back to:
  - `QB_ITEM_INCOME_ACCOUNT_DEFAULT`
  - `QB_ITEM_COGS_ACCOUNT_DEFAULT`
  - `QB_ITEM_ASSET_ACCOUNT_DEFAULT`
- If item creation succeeds (or QB returns duplicate-name `3100`), the service continues with the original event.

Queue QB-only zero-outs directly from live cache (no manual QB export):
```bash
python execution/queue_qb_only_zero_cleanup.py --qb-items-live-url http://127.0.0.1:8085/qbwc/items-cache --prod --effective-date 2026-01-01
```

## Production Cutover (Batch 4)
1. Generate inventory security tokens:
```bash
python execution/generate_inventory_security_tokens.py
```
2. Add generated values to `.env` on the QuickBooks host:
```env
INVENTORY_WRITE_TOKEN=<generated>
INVENTORY_ADMIN_TOKEN=<generated>
INVENTORY_REQUIRE_APPROVAL=true
INVENTORY_APPROVAL_QTY_THRESHOLD=25
```
3. Validate middleware host + QWC:
```bash
python execution/validate_qbwc_live_host.py --start-service --generate-qwc
```
4. Validate inventory API security/approval controls:
```bash
python execution/smoke_inventory_batch4.py
```
Or run full cutover readiness in one command:
```bash
python execution/validate_cutover_ready.py --generate-qwc
```
5. Start production services:
```bash
python execution/start_qbwc_service.py
python run_dashboard.py
```
