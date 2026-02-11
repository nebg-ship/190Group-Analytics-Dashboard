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
