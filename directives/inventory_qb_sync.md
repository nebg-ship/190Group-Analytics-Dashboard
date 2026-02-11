# Inventory + QuickBooks Sync SOP

## Goal
Operate inventory in Convex while keeping QuickBooks Desktop Enterprise inventory transactions synchronized and auditable.

## Scope (Batch 1)
- Convex operational inventory:
  - Transfer events between locations
  - Adjustment events (delta and set modes)
  - Immutable event lines + materialized balances
- QuickBooks sync queue states:
  - `pending`, `in_flight`, `applied`, `error`
  - retry metadata and idempotency keys

## Scope (Batch 2)
- QBWC SOAP middleware endpoint at `/qbwc`
- qbXML generation for:
  - `TransferInventoryAddRq`
  - `InventoryAdjustmentAddRq`
- qbXML response parsing and Convex sync result application

## Scope (Batch 3)
- Employee-facing inventory operations dashboard at `/inventory`
- Flask inventory API endpoints for:
  - location lookup + upsert
  - inventory overview and item detail
  - transfer + adjustment event creation
  - event void + manual retry
  - queue summary monitoring

## Scope (Batch 4)
- Token-gated inventory write/admin actions
- Approval queue for high-risk inventory changes
- Inventory API audit log for operational traceability
- Dashboard controls for auth headers + approval review

## Source of Truth
- Convex owns operational QOH and employee workflows.
- QuickBooks is updated from Convex events for accounting parity.

## Prerequisites
1. Convex deployment configured (`.env.local` with `CONVEX_DEPLOYMENT` and `CONVEX_URL`).
2. Latest Convex schema/functions pushed (`npx convex dev` or `npx convex run --push ...`).
3. Inventory CSV available at:
   - `amazon_economics/Master_Updated_web_accounts_v14_1 (1).csv`

## Data Model Overview
- `inventory_parts`: SKU master + account fields + optional QuickBooks mapping fields.
- `inventory_locations`: canonical location/site mapping.
- `inventory_balances`: current on-hand/reserved/available by SKU/location.
- `inventory_events`: event header and QuickBooks sync state.
- `inventory_event_lines`: immutable lines for transfer/adjustment.
- `inventory_reason_accounts`: reason code to QuickBooks account mapping.
- `qb_sync_sessions`: Web Connector ticket/session tracking.

## Standard Operations

### 1) Load or refresh SKU master
Run:
```bash
python execution/inventory_import_items.py --push-first
```

Notes:
- Upsert is keyed by `Sku`.
- Re-running the script is safe and idempotent.
- Use `--dry-run` to validate CSV parsing before writes.

### 2) Upsert locations
Use `inventory:upsertLocation` to create or update each location code and QuickBooks site mapping.

Required fields:
- `code`
- `displayName`

Optional but recommended:
- `qbSiteFullName`
- `qbSiteListId`
- `isVirtual`

### 3) Create inventory movements
- Transfers: `inventory:createTransferEvent`
- Adjustments: `inventory:createAdjustmentEvent`

Behavior:
- Event + lines are immutable after creation.
- Balances are updated atomically in the same mutation.
- New events are queued for QuickBooks with `qbStatus = pending`.

### 4) Read inventory and audit trail
- Overview: `inventory:getInventoryOverview`
- Item drill-down: `inventory:getItemDetail`
- Recent events: `inventory:listRecentEvents`

### 5) Sync queue lifecycle
- Poll next work: `qbQueue:getNextPendingQbEvent`
- Mark work in progress: `qbQueue:markEventInFlight`
- Apply QuickBooks result: `qbQueue:applyQbResult`
- Manual retry after hard failure: `qbQueue:retryFailedEvent`

### 5.1) Run QBWC middleware (Batch 2)
Run:
```bash
python execution/start_qbwc_service.py
```

Expected endpoint:
- `http://<host>:<port>/qbwc`

Note:
- Set `CONVEX_ENV_FILE` only when you need explicit deployment selection.

### 5.2) Generate and import QWC file
Run:
```bash
python execution/generate_qwc.py
```

Then:
1. Open QuickBooks Web Connector.
2. Add application and select the generated `.qwc` file.
3. Enter the `QBWC_PASSWORD` value when prompted.
4. Trigger update and verify events move from `pending` to `applied`.

### 5.3) Run live-host preflight (recommended before first QBWC connect)
Run:
```bash
python execution/validate_qbwc_live_host.py --start-service --generate-qwc
```

Expected behavior:
- Writes report to `.tmp/qbwc_live_validation_report.md`
- Exits `0` only when all checks pass
- Exits non-zero when required environment or SOAP checks fail

Required environment on the QuickBooks host:
- `QBWC_USERNAME`
- `QBWC_PASSWORD`
- `QBWC_APP_URL` (public URL QBWC can reach, used in QWC generation)

Optional environment:
- `QBWC_BIND_HOST` (default `127.0.0.1`)
- `QBWC_BIND_PORT` (default `8085`)
- `QBWC_QWC_OUTPUT` (default `.tmp/qb_inventory_sync.qwc`)

### 6) Clean up smoke-test records
Run:
```bash
python execution/cleanup_smoke_inventory.py --dry-run
python execution/cleanup_smoke_inventory.py
```

Defaults target:
- SKU prefix `SMOKE-SKU-`
- location code prefix `SMOKE_`
- event `createdBy` value `smoke-test`

### 7) Run employee inventory dashboard (Batch 3)
Run:
```bash
python run_dashboard.py
```

Open:
- Executive dashboard: `http://localhost:5000/`
- Inventory dashboard: `http://localhost:5000/inventory`

Inventory API routes (served by Flask):
- `GET /api/inventory/security-config`
- `GET /api/inventory/health`
- `GET /api/inventory/locations`
- `GET /api/inventory/overview`
- `GET /api/inventory/events`
- `GET /api/inventory/queue-summary`
- `GET /api/inventory/item/<sku>`
- `GET /api/inventory/approvals`
- `GET /api/inventory/audit`
- `POST /api/inventory/location`
- `POST /api/inventory/transfer`
- `POST /api/inventory/adjustment`
- `POST /api/inventory/events/<eventId>/void`
- `POST /api/inventory/events/<eventId>/retry`
- `POST /api/inventory/approvals/<requestId>/approve`
- `POST /api/inventory/approvals/<requestId>/reject`

### 8) Batch 3 smoke test
Run:
```bash
python execution/smoke_inventory_api.py
```

Expected:
- Output contains `INVENTORY_API_SMOKE_PASS`
- Smoke locations/SKUs/events are cleaned automatically at end

### 9) Enable Batch 4 security + approval controls
Set environment variables (recommended for production use):
- `INVENTORY_WRITE_TOKEN` (required for transfer/adjustment)
- `INVENTORY_ADMIN_TOKEN` (required for location/void/retry/approval actions)
- `INVENTORY_REQUIRE_APPROVAL=true`
- `INVENTORY_APPROVAL_QTY_THRESHOLD=25` (or lower for stricter control)

Approval behavior:
- Transfers/adjustments above threshold are stored as pending approvals.
- Set-mode adjustments always require approval.
- Admin must explicitly approve to execute the underlying Convex mutation.

### 10) Batch 4 smoke test
Run:
```bash
python execution/smoke_inventory_batch4.py
```

Expected:
- Output contains `INVENTORY_BATCH4_SMOKE_PASS`
- Unauthorized write is rejected with `401`
- Approved request executes and enters QuickBooks queue lifecycle

### 11) Production cutover checklist (Batch 4)
1. Generate/rotate tokens:
```bash
python execution/generate_inventory_security_tokens.py
```
2. Copy generated values into `.env` on the production host:
```env
INVENTORY_WRITE_TOKEN=<generated>
INVENTORY_ADMIN_TOKEN=<generated>
INVENTORY_REQUIRE_APPROVAL=true
INVENTORY_APPROVAL_QTY_THRESHOLD=25
```
3. Restart Flask/QBWC processes so new env values are loaded.
4. Run preflight and smoke checks:
```bash
python execution/validate_qbwc_live_host.py --start-service --generate-qwc
python execution/smoke_inventory_batch4.py
```
or run a single consolidated check:
```bash
python execution/validate_cutover_ready.py --generate-qwc
```
5. Validate operator views in `http://localhost:5000/inventory`:
   - Security line shows approval enabled.
   - Employee write without token fails.
   - Admin can approve/reject requests.
   - Audit log records actions.
6. Keep admin token restricted to supervisors/controllers only.
7. Rotate both tokens immediately after staffing changes or suspected leak.

Rollback:
- Set `INVENTORY_REQUIRE_APPROVAL=false` and restart services.
- Keep tokens enabled to preserve write/admin separation.
- If needed for emergency, temporarily unset tokens, restart, and restore within same change window.

### 12) One-command readiness check
Run:
```bash
python execution/validate_cutover_ready.py --generate-qwc
```

Outputs:
- Markdown report: `.tmp/cutover_readiness_report.md`
- JSON check list in terminal output
- Exit code `0` only when all Batch 2 + Batch 4 checks pass

## Retry Policy
- Delay ladder (seconds): `60`, `300`, `900`, `3600`.
- Max retries: `10`.
- Event transitions to `error` when retries are exhausted or non-retryable errors occur.

## Guardrails
- Transfer quantity must be positive.
- Transfers cannot use same source and destination.
- Negative on-hand is blocked.
- Events already `in_flight` or `applied` cannot be voided.

## Troubleshooting
1. `SKU not found`:
   - Re-run `inventory_import_items.py` and verify SKU exists in `inventory_parts`.
2. `Location is inactive`:
   - Update location via `inventory:upsertLocation` with `active=true`.
3. Event stuck in `error`:
   - Review error fields on `inventory_events`.
   - Fix root cause, then run `qbQueue:retryFailedEvent`.
4. Mutation not found during import:
   - Ensure latest code is deployed (`--push-first` or run `npx convex dev`).
5. `authenticate` returns `nvu` in QBWC:
   - Username/password mismatch between QBWC UI and service env.
6. `sendRequestXML` stays empty:
   - Confirm events exist in Convex with `qbStatus = pending`.
   - Confirm middleware can run Convex functions with current deployment/env file.

## Operational Notes
- Batch 1 does not include the QBWC SOAP middleware yet.
- Batch 2 will add qbXML request generation and response handling on top of this queue.
