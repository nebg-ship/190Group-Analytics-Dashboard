# Batch 1 Implementation: Convex Inventory Core + QB Queue

## Summary
Implement the operational inventory core in Convex first, with immutable event ledger, materialized balances, and QuickBooks sync queue states. This gives employees usable inventory operations immediately and creates stable inputs for QBWC middleware in Batch 2.

## Files to Add or Change
1. `convex/schema.ts`
- Add tables: `inventory_locations`, `inventory_balances`, `inventory_events`, `inventory_event_lines`, `inventory_reason_accounts`, `qb_sync_sessions`.
- Keep existing `inventory_parts` table and add optional QB mapping fields.
- Add indexes: by SKU, location, event status, qb status, event id relations.

2. `convex/inventory.ts`
- Add mutations:
  - `createTransferEvent`
  - `createAdjustmentEvent`
  - `voidEvent`
  - `upsertLocation`
  - `upsertReasonAccount`
- Add queries:
  - `getInventoryOverview`
  - `getItemDetail`
  - `listRecentEvents`
- Enforce validations:
  - SKU must exist
  - location active
  - transfer qty > 0 and from != to
  - transfer cannot drive negative on-hand

3. `convex/qb_queue.ts`
- Add queue/sync API:
  - `getNextPendingQbEvent`
  - `markEventInFlight`
  - `applyQbResult`
  - `retryFailedEvent`
- Add retry policy constants:
  - delays `[60, 300, 900, 3600]` seconds capped; max 10 retries.

4. `convex/functions.ts`
- Export basic read endpoint for smoke tests (replace placeholder "Hello world").

5. `execution/inventory_import_items.py`
- Deterministic script to load `amazon_economics/Master_Updated_web_accounts_v14_1 (1).csv` into Convex `inventory_parts`.
- Preserve SKU as string.
- Idempotent upsert keyed by SKU.

6. `directives/inventory_qb_sync.md`
- SOP for event lifecycle, retry handling, and operator runbook.

## Public APIs / Interfaces Added
- Convex mutations:
  - `inventory:createTransferEvent`
  - `inventory:createAdjustmentEvent`
  - `inventory:voidEvent`
  - `inventory:upsertLocation`
  - `inventory:upsertReasonAccount`
- Convex queries:
  - `inventory:getInventoryOverview`
  - `inventory:getItemDetail`
  - `inventory:listRecentEvents`
- Convex queue endpoints:
  - `qbQueue:getNextPendingQbEvent`
  - `qbQueue:markEventInFlight`
  - `qbQueue:applyQbResult`
  - `qbQueue:retryFailedEvent`

## Test Cases and Scenarios
1. Transfer success
- From-location decreases, to-location increases, event marked `pending`.

2. Transfer rejection
- Insufficient on-hand returns validation error, no writes.

3. Adjustment delta mode
- Positive/negative deltas update balance correctly.

4. Adjustment set mode
- New quantity overwrites on-hand exactly.

5. Void pre-sync event
- Reverses prior balance effect and marks event `voided`.

6. QB queue behavior
- `getNextPendingQbEvent` returns oldest eligible event.
- `applyQbResult(success)` marks `applied` with TxnID.
- `applyQbResult(failure)` increments retry and schedules next retry.

## Assumptions and Defaults
- Convex is source of truth for operational QOH.
- v1 event types are only `transfer` and `adjustment`.
- Employees do not manually adjust QOH in QuickBooks except controlled admin workflows.
- Retryable QB failures use exponential-like stepped backoff with hard cap.
