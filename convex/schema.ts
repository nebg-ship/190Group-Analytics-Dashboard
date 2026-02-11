import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
    inventory_parts: defineTable(v.any()).index("by_sku", ["Sku"]),
    inventory_locations: defineTable({
        code: v.string(),
        displayName: v.string(),
        active: v.boolean(),
        qbSiteFullName: v.optional(v.string()),
        qbSiteListId: v.optional(v.string()),
        isVirtual: v.boolean(),
        createdAt: v.number(),
        updatedAt: v.number(),
    })
        .index("by_code", ["code"])
        .index("by_active", ["active"]),
    inventory_balances: defineTable({
        sku: v.string(),
        locationId: v.id("inventory_locations"),
        onHand: v.number(),
        reserved: v.number(),
        available: v.number(),
        updatedAt: v.number(),
    })
        .index("by_sku", ["sku"])
        .index("by_locationId", ["locationId"])
        .index("by_sku_locationId", ["sku", "locationId"]),
    inventory_events: defineTable({
        eventType: v.union(v.literal("transfer"), v.literal("adjustment")),
        status: v.union(v.literal("draft"), v.literal("committed"), v.literal("voided")),
        effectiveDate: v.string(),
        createdAt: v.number(),
        createdBy: v.optional(v.string()),
        memo: v.optional(v.string()),
        qbStatus: v.union(
            v.literal("not_ready"),
            v.literal("pending"),
            v.literal("in_flight"),
            v.literal("applied"),
            v.literal("error"),
        ),
        qbTxnType: v.optional(v.string()),
        qbTxnId: v.optional(v.string()),
        qbErrorCode: v.optional(v.string()),
        qbErrorMessage: v.optional(v.string()),
        retryCount: v.number(),
        retryAt: v.number(),
        idempotencyKey: v.string(),
        qbLastAttemptAt: v.optional(v.number()),
    })
        .index("by_status", ["status"])
        .index("by_qbStatus", ["qbStatus"])
        .index("by_effectiveDate", ["effectiveDate"])
        .index("by_createdAt", ["createdAt"])
        .index("by_idempotencyKey", ["idempotencyKey"])
        .index("by_qbStatus_retryAt_createdAt", ["qbStatus", "retryAt", "createdAt"]),
    inventory_event_lines: defineTable({
        eventId: v.id("inventory_events"),
        sku: v.string(),
        qty: v.number(),
        fromLocationId: v.optional(v.id("inventory_locations")),
        toLocationId: v.optional(v.id("inventory_locations")),
        locationId: v.optional(v.id("inventory_locations")),
        newQty: v.optional(v.number()),
        reasonCode: v.optional(v.string()),
    })
        .index("by_eventId", ["eventId"])
        .index("by_sku", ["sku"])
        .index("by_eventId_sku", ["eventId", "sku"]),
    inventory_reason_accounts: defineTable({
        reasonCode: v.string(),
        qbAccountFullName: v.string(),
        updatedAt: v.number(),
        updatedBy: v.optional(v.string()),
    }).index("by_reasonCode", ["reasonCode"]),
    qb_sync_sessions: defineTable({
        ticket: v.string(),
        startedAt: v.number(),
        lastSeenAt: v.number(),
        inFlightEventId: v.optional(v.id("inventory_events")),
        lastError: v.optional(v.string()),
    })
        .index("by_ticket", ["ticket"])
        .index("by_inFlightEventId", ["inFlightEventId"]),
});
