import { mutation, query } from "./_generated/server";
import { v } from "convex/values";

const DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const EPSILON = 1e-9;

const inventoryPartValidator = v.object({
    Account: v.string(),
    Accumulated_Depreciation: v.number(),
    Active_Status: v.string(),
    Asset_Account: v.string(),
    COGS_Account: v.string(),
    Category: v.string(),
    Cost: v.number(),
    Description: v.string(),
    MPN: v.string(),
    Preferred_Vendor: v.string(),
    Price: v.number(),
    Purchase_Description: v.string(),
    Quantity_On_Hand_2025: v.number(),
    Reorder_Pt_Min: v.number(),
    Sales_Tax_Code: v.string(),
    Sku: v.string(),
    Tax_Agency: v.string(),
    Type: v.string(),
    U_M: v.string(),
    U_M_Set: v.string(),
    qbItemFullName: v.optional(v.string()),
    qbItemListId: v.optional(v.string()),
    isActive: v.optional(v.boolean()),
});

function assertIsoDate(dateValue: string): void {
    if (!DATE_REGEX.test(dateValue)) {
        throw new Error("effectiveDate must be in YYYY-MM-DD format.");
    }
}

function assertFiniteNumber(value: number, message: string): void {
    if (!Number.isFinite(value)) {
        throw new Error(message);
    }
}

function normalizeSku(sku: string): string {
    return sku.trim();
}

function isPartActive(part: {
    Active_Status: string;
    isActive?: boolean;
}): boolean {
    if (typeof part.isActive === "boolean") {
        return part.isActive;
    }
    return part.Active_Status.toLowerCase() === "active";
}

async function getPartBySkuOrThrow(ctx: any, sku: string) {
    const part = await ctx.db
        .query("inventory_parts")
        .withIndex("by_sku", (q: any) => q.eq("Sku", sku))
        .first();
    if (!part) {
        throw new Error(`SKU not found: ${sku}`);
    }
    return part;
}

async function getLocationOrThrow(ctx: any, locationId: any, requireActive = true) {
    const location = await ctx.db.get(locationId);
    if (!location) {
        throw new Error(`Location not found: ${locationId}`);
    }
    if (requireActive && !location.active) {
        throw new Error(`Location is inactive: ${location.code}`);
    }
    return location;
}

async function getOrCreateBalance(ctx: any, sku: string, locationId: any) {
    const existing = await ctx.db
        .query("inventory_balances")
        .withIndex("by_sku_locationId", (q: any) =>
            q.eq("sku", sku).eq("locationId", locationId),
        )
        .first();
    if (existing) {
        return existing;
    }
    const now = Date.now();
    const newId = await ctx.db.insert("inventory_balances", {
        sku,
        locationId,
        onHand: 0,
        reserved: 0,
        available: 0,
        updatedAt: now,
    });
    return await ctx.db.get(newId);
}

async function applyBalanceDelta(ctx: any, sku: string, locationId: any, delta: number) {
    const balance = await getOrCreateBalance(ctx, sku, locationId);
    if (!balance) {
        throw new Error("Unable to initialize inventory balance.");
    }
    const nextOnHand = balance.onHand + delta;
    if (nextOnHand < -EPSILON) {
        throw new Error(
            `Inventory cannot go negative for SKU ${sku} at location ${locationId}.`,
        );
    }
    const normalizedOnHand = nextOnHand < 0 ? 0 : nextOnHand;
    const available = normalizedOnHand - balance.reserved;
    await ctx.db.patch(balance._id, {
        onHand: normalizedOnHand,
        available,
        updatedAt: Date.now(),
    });
}

function resolveReasonCode(lineReasonCode: string | undefined, eventReasonCode: string | undefined) {
    if (lineReasonCode && lineReasonCode.trim()) {
        return lineReasonCode.trim();
    }
    if (eventReasonCode && eventReasonCode.trim()) {
        return eventReasonCode.trim();
    }
    return undefined;
}

export const upsertInventoryPartsBatch = mutation({
    args: {
        parts: v.array(inventoryPartValidator),
    },
    handler: async (ctx, args) => {
        let inserted = 0;
        let updated = 0;

        for (const part of args.parts) {
            const sku = normalizeSku(part.Sku);
            if (!sku) {
                throw new Error("Every part must include a non-empty Sku.");
            }

            const payload = {
                ...part,
                Sku: sku,
                isActive:
                    typeof part.isActive === "boolean"
                        ? part.isActive
                        : part.Active_Status.toLowerCase() === "active",
            };

            const existing = await ctx.db
                .query("inventory_parts")
                .withIndex("by_sku", (q) => q.eq("Sku", sku))
                .first();

            if (existing) {
                await ctx.db.patch(existing._id, payload);
                updated += 1;
            } else {
                await ctx.db.insert("inventory_parts", payload);
                inserted += 1;
            }
        }

        return {
            processed: args.parts.length,
            inserted,
            updated,
        };
    },
});

export const upsertLocation = mutation({
    args: {
        code: v.string(),
        displayName: v.string(),
        active: v.optional(v.boolean()),
        qbSiteFullName: v.optional(v.string()),
        qbSiteListId: v.optional(v.string()),
        isVirtual: v.optional(v.boolean()),
    },
    handler: async (ctx, args) => {
        const code = args.code.trim();
        const displayName = args.displayName.trim();
        if (!code) {
            throw new Error("Location code cannot be empty.");
        }
        if (!displayName) {
            throw new Error("Location displayName cannot be empty.");
        }

        const now = Date.now();
        const existing = await ctx.db
            .query("inventory_locations")
            .withIndex("by_code", (q) => q.eq("code", code))
            .first();

        const patch = {
            code,
            displayName,
            active: args.active ?? true,
            qbSiteFullName: args.qbSiteFullName,
            qbSiteListId: args.qbSiteListId,
            isVirtual: args.isVirtual ?? false,
            updatedAt: now,
        };

        if (existing) {
            await ctx.db.patch(existing._id, patch);
            return {
                locationId: existing._id,
                created: false,
            };
        }

        const locationId = await ctx.db.insert("inventory_locations", {
            ...patch,
            createdAt: now,
        });
        return {
            locationId,
            created: true,
        };
    },
});

export const upsertReasonAccount = mutation({
    args: {
        reasonCode: v.string(),
        qbAccountFullName: v.string(),
        updatedBy: v.optional(v.string()),
    },
    handler: async (ctx, args) => {
        const reasonCode = args.reasonCode.trim();
        const qbAccountFullName = args.qbAccountFullName.trim();
        if (!reasonCode) {
            throw new Error("reasonCode cannot be empty.");
        }
        if (!qbAccountFullName) {
            throw new Error("qbAccountFullName cannot be empty.");
        }

        const now = Date.now();
        const existing = await ctx.db
            .query("inventory_reason_accounts")
            .withIndex("by_reasonCode", (q) => q.eq("reasonCode", reasonCode))
            .first();

        if (existing) {
            await ctx.db.patch(existing._id, {
                qbAccountFullName,
                updatedAt: now,
                updatedBy: args.updatedBy,
            });
            return {
                reasonAccountId: existing._id,
                created: false,
            };
        }

        const reasonAccountId = await ctx.db.insert("inventory_reason_accounts", {
            reasonCode,
            qbAccountFullName,
            updatedAt: now,
            updatedBy: args.updatedBy,
        });
        return {
            reasonAccountId,
            created: true,
        };
    },
});

export const createTransferEvent = mutation({
    args: {
        effectiveDate: v.string(),
        memo: v.optional(v.string()),
        createdBy: v.optional(v.string()),
        lines: v.array(
            v.object({
                sku: v.string(),
                qty: v.number(),
                fromLocationId: v.id("inventory_locations"),
                toLocationId: v.id("inventory_locations"),
            }),
        ),
    },
    handler: async (ctx, args) => {
        assertIsoDate(args.effectiveDate);
        if (!args.lines.length) {
            throw new Error("Transfer must include at least one line.");
        }

        const now = Date.now();
        const eventId = await ctx.db.insert("inventory_events", {
            eventType: "transfer",
            status: "committed",
            effectiveDate: args.effectiveDate,
            createdAt: now,
            createdBy: args.createdBy,
            memo: args.memo?.trim() || undefined,
            qbStatus: "pending",
            qbTxnType: "TransferInventoryAdd",
            retryCount: 0,
            retryAt: now,
            idempotencyKey: "__pending__",
        });
        await ctx.db.patch(eventId, { idempotencyKey: eventId });

        for (const line of args.lines) {
            const sku = normalizeSku(line.sku);
            assertFiniteNumber(line.qty, "Transfer qty must be finite.");
            if (!sku) {
                throw new Error("Transfer line SKU cannot be empty.");
            }
            if (line.qty <= 0) {
                throw new Error(`Transfer qty must be > 0 for SKU ${sku}.`);
            }
            if (line.fromLocationId === line.toLocationId) {
                throw new Error(`Transfer line has the same source and destination for SKU ${sku}.`);
            }

            await getPartBySkuOrThrow(ctx, sku);
            await getLocationOrThrow(ctx, line.fromLocationId, true);
            await getLocationOrThrow(ctx, line.toLocationId, true);

            await applyBalanceDelta(ctx, sku, line.fromLocationId, -line.qty);
            await applyBalanceDelta(ctx, sku, line.toLocationId, line.qty);

            await ctx.db.insert("inventory_event_lines", {
                eventId,
                sku,
                qty: line.qty,
                fromLocationId: line.fromLocationId,
                toLocationId: line.toLocationId,
            });
        }

        return {
            eventId,
            qbStatus: "pending",
        };
    },
});

export const createAdjustmentEvent = mutation({
    args: {
        effectiveDate: v.string(),
        locationId: v.id("inventory_locations"),
        mode: v.union(v.literal("delta"), v.literal("set")),
        memo: v.optional(v.string()),
        createdBy: v.optional(v.string()),
        reasonCode: v.optional(v.string()),
        lines: v.array(
            v.object({
                sku: v.string(),
                qty: v.optional(v.number()),
                newQty: v.optional(v.number()),
                reasonCode: v.optional(v.string()),
            }),
        ),
    },
    handler: async (ctx, args) => {
        assertIsoDate(args.effectiveDate);
        if (!args.lines.length) {
            throw new Error("Adjustment must include at least one line.");
        }

        await getLocationOrThrow(ctx, args.locationId, true);
        const now = Date.now();
        const eventId = await ctx.db.insert("inventory_events", {
            eventType: "adjustment",
            status: "committed",
            effectiveDate: args.effectiveDate,
            createdAt: now,
            createdBy: args.createdBy,
            memo: args.memo?.trim() || undefined,
            qbStatus: "pending",
            qbTxnType: "InventoryAdjustmentAdd",
            retryCount: 0,
            retryAt: now,
            idempotencyKey: "__pending__",
        });
        await ctx.db.patch(eventId, { idempotencyKey: eventId });

        for (const line of args.lines) {
            const sku = normalizeSku(line.sku);
            if (!sku) {
                throw new Error("Adjustment line SKU cannot be empty.");
            }
            await getPartBySkuOrThrow(ctx, sku);

            const balance = await getOrCreateBalance(ctx, sku, args.locationId);
            if (!balance) {
                throw new Error("Unable to initialize balance for adjustment.");
            }

            let qtyDelta = 0;
            let newQty: number | undefined = undefined;
            if (args.mode === "delta") {
                if (line.qty === undefined) {
                    throw new Error(`Adjustment delta qty is required for SKU ${sku}.`);
                }
                assertFiniteNumber(line.qty, `Adjustment qty must be finite for SKU ${sku}.`);
                if (Math.abs(line.qty) < EPSILON) {
                    throw new Error(`Adjustment qty cannot be 0 for SKU ${sku}.`);
                }
                qtyDelta = line.qty;
            } else {
                if (line.newQty === undefined) {
                    throw new Error(`Adjustment newQty is required for SKU ${sku}.`);
                }
                assertFiniteNumber(line.newQty, `Adjustment newQty must be finite for SKU ${sku}.`);
                if (line.newQty < 0) {
                    throw new Error(`Adjustment newQty cannot be negative for SKU ${sku}.`);
                }
                newQty = line.newQty;
                qtyDelta = line.newQty - balance.onHand;
            }

            await applyBalanceDelta(ctx, sku, args.locationId, qtyDelta);

            await ctx.db.insert("inventory_event_lines", {
                eventId,
                sku,
                qty: qtyDelta,
                locationId: args.locationId,
                newQty,
                reasonCode: resolveReasonCode(line.reasonCode, args.reasonCode),
            });
        }

        return {
            eventId,
            qbStatus: "pending",
        };
    },
});

export const voidEvent = mutation({
    args: {
        eventId: v.id("inventory_events"),
    },
    handler: async (ctx, args) => {
        const event = await ctx.db.get(args.eventId);
        if (!event) {
            throw new Error("Event not found.");
        }
        if (event.status === "voided") {
            return {
                eventId: args.eventId,
                status: event.status,
                qbStatus: event.qbStatus,
            };
        }
        if (event.qbStatus === "in_flight" || event.qbStatus === "applied") {
            throw new Error("Event cannot be voided after QuickBooks sync has started.");
        }

        const lines = await ctx.db
            .query("inventory_event_lines")
            .withIndex("by_eventId", (q) => q.eq("eventId", args.eventId))
            .collect();

        for (const line of lines) {
            if (event.eventType === "transfer") {
                if (!line.fromLocationId || !line.toLocationId) {
                    throw new Error("Transfer line missing location references.");
                }
                await applyBalanceDelta(ctx, line.sku, line.fromLocationId, line.qty);
                await applyBalanceDelta(ctx, line.sku, line.toLocationId, -line.qty);
            } else {
                if (!line.locationId) {
                    throw new Error("Adjustment line missing locationId.");
                }
                await applyBalanceDelta(ctx, line.sku, line.locationId, -line.qty);
            }
        }

        await ctx.db.patch(args.eventId, {
            status: "voided",
            qbStatus: "not_ready",
            qbErrorCode: "VOIDED",
            qbErrorMessage: "Event voided before QuickBooks sync.",
            retryAt: 0,
        });

        return {
            eventId: args.eventId,
            status: "voided",
            qbStatus: "not_ready",
        };
    },
});

export const getInventoryOverview = query({
    args: {
        locationId: v.optional(v.id("inventory_locations")),
        search: v.optional(v.string()),
        includeInactive: v.optional(v.boolean()),
        limit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        if (args.locationId) {
            await getLocationOrThrow(ctx, args.locationId, false);
        }

        const includeInactive = args.includeInactive ?? false;
        const limit = Math.min(Math.max(Math.floor(args.limit ?? 500), 1), 5000);
        const search = args.search?.trim().toLowerCase() ?? "";

        const parts = await ctx.db.query("inventory_parts").collect();
        const activeParts = includeInactive ? parts : parts.filter(isPartActive);
        const filteredParts = search
            ? activeParts.filter(
                  (part) =>
                      part.Sku.toLowerCase().includes(search) ||
                      part.Description.toLowerCase().includes(search),
              )
            : activeParts;

        const balances = args.locationId
            ? await ctx.db
                  .query("inventory_balances")
                  .withIndex("by_locationId", (q) => q.eq("locationId", args.locationId!))
                  .collect()
            : await ctx.db.query("inventory_balances").collect();

        const balanceBySku = new Map<string, { onHand: number; reserved: number; available: number }>();
        for (const balance of balances) {
            const existing = balanceBySku.get(balance.sku) ?? { onHand: 0, reserved: 0, available: 0 };
            existing.onHand += balance.onHand;
            existing.reserved += balance.reserved;
            existing.available += balance.available;
            balanceBySku.set(balance.sku, existing);
        }

        const rows = filteredParts
            .map((part) => {
                const totals = balanceBySku.get(part.Sku) ?? { onHand: 0, reserved: 0, available: 0 };
                const incomeAccount = part.Account.trim() ? part.Account : part.Category;
                return {
                    sku: part.Sku,
                    description: part.Description,
                    category: incomeAccount,
                    incomeAccount: part.Account,
                    reorderPoint: part.Reorder_Pt_Min,
                    active: isPartActive(part),
                    onHand: totals.onHand,
                    reserved: totals.reserved,
                    available: totals.available,
                    lowStock: totals.available <= part.Reorder_Pt_Min,
                };
            })
            .sort((a, b) => a.sku.localeCompare(b.sku))
            .slice(0, limit);

        return {
            rows,
            totalRows: rows.length,
            locationId: args.locationId ?? null,
            generatedAt: Date.now(),
        };
    },
});

export const getItemDetail = query({
    args: {
        sku: v.string(),
        eventLimit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const sku = normalizeSku(args.sku);
        if (!sku) {
            throw new Error("sku is required.");
        }

        const part = await ctx.db
            .query("inventory_parts")
            .withIndex("by_sku", (q) => q.eq("Sku", sku))
            .first();
        if (!part) {
            return null;
        }

        const balances = await ctx.db
            .query("inventory_balances")
            .withIndex("by_sku", (q) => q.eq("sku", sku))
            .collect();

        const uniqueLocationIds = Array.from(new Set(balances.map((balance) => balance.locationId)));
        const locations = await Promise.all(uniqueLocationIds.map((locationId) => ctx.db.get(locationId)));
        const locationMap = new Map<string, any>();
        for (const location of locations) {
            if (location) {
                locationMap.set(location._id, location);
            }
        }

        const balanceRows = balances
            .map((balance) => {
                const location = locationMap.get(balance.locationId);
                return {
                    locationId: balance.locationId,
                    locationCode: location?.code ?? null,
                    locationName: location?.displayName ?? null,
                    onHand: balance.onHand,
                    reserved: balance.reserved,
                    available: balance.available,
                    updatedAt: balance.updatedAt,
                };
            })
            .sort((a, b) => String(a.locationCode ?? "").localeCompare(String(b.locationCode ?? "")));

        const eventLimit = Math.min(Math.max(Math.floor(args.eventLimit ?? 20), 1), 100);
        const lines = await ctx.db
            .query("inventory_event_lines")
            .withIndex("by_sku", (q) => q.eq("sku", sku))
            .collect();
        const eventCache = new Map<string, any>();
        const eventRows = [];
        for (const line of lines) {
            let event = eventCache.get(line.eventId);
            if (!event) {
                event = await ctx.db.get(line.eventId);
                eventCache.set(line.eventId, event);
            }
            if (!event) {
                continue;
            }

            const locationId = line.locationId ?? line.fromLocationId ?? line.toLocationId ?? null;
            const location = locationId ? locationMap.get(locationId) ?? (await ctx.db.get(locationId)) : null;
            eventRows.push({
                eventId: event._id,
                eventType: event.eventType,
                status: event.status,
                qbStatus: event.qbStatus,
                effectiveDate: event.effectiveDate,
                createdAt: event.createdAt,
                createdBy: event.createdBy ?? null,
                memo: event.memo ?? null,
                qty: line.qty,
                newQty: line.newQty ?? null,
                reasonCode: line.reasonCode ?? null,
                fromLocationId: line.fromLocationId ?? null,
                toLocationId: line.toLocationId ?? null,
                locationId: line.locationId ?? null,
                locationCode: location?.code ?? null,
                locationName: location?.displayName ?? null,
            });
        }

        const recentEvents = eventRows
            .sort((a, b) => b.createdAt - a.createdAt)
            .slice(0, eventLimit);

        return {
            sku: part.Sku,
            description: part.Description,
            category: part.Category,
            active: isPartActive(part),
            reorderPoint: part.Reorder_Pt_Min,
            preferredVendor: part.Preferred_Vendor,
            cost: part.Cost,
            price: part.Price,
            account: part.Account,
            cogsAccount: part.COGS_Account,
            assetAccount: part.Asset_Account,
            balances: balanceRows,
            recentEvents,
        };
    },
});

export const listLocations = query({
    args: {
        includeInactive: v.optional(v.boolean()),
    },
    handler: async (ctx, args) => {
        const includeInactive = args.includeInactive ?? true;
        const locations = await ctx.db.query("inventory_locations").collect();

        const rows = (includeInactive ? locations : locations.filter((location) => location.active))
            .map((location) => ({
                locationId: location._id,
                code: location.code,
                displayName: location.displayName,
                active: location.active,
                qbSiteFullName: location.qbSiteFullName ?? null,
                qbSiteListId: location.qbSiteListId ?? null,
                isVirtual: location.isVirtual,
                updatedAt: location.updatedAt,
            }))
            .sort((a, b) => a.code.localeCompare(b.code));

        return {
            rows,
            generatedAt: Date.now(),
        };
    },
});

export const listPartQuantities = query({
    args: {
        includeInactive: v.optional(v.boolean()),
        limit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const includeInactive = args.includeInactive ?? true;
        const limit = Math.min(Math.max(Math.floor(args.limit ?? 20000), 1), 20000);
        const parts = await ctx.db.query("inventory_parts").collect();

        const rows = (includeInactive ? parts : parts.filter(isPartActive))
            .map((part) => ({
                sku: part.Sku,
                quantityOnHand2025: part.Quantity_On_Hand_2025,
                active: isPartActive(part),
                description: part.Description,
                category: part.Category,
            }))
            .sort((a, b) => a.sku.localeCompare(b.sku))
            .slice(0, limit);

        return {
            rows,
            totalRows: rows.length,
            generatedAt: Date.now(),
        };
    },
});

export const getQueueSummary = query({
    args: {
        recentLimit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const recentLimit = Math.min(Math.max(Math.floor(args.recentLimit ?? 20), 1), 100);
        const allEvents = await ctx.db.query("inventory_events").collect();

        const statusCounts = {
            not_ready: 0,
            pending: 0,
            in_flight: 0,
            applied: 0,
            error: 0,
        };

        for (const event of allEvents) {
            if (event.qbStatus === "not_ready") {
                statusCounts.not_ready += 1;
            } else if (event.qbStatus === "pending") {
                statusCounts.pending += 1;
            } else if (event.qbStatus === "in_flight") {
                statusCounts.in_flight += 1;
            } else if (event.qbStatus === "applied") {
                statusCounts.applied += 1;
            } else if (event.qbStatus === "error") {
                statusCounts.error += 1;
            }
        }

        const recentEvents = await ctx.db
            .query("inventory_events")
            .withIndex("by_createdAt")
            .order("desc")
            .take(recentLimit);

        const recent = recentEvents.map((event) => ({
            eventId: event._id,
            eventType: event.eventType,
            status: event.status,
            qbStatus: event.qbStatus,
            createdAt: event.createdAt,
            createdBy: event.createdBy ?? null,
            qbErrorCode: event.qbErrorCode ?? null,
            qbErrorMessage: event.qbErrorMessage ?? null,
            retryCount: event.retryCount,
            retryAt: event.retryAt,
        }));

        return {
            counts: statusCounts,
            totalEvents: allEvents.length,
            recent,
            generatedAt: Date.now(),
        };
    },
});

export const listRecentEvents = query({
    args: {
        limit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const limit = Math.min(Math.max(Math.floor(args.limit ?? 25), 1), 100);
        const events = await ctx.db
            .query("inventory_events")
            .withIndex("by_createdAt")
            .order("desc")
            .take(limit);

        const rows = await Promise.all(
            events.map(async (event) => {
                const lines = await ctx.db
                    .query("inventory_event_lines")
                    .withIndex("by_eventId", (q) => q.eq("eventId", event._id))
                    .collect();
                return {
                    eventId: event._id,
                    eventType: event.eventType,
                    status: event.status,
                    qbStatus: event.qbStatus,
                    effectiveDate: event.effectiveDate,
                    createdAt: event.createdAt,
                    createdBy: event.createdBy ?? null,
                    memo: event.memo ?? null,
                    lineCount: lines.length,
                };
            }),
        );

        return {
            rows,
            generatedAt: Date.now(),
        };
    },
});

export const cleanupSmokeData = mutation({
    args: {
        skuPrefix: v.optional(v.string()),
        locationCodePrefix: v.optional(v.string()),
        createdBy: v.optional(v.string()),
        dryRun: v.optional(v.boolean()),
    },
    handler: async (ctx, args) => {
        const skuPrefix = (args.skuPrefix ?? "SMOKE-SKU-").trim();
        const locationCodePrefix = (args.locationCodePrefix ?? "SMOKE_").trim();
        const createdBy = (args.createdBy ?? "smoke-test").trim();
        const dryRun = args.dryRun ?? false;

        if (!skuPrefix && !locationCodePrefix && !createdBy) {
            throw new Error("At least one cleanup selector must be provided.");
        }

        const parts = await ctx.db.query("inventory_parts").collect();
        const locations = await ctx.db.query("inventory_locations").collect();

        const smokeParts = skuPrefix ? parts.filter((part) => part.Sku.startsWith(skuPrefix)) : [];
        const smokeLocations = locationCodePrefix
            ? locations.filter((location) => location.code.startsWith(locationCodePrefix))
            : [];

        const smokeSkuSet = new Set(smokeParts.map((part) => part.Sku));
        const smokeLocationIdKeySet = new Set(smokeLocations.map((location) => String(location._id)));

        const eventIdKeys = new Set<string>();
        const eventIds: any[] = [];
        const lineIdKeys = new Set<string>();
        const lineIds: any[] = [];
        const balanceIdKeys = new Set<string>();
        const balanceIds: any[] = [];
        const sessionIdKeys = new Set<string>();
        const sessionIds: any[] = [];

        const addEventId = (eventId: any) => {
            const key = String(eventId);
            if (!eventIdKeys.has(key)) {
                eventIdKeys.add(key);
                eventIds.push(eventId);
            }
        };
        const addLineId = (lineId: any) => {
            const key = String(lineId);
            if (!lineIdKeys.has(key)) {
                lineIdKeys.add(key);
                lineIds.push(lineId);
            }
        };
        const addBalanceId = (balanceId: any) => {
            const key = String(balanceId);
            if (!balanceIdKeys.has(key)) {
                balanceIdKeys.add(key);
                balanceIds.push(balanceId);
            }
        };
        const addSessionId = (sessionId: any) => {
            const key = String(sessionId);
            if (!sessionIdKeys.has(key)) {
                sessionIdKeys.add(key);
                sessionIds.push(sessionId);
            }
        };

        const allLines = await ctx.db.query("inventory_event_lines").collect();
        for (const line of allLines) {
            const lineHasSmokeSku = smokeSkuSet.has(line.sku) || (skuPrefix && line.sku.startsWith(skuPrefix));
            const lineHasSmokeLocation =
                (line.locationId && smokeLocationIdKeySet.has(String(line.locationId))) ||
                (line.fromLocationId && smokeLocationIdKeySet.has(String(line.fromLocationId))) ||
                (line.toLocationId && smokeLocationIdKeySet.has(String(line.toLocationId)));
            if (lineHasSmokeSku || lineHasSmokeLocation) {
                addLineId(line._id);
                addEventId(line.eventId);
            }
        }

        if (createdBy) {
            const allEvents = await ctx.db.query("inventory_events").collect();
            for (const event of allEvents) {
                if ((event.createdBy ?? "") === createdBy) {
                    addEventId(event._id);
                }
            }
        }

        for (const eventId of eventIds) {
            const linesForEvent = await ctx.db
                .query("inventory_event_lines")
                .withIndex("by_eventId", (q) => q.eq("eventId", eventId))
                .collect();
            for (const line of linesForEvent) {
                addLineId(line._id);
            }
        }

        for (const location of smokeLocations) {
            const balancesForLocation = await ctx.db
                .query("inventory_balances")
                .withIndex("by_locationId", (q) => q.eq("locationId", location._id))
                .collect();
            for (const balance of balancesForLocation) {
                addBalanceId(balance._id);
            }
        }

        for (const part of smokeParts) {
            const balancesForSku = await ctx.db
                .query("inventory_balances")
                .withIndex("by_sku", (q) => q.eq("sku", part.Sku))
                .collect();
            for (const balance of balancesForSku) {
                addBalanceId(balance._id);
            }
        }

        const sessions = await ctx.db.query("qb_sync_sessions").collect();
        for (const session of sessions) {
            if (session.inFlightEventId && eventIdKeys.has(String(session.inFlightEventId))) {
                addSessionId(session._id);
            }
        }

        const summary = {
            dryRun,
            selectors: {
                skuPrefix,
                locationCodePrefix,
                createdBy,
            },
            matched: {
                parts: smokeParts.length,
                locations: smokeLocations.length,
                balances: balanceIds.length,
                events: eventIds.length,
                eventLines: lineIds.length,
                sessions: sessionIds.length,
            },
        };

        if (dryRun) {
            return summary;
        }

        for (const lineId of lineIds) {
            await ctx.db.delete(lineId);
        }
        for (const eventId of eventIds) {
            await ctx.db.delete(eventId);
        }
        for (const balanceId of balanceIds) {
            await ctx.db.delete(balanceId);
        }
        for (const part of smokeParts) {
            await ctx.db.delete(part._id);
        }
        for (const location of smokeLocations) {
            await ctx.db.delete(location._id);
        }
        for (const sessionId of sessionIds) {
            await ctx.db.delete(sessionId);
        }

        return {
            ...summary,
            deleted: {
                parts: smokeParts.length,
                locations: smokeLocations.length,
                balances: balanceIds.length,
                events: eventIds.length,
                eventLines: lineIds.length,
                sessions: sessionIds.length,
            },
        };
    },
});
