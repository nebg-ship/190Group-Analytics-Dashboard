import { mutation, query } from "./_generated/server";
import { v } from "convex/values";

const RETRY_DELAYS_SECONDS = [60, 300, 900, 3600];
const MAX_RETRIES = 10;

function getRetryDelaySeconds(retryCount: number): number {
    const index = Math.min(Math.max(retryCount - 1, 0), RETRY_DELAYS_SECONDS.length - 1);
    return RETRY_DELAYS_SECONDS[index];
}

function isRetryableQbError(code?: string): boolean {
    if (!code) {
        return true;
    }
    const nonRetryableCodes = new Set([
        "3100",
        "3140",
        "3170",
        "3200",
        "3250",
    ]);
    return !nonRetryableCodes.has(code);
}

export const getNextPendingQbEvent = query({
    args: {
        nowMs: v.optional(v.number()),
        limit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const nowMs = args.nowMs ?? Date.now();
        const limit = Math.min(Math.max(Math.floor(args.limit ?? 1), 1), 20);

        const candidateEvents = await ctx.db
            .query("inventory_events")
            .withIndex("by_qbStatus_retryAt_createdAt", (q) =>
                q.eq("qbStatus", "pending").lte("retryAt", nowMs),
            )
            .take(limit * 5);

        const events = candidateEvents
            .filter((event) => event.status === "committed")
            .slice(0, limit);

        const partCache = new Map<string, any>();
        const locationCache = new Map<string, any>();
        const reasonAccountCache = new Map<string, any>();

        const getPart = async (sku: string) => {
            const cacheKey = sku;
            if (partCache.has(cacheKey)) {
                return partCache.get(cacheKey);
            }
            const part = await ctx.db
                .query("inventory_parts")
                .withIndex("by_sku", (q) => q.eq("Sku", sku))
                .first();
            partCache.set(cacheKey, part);
            return part;
        };

        const getLocation = async (locationId: any) => {
            if (!locationId) {
                return null;
            }
            if (locationCache.has(locationId)) {
                return locationCache.get(locationId);
            }
            const location = await ctx.db.get(locationId);
            locationCache.set(locationId, location);
            return location;
        };

        const getReasonAccount = async (reasonCode: string | undefined) => {
            if (!reasonCode) {
                return null;
            }
            if (reasonAccountCache.has(reasonCode)) {
                return reasonAccountCache.get(reasonCode);
            }
            const reasonAccount = await ctx.db
                .query("inventory_reason_accounts")
                .withIndex("by_reasonCode", (q) => q.eq("reasonCode", reasonCode))
                .first();
            reasonAccountCache.set(reasonCode, reasonAccount);
            return reasonAccount;
        };

        const hydratedEvents = await Promise.all(
            events.map(async (event) => {
                const lines = await ctx.db
                    .query("inventory_event_lines")
                    .withIndex("by_eventId", (q) => q.eq("eventId", event._id))
                    .collect();

                const hydratedLines = await Promise.all(
                    lines.map(async (line) => {
                        const part = await getPart(line.sku);
                        const fromLocation = await getLocation(line.fromLocationId);
                        const toLocation = await getLocation(line.toLocationId);
                        const location = await getLocation(line.locationId);
                        const reasonAccount = await getReasonAccount(line.reasonCode);

                        return {
                            lineId: line._id,
                            sku: line.sku,
                            qty: line.qty,
                            newQty: line.newQty ?? null,
                            reasonCode: line.reasonCode ?? null,
                            qbAccountFullName:
                                reasonAccount?.qbAccountFullName ??
                                part?.COGS_Account ??
                                null,
                            qbItemFullName: part?.qbItemFullName ?? part?.Sku ?? line.sku,
                            qbItemListId: part?.qbItemListId ?? null,
                            itemIncomeAccountFullName: part?.Account ?? part?.Category ?? null,
                            itemCogsAccountFullName: part?.COGS_Account ?? null,
                            itemAssetAccountFullName: part?.Asset_Account ?? null,
                            itemSalesDescription: part?.Description ?? null,
                            itemPurchaseDescription: part?.Purchase_Description ?? null,
                            itemSalesPrice:
                                typeof part?.Price === "number" && Number.isFinite(part.Price)
                                    ? part.Price
                                    : null,
                            itemPurchaseCost:
                                typeof part?.Cost === "number" && Number.isFinite(part.Cost)
                                    ? part.Cost
                                    : null,
                            itemIsActive:
                                typeof part?.isActive === "boolean"
                                    ? part.isActive
                                    : (
                                        typeof part?.Active_Status === "string"
                                        && part.Active_Status.toLowerCase() === "active"
                                    ),
                            fromLocationId: line.fromLocationId ?? null,
                            fromSiteFullName: fromLocation?.qbSiteFullName ?? null,
                            toLocationId: line.toLocationId ?? null,
                            toSiteFullName: toLocation?.qbSiteFullName ?? null,
                            locationId: line.locationId ?? null,
                            siteFullName: location?.qbSiteFullName ?? null,
                        };
                    }),
                );

                return {
                    eventId: event._id,
                    eventType: event.eventType,
                    status: event.status,
                    qbStatus: event.qbStatus,
                    qbTxnType: event.qbTxnType ?? null,
                    effectiveDate: event.effectiveDate,
                    createdAt: event.createdAt,
                    createdBy: event.createdBy ?? null,
                    memo: event.memo ?? null,
                    retryCount: event.retryCount,
                    retryAt: event.retryAt,
                    idempotencyKey: event.idempotencyKey,
                    lines: hydratedLines,
                };
            }),
        );

        return {
            events: hydratedEvents,
            generatedAt: nowMs,
        };
    },
});

export const markEventInFlight = mutation({
    args: {
        eventId: v.id("inventory_events"),
        ticket: v.string(),
    },
    handler: async (ctx, args) => {
        const ticket = args.ticket.trim();
        if (!ticket) {
            throw new Error("ticket is required.");
        }

        const event = await ctx.db.get(args.eventId);
        if (!event) {
            throw new Error("Event not found.");
        }
        if (event.status !== "committed") {
            throw new Error(`Event ${args.eventId} is not committed.`);
        }
        if (event.qbStatus !== "pending" && event.qbStatus !== "in_flight") {
            throw new Error(`Event ${args.eventId} is not eligible for in-flight transition.`);
        }

        const now = Date.now();
        await ctx.db.patch(args.eventId, {
            qbStatus: "in_flight",
            qbLastAttemptAt: now,
        });

        const session = await ctx.db
            .query("qb_sync_sessions")
            .withIndex("by_ticket", (q) => q.eq("ticket", ticket))
            .first();
        if (session) {
            await ctx.db.patch(session._id, {
                lastSeenAt: now,
                inFlightEventId: args.eventId,
            });
        } else {
            await ctx.db.insert("qb_sync_sessions", {
                ticket,
                startedAt: now,
                lastSeenAt: now,
                inFlightEventId: args.eventId,
            });
        }

        return {
            eventId: args.eventId,
            qbStatus: "in_flight",
            ticket,
            markedAt: now,
        };
    },
});

export const applyQbResult = mutation({
    args: {
        eventId: v.id("inventory_events"),
        ticket: v.optional(v.string()),
        success: v.boolean(),
        qbTxnId: v.optional(v.string()),
        qbTxnType: v.optional(v.string()),
        qbErrorCode: v.optional(v.string()),
        qbErrorMessage: v.optional(v.string()),
        retryable: v.optional(v.boolean()),
    },
    handler: async (ctx, args) => {
        const event = await ctx.db.get(args.eventId);
        if (!event) {
            throw new Error("Event not found.");
        }

        const now = Date.now();
        const ticket = args.ticket?.trim();
        if (ticket) {
            const session = await ctx.db
                .query("qb_sync_sessions")
                .withIndex("by_ticket", (q) => q.eq("ticket", ticket))
                .first();
            if (session) {
                await ctx.db.patch(session._id, {
                    lastSeenAt: now,
                    lastError: args.success ? undefined : args.qbErrorMessage,
                });
            }
        }

        if (args.success) {
            await ctx.db.patch(args.eventId, {
                qbStatus: "applied",
                qbTxnId: args.qbTxnId,
                qbTxnType: args.qbTxnType ?? event.qbTxnType,
                qbErrorCode: undefined,
                qbErrorMessage: undefined,
                qbLastAttemptAt: now,
            });
            return {
                eventId: args.eventId,
                qbStatus: "applied",
                retryCount: event.retryCount,
            };
        }

        const nextRetryCount = event.retryCount + 1;
        const requestedRetryable = args.retryable ?? true;
        const retryable = requestedRetryable && isRetryableQbError(args.qbErrorCode);
        const shouldRetry = retryable && nextRetryCount < MAX_RETRIES;
        const retryAt = shouldRetry
            ? now + getRetryDelaySeconds(nextRetryCount) * 1000
            : now;

        await ctx.db.patch(args.eventId, {
            qbStatus: shouldRetry ? "pending" : "error",
            qbErrorCode: args.qbErrorCode ?? "UNKNOWN",
            qbErrorMessage: args.qbErrorMessage ?? "QuickBooks sync failed.",
            retryCount: nextRetryCount,
            retryAt,
            qbLastAttemptAt: now,
        });

        return {
            eventId: args.eventId,
            qbStatus: shouldRetry ? "pending" : "error",
            retryCount: nextRetryCount,
            retryAt,
        };
    },
});

export const retryFailedEvent = mutation({
    args: {
        eventId: v.id("inventory_events"),
    },
    handler: async (ctx, args) => {
        const event = await ctx.db.get(args.eventId);
        if (!event) {
            throw new Error("Event not found.");
        }
        if (event.qbStatus !== "error") {
            throw new Error("Only errored events can be retried manually.");
        }

        const now = Date.now();
        await ctx.db.patch(args.eventId, {
            qbStatus: "pending",
            retryAt: now,
            qbErrorCode: undefined,
            qbErrorMessage: undefined,
        });

        return {
            eventId: args.eventId,
            qbStatus: "pending",
            retryAt: now,
        };
    },
});

export const releaseStaleInFlightEvents = mutation({
    args: {
        olderThanMs: v.optional(v.number()),
        limit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const now = Date.now();
        const olderThanMs = Math.max(Math.floor(args.olderThanMs ?? 10 * 60 * 1000), 0);
        const limit = Math.min(Math.max(Math.floor(args.limit ?? 50), 1), 500);
        const cutoff = now - olderThanMs;

        const inFlightEvents = await ctx.db
            .query("inventory_events")
            .withIndex("by_qbStatus", (q) => q.eq("qbStatus", "in_flight"))
            .collect();

        const staleEvents = inFlightEvents
            .filter((event) => (event.qbLastAttemptAt ?? event.createdAt) <= cutoff)
            .slice(0, limit);

        for (const event of staleEvents) {
            await ctx.db.patch(event._id, {
                qbStatus: "pending",
                retryAt: now,
            });
        }

        const staleEventIds = staleEvents.map((event) => event._id);
        if (staleEventIds.length > 0) {
            const sessions = await ctx.db
                .query("qb_sync_sessions")
                .withIndex("by_inFlightEventId")
                .collect();
            for (const session of sessions) {
                if (session.inFlightEventId && staleEventIds.includes(session.inFlightEventId)) {
                    await ctx.db.patch(session._id, { inFlightEventId: undefined });
                }
            }
        }

        return {
            releasedCount: staleEvents.length,
            releasedEventIds: staleEventIds,
            cutoff,
            now,
        };
    },
});

export const remapCleanupSkusAndRetry = mutation({
    args: {
        createdBy: v.optional(v.string()),
        fromTo: v.array(
            v.object({
                fromSku: v.string(),
                toSku: v.string(),
            }),
        ),
        statuses: v.optional(v.array(v.string())),
    },
    handler: async (ctx, args) => {
        const createdBy = (args.createdBy ?? "qb-pre-sync-cleanup").trim();
        if (!createdBy) {
            throw new Error("createdBy cannot be empty.");
        }
        if (!args.fromTo.length) {
            throw new Error("fromTo must include at least one remap pair.");
        }

        const remap = new Map<string, string>();
        for (const pair of args.fromTo) {
            const fromSku = pair.fromSku.trim();
            const toSku = pair.toSku.trim();
            if (!fromSku || !toSku) {
                continue;
            }
            remap.set(fromSku.toLowerCase(), toSku);
        }
        if (!remap.size) {
            throw new Error("No valid remap pairs provided.");
        }

        const allowedStatuses = new Set(["pending", "in_flight", "error", "not_ready", "applied"]);
        const requestedStatuses = args.statuses?.length ? args.statuses : ["error"];
        const statusFilter = new Set(
            requestedStatuses
                .map((status) => status.trim())
                .filter((status) => allowedStatuses.has(status)),
        );
        if (!statusFilter.size) {
            throw new Error("No valid statuses provided.");
        }

        const allEvents = await ctx.db.query("inventory_events").collect();
        const candidateEvents = allEvents.filter(
            (event) =>
                (event.createdBy ?? "") === createdBy &&
                statusFilter.has(event.qbStatus),
        );

        const now = Date.now();
        const touchedEventIds: string[] = [];
        const touchedLines: Array<{
            eventId: string;
            lineId: string;
            fromSku: string;
            toSku: string;
        }> = [];
        const remapCounts = new Map<string, number>();

        for (const event of candidateEvents) {
            const lines = await ctx.db
                .query("inventory_event_lines")
                .withIndex("by_eventId", (q) => q.eq("eventId", event._id))
                .collect();

            let eventTouched = false;
            for (const line of lines) {
                const currentSku = line.sku.trim();
                if (!currentSku) {
                    continue;
                }
                const mappedSku = remap.get(currentSku.toLowerCase());
                if (!mappedSku || mappedSku === currentSku) {
                    continue;
                }

                await ctx.db.patch(line._id, { sku: mappedSku });
                eventTouched = true;
                touchedLines.push({
                    eventId: String(event._id),
                    lineId: String(line._id),
                    fromSku: currentSku,
                    toSku: mappedSku,
                });
                const remapKey = `${currentSku}=>${mappedSku}`;
                remapCounts.set(remapKey, (remapCounts.get(remapKey) ?? 0) + 1);
            }

            if (!eventTouched) {
                continue;
            }

            touchedEventIds.push(String(event._id));
            if (event.qbStatus === "error") {
                await ctx.db.patch(event._id, {
                    qbStatus: "pending",
                    retryAt: now,
                    qbErrorCode: undefined,
                    qbErrorMessage: undefined,
                });
            } else if (event.qbStatus === "in_flight") {
                await ctx.db.patch(event._id, {
                    qbStatus: "pending",
                    retryAt: now,
                });
            }
        }

        return {
            createdBy,
            candidateEventCount: candidateEvents.length,
            touchedEventCount: touchedEventIds.length,
            touchedEventIds,
            touchedLineCount: touchedLines.length,
            remapCounts: Array.from(remapCounts.entries()).map(([pair, count]) => ({ pair, count })),
            touchedLines,
            retriedAt: now,
        };
    },
});

export const releasePendingRetryBackoff = mutation({
    args: {
        createdBy: v.optional(v.string()),
        qbErrorCode: v.optional(v.string()),
        limit: v.optional(v.number()),
    },
    handler: async (ctx, args) => {
        const now = Date.now();
        const limit = Math.min(Math.max(Math.floor(args.limit ?? 500), 1), 5000);
        const createdByFilter = args.createdBy?.trim();
        const errorCodeFilter = args.qbErrorCode?.trim();

        const pendingEvents = await ctx.db
            .query("inventory_events")
            .withIndex("by_qbStatus", (q) => q.eq("qbStatus", "pending"))
            .collect();

        const candidates = pendingEvents
            .filter((event) => {
                if (event.status !== "committed") {
                    return false;
                }
                if ((event.retryAt ?? 0) <= now) {
                    return false;
                }
                if (createdByFilter && (event.createdBy ?? "") !== createdByFilter) {
                    return false;
                }
                if (errorCodeFilter && (event.qbErrorCode ?? "") !== errorCodeFilter) {
                    return false;
                }
                return true;
            })
            .slice(0, limit);

        for (const event of candidates) {
            await ctx.db.patch(event._id, { retryAt: now });
        }

        return {
            releasedCount: candidates.length,
            releasedEventIds: candidates.map((event) => event._id),
            now,
            filters: {
                createdBy: createdByFilter ?? null,
                qbErrorCode: errorCodeFilter ?? null,
            },
        };
    },
});
