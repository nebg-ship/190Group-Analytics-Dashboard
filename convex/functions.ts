import { query } from "./_generated/server";

async function buildHealthPayload(ctx: any) {
    const locations = await ctx.db.query("inventory_locations").collect();
    const events = await ctx.db.query("inventory_events").collect();
    const pendingEvents = events.filter((event: any) => event.qbStatus === "pending").length;
    const failedEvents = events.filter((event: any) => event.qbStatus === "error").length;

    return {
        ok: true,
        service: "inventory-core",
        timestamp: Date.now(),
        totals: {
            locations: locations.length,
            events: events.length,
            pendingEvents,
            failedEvents,
        },
    };
}

export const health = query({
    args: {},
    handler: async (ctx) => {
        return await buildHealthPayload(ctx);
    },
});

// Backward compatible alias for older scripts that call `functions:get`.
export const get = query({
    args: {},
    handler: async (ctx) => {
        return await buildHealthPayload(ctx);
    },
});
