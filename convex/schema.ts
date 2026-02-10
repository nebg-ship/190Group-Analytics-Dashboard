import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
    inventory_parts: defineTable({
        Account: v.string(),
        "Accumulated Depreciation": v.number(),
        "Active Status": v.string(),
        "Asset Account": v.string(),
        "COGS Account": v.string(),
        Category: v.string(),
        Cost: v.number(),
        Description: v.string(),
        MPN: v.string(),
        "Preferred Vendor": v.string(),
        Price: v.number(),
        "Purchase Description": v.string(),
        "Quantity On Hand (2025)": v.number(),
        "Reorder Pt (Min)": v.number(),
        "Sales Tax Code": v.string(),
        Sku: v.string(),
        "Tax Agency": v.string(),
        Type: v.string(),
        "U/M": v.string(),
        "U/M Set": v.string(),
    }),
});
