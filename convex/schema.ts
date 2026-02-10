import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
    inventory_parts: defineTable({
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
    }),
});
