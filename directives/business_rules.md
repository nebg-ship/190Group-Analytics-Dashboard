# Business Rules & Category Mappings

This document tracks reporting logic, category mappings, and other business rules for the Bonsai Outlet Analytics Dashboard.

## Category Mappings

To ensure consistent year-over-year analysis, the following categories should be treated as identical:

| Original Category | Target Category | Reason |
| :--- | :--- | :--- |
| `All Bonsai Trees` | `View All Bonsai Trees` | Category renaming/migration (2025 -> 2026). |
| `View All Bonsai Trees` | `View All Bonsai Trees` | Primary category name. |

## Reporting Logic

- When calculating category-level performance, group the above categories together to avoid showing false drops or gains due to naming changes.
- Executive dashboard top categories use Convex `inventory:listPartIncomeAccounts` income account data as the category source. When joining BigCommerce sales to Convex inventory, prefer variant SKU first, then product name, then product SKU. Amazon category coverage is lower until Amazon MSKUs are mapped to canonical inventory SKUs.

## SKU Mappings

To align Amazon (SP-API) SKUs with internal product codes (BigCommerce/Settlements), use the following translation:

| Amazon MSKU | Target SKU | Notes |
| :--- | :--- | :--- |
| `AX-4L4U-UPMZ` | `PPT6-9` | Amazon code for PPT6-9 |
| `8V-JLTS-MCY9` | `PPT8-9` | Amazon code for PPT8-9 |
