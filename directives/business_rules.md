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
