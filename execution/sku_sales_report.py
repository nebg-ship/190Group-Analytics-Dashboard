"""
Multi-channel SKU-level sales report.

Channels covered:
  - BC Retail  (sales.bc_order_line_items)
  - BC Wholesale (sales_wholesale.bc_order_line_items)
  - Amazon     (amazon_econ.fact_sku_day_us)

Metrics per SKU:
  - Product name, brand
  - Units sold, gross sales, orders, refund rate  (per channel + total)
  - Landed cost, net proceeds / selling price
  - Gross margin dollars (where COGS loaded)
  - Current inventory on hand (from BC product catalog)

SKU prefixes targeted: btkn, bta, btc, btd, tr-, trk-
Windows: 12 months and 24 months

Usage:
    python execution/sku_sales_report.py

Outputs:
  - .tmp/sku_sales_report_12m.csv
  - .tmp/sku_sales_report_24m.csv
"""

import os
import csv
import logging
from datetime import date, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

GCP_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "bonsai-outlet")
BQ_DATASET  = os.getenv("BIGQUERY_DATASET", "amazon_econ")

SKU_PREFIXES = ["btkn", "bta", "btc", "btd", "tr-", "trk-"]

# Amazon MSKU → canonical BC SKU.  Add new mappings here as discovered.
AMZ_SKU_MAP = {
    "trk-05":     "trk-05b",   # confirmed same product
    "tr-06rn-m":  "tr-06rn",   # -m suffix = marketplace variant
    "trk-02b-m":  "trk-02b",   # same
}

def prefix_filter(col: str) -> str:
    return "(" + " OR ".join(f"LOWER({col}) LIKE '{p}%'" for p in SKU_PREFIXES) + ")"


def run_query(client: bigquery.Client, window_days: int) -> list:
    start_dt  = (date.today() - timedelta(days=window_days)).isoformat()
    end_dt    = date.today().isoformat()
    # BC uses DATETIME; Amazon uses DATE
    start_bc  = start_dt   # used in DATETIME comparison

    sku_filter_amz = prefix_filter("e.msku")
    amz_map_rows = ", ".join(
        f"STRUCT('{raw}' AS raw_msku, '{canonical}' AS canonical_sku)"
        for raw, canonical in AMZ_SKU_MAP.items()
    )

    sql = f"""
    -- ── 1. PRODUCT CATALOG ────────────────────────────────────────────────────
    WITH catalog AS (
        SELECT
            LOWER(p.sku)                      AS sku,
            p.product_name,
            COALESCE(p.brand_name, 'Other')   AS brand,
            p.price                           AS list_price,
            p.cost_price                      AS bc_cost_price,
            COALESCE(p.inventory_level, 0)    AS inventory_on_hand
        FROM `{GCP_PROJECT}.sales.bc_product` p
        WHERE {prefix_filter('p.sku')}
    ),

    -- ── 2. COGS from dim_sku_costs ─────────────────────────────────────────────
    cogs AS (
        SELECT
            LOWER(c.msku)   AS sku,
            c.cost_per_unit
        FROM `{GCP_PROJECT}.{BQ_DATASET}.dim_sku_costs_us` c
        WHERE c.valid_to >= '{end_dt}' OR c.valid_to IS NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(c.msku) ORDER BY c.valid_from DESC) = 1
    ),

    -- ── 3. BC RETAIL SALES (split paid vs bundled-free) ──────────────────────
    bc_retail_sales AS (
        SELECT
            LOWER(COALESCE(v.variants_sku, p.sku))   AS sku,
            -- Paid: line item had a non-zero price
            SUM(CASE WHEN li.product_price > 0 THEN li.quantity ELSE 0 END)     AS units,
            SUM(CASE WHEN li.product_price > 0 THEN li.base_total ELSE 0 END)   AS gross_sales,
            COUNT(DISTINCT CASE WHEN li.product_price > 0 THEN li.order_id END) AS orders,
            -- Bundled free units (add-ons shipped at $0)
            SUM(CASE WHEN li.product_price = 0 THEN li.quantity ELSE 0 END)     AS bundled_units
        FROM `{GCP_PROJECT}.sales.bc_order_line_items` li
        JOIN `{GCP_PROJECT}.sales.bc_order` o
            ON o.order_id = li.order_id
        JOIN `{GCP_PROJECT}.sales.bc_product` p
            ON p.product_id = li.product_id
        LEFT JOIN `{GCP_PROJECT}.sales.bc_product_variants` v
            ON v.variants_id = li.variant_id AND v.product_id = li.product_id
        WHERE
            o.order_created_date_time >= DATETIME '{start_bc} 00:00:00'
            AND o.order_status_id NOT IN (4, 6)
            AND {prefix_filter('COALESCE(v.variants_sku, p.sku)')}
        GROUP BY 1
    ),

    -- ── 4. BC RETAIL REFUNDS ───────────────────────────────────────────────────
    bc_retail_refunds AS (
        SELECT
            LOWER(COALESCE(v.variants_sku, p.sku))   AS sku,
            SUM(COALESCE(rli.quantity, 0))            AS refund_units,
            SUM(COALESCE(rli.requested_amount, 0))    AS refund_amount
        FROM `{GCP_PROJECT}.sales.bc_refund_line_items` rli
        JOIN `{GCP_PROJECT}.sales.bc_order_line_items` li
            ON li.order_line_item_id = rli.order_line_item_item_id
        JOIN `{GCP_PROJECT}.sales.bc_product` p
            ON p.product_id = li.product_id
        LEFT JOIN `{GCP_PROJECT}.sales.bc_product_variants` v
            ON v.variants_id = li.variant_id AND v.product_id = li.product_id
        JOIN `{GCP_PROJECT}.sales.bc_order` o
            ON o.order_id = rli.order_id
        WHERE
            o.order_created_date_time >= DATETIME '{start_bc} 00:00:00'
            AND {prefix_filter('COALESCE(v.variants_sku, p.sku)')}
        GROUP BY 1
    ),

    -- ── 5. BC WHOLESALE SALES ─────────────────────────────────────────────────
    bc_wholesale_sales AS (
        SELECT
            LOWER(COALESCE(v.variants_sku, p.sku))   AS sku,
            SUM(li.quantity)                          AS units,
            SUM(li.base_total)                        AS gross_sales,
            COUNT(DISTINCT li.order_id)               AS orders
        FROM `{GCP_PROJECT}.sales_wholesale.bc_order_line_items` li
        JOIN `{GCP_PROJECT}.sales_wholesale.bc_order` o
            ON o.order_id = li.order_id
        JOIN `{GCP_PROJECT}.sales_wholesale.bc_product` p
            ON p.product_id = li.product_id
        LEFT JOIN `{GCP_PROJECT}.sales_wholesale.bc_product_variants` v
            ON v.variants_id = li.variant_id AND v.product_id = li.product_id
        WHERE
            o.order_created_date_time >= DATETIME '{start_bc} 00:00:00'
            AND o.order_status_id NOT IN (4, 6)
            AND {prefix_filter('COALESCE(v.variants_sku, p.sku)')}
        GROUP BY 1
    ),

    -- ── 6. AMAZON SKU NORMALIZATION ───────────────────────────────────────────
    -- Map Amazon MSKUs to canonical BC SKUs before aggregating
    amz_sku_map AS (
        SELECT t.raw_msku, t.canonical_sku
        FROM UNNEST([{amz_map_rows}]) AS t
    ),

    -- ── 7. AMAZON SALES ───────────────────────────────────────────────────────
    amz_sales AS (
        SELECT
            COALESCE(LOWER(m.canonical_sku), LOWER(e.msku))  AS sku,
            SUM(e.units)                              AS units,
            SUM(e.gross_sales)                        AS gross_sales,
            SUM(ABS(e.refunds))                       AS refunds,
            SUM(e.net_sales)                          AS net_sales,
            SUM(e.amazon_fees)                        AS amazon_fees,
            SUM(e.ad_spend)                           AS ad_spend,
            SUM(e.net_proceeds)                       AS net_proceeds
        FROM `{GCP_PROJECT}.{BQ_DATASET}.fact_sku_day_us` e
        LEFT JOIN amz_sku_map m ON LOWER(e.msku) = LOWER(m.raw_msku)
        WHERE
            e.business_date BETWEEN '{start_dt}' AND '{end_dt}'
            AND {sku_filter_amz}
        GROUP BY 1
    ),

    -- ── 8. AMAZON ORDER COUNTS from settlements ───────────────────────────────
    amz_orders AS (
        SELECT
            COALESCE(LOWER(m.canonical_sku), LOWER(s.sku))   AS sku,
            COUNT(DISTINCT s.order_id)                AS orders
        FROM `{GCP_PROJECT}.{BQ_DATASET}.fact_settlements_us` s
        LEFT JOIN amz_sku_map m ON LOWER(s.sku) = LOWER(m.raw_msku)
        WHERE
            s.posted_date_time BETWEEN '{start_dt}' AND '{end_dt}'
            AND s.transaction_type = 'Order'
            AND s.order_id IS NOT NULL
            AND {prefix_filter('s.sku')}
        GROUP BY 1
    ),

    -- ── 9. UNION ALL SKUS ─────────────────────────────────────────────────────
    all_skus AS (
        SELECT sku FROM catalog
        UNION DISTINCT
        SELECT sku FROM bc_retail_sales
        UNION DISTINCT
        SELECT sku FROM bc_wholesale_sales
        UNION DISTINCT
        SELECT sku FROM amz_sales
    )

    -- ── 9. FINAL JOIN ─────────────────────────────────────────────────────────
    SELECT
        s.sku,
        COALESCE(cat.product_name, s.sku)              AS product_name,
        COALESCE(cat.brand, 'Other')                   AS brand,
        cat.list_price,
        COALESCE(cg.cost_per_unit, cat.bc_cost_price)  AS cost_per_unit,
        COALESCE(cat.inventory_on_hand, 0)             AS inventory_on_hand,

        -- BC Retail (paid sales only; bundled-free units tracked separately)
        COALESCE(bcr.units, 0)                         AS bc_units,
        COALESCE(bcr.gross_sales, 0)                   AS bc_gross_sales,
        COALESCE(bcr.orders, 0)                        AS bc_orders,
        COALESCE(bcr.bundled_units, 0)                 AS bc_bundled_units,
        COALESCE(rfnd.refund_units, 0)                 AS bc_refund_units,
        COALESCE(rfnd.refund_amount, 0)                AS bc_refund_amount,
        SAFE_DIVIDE(rfnd.refund_amount, bcr.gross_sales) AS bc_refund_rate,

        -- BC Wholesale
        COALESCE(bcw.units, 0)                         AS whl_units,
        COALESCE(bcw.gross_sales, 0)                   AS whl_gross_sales,
        COALESCE(bcw.orders, 0)                        AS whl_orders,

        -- Amazon
        COALESCE(amz.units, 0)                         AS amz_units,
        COALESCE(amz.gross_sales, 0)                   AS amz_gross_sales,
        COALESCE(amz.net_proceeds, 0)                  AS amz_net_proceeds,
        COALESCE(amz.amazon_fees, 0)                   AS amz_fees,
        COALESCE(amz.ad_spend, 0)                      AS amz_ad_spend,
        COALESCE(amz.refunds, 0)                       AS amz_refunds,
        COALESCE(ao.orders, 0)                         AS amz_orders,

        -- Totals
        COALESCE(bcr.units, 0) + COALESCE(bcw.units, 0) + COALESCE(amz.units, 0)           AS total_units,
        COALESCE(bcr.gross_sales, 0) + COALESCE(bcw.gross_sales, 0)
            + COALESCE(amz.gross_sales, 0)                                                   AS total_gross_sales,
        COALESCE(bcr.orders, 0) + COALESCE(bcw.orders, 0) + COALESCE(ao.orders, 0)         AS total_orders

    FROM all_skus s
    LEFT JOIN catalog         cat  ON cat.sku  = s.sku
    LEFT JOIN cogs            cg   ON cg.sku   = s.sku
    LEFT JOIN bc_retail_sales bcr  ON bcr.sku  = s.sku
    LEFT JOIN bc_retail_refunds rfnd ON rfnd.sku = s.sku
    LEFT JOIN bc_wholesale_sales bcw ON bcw.sku = s.sku
    LEFT JOIN amz_sales       amz  ON amz.sku  = s.sku
    LEFT JOIN amz_orders      ao   ON ao.sku   = s.sku
    WHERE
        -- Only include SKUs that had at least some activity or catalog entry
        cat.sku IS NOT NULL
        OR bcr.units > 0
        OR bcw.units > 0
        OR amz.units > 0
    ORDER BY total_gross_sales DESC
    """

    logger.info(f"Running {window_days}-day multi-channel query ({start_dt} to {end_dt})...")
    rows = list(client.query(sql).result())
    logger.info(f"  {len(rows)} SKUs returned.")
    return rows


def fmt_dollars(v) -> str:
    if v is None:
        return "N/A"
    return f"${v:,.2f}"

def fmt_pct(v) -> str:
    if v is None:
        return "N/A"
    return f"{v*100:.1f}%"


def to_dicts(rows) -> list[dict]:
    out = []
    for r in rows:
        cost = r.cost_per_unit
        margin = None
        if cost is not None and r.total_units > 0:
            # For BC: margin = gross_sales - (cost * units)
            # For Amazon: margin = net_proceeds - (cost * units)
            bc_margin  = (r.bc_gross_sales + r.whl_gross_sales) - cost * (r.bc_units + r.whl_units)
            amz_margin = r.amz_net_proceeds - cost * r.amz_units
            margin = bc_margin + amz_margin

        out.append({
            "SKU":               r.sku,
            "Product Name":      r.product_name,
            "Brand":             r.brand,
            "List Price":        r.list_price,
            "Cost/Unit":         cost,
            "Inventory":         r.inventory_on_hand,
            # BC Retail
            "BC Units":          r.bc_units,
            "BC Bundled (free)": r.bc_bundled_units,
            "BC Gross Sales":    r.bc_gross_sales,
            "BC Orders":         r.bc_orders,
            "BC Refund Units":   r.bc_refund_units,
            "BC Refund $":       r.bc_refund_amount,
            "BC Refund Rate":    r.bc_refund_rate,
            # Wholesale
            "WHL Units":         r.whl_units,
            "WHL Gross Sales":   r.whl_gross_sales,
            "WHL Orders":        r.whl_orders,
            # Amazon
            "AMZ Units":         r.amz_units,
            "AMZ Gross Sales":   r.amz_gross_sales,
            "AMZ Net Proceeds":  r.amz_net_proceeds,
            "AMZ Fees":          r.amz_fees,
            "AMZ Ad Spend":      r.amz_ad_spend,
            "AMZ Refunds":       r.amz_refunds,
            "AMZ Orders":        r.amz_orders,
            # Totals
            "Total Units":       r.total_units,
            "Total Gross Sales": r.total_gross_sales,
            "Total Orders":      r.total_orders,
            "Gross Margin $":    margin,
        })
    return out


def write_csv(rows: list[dict], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        logger.warning(f"No data to write: {path}")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"CSV written: {path}")


def print_summary(rows: list[dict], label: str):
    # Only print SKUs with any sales (skip pure zero rows)
    active = [r for r in rows if r["Total Units"] > 0 or r["Total Gross Sales"] > 0]

    print(f"\n{'='*130}")
    print(f"  {label}  ({len(active)} SKUs with sales)")
    print(f"{'='*130}")

    hdr = (
        f"{'SKU':<22} {'Brand':<12} {'Inv':>5}  "
        f"{'BC Paid (+free)':<14} {'BC $':>10}  "
        f"{'WHL':>5} {'WHL $':>9}  "
        f"{'AMZ':>5} {'AMZ $':>10}  "
        f"{'TOT':>6} {'TOT $':>11}  "
        f"{'Cost':>8}  {'Margin $':>11}  {'BC Rfnd%':>8}"
    )
    print(hdr)
    print("-" * 130)

    for r in active:
        margin_str = fmt_dollars(r["Gross Margin $"]) if r["Gross Margin $"] is not None else "no COGS"
        refund_str = fmt_pct(r["BC Refund Rate"])
        cost_str   = fmt_dollars(r["Cost/Unit"]) if r["Cost/Unit"] else "N/A"
        bundled = r["BC Bundled (free)"]
        bundled_tag = f" +{bundled}free" if bundled > 0 else ""
        bc_units_str = f"{r['BC Units']:,}{bundled_tag}"
        print(
            f"{r['SKU']:<22} {r['Brand']:<12} {r['Inventory']:>5}  "
            f"{bc_units_str:<14} {r['BC Gross Sales']:>10,.0f}  "
            f"{r['WHL Units']:>5,} {r['WHL Gross Sales']:>9,.0f}  "
            f"{r['AMZ Units']:>5,} {r['AMZ Gross Sales']:>10,.0f}  "
            f"{r['Total Units']:>6,} {r['Total Gross Sales']:>11,.0f}  "
            f"{cost_str:>8}  {margin_str:>11}  {refund_str:>8}"
        )

    print("-" * 130)
    tu   = sum(r["Total Units"] for r in active)
    tgs  = sum(r["Total Gross Sales"] for r in active)
    tm   = sum(r["Gross Margin $"] for r in active if r["Gross Margin $"] is not None)
    bc_s = sum(r["BC Gross Sales"] for r in active)
    whl_s= sum(r["WHL Gross Sales"] for r in active)
    amz_s= sum(r["AMZ Gross Sales"] for r in active)
    print(
        f"{'TOTAL':<22} {'':<12} {'':>5}  "
        f"{'':>7} {bc_s:>10,.0f}  "
        f"{'':>5} {whl_s:>9,.0f}  "
        f"{'':>5} {amz_s:>10,.0f}  "
        f"{tu:>6,} {tgs:>11,.0f}  "
        f"{'':>8}  {fmt_dollars(tm):>11}  {'':>8}"
    )
    print()
    print("  Channel split (gross sales):")
    total = bc_s + whl_s + amz_s
    if total > 0:
        print(f"    BC Retail:   {fmt_dollars(bc_s)}  ({bc_s/total*100:.0f}%)")
        print(f"    Wholesale:   {fmt_dollars(whl_s)}  ({whl_s/total*100:.0f}%)")
        print(f"    Amazon:      {fmt_dollars(amz_s)}  ({amz_s/total*100:.0f}%)")
    print()
    print("  NOTES:")
    print("    Inventory = BC product catalog (not QuickBooks).")
    print("    AMZ Orders = settlements only (Nov 2025-present).")
    print("    Margin = (gross sales - COGS*units for BC/WHL) + (net proceeds - COGS*units for AMZ).")
    print("    COGS missing for many SKUs; only those loaded in dim_sku_costs_us show margin.")


def main():
    client = bigquery.Client(project=GCP_PROJECT)

    raw_12 = run_query(client, 365)
    raw_24 = run_query(client, 730)

    rows_12 = to_dicts(raw_12)
    rows_24 = to_dicts(raw_24)

    print_summary(rows_12, "SKU SALES REPORT — LAST 12 MONTHS (All Channels)")
    print_summary(rows_24, "SKU SALES REPORT — LAST 24 MONTHS (All Channels)")

    write_csv(rows_12, ".tmp/sku_sales_report_12m.csv")
    write_csv(rows_24, ".tmp/sku_sales_report_24m.csv")


if __name__ == "__main__":
    main()
