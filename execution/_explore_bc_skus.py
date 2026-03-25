"""Temporary exploration: BC product/variant/order data for target SKUs across all channels."""
from dotenv import load_dotenv; load_dotenv()
from google.cloud import bigquery

c = bigquery.Client(project='bonsai-outlet')

PREFIXES = ['tr-', 'trk-', 'btkn', 'bta', 'btc', 'btd']

def prefix_filter(col):
    return " OR ".join(f"LOWER({col}) LIKE '{p}%'" for p in PREFIXES)

# --- 1. Product catalog from bc_product (base SKU) ---
print("\n=== PRODUCT CATALOG (sales.bc_product) ===")
q = f"""
SELECT p.sku, p.product_name, p.brand_name, p.price, p.cost_price,
       p.inventory_level, p.availability
FROM `bonsai-outlet.sales.bc_product` p
WHERE {prefix_filter('p.sku')}
ORDER BY p.sku
"""
rows = list(c.query(q).result())
print(f"{len(rows)} base products")
for r in rows:
    print(f"  {r.sku:<24} | inv={r.inventory_level:>5} | ${r.price} | brand={r.brand_name} | {r.product_name[:50]}")

# --- 2. BC retail channel sales (last 24 months) ---
# SKU comes from bc_product_variants.variants_sku via variant_id
# Fallback to bc_product.sku when variant_id not found
print("\n=== BC RETAIL SALES — 24 MONTHS ===")
q = f"""
SELECT
    LOWER(COALESCE(v.variants_sku, p.sku))    AS sku,
    p.product_name,
    p.brand_name,
    SUM(li.quantity)                           AS units_sold,
    SUM(li.base_total)                         AS gross_sales,
    COUNT(DISTINCT li.order_id)                AS orders,
    MIN(o.order_created_date_time)             AS first_sale,
    MAX(o.order_created_date_time)             AS last_sale
FROM `bonsai-outlet.sales.bc_order_line_items` li
JOIN `bonsai-outlet.sales.bc_order` o
    ON o.order_id = li.order_id
JOIN `bonsai-outlet.sales.bc_product` p
    ON p.product_id = li.product_id
LEFT JOIN `bonsai-outlet.sales.bc_product_variants` v
    ON v.variants_id = li.variant_id AND v.product_id = li.product_id
WHERE
    o.order_created_date_time >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 730 DAY)
    AND o.order_status_id NOT IN (4, 6)
    AND (
        {prefix_filter('COALESCE(v.variants_sku, p.sku)')}
    )
GROUP BY 1, 2, 3
ORDER BY gross_sales DESC
"""
rows = list(c.query(q).result())
print(f"{len(rows)} SKUs")
for r in rows:
    print(f"  {r.sku:<24} | units={r.units_sold:>5} | ${r.gross_sales:>10,.2f} | orders={r.orders:>4} | {str(r.first_sale)[:10]}..{str(r.last_sale)[:10]}")

# --- 3. BC retail channel sales 12m ---
print("\n=== BC RETAIL SALES — 12 MONTHS ===")
q = f"""
SELECT
    LOWER(COALESCE(v.variants_sku, p.sku))    AS sku,
    SUM(li.quantity)                           AS units_sold,
    SUM(li.base_total)                         AS gross_sales,
    COUNT(DISTINCT li.order_id)                AS orders
FROM `bonsai-outlet.sales.bc_order_line_items` li
JOIN `bonsai-outlet.sales.bc_order` o ON o.order_id = li.order_id
JOIN `bonsai-outlet.sales.bc_product` p ON p.product_id = li.product_id
LEFT JOIN `bonsai-outlet.sales.bc_product_variants` v
    ON v.variants_id = li.variant_id AND v.product_id = li.product_id
WHERE
    o.order_created_date_time >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 365 DAY)
    AND o.order_status_id NOT IN (4, 6)
    AND ({prefix_filter('COALESCE(v.variants_sku, p.sku)')})
GROUP BY 1 ORDER BY gross_sales DESC
"""
rows_12 = list(c.query(q).result())
print(f"{len(rows_12)} SKUs")
for r in rows_12:
    print(f"  {r.sku:<24} | units={r.units_sold:>5} | ${r.gross_sales:>10,.2f} | orders={r.orders:>4}")

# --- 4. BC refunds (last 24 months) ---
print("\n=== BC RETAIL REFUNDS — 24 MONTHS ===")
q = f"""
SELECT
    LOWER(COALESCE(v.variants_sku, p.sku))    AS sku,
    COUNT(DISTINCT rli.order_id)               AS refund_orders,
    SUM(rli.quantity)                          AS refund_units,
    SUM(rli.requested_amount)                  AS refund_amount
FROM `bonsai-outlet.sales.bc_refund_line_items` rli
JOIN `bonsai-outlet.sales.bc_order_line_items` li
    ON li.order_line_item_id = rli.order_line_item_item_id
JOIN `bonsai-outlet.sales.bc_product` p ON p.product_id = li.product_id
LEFT JOIN `bonsai-outlet.sales.bc_product_variants` v
    ON v.variants_id = li.variant_id AND v.product_id = li.product_id
JOIN `bonsai-outlet.sales.bc_order` o ON o.order_id = rli.order_id
WHERE
    o.order_created_date_time >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 730 DAY)
    AND ({prefix_filter('COALESCE(v.variants_sku, p.sku)')})
GROUP BY 1 ORDER BY refund_amount DESC
"""
rows = list(c.query(q).result())
print(f"{len(rows)} SKUs with refunds")
for r in rows:
    print(f"  {r.sku:<24} | refund_units={r.refund_units} | ${r.refund_amount:>8,.2f}")

# --- 5. Wholesale channel sales (last 24 months) ---
print("\n=== WHOLESALE SALES — 24 MONTHS ===")
q = f"""
SELECT
    LOWER(COALESCE(v.variants_sku, p.sku))    AS sku,
    p.product_name,
    SUM(li.quantity)                           AS units_sold,
    SUM(li.base_total)                         AS gross_sales,
    COUNT(DISTINCT li.order_id)                AS orders
FROM `bonsai-outlet.sales_wholesale.bc_order_line_items` li
JOIN `bonsai-outlet.sales_wholesale.bc_order` o ON o.order_id = li.order_id
JOIN `bonsai-outlet.sales_wholesale.bc_product` p ON p.product_id = li.product_id
LEFT JOIN `bonsai-outlet.sales_wholesale.bc_product_variants` v
    ON v.variants_id = li.variant_id AND v.product_id = li.product_id
WHERE
    o.order_created_date_time >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 730 DAY)
    AND o.order_status_id NOT IN (4, 6)
    AND ({prefix_filter('COALESCE(v.variants_sku, p.sku)')})
GROUP BY 1, 2 ORDER BY gross_sales DESC
"""
rows = list(c.query(q).result())
print(f"{len(rows)} SKUs")
for r in rows:
    print(f"  {r.sku:<24} | units={r.units_sold:>5} | ${r.gross_sales:>10,.2f} | orders={r.orders:>4} | {r.product_name[:40]}")
