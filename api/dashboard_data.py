"""
CEO Dashboard API - Data endpoint for fetching combined Amazon + Bonsai metrics
"""
import json
import os
import time
import urllib.request
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pathlib import Path
from inventory_api import inventory_api

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)
app.register_blueprint(inventory_api)

# Get project root
PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env.local", override=False)

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
GA4_DATASET = os.getenv('GA4_DATASET')
SALES_DATASET = os.getenv('SALES_DATASET', 'sales')
AMAZON_ECON_DATASET = os.getenv('AMAZON_ECON_DATASET') or os.getenv('BIGQUERY_DATASET', 'amazon_econ')
WHOLESALE_DATASET = os.getenv('WHOLESALE_DATASET', 'wholesale')
GOOGLE_ADS_DATASET = os.getenv('GOOGLE_ADS_DATASET', 'google_ads_190')
CONVEX_CATEGORY_CACHE_TTL_SECONDS = 300
CONVEX_CATEGORY_CACHE = {
    'loaded_at': 0.0,
    'categories': {},
    'source': None,
}

# Hardwired BigCommerce line-item table + columns (per user confirmation)
LINE_ITEMS_TABLE = 'bc_order_line_items'
LINE_ITEMS_SKU_COL = 'sku'
LINE_ITEMS_NAME_COL = 'product_name'
LINE_ITEMS_QTY_COL = 'quantity'
LINE_ITEMS_ORDER_ID_COL = 'order_id'
LINE_ITEMS_TOTAL_COL = 'total_ex_tax'

def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)

def normalize_sku(value):
    return str(value or '').strip().upper()

def convex_query(function_name, args_obj):
    convex_url = os.getenv('CONVEX_URL', '').strip()
    if not convex_url:
        raise RuntimeError('CONVEX_URL is not configured. Start Convex locally or set the deployment URL.')

    body = json.dumps({
        'path': function_name,
        'format': 'convex_encoded_json',
        'args': [args_obj],
    }).encode('utf-8')
    req = urllib.request.Request(
        f"{convex_url.rstrip('/')}/api/query",
        data=body,
        headers={
            'Content-Type': 'application/json',
            'Convex-Client': 'npm-1.31.7',
        },
        method='POST',
    )

    with urllib.request.urlopen(req, timeout=20) as resp:
        payload = json.loads(resp.read().decode('utf-8'))

    if payload.get('status') != 'success':
        raise RuntimeError(payload.get('errorMessage') or 'Convex query failed.')

    return payload.get('value')

def get_convex_category_map(force_refresh=False):
    now = time.time()
    cached_categories = CONVEX_CATEGORY_CACHE.get('categories') or {}
    if (
        not force_refresh
        and cached_categories
        and now - CONVEX_CATEGORY_CACHE.get('loaded_at', 0) < CONVEX_CATEGORY_CACHE_TTL_SECONDS
    ):
        return cached_categories

    source = 'inventory:listPartIncomeAccounts'
    try:
        payload = convex_query(
            source,
            {'includeInactive': True, 'limit': 20000},
        )
    except Exception:
        source = 'inventory:getInventoryOverview'
        payload = convex_query(
            source,
            {'includeInactive': True, 'limit': 5000},
        )
    categories = {}
    for row in (payload or {}).get('rows', []):
        sku = normalize_sku(row.get('sku'))
        category = str(row.get('incomeAccount') or row.get('category') or '').strip()
        if sku and category:
            categories[sku] = category

    CONVEX_CATEGORY_CACHE.update({
        'loaded_at': now,
        'categories': categories,
        'source': source,
    })
    return categories

def query_sales_by_sku(client, start_date, end_date, channel='all'):
    sku_expr = "UPPER(TRIM(COALESCE(NULLIF(v.variants_sku, ''), NULLIF(p.product_name, ''), NULLIF(p.sku, ''))))"
    query = f"""
        WITH bonsai AS (
          SELECT
            'bonsai' AS channel,
            {sku_expr} AS sku,
            ROUND(SUM(CAST(li.total_ex_tax AS FLOAT64)), 2) AS revenue,
            SUM(CAST(li.quantity AS FLOAT64)) AS units,
            ROUND(SUM(
              CASE
                WHEN CAST(li.base_cost_price AS FLOAT64) > 0 THEN CAST(li.base_cost_price AS FLOAT64) * li.quantity
                WHEN c.cost_per_unit IS NOT NULL THEN CAST(c.cost_per_unit AS FLOAT64) * li.quantity
                ELSE 0
              END
            ), 2) AS cogs
          FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items` li
          JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_order` o ON li.order_id = o.order_id
          LEFT JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product` p ON li.product_id = p.product_id
          LEFT JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product_variants` v
            ON v.variants_id = li.variant_id AND v.product_id = li.product_id
          LEFT JOIN `{PROJECT_ID}.{AMAZON_ECON_DATASET}.dim_sku_costs_us` c
            ON {sku_expr} = UPPER(TRIM(c.msku))
          WHERE DATE(o.order_created_date_time) BETWEEN @start_date AND @end_date
            AND o.order_status_id IN (2, 10, 11, 3)
          GROUP BY 1, 2
        ),
        amazon AS (
          SELECT
            'amazon' AS channel,
            UPPER(TRIM(a.msku)) AS sku,
            ROUND(SUM(CAST(a.gross_sales AS FLOAT64)), 2) AS revenue,
            SUM(CAST(a.units AS FLOAT64)) AS units,
            ROUND(SUM(
              CASE
                WHEN c.cost_per_unit IS NOT NULL THEN CAST(c.cost_per_unit AS FLOAT64) * a.units
                ELSE 0
              END
            ), 2) AS cogs
          FROM `{PROJECT_ID}.{AMAZON_ECON_DATASET}.fact_sku_day_us` a
          LEFT JOIN `{PROJECT_ID}.{AMAZON_ECON_DATASET}.dim_sku_costs_us` c
            ON UPPER(TRIM(a.msku)) = UPPER(TRIM(c.msku))
          WHERE a.business_date BETWEEN @start_date AND @end_date
          GROUP BY 1, 2
        ),
        wholesale AS (
          SELECT
            'wholesale' AS channel,
            {sku_expr} AS sku,
            ROUND(SUM(CAST(li.total_ex_tax AS FLOAT64)), 2) AS revenue,
            SUM(CAST(li.quantity AS FLOAT64)) AS units,
            ROUND(SUM(
              CASE
                WHEN c.cost_per_unit IS NOT NULL THEN CAST(c.cost_per_unit AS FLOAT64) * li.quantity
                WHEN CAST(li.base_cost_price AS FLOAT64) > 0 THEN CAST(li.base_cost_price AS FLOAT64) * li.quantity
                ELSE 0
              END
            ), 2) AS cogs
          FROM `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order_line_items` li
          JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order` o ON li.order_id = o.order_id
          LEFT JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_product` p ON li.product_id = p.product_id
          LEFT JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_product_variants` v
            ON v.variants_id = li.variant_id AND v.product_id = li.product_id
          LEFT JOIN `{PROJECT_ID}.{AMAZON_ECON_DATASET}.dim_sku_costs_us` c
            ON {sku_expr} = UPPER(TRIM(c.msku))
          WHERE DATE(o.order_created_date_time) BETWEEN @start_date AND @end_date
            AND o.order_status_id IN (2, 10)
          GROUP BY 1, 2
        ),
        combined AS (
          SELECT * FROM bonsai
          UNION ALL
          SELECT * FROM amazon
          UNION ALL
          SELECT * FROM wholesale
        )
        SELECT
          sku,
          ROUND(SUM(revenue), 2) AS revenue,
          SUM(units) AS units,
          ROUND(SUM(cogs), 2) AS cogs,
          COUNT(DISTINCT channel) AS channel_count
        FROM combined
        WHERE sku IS NOT NULL
          AND sku != ''
          AND (@channel = 'all' OR channel = @channel)
        GROUP BY 1
        ORDER BY revenue DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('start_date', 'DATE', start_date),
            bigquery.ScalarQueryParameter('end_date', 'DATE', end_date),
            bigquery.ScalarQueryParameter('channel', 'STRING', channel),
        ],
        maximum_bytes_billed=250_000_000,
    )
    return [dict(row) for row in client.query(query, job_config=job_config).result()]

def summarize_category_coverage(sku_rows, category_map):
    total_revenue = 0.0
    matched_revenue = 0.0
    matched_skus = 0
    unmapped_skus = 0
    for row in sku_rows:
        revenue = float(row.get('revenue') or 0)
        total_revenue += revenue
        if category_map.get(normalize_sku(row.get('sku'))):
            matched_revenue += revenue
            matched_skus += 1
        else:
            unmapped_skus += 1

    return {
        'total_skus': len(sku_rows),
        'matched_skus': matched_skus,
        'unmapped_skus': unmapped_skus,
        'total_revenue': round(total_revenue, 2),
        'matched_revenue': round(matched_revenue, 2),
        'unmapped_revenue': round(total_revenue - matched_revenue, 2),
        'matched_revenue_pct': round((matched_revenue / total_revenue * 100), 1) if total_revenue else 0,
    }

def aggregate_top_categories(current_rows, previous_rows, category_map, limit=8):
    category_totals = {}

    def ensure_category(category):
        return category_totals.setdefault(category, {
            'category': category,
            'revenue': 0.0,
            'units': 0.0,
            'cogs': 0.0,
            'previous_revenue': 0.0,
            'previous_units': 0.0,
            'previous_cogs': 0.0,
            'sku_count': 0,
            'previous_sku_count': 0,
        })

    for row in current_rows:
        category = category_map.get(normalize_sku(row.get('sku')))
        if not category:
            continue
        totals = ensure_category(category)
        totals['revenue'] += float(row.get('revenue') or 0)
        totals['units'] += float(row.get('units') or 0)
        totals['cogs'] += float(row.get('cogs') or 0)
        totals['sku_count'] += 1

    for row in previous_rows:
        category = category_map.get(normalize_sku(row.get('sku')))
        if not category:
            continue
        totals = ensure_category(category)
        totals['previous_revenue'] += float(row.get('revenue') or 0)
        totals['previous_units'] += float(row.get('units') or 0)
        totals['previous_cogs'] += float(row.get('cogs') or 0)
        totals['previous_sku_count'] += 1

    rows = []
    for totals in category_totals.values():
        revenue = totals['revenue']
        cogs = totals['cogs']
        previous_revenue = totals['previous_revenue']
        previous_cogs = totals['previous_cogs']
        profit = revenue - cogs
        previous_profit = previous_revenue - previous_cogs
        rows.append({
            **totals,
            'revenue': round(revenue, 2),
            'units': round(totals['units'], 2),
            'cogs': round(cogs, 2),
            'profit': round(profit, 2),
            'gm_pct': round((profit / revenue * 100), 1) if revenue else None,
            'previous_revenue': round(previous_revenue, 2),
            'previous_units': round(totals['previous_units'], 2),
            'previous_cogs': round(previous_cogs, 2),
            'previous_profit': round(previous_profit, 2),
            'delta': round(revenue - previous_revenue, 2),
            'delta_pct': round(((revenue - previous_revenue) / abs(previous_revenue) * 100), 1) if previous_revenue else None,
        })

    positive = [row for row in rows if row['delta'] > 0]
    negative = [row for row in rows if row['delta'] < 0]
    top_positive = max(positive, key=lambda row: row['delta'], default=None)
    top_negative = min(negative, key=lambda row: row['delta'], default=None)

    return {
        'rows': sorted(rows, key=lambda row: row['revenue'], reverse=True)[:limit],
        'top_positive': top_positive,
        'top_negative': top_negative,
    }

def make_daily_dashboard_query(weekly_query):
    """Convert the main dashboard query from display buckets to exact day buckets."""
    replacements = {
        "GREATEST(DATE_TRUNC(DATE(o.order_created_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(o.order_created_date_time), MONTH))": "DATE(o.order_created_date_time)",
        "GREATEST(DATE_TRUNC(DATE(order_created_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(order_created_date_time), MONTH))": "DATE(order_created_date_time)",
        "GREATEST(DATE_TRUNC(DATE(business_date), WEEK(MONDAY)), DATE_TRUNC(DATE(business_date), MONTH))": "DATE(business_date)",
        "GREATEST(DATE_TRUNC(DATE(report_date), WEEK(MONDAY)), DATE_TRUNC(DATE(report_date), MONTH))": "DATE(report_date)",
        "GREATEST(DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(MONDAY)), DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH))": "PARSE_DATE('%Y%m%d', event_date)",
        "GREATEST(DATE_TRUNC(hb.`date`, WEEK(MONDAY)), DATE_TRUNC(hb.`date`, MONTH))": "hb.`date`",
        "GREATEST(DATE_TRUNC(DATE(posted_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(posted_date_time), MONTH))": "DATE(posted_date_time)",
        "GREATEST(DATE_TRUNC(DATE(segments_date), WEEK(MONDAY)), DATE_TRUNC(DATE(segments_date), MONTH))": "DATE(segments_date)",
        "GREATEST(DATE_TRUNC(DATE(a.business_date), WEEK(MONDAY)), DATE_TRUNC(DATE(a.business_date), MONTH))": "DATE(a.business_date)",
    }

    daily_query = weekly_query
    for bucket_expr, day_expr in replacements.items():
        daily_query = daily_query.replace(bucket_expr, day_expr)

    return daily_query.replace("LIMIT 100", "LIMIT 800")

def query_top_sku(client, start_date, end_date, compare_start=None, compare_end=None):
    base_query = f"""
        SELECT
            p.sku AS sku,
            p.product_name AS product_name,
            SUM(li.quantity) AS units,
            ROUND(SUM(li.total_ex_tax), 2) AS revenue
        FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items` AS li
        JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_order` o ON li.order_id = o.order_id
        JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product` p ON li.product_id = p.product_id
        WHERE DATE(o.order_created_date_time) BETWEEN @start_date AND @end_date
          AND o.order_status_id IN (2, 10, 11, 3)
          AND p.sku IS NOT NULL AND p.sku != ''
          AND NOT (LOWER(p.sku) LIKE 'web%' OR LOWER(p.sku) LIKE 'tweb%')
        GROUP BY 1, 2
        ORDER BY revenue DESC
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('start_date', 'DATE', start_date),
            bigquery.ScalarQueryParameter('end_date', 'DATE', end_date)
        ]
    )
    current_row = next(iter(client.query(base_query, job_config=job_config).result()), None)
    if not current_row:
        return None

    result = {
        'sku': current_row.get('sku'),
        'name': current_row.get('product_name'),
        'current_revenue': float(current_row.get('revenue') or 0),
        'current_units': int(current_row.get('units') or 0),
        'previous_revenue': 0.0,
        'previous_units': 0
    }

    if compare_start and compare_end:
        compare_query = f"""
            SELECT
                SUM(li.quantity) AS units,
                ROUND(SUM(li.total_ex_tax), 2) AS revenue
            FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items` AS li
            JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_order` o ON li.order_id = o.order_id
            JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product` p ON li.product_id = p.product_id
            WHERE DATE(o.order_created_date_time) BETWEEN @start_date AND @end_date
              AND o.order_status_id IN (2, 10, 11, 3)
              AND p.sku = @sku
        """
        compare_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('start_date', 'DATE', compare_start),
                bigquery.ScalarQueryParameter('end_date', 'DATE', compare_end),
                bigquery.ScalarQueryParameter('sku', 'STRING', result['sku'])
            ]
        )
        compare_row = next(iter(client.query(compare_query, job_config=compare_config).result()), None)
        if compare_row:
            result['previous_revenue'] = float(compare_row.get('revenue') or 0)
            result['previous_units'] = int(compare_row.get('units') or 0)

    return result

@app.route('/api/dashboard', methods=['GET'])
def get_dashboard_data():
    """Fetch combined Amazon + Bonsai metrics for CEO dashboard"""
    try:
        client = get_bigquery_client()
        
        query = f"""
        -- CEO Dashboard: Combined Amazon + Bonsai Outlet Metrics
        WITH bonsai_customers AS (
          SELECT
            customer_id,
            MIN(order_created_date_time) as first_order_date
          FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order`
          WHERE order_status_id IN (2, 10, 11, 3)
          GROUP BY 1
        ),
        bonsai_weekly_types AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(o.order_created_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(o.order_created_date_time), MONTH)) as week_start,
            EXTRACT(YEAR FROM DATE(o.order_created_date_time)) as year,
            COUNT(DISTINCT IF(DATE(o.order_created_date_time) = DATE(c.first_order_date), o.customer_id, NULL)) as new_customers,
            COUNT(DISTINCT IF(DATE(o.order_created_date_time) > DATE(c.first_order_date), o.customer_id, NULL)) as returning_customers
          FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order` o
          JOIN bonsai_customers c ON o.customer_id = c.customer_id
          WHERE DATE(o.order_created_date_time) >= '2025-01-01'
            AND o.order_status_id IN (2, 10, 11, 3)
          GROUP BY 1, 2
        ),
        bonsai_weekly AS (
          SELECT
            -- Split weeks at month boundaries for accurate MTD/QTD sums
            GREATEST(DATE_TRUNC(DATE(order_created_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(order_created_date_time), MONTH)) as week_start,
            EXTRACT(YEAR FROM DATE(order_created_date_time)) as year,
            COUNT(DISTINCT order_id) as total_orders,
            ROUND(SUM(CAST(total_excluding_tax AS FLOAT64)), 2) as total_revenue,
            ROUND(AVG(CAST(total_excluding_tax AS FLOAT64)), 2) as avg_order_value,
            COUNT(DISTINCT customer_id) as unique_customers
          FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order`
          WHERE DATE(order_created_date_time) >= '2025-01-01'
            -- Include: 2 (Shipped), 10 (Completed), 11 (Awaiting Fulfillment), 3 (Partially Shipped)
            AND order_status_id IN (2, 10, 11, 3)
          GROUP BY week_start, year
        ),
        amazon_weekly AS (
          SELECT 
            -- Align daily data to the same Monday-start weeks as Bonsai
            GREATEST(DATE_TRUNC(DATE(business_date), WEEK(MONDAY)), DATE_TRUNC(DATE(business_date), MONTH)) as week_start,
            EXTRACT(YEAR FROM DATE(business_date)) as year,
            ROUND(SUM(CAST(gross_sales AS FLOAT64)), 2) as total_sales,
            SUM(units) as total_units,
            ROUND(SUM(CAST(net_proceeds AS FLOAT64)), 2) as net_proceeds,
            ROUND(SUM(CAST(ad_spend AS FLOAT64)), 2) as total_ad_spend
          FROM `{PROJECT_ID}.{AMAZON_ECON_DATASET}.fact_sku_day_us`
          WHERE business_date >= '2025-01-01'
          GROUP BY 1, 2
        ),
        amazon_traffic_weekly AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(report_date), WEEK(MONDAY)), DATE_TRUNC(DATE(report_date), MONTH)) as week_start,
            EXTRACT(YEAR FROM DATE(report_date)) as year,
            SUM(sessions) as sessions
          FROM `{PROJECT_ID}.{AMAZON_ECON_DATASET}.fact_business_reports_us`
          WHERE report_date >= '2025-01-01'
          GROUP BY 1, 2
        ),
        ga4_traffic AS (
          SELECT 
            week_start, 
            year, 
            SUM(sessions) as sessions, 
            SUM(users) as users,
            SUM(organic_sessions) as organic_sessions,
            SUM(organic_users) as organic_users,
            SUM(organic_revenue) as organic_revenue,
            SUM(organic_orders) as organic_orders
          FROM (
            -- Live BigQuery Export Data (where available)
            SELECT
              GREATEST(DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(MONDAY)), DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), MONTH)) as week_start,
              EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', event_date)) as year,
              COUNTIF(event_name = 'session_start') as sessions,
              COUNT(DISTINCT user_pseudo_id) as users,
              COUNTIF(event_name = 'session_start' AND (
                session_traffic_source_last_click.cross_channel_campaign.primary_channel_group IN ('Organic Search', 'Organic Social', 'Organic Video', 'Organic Shopping')
                OR LOWER(traffic_source.medium) = 'organic'
              )) as organic_sessions,
              COUNT(DISTINCT IF(
                session_traffic_source_last_click.cross_channel_campaign.primary_channel_group IN ('Organic Search', 'Organic Social', 'Organic Video', 'Organic Shopping')
                OR LOWER(traffic_source.medium) = 'organic',
                user_pseudo_id, NULL
              )) as organic_users,
              SUM(IF(
                event_name = 'purchase' AND (
                  session_traffic_source_last_click.cross_channel_campaign.primary_channel_group IN ('Organic Search', 'Organic Social', 'Organic Video', 'Organic Shopping')
                  OR LOWER(traffic_source.medium) = 'organic'
                ),
                CAST(ecommerce.purchase_revenue AS FLOAT64), 0
              )) as organic_revenue,
              COUNT(DISTINCT IF(
                event_name = 'purchase' AND (
                  session_traffic_source_last_click.cross_channel_campaign.primary_channel_group IN ('Organic Search', 'Organic Social', 'Organic Video', 'Organic Shopping')
                  OR LOWER(traffic_source.medium) = 'organic'
                ),
                ecommerce.transaction_id, NULL
              )) as organic_orders
            FROM `{PROJECT_ID}.{GA4_DATASET}.events_*`
            WHERE _TABLE_SUFFIX >= '20250430'
            GROUP BY 1, 2

            UNION ALL

            -- Backfilled Historical Data (Full year 2025)
            -- We only use backfill data for dates where the BQ export is missing or incomplete
            SELECT
              GREATEST(DATE_TRUNC(hb.`date`, WEEK(MONDAY)), DATE_TRUNC(hb.`date`, MONTH)) as week_start,
              EXTRACT(YEAR FROM hb.`date`) as year,
              SUM(hb.sessions) as sessions,
              SUM(hb.users) as users,
              0 as organic_sessions,
              0 as organic_users,
              0 as organic_revenue,
              0 as organic_orders
            FROM `{PROJECT_ID}.{GA4_DATASET}.ga4_historical_summary` AS hb
            GROUP BY 1, 2
          )
          GROUP BY 1, 2
        ),
        amazon_orders_weekly AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(posted_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(posted_date_time), MONTH)) as week_start,
            EXTRACT(YEAR FROM DATE(posted_date_time)) as year,
            COUNT(DISTINCT order_id) as total_orders
          FROM `{PROJECT_ID}.{AMAZON_ECON_DATASET}.fact_settlements_us`
          WHERE DATE(posted_date_time) >= '2025-01-01'
          GROUP BY 1, 2
        ),
        wholesale_weekly AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(order_created_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(order_created_date_time), MONTH)) as week_start,
            EXTRACT(YEAR FROM DATE(order_created_date_time)) as year,
            COUNT(DISTINCT order_id) as total_orders,
            ROUND(SUM(IF(order_status_id IN (2, 10), CAST(sub_total_excluding_tax AS FLOAT64), 0)), 2) as total_revenue,
            ROUND(SUM(IF(order_status_id IN (8, 11), CAST(sub_total_excluding_tax AS FLOAT64), 0)), 2) as future_revenue
          FROM `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order`
          WHERE DATE(order_created_date_time) >= '2025-01-01'
            AND order_status_id NOT IN (0, 5, 6)
          GROUP BY 1, 2
        ),
        google_ads_weekly AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(segments_date), WEEK(MONDAY)), DATE_TRUNC(DATE(segments_date), MONTH)) as week_start,
            EXTRACT(YEAR FROM DATE(segments_date)) as year,
            ROUND(SUM(CAST(metrics_cost_micros AS FLOAT64)) / 1000000, 2) as total_ad_spend
          FROM `{PROJECT_ID}.{GOOGLE_ADS_DATASET}.p_ads_CampaignStats_*`
          WHERE segments_date >= '2025-01-01'
          GROUP BY 1, 2
        ),
        -- COGS: Bonsai channel - use base_cost_price from BigCommerce line items
        -- Falls back to dim_sku_costs_us lookup when base_cost_price is 0
        bonsai_cogs_weekly AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(o.order_created_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(o.order_created_date_time), MONTH)) as week_start,
            EXTRACT(YEAR FROM DATE(o.order_created_date_time)) as year,
            ROUND(SUM(
              CASE
                WHEN CAST(li.base_cost_price AS FLOAT64) > 0 THEN CAST(li.base_cost_price AS FLOAT64) * li.quantity
                WHEN c.cost_per_unit IS NOT NULL THEN CAST(c.cost_per_unit AS FLOAT64) * li.quantity
                ELSE 0
              END
            ), 2) as total_cogs
          FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items` li
          JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_order` o ON li.order_id = o.order_id
          JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product` p ON li.product_id = p.product_id
          LEFT JOIN `{PROJECT_ID}.{AMAZON_ECON_DATASET}.dim_sku_costs_us` c ON UPPER(p.sku) = UPPER(c.msku)
          WHERE DATE(o.order_created_date_time) >= '2025-01-01'
            AND o.order_status_id IN (2, 10, 11, 3)
          GROUP BY 1, 2
        ),
        -- COGS: Amazon channel - join daily SKU data with cost lookup
        amazon_cogs_weekly AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(a.business_date), WEEK(MONDAY)), DATE_TRUNC(DATE(a.business_date), MONTH)) as week_start,
            EXTRACT(YEAR FROM DATE(a.business_date)) as year,
            ROUND(SUM(
              CASE
                WHEN c.cost_per_unit IS NOT NULL THEN CAST(c.cost_per_unit AS FLOAT64) * a.units
                ELSE 0
              END
            ), 2) as total_cogs
          FROM `{PROJECT_ID}.{AMAZON_ECON_DATASET}.fact_sku_day_us` a
          LEFT JOIN `{PROJECT_ID}.{AMAZON_ECON_DATASET}.dim_sku_costs_us` c ON UPPER(a.msku) = UPPER(c.msku)
          WHERE a.business_date >= '2025-01-01'
          GROUP BY 1, 2
        ),
        -- COGS: Wholesale channel - use BigCommerce line items from wholesale dataset
        wholesale_cogs_weekly AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(o.order_created_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(o.order_created_date_time), MONTH)) as week_start,
            EXTRACT(YEAR FROM DATE(o.order_created_date_time)) as year,
            ROUND(SUM(
              CASE
                WHEN c.cost_per_unit IS NOT NULL THEN CAST(c.cost_per_unit AS FLOAT64) * li.quantity
                WHEN CAST(li.base_cost_price AS FLOAT64) > 0 THEN CAST(li.base_cost_price AS FLOAT64) * li.quantity
                ELSE 0
              END
            ), 2) as total_cogs
          FROM `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order_line_items` li
          JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order` o ON li.order_id = o.order_id
          LEFT JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_product` p ON li.product_id = p.product_id
          LEFT JOIN `{PROJECT_ID}.{AMAZON_ECON_DATASET}.dim_sku_costs_us` c ON UPPER(p.sku) = UPPER(c.msku)
          WHERE DATE(o.order_created_date_time) >= '2025-01-01'
            AND o.order_status_id NOT IN (0, 5, 6)
          GROUP BY 1, 2
        ),
        weeks AS (
          SELECT week_start, year FROM bonsai_weekly
          UNION DISTINCT
          SELECT week_start, year FROM amazon_weekly
          UNION DISTINCT
          SELECT week_start, year FROM amazon_traffic_weekly
          UNION DISTINCT
          SELECT week_start, year FROM ga4_traffic
          UNION DISTINCT
          SELECT week_start, year FROM amazon_orders_weekly
          UNION DISTINCT
          SELECT week_start, year FROM wholesale_weekly
          UNION DISTINCT
          SELECT week_start, year FROM google_ads_weekly
          UNION DISTINCT
          SELECT week_start, year FROM bonsai_cogs_weekly
          UNION DISTINCT
          SELECT week_start, year FROM amazon_cogs_weekly
          UNION DISTINCT
          SELECT week_start, year FROM wholesale_cogs_weekly
        )
        SELECT 
          FORMAT_DATE('%Y-%m-%d', w.week_start) as week_start,
          w.year,
          COALESCE(b.total_orders, 0) as bonsai_orders,
          COALESCE(b.total_revenue, 0) as bonsai_revenue,
          COALESCE(b.avg_order_value, 0) as bonsai_aov,
          COALESCE(b.unique_customers, 0) as bonsai_customers,
          COALESCE(bt.new_customers, 0) as bonsai_new_customers,
          COALESCE(bt.returning_customers, 0) as bonsai_returning_customers,
          COALESCE(g.sessions, 0) as bonsai_sessions,
          COALESCE(g.users, 0) as bonsai_users,
          COALESCE(g.organic_sessions, 0) as organic_sessions,
          COALESCE(g.organic_users, 0) as organic_users,
          COALESCE(g.organic_revenue, 0) as organic_revenue,
          COALESCE(g.organic_orders, 0) as organic_orders,
          ROUND(SAFE_DIVIDE(COALESCE(b.total_orders, 0), COALESCE(g.sessions, 0)) * 100, 2) as bonsai_cvr,
          COALESCE(a.total_units, 0) as amazon_units,
          COALESCE(a.total_sales, 0) as amazon_revenue,
          COALESCE(ao.total_orders, 0) as amazon_orders,
          COALESCE(a.net_proceeds, 0) as amazon_net_proceeds,
          COALESCE(t.sessions, 0) as amazon_sessions,
          ROUND(SAFE_DIVIDE(COALESCE(a.total_units, 0), COALESCE(t.sessions, 0)) * 100, 2) as amazon_cvr,
          ROUND((COALESCE(a.net_proceeds, 0) / NULLIF(COALESCE(a.total_sales, 0), 0)) * 100, 2) as amazon_margin_pct,
          COALESCE(wh.total_orders, 0) as wholesale_orders,
          COALESCE(wh.total_revenue, 0) as wholesale_revenue,
          COALESCE(wh.future_revenue, 0) as wholesale_future_revenue,
          ROUND(SAFE_DIVIDE(COALESCE(CAST(wh.total_revenue AS FLOAT64)), NULLIF(COALESCE(wh.total_orders, 0), 0)), 2) as wholesale_aov,
          COALESCE(a.total_ad_spend, 0) as amazon_ad_spend,
          COALESCE(gads.total_ad_spend, 0) as google_ad_spend,
          ROUND(COALESCE(a.total_ad_spend, 0) + COALESCE(gads.total_ad_spend, 0), 2) as total_ad_spend,
          ROUND(COALESCE(b.total_revenue, 0) + COALESCE(a.total_sales, 0) + COALESCE(wh.total_revenue, 0), 2) as total_company_revenue,
          COALESCE(bc.total_cogs, 0) as bonsai_cogs,
          COALESCE(ac.total_cogs, 0) as amazon_cogs,
          COALESCE(wc.total_cogs, 0) as wholesale_cogs,
          ROUND(COALESCE(bc.total_cogs, 0) + COALESCE(ac.total_cogs, 0) + COALESCE(wc.total_cogs, 0), 2) as total_cogs,
          ROUND(
            (COALESCE(b.total_revenue, 0) - COALESCE(bc.total_cogs, 0)) +
            (COALESCE(a.net_proceeds, 0) - COALESCE(ac.total_cogs, 0)) +
            (COALESCE(wh.total_revenue, 0) - COALESCE(wc.total_cogs, 0)),
          2) as estimated_company_profit
        FROM weeks w
        LEFT JOIN bonsai_weekly b ON w.week_start = b.week_start AND w.year = b.year
        LEFT JOIN bonsai_weekly_types bt ON w.week_start = bt.week_start AND w.year = bt.year
        LEFT JOIN amazon_weekly a ON w.week_start = a.week_start AND w.year = a.year
        LEFT JOIN amazon_traffic_weekly t ON w.week_start = t.week_start AND w.year = t.year
        LEFT JOIN ga4_traffic g ON w.week_start = g.week_start AND w.year = g.year
        LEFT JOIN amazon_orders_weekly ao ON w.week_start = ao.week_start AND w.year = ao.year
        LEFT JOIN wholesale_weekly wh ON w.week_start = wh.week_start AND w.year = wh.year
        LEFT JOIN google_ads_weekly gads ON w.week_start = gads.week_start AND w.year = gads.year
        LEFT JOIN bonsai_cogs_weekly bc ON w.week_start = bc.week_start AND w.year = bc.year
        LEFT JOIN amazon_cogs_weekly ac ON w.week_start = ac.week_start AND w.year = ac.year
        LEFT JOIN wholesale_cogs_weekly wc ON w.week_start = wc.week_start AND w.year = wc.year
        ORDER BY w.week_start DESC
        LIMIT 100
        """
        
        daily_query = make_daily_dashboard_query(query)

        query_job = client.query(query)
        daily_query_job = client.query(daily_query)
        # BigQuery does not support multiple result sets in one query execution directly via the standard client.query() returning an iterator for each.
        # However, we can execute the CTEs.
        # Actually, best practice to get two different datasets is two queries or array agg.
        
        # Let's adjust the query to returning the main data, and then strict the customers separately or hack it.
        # Simpler approach: Run a separate query for top wholesale customers or use JSON_AGG.
        
        # Re-writing the main query execution to include a second query for wholesale customers
        # or separate them. Given the structure effectively, I will separate them to ensure clarity.
        
        # Execute Main Query
        results = query_job.result()
        daily_results = daily_query_job.result()
        
        # Fetch Top Wholesale Customers separately to keep it clean
        customer_query = f"""
        SELECT
            COALESCE(NULLIF(ba.company, ''), ba.full_name) as company_name,
            STRING_AGG(DISTINCT ba.full_name, ', ') as customer_name,
            COUNT(DISTINCT o.order_id) as total_orders,
            ROUND(SUM(IF(o.order_status_id IN (2, 10), CAST(o.sub_total_excluding_tax AS FLOAT64), 0)), 2) as total_revenue,
            ROUND(SUM(IF(o.order_status_id IN (8, 11), CAST(o.sub_total_excluding_tax AS FLOAT64), 0)), 2) as future_revenue
        FROM `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order` o
        LEFT JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order_billing_addresses` ba ON o.order_id = ba.order_id
        WHERE DATE(o.order_created_date_time) >= '2026-01-01'
            AND o.order_status_id NOT IN (0, 5, 6)
        GROUP BY 1
        ORDER BY (total_revenue + future_revenue) DESC
        LIMIT 20
        """
        customer_job = client.query(customer_query)
        customer_results = customer_job.result()
        
        # Convert to list of dicts for main data

        data = []
        for row in results:
            data.append(dict(row))

        daily_data = []
        for row in daily_results:
            daily_data.append(dict(row))
            
        wholesale_customers = []
        for row in customer_results:
            wholesale_customers.append(dict(row))
        
        return jsonify({
            'success': True,
            'data': data,
            'daily_data': daily_data,
            'wholesale_customers': wholesale_customers,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def query_top_skus_by_channel(client, start_date, end_date):
    """Query top 5 SKUs for Amazon and Bonsai."""
    # Bonsai Query
    bonsai_query = f"""
        SELECT
            p.product_id,
            p.sku,
            p.product_name,
            SUM(li.quantity) AS units,
            ROUND(SUM(li.total_ex_tax), 2) AS revenue
        FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items` AS li
        JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_order` o ON li.order_id = o.order_id
        JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product` p ON li.product_id = p.product_id
        WHERE DATE(o.order_created_date_time) BETWEEN @start_date AND @end_date
          AND o.order_status_id IN (2, 10, 11, 3)
          AND p.sku IS NOT NULL AND p.sku != ''
          AND NOT (LOWER(p.sku) LIKE 'web%' OR LOWER(p.sku) LIKE 'tweb%')
        GROUP BY 1, 2, 3
        ORDER BY revenue DESC
        LIMIT 5
    """
    
    # Amazon Query
    amazon_query = f"""
        SELECT
            msku AS sku,
            msku AS product_name, -- Use MSKU as name for now
            SUM(ordered_product_sales) AS revenue,
            SUM(units_ordered) AS units
        FROM `{PROJECT_ID}.{AMAZON_ECON_DATASET}.fact_business_reports_us`
        WHERE report_date BETWEEN @start_date AND @end_date
        GROUP BY 1, 2
        ORDER BY revenue DESC
        LIMIT 5
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('start_date', 'DATE', start_date),
            bigquery.ScalarQueryParameter('end_date', 'DATE', end_date)
        ]
    )

    bonsai_results = [dict(row) for row in client.query(bonsai_query, job_config=job_config).result()]
    amazon_results = [dict(row) for row in client.query(amazon_query, job_config=job_config).result()]

    return {
        'bonsai': bonsai_results,
        'amazon': amazon_results
    }

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/api/top-sku', methods=['GET'])
def get_top_sku():
    """Fetch top Bonsai SKU for a date range."""
    try:
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        compare_start = request.args.get('compare_start')
        compare_end = request.args.get('compare_end')

        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'error': 'start and end query parameters are required (YYYY-MM-DD).'
            }), 400

        client = get_bigquery_client()
        top_sku = query_top_sku(client, start_date, end_date, compare_start, compare_end)

        return jsonify({
            'success': True,
            'top_sku': top_sku,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/top-skus-channel', methods=['GET'])
def get_top_skus_channel():
    """Fetch top 5 SKUs per channel."""
    try:
        start_date = request.args.get('start')
        end_date = request.args.get('end')

        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'error': 'start and end query parameters are required (YYYY-MM-DD).'
            }), 400

        client = get_bigquery_client()
        skus = query_top_skus_by_channel(client, start_date, end_date)

        return jsonify({
            'success': True,
            'data': skus,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/top-categories', methods=['GET'])
def get_top_categories():
    """Fetch top income-account categories using Convex inventory as the category source."""
    try:
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        compare_start = request.args.get('compare_start')
        compare_end = request.args.get('compare_end')
        channel = request.args.get('channel', 'all')
        try:
            limit = min(max(int(request.args.get('limit', '8')), 1), 25)
        except ValueError:
            limit = 8

        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'error': 'start and end query parameters are required (YYYY-MM-DD).'
            }), 400

        if channel not in {'all', 'bonsai', 'amazon', 'wholesale', 'retail'}:
            return jsonify({
                'success': False,
                'error': 'channel must be one of: all, bonsai, amazon, wholesale, retail.'
            }), 400

        client = get_bigquery_client()
        category_map = get_convex_category_map()
        current_rows = query_sales_by_sku(client, start_date, end_date, channel)
        previous_rows = (
            query_sales_by_sku(client, compare_start, compare_end, channel)
            if compare_start and compare_end
            else []
        )
        categories = aggregate_top_categories(current_rows, previous_rows, category_map, limit=limit)

        return jsonify({
            'success': True,
            'data': categories['rows'],
            'top_positive': categories['top_positive'],
            'top_negative': categories['top_negative'],
            'coverage': summarize_category_coverage(current_rows, category_map),
            'category_source': CONVEX_CATEGORY_CACHE.get('source'),
            'channel': channel,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def query_sku_variations(client, product_id, start_date, end_date):
    """Query variation breakdown for a specific product."""
    query = f"""
        SELECT
            li.variant_id,
            COALESCE(v.variants_sku, 'No SKU') as sku,
            SUM(li.quantity) as units,
            ROUND(SUM(li.total_ex_tax), 2) as revenue
        FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items` li
        LEFT JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product_variants` v ON li.variant_id = v.variants_id
        WHERE li.product_id = @product_id
          AND EXISTS (
              SELECT 1 FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order` o 
              WHERE o.order_id = li.order_id 
                AND DATE(o.order_created_date_time) BETWEEN @start_date AND @end_date
                AND o.order_status_id IN (2, 10)
          )
        GROUP BY 1, 2
        ORDER BY revenue DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('product_id', 'INTEGER', product_id),
            bigquery.ScalarQueryParameter('start_date', 'DATE', start_date),
            bigquery.ScalarQueryParameter('end_date', 'DATE', end_date)
        ]
    )
    return [dict(row) for row in client.query(query, job_config=job_config).result()]

@app.route('/api/sku-variations', methods=['GET'])
def get_sku_variations():
    """Fetch variation breakdown for a specific product."""
    try:
        product_id = request.args.get('product_id')
        start_date = request.args.get('start')
        end_date = request.args.get('end')

        if not product_id or not start_date or not end_date:
            return jsonify({
                'success': False,
                'error': 'product_id, start, and end query parameters are required.'
            }), 400

        client = get_bigquery_client()
        variations = query_sku_variations(client, int(product_id), start_date, end_date)

        return jsonify({
            'success': True,
            'data': variations,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/pl', methods=['GET'])
def get_pl_data():
    """Monthly P&L data: trailing 13 months (T12M + same-month prior year for variance)."""
    try:
        client = get_bigquery_client()

        query = f"""
        -- P&L Dashboard: Monthly grain, 13-month lookback
        WITH months_spine AS (
          SELECT month FROM UNNEST(GENERATE_DATE_ARRAY(
            DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH), MONTH),
            DATE_TRUNC(CURRENT_DATE(), MONTH),
            INTERVAL 1 MONTH
          )) AS month
        ),
        amazon_monthly AS (
          SELECT
            DATE_TRUNC(business_date, MONTH) AS month,
            ROUND(SUM(CAST(gross_sales AS FLOAT64)), 2) AS gross_sales,
            ROUND(SUM(CAST(refunds AS FLOAT64)), 2) AS returns,
            ROUND(SUM(CAST(net_sales AS FLOAT64)), 2) AS amazon_net_sales,
            ROUND(SUM(CAST(amazon_fees AS FLOAT64)), 2) AS marketplace_fees,
            ROUND(SUM(CAST(ad_spend AS FLOAT64)), 2) AS amazon_ad_spend,
            ROUND(SUM(CAST(net_proceeds AS FLOAT64)), 2) AS net_proceeds,
            SUM(units) AS units
          FROM `{PROJECT_ID}.{AMAZON_ECON_DATASET}.fact_sku_day_us`
          WHERE business_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH), MONTH)
          GROUP BY 1
        ),
        amazon_cogs_monthly AS (
          SELECT
            DATE_TRUNC(DATE(a.business_date), MONTH) AS month,
            ROUND(SUM(
              CASE
                WHEN c.cost_per_unit IS NOT NULL THEN CAST(c.cost_per_unit AS FLOAT64) * a.units
                ELSE 0
              END
            ), 2) AS amazon_cogs
          FROM `{PROJECT_ID}.{AMAZON_ECON_DATASET}.fact_sku_day_us` a
          LEFT JOIN `{PROJECT_ID}.{AMAZON_ECON_DATASET}.dim_sku_costs_us` c ON UPPER(a.msku) = UPPER(c.msku)
          WHERE a.business_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH), MONTH)
          GROUP BY 1
        ),
        bonsai_monthly AS (
          SELECT
            DATE_TRUNC(DATE(order_created_date_time), MONTH) AS month,
            ROUND(SUM(CAST(total_excluding_tax AS FLOAT64)), 2) AS bonsai_revenue,
            COUNT(DISTINCT order_id) AS bonsai_orders
          FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order`
          WHERE DATE(order_created_date_time) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH), MONTH)
            AND order_status_id IN (2, 10, 11, 3)
          GROUP BY 1
        ),
        bonsai_cogs_monthly AS (
          SELECT
            DATE_TRUNC(DATE(o.order_created_date_time), MONTH) AS month,
            ROUND(SUM(
              CASE
                WHEN CAST(li.base_cost_price AS FLOAT64) > 0 THEN CAST(li.base_cost_price AS FLOAT64) * li.quantity
                WHEN c.cost_per_unit IS NOT NULL THEN CAST(c.cost_per_unit AS FLOAT64) * li.quantity
                ELSE 0
              END
            ), 2) AS bonsai_cogs
          FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order_line_items` li
          JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_order` o ON li.order_id = o.order_id
          JOIN `{PROJECT_ID}.{SALES_DATASET}.bc_product` p ON li.product_id = p.product_id
          LEFT JOIN `{PROJECT_ID}.{AMAZON_ECON_DATASET}.dim_sku_costs_us` c ON UPPER(p.sku) = UPPER(c.msku)
          WHERE DATE(o.order_created_date_time) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH), MONTH)
            AND o.order_status_id IN (2, 10, 11, 3)
          GROUP BY 1
        ),
        wholesale_monthly AS (
          SELECT
            DATE_TRUNC(DATE(order_created_date_time), MONTH) AS month,
            ROUND(SUM(IF(order_status_id IN (2, 10), CAST(sub_total_excluding_tax AS FLOAT64), 0)), 2) AS wholesale_revenue
          FROM `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order`
          WHERE DATE(order_created_date_time) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH), MONTH)
            AND order_status_id NOT IN (0, 5, 6)
          GROUP BY 1
        ),
        wholesale_cogs_monthly AS (
          SELECT
            DATE_TRUNC(DATE(o.order_created_date_time), MONTH) AS month,
            ROUND(SUM(
              CASE
                WHEN c.cost_per_unit IS NOT NULL THEN CAST(c.cost_per_unit AS FLOAT64) * li.quantity
                WHEN CAST(li.base_cost_price AS FLOAT64) > 0 THEN CAST(li.base_cost_price AS FLOAT64) * li.quantity
                ELSE 0
              END
            ), 2) AS wholesale_cogs
          FROM `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order_line_items` li
          JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_order` o ON li.order_id = o.order_id
          LEFT JOIN `{PROJECT_ID}.{WHOLESALE_DATASET}.bc_product` p ON li.product_id = p.product_id
          LEFT JOIN `{PROJECT_ID}.{AMAZON_ECON_DATASET}.dim_sku_costs_us` c ON UPPER(p.sku) = UPPER(c.msku)
          WHERE DATE(o.order_created_date_time) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH), MONTH)
            AND o.order_status_id NOT IN (0, 5, 6)
          GROUP BY 1
        ),
        google_ads_monthly AS (
          SELECT
            DATE_TRUNC(DATE(segments_date), MONTH) AS month,
            ROUND(SUM(CAST(metrics_cost_micros AS FLOAT64)) / 1000000, 2) AS google_ad_spend
          FROM `{PROJECT_ID}.{GOOGLE_ADS_DATASET}.p_ads_CampaignStats_*`
          WHERE segments_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH), MONTH)
          GROUP BY 1
        )
        SELECT
          FORMAT_DATE('%Y-%m', m.month) AS month,
          -- Revenue by channel
          COALESCE(a.gross_sales, 0)          AS amazon_gross_sales,
          COALESCE(a.returns, 0)              AS amazon_returns,
          COALESCE(b.bonsai_revenue, 0)       AS bonsai_revenue,
          COALESCE(wh.wholesale_revenue, 0)   AS wholesale_revenue,
          -- Combined top-line
          ROUND(COALESCE(a.gross_sales, 0) + COALESCE(b.bonsai_revenue, 0) + COALESCE(wh.wholesale_revenue, 0), 2) AS gross_sales,
          -- Returns (Amazon only; Bonsai/Wholesale not connected)
          COALESCE(a.returns, 0) AS returns,
          -- Net sales
          ROUND(COALESCE(a.gross_sales, 0) + COALESCE(a.returns, 0) + COALESCE(b.bonsai_revenue, 0) + COALESCE(wh.wholesale_revenue, 0), 2) AS net_sales,
          -- COGS by channel
          COALESCE(ac.amazon_cogs, 0)         AS amazon_cogs,
          COALESCE(bc.bonsai_cogs, 0)         AS bonsai_cogs,
          COALESCE(wc.wholesale_cogs, 0)      AS wholesale_cogs,
          ROUND(COALESCE(ac.amazon_cogs, 0) + COALESCE(bc.bonsai_cogs, 0) + COALESCE(wc.wholesale_cogs, 0), 2) AS cogs,
          -- Gross profit
          ROUND(
            (COALESCE(a.gross_sales, 0) + COALESCE(a.returns, 0) + COALESCE(b.bonsai_revenue, 0) + COALESCE(wh.wholesale_revenue, 0))
            - (COALESCE(ac.amazon_cogs, 0) + COALESCE(bc.bonsai_cogs, 0) + COALESCE(wc.wholesale_cogs, 0)),
          2) AS gross_profit,
          -- Variable costs (amazon_fees is typically negative in SP-API data)
          COALESCE(a.marketplace_fees, 0)     AS marketplace_fees,
          COALESCE(a.amazon_ad_spend, 0)      AS amazon_ad_spend,
          COALESCE(g.google_ad_spend, 0)      AS google_ad_spend,
          ROUND(COALESCE(a.amazon_ad_spend, 0) + COALESCE(g.google_ad_spend, 0), 2) AS ad_spend,
          -- Contribution profit (partial: excl. variable fulfillment, payment fees)
          ROUND(
            (COALESCE(a.gross_sales, 0) + COALESCE(a.returns, 0) + COALESCE(b.bonsai_revenue, 0) + COALESCE(wh.wholesale_revenue, 0))
            - (COALESCE(ac.amazon_cogs, 0) + COALESCE(bc.bonsai_cogs, 0) + COALESCE(wc.wholesale_cogs, 0))
            + COALESCE(a.marketplace_fees, 0)
            - (COALESCE(a.amazon_ad_spend, 0) + COALESCE(g.google_ad_spend, 0)),
          2) AS contribution_profit_partial,
          -- Amazon net proceeds (sanity check field)
          COALESCE(a.net_proceeds, 0)         AS amazon_net_proceeds,
          COALESCE(a.units, 0)                AS amazon_units
        FROM months_spine m
        LEFT JOIN amazon_monthly a       ON m.month = a.month
        LEFT JOIN amazon_cogs_monthly ac ON m.month = ac.month
        LEFT JOIN bonsai_monthly b       ON m.month = b.month
        LEFT JOIN bonsai_cogs_monthly bc ON m.month = bc.month
        LEFT JOIN wholesale_monthly wh   ON m.month = wh.month
        LEFT JOIN wholesale_cogs_monthly wc ON m.month = wc.month
        LEFT JOIN google_ads_monthly g   ON m.month = g.month
        ORDER BY m.month DESC
        """

        results = client.query(query).result()
        months = []
        for row in results:
            r = dict(row)
            net_sales = r.get('net_sales') or 0
            gross_profit = r.get('gross_profit') or 0
            contrib = r.get('contribution_profit_partial') or 0
            r['gross_margin_pct'] = round((gross_profit / net_sales * 100), 1) if net_sales else 0
            r['contribution_margin_pct'] = round((contrib / net_sales * 100), 1) if net_sales else 0
            months.append(r)

        return jsonify({
            'success': True,
            'months': months,
            'current_month': months[0] if months else None,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/')
def serve_dashboard():
    """Serve the main dashboard HTML"""
    return send_from_directory(PROJECT_ROOT / 'dashboard', 'index.html')

@app.route('/inventory')
def serve_inventory_dashboard():
    """Serve the inventory operations dashboard HTML"""
    return send_from_directory(PROJECT_ROOT / 'dashboard', 'inventory.html')

@app.route('/css/<path:filename>')
def serve_css(filename):
    """Serve CSS files"""
    return send_from_directory(PROJECT_ROOT / 'dashboard' / 'css', filename)

@app.route('/js/<path:filename>')
def serve_js(filename):
    """Serve JavaScript files"""
    return send_from_directory(PROJECT_ROOT / 'dashboard' / 'js', filename)

if __name__ == '__main__':
    import webbrowser
    import os
    from threading import Timer

    def open_browser():
        webbrowser.open_new('http://localhost:5000/')

    if not os.environ.get("WERKZEUG_RUN_MAIN"):
        Timer(1.5, open_browser).start()

    print("\n" + "="*60)
    print("Dashboard available at: http://localhost:5000")
    print("API endpoint: http://localhost:5000/api/dashboard")
    print("="*60 + "\n")
    app.run(debug=True, port=5000)
