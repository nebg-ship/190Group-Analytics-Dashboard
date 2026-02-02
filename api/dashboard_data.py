"""
CEO Dashboard API - Data endpoint for fetching combined Amazon + Bonsai metrics
"""
import os
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pathlib import Path

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)

# Get project root
PROJECT_ROOT = Path(__file__).parent.parent

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
SALES_DATASET = 'sales'

# Hardwired BigCommerce line-item table + columns (per user confirmation)
LINE_ITEMS_TABLE = 'bc_order_product'
LINE_ITEMS_SKU_COL = 'sku'
LINE_ITEMS_NAME_COL = 'product_name'
LINE_ITEMS_QTY_COL = 'quantity'
LINE_ITEMS_ORDER_ID_COL = 'order_id'
LINE_ITEMS_TOTAL_COL = 'total_ex_tax'

def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)

def query_top_sku(client, start_date, end_date, compare_start=None, compare_end=None):
    table_name = LINE_ITEMS_TABLE
    sku_expr = f"CAST(li.`{LINE_ITEMS_SKU_COL}` AS STRING)"
    name_expr = f"CAST(li.`{LINE_ITEMS_NAME_COL}` AS STRING)"
    units_value_expr = f"COALESCE(SAFE_CAST(li.`{LINE_ITEMS_QTY_COL}` AS INT64), 0)"
    revenue_value_expr = f"COALESCE(SAFE_CAST(li.`{LINE_ITEMS_TOTAL_COL}` AS FLOAT64), 0)"

    base_query = f"""
        WITH orders AS (
            SELECT order_id
            FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order`
            WHERE DATE(order_created_date_time) BETWEEN @start_date AND @end_date
              AND order_status_id NOT IN (0, 3, 5, 6)
        )
        SELECT
            {sku_expr} AS sku,
            {name_expr} AS product_name,
            SUM({units_value_expr}) AS units,
            ROUND(SUM({revenue_value_expr}), 2) AS revenue
        FROM `{PROJECT_ID}.{SALES_DATASET}.{table_name}` AS li
        JOIN orders o ON li.`{LINE_ITEMS_ORDER_ID_COL}` = o.order_id
        WHERE {sku_expr} IS NOT NULL AND {sku_expr} != ''
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
            WITH orders AS (
                SELECT order_id
                FROM `{PROJECT_ID}.{SALES_DATASET}.bc_order`
                WHERE DATE(order_created_date_time) BETWEEN @start_date AND @end_date
                  AND order_status_id NOT IN (0, 3, 5, 6)
            )
            SELECT
                SUM({units_value_expr}) AS units,
                ROUND(SUM({revenue_value_expr}), 2) AS revenue
            FROM `{PROJECT_ID}.{SALES_DATASET}.{table_name}` AS li
            JOIN orders o ON li.`{LINE_ITEMS_ORDER_ID_COL}` = o.order_id
            WHERE {sku_expr} = @sku
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
        
        query = """
        -- CEO Dashboard: Combined Amazon + Bonsai Outlet Metrics
        WITH bonsai_customers AS (
          SELECT
            customer_id,
            MIN(order_created_date_time) as first_order_date
          FROM `bonsai-outlet.sales.bc_order`
          WHERE order_status_id NOT IN (0, 3, 5, 6)
          GROUP BY 1
        ),
        bonsai_weekly_types AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(o.order_created_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(o.order_created_date_time), YEAR)) as week_start,
            EXTRACT(YEAR FROM DATE(o.order_created_date_time)) as year,
            COUNT(DISTINCT IF(DATE(o.order_created_date_time) = DATE(c.first_order_date), o.customer_id, NULL)) as new_customers,
            COUNT(DISTINCT IF(DATE(o.order_created_date_time) > DATE(c.first_order_date), o.customer_id, NULL)) as returning_customers
          FROM `bonsai-outlet.sales.bc_order` o
          JOIN bonsai_customers c ON o.customer_id = c.customer_id
          WHERE DATE(o.order_created_date_time) >= '2025-01-01'
            AND o.order_status_id NOT IN (0, 3, 5, 6)
          GROUP BY 1, 2
        ),
        bonsai_weekly AS (
          SELECT
            -- Split week at year boundary for accurate YTD sums
            GREATEST(DATE_TRUNC(DATE(order_created_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(order_created_date_time), YEAR)) as week_start,
            EXTRACT(YEAR FROM DATE(order_created_date_time)) as year,
            COUNT(DISTINCT order_id) as total_orders,
            ROUND(SUM(CAST(sub_total_excluding_tax AS FLOAT64)), 2) as total_revenue,
            ROUND(AVG(CAST(sub_total_excluding_tax AS FLOAT64)), 2) as avg_order_value,
            COUNT(DISTINCT customer_id) as unique_customers
          FROM `bonsai-outlet.sales.bc_order`
          WHERE DATE(order_created_date_time) >= '2025-01-01'
            -- Exclude: 0 (Incomplete), 3 (Partially Shipped), 5 (Cancelled), 6 (Declined)
            AND order_status_id NOT IN (0, 3, 5, 6)
          GROUP BY week_start, year
        ),
        amazon_weekly AS (
          SELECT 
            -- Align daily data to the same Monday-start weeks as Bonsai
            GREATEST(DATE_TRUNC(DATE(business_date), WEEK(MONDAY)), DATE_TRUNC(DATE(business_date), YEAR)) as week_start,
            EXTRACT(YEAR FROM DATE(business_date)) as year,
            ROUND(SUM(CAST(gross_sales AS FLOAT64)), 2) as total_sales,
            SUM(units) as total_units,
            ROUND(SUM(CAST(net_proceeds AS FLOAT64)), 2) as net_proceeds
          FROM `bonsai-outlet.amazon_econ.fact_sku_day_us`
          WHERE business_date >= '2025-01-01'
          GROUP BY 1, 2
        ),
        amazon_traffic_weekly AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(report_date), WEEK(MONDAY)), DATE_TRUNC(DATE(report_date), YEAR)) as week_start,
            EXTRACT(YEAR FROM DATE(report_date)) as year,
            SUM(sessions) as sessions
          FROM `bonsai-outlet.amazon_econ.fact_business_reports_us`
          WHERE report_date >= '2025-01-01'
          GROUP BY 1, 2
        ),
        ga4_traffic AS (
          SELECT 
            week_start, 
            year, 
            SUM(sessions) as sessions, 
            SUM(users) as users
          FROM (
            -- Live BigQuery Export Data (where available)
            SELECT
              GREATEST(DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(MONDAY)), DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), YEAR)) as week_start,
              EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', event_date)) as year,
              COUNTIF(event_name = 'session_start') as sessions,
              COUNT(DISTINCT user_pseudo_id) as users
            FROM `bonsai-outlet.analytics_250808038.events_*`
            WHERE _TABLE_SUFFIX >= '20250430'
            GROUP BY 1, 2

            UNION ALL

            -- Backfilled Historical Data (Full year 2025)
            -- We only use backfill data for dates where the BQ export is missing or incomplete
            SELECT
              GREATEST(DATE_TRUNC(hb.`date`, WEEK(MONDAY)), DATE_TRUNC(hb.`date`, YEAR)) as week_start,
              EXTRACT(YEAR FROM hb.`date`) as year,
              SUM(hb.sessions) as sessions,
              SUM(hb.users) as users
            FROM `bonsai-outlet.analytics_250808038.ga4_historical_summary` AS hb
            GROUP BY 1, 2
          )
          GROUP BY 1, 2
        ),
        amazon_orders_weekly AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(posted_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(posted_date_time), YEAR)) as week_start,
            EXTRACT(YEAR FROM DATE(posted_date_time)) as year,
            COUNT(DISTINCT order_id) as total_orders
          FROM `bonsai-outlet.amazon_econ.fact_settlements_us`
          WHERE DATE(posted_date_time) >= '2025-01-01'
          GROUP BY 1, 2
        ),
        wholesale_weekly AS (
          SELECT
            GREATEST(DATE_TRUNC(DATE(COALESCE(order_date, DATE(ingested_at))), WEEK(MONDAY)), DATE_TRUNC(DATE(COALESCE(order_date, DATE(ingested_at))), YEAR)) as week_start,
            EXTRACT(YEAR FROM DATE(COALESCE(order_date, DATE(ingested_at)))) as year,
            COUNT(DISTINCT order_id) as total_orders,
            ROUND(SUM(CAST(grand_total AS FLOAT64)), 2) as total_revenue
          FROM (
            -- Deduplicate on the fly: take the max total and date for each order_id
            SELECT order_id, MAX(order_date) as order_date, MAX(grand_total) as grand_total, MAX(ingested_at) as ingested_at
            FROM `bonsai-outlet.wholesale.order_header`
            WHERE grand_total > 0
            GROUP BY order_id
          )
          WHERE DATE(COALESCE(order_date, DATE(ingested_at))) >= '2025-01-01'
          GROUP BY 1, 2
        ),
        wholesale_customers AS (
          SELECT
            COALESCE(NULLIF(company_name, ''), customer_name) as display_company,
            COUNT(DISTINCT order_id) as total_orders,
            ROUND(SUM(CAST(grand_total AS FLOAT64)), 2) as total_revenue
          FROM `bonsai-outlet.wholesale.order_header`
          WHERE DATE(COALESCE(order_date, DATE(ingested_at))) >= '2026-01-01'
            AND grand_total > 0
          GROUP BY 1
          ORDER BY 3 DESC
          LIMIT 20
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
          ROUND(SAFE_DIVIDE(COALESCE(CAST(wh.total_revenue AS FLOAT64), 0), NULLIF(COALESCE(wh.total_orders, 0), 0)), 2) as wholesale_aov,
          ROUND(COALESCE(b.total_revenue, 0) + COALESCE(a.total_sales, 0) + COALESCE(wh.total_revenue, 0), 2) as total_company_revenue,
          ROUND(COALESCE(b.total_revenue, 0) + COALESCE(a.net_proceeds, 0) + COALESCE(wh.total_revenue, 0), 2) as estimated_company_profit
        FROM weeks w
        LEFT JOIN bonsai_weekly b ON w.week_start = b.week_start AND w.year = b.year
        LEFT JOIN bonsai_weekly_types bt ON w.week_start = bt.week_start AND w.year = bt.year
        LEFT JOIN amazon_weekly a ON w.week_start = a.week_start AND w.year = a.year
        LEFT JOIN amazon_traffic_weekly t ON w.week_start = t.week_start AND w.year = t.year
        LEFT JOIN ga4_traffic g ON w.week_start = g.week_start AND w.year = g.year
        LEFT JOIN amazon_orders_weekly ao ON w.week_start = ao.week_start AND w.year = ao.year
        LEFT JOIN wholesale_weekly wh ON w.week_start = wh.week_start AND w.year = wh.year
        ORDER BY w.week_start DESC
        LIMIT 100
        """
        
        query_job = client.query(query)
        # BigQuery does not support multiple result sets in one query execution directly via the standard client.query() returning an iterator for each.
        # However, we can execute the CTEs.
        # Actually, best practice to get two different datasets is two queries or array agg.
        
        # Let's adjust the query to returning the main data, and then strict the customers separately or hack it.
        # Simpler approach: Run a separate query for top wholesale customers or use JSON_AGG.
        
        # Re-writing the main query execution to include a second query for wholesale customers
        # or separate them. Given the structure effectively, I will separate them to ensure clarity.
        
        # Execute Main Query
        results = query_job.result()
        
        # Fetch Top Wholesale Customers separately to keep it clean
        customer_query = """
        WITH deduplicated_orders AS (
            SELECT 
                order_id, 
                MAX(company_name) as company_name, 
                MAX(customer_name) as customer_name, 
                MAX(order_date) as order_date, 
                MAX(grand_total) as grand_total,
                MAX(ingested_at) as ingested_at
            FROM `bonsai-outlet.wholesale.order_header`
            WHERE grand_total > 0
            GROUP BY order_id
        ),
        consolidated_customers AS (
            SELECT
                COALESCE(NULLIF(company_name, ''), customer_name) as display_company,
                STRING_AGG(DISTINCT customer_name, ', ') as contact_names,
                COUNT(DISTINCT order_id) as total_orders,
                SUM(CAST(grand_total AS FLOAT64)) as total_revenue,
                MAX(DATE(COALESCE(order_date, DATE(ingested_at)))) as latest_order
            FROM deduplicated_orders
            WHERE DATE(COALESCE(order_date, DATE(ingested_at))) >= '2026-01-01'
            GROUP BY 1
        )
        SELECT
            display_company as company_name,
            contact_names as customer_name,
            total_orders,
            ROUND(total_revenue, 2) as total_revenue
        FROM consolidated_customers
        ORDER BY total_revenue DESC
        LIMIT 20
        """
        customer_job = client.query(customer_query)
        customer_results = customer_job.result()
        
        # Convert to list of dicts for main data

        data = []
        for row in results:
            data.append(dict(row))
            
        wholesale_customers = []
        for row in customer_results:
            wholesale_customers.append(dict(row))
        
        return jsonify({
            'success': True,
            'data': data,
            'wholesale_customers': wholesale_customers,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

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

@app.route('/')
def serve_dashboard():
    """Serve the main dashboard HTML"""
    return send_from_directory(PROJECT_ROOT / 'dashboard', 'index.html')

@app.route('/css/<path:filename>')
def serve_css(filename):
    """Serve CSS files"""
    return send_from_directory(PROJECT_ROOT / 'dashboard' / 'css', filename)

@app.route('/js/<path:filename>')
def serve_js(filename):
    """Serve JavaScript files"""
    return send_from_directory(PROJECT_ROOT / 'dashboard' / 'js', filename)

if __name__ == '__main__':
    print("\n" + "="*60)
    print("Dashboard available at: http://localhost:5000")
    print("API endpoint: http://localhost:5000/api/dashboard")
    print("="*60 + "\n")
    app.run(debug=True, port=5000)
