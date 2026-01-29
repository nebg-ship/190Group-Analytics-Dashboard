"""
CEO Dashboard API - Data endpoint for fetching combined Amazon + Bonsai metrics
"""
import os
from flask import Flask, jsonify, send_from_directory
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

def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)

@app.route('/api/dashboard', methods=['GET'])
def get_dashboard_data():
    """Fetch combined Amazon + Bonsai metrics for CEO dashboard"""
    try:
        client = get_bigquery_client()
        
        query = """
        -- CEO Dashboard: Combined Amazon + Bonsai Outlet Metrics
        WITH bonsai_weekly AS (
          SELECT
            -- Split week at year boundary for accurate YTD sums
            GREATEST(DATE_TRUNC(DATE(order_created_date_time), WEEK(MONDAY)), DATE_TRUNC(DATE(order_created_date_time), YEAR)) as week_start,
            EXTRACT(YEAR FROM DATE(order_created_date_time)) as year,
            COUNT(DISTINCT order_id) as total_orders,
            ROUND(SUM(CAST(total_including_tax AS FLOAT64)), 2) as total_revenue,
            ROUND(AVG(CAST(total_including_tax AS FLOAT64)), 2) as avg_order_value,
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
            GREATEST(DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), WEEK(MONDAY)), DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), YEAR)) as week_start,
            EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', event_date)) as year,
            COUNTIF(event_name = 'session_start') as sessions,
            COUNT(DISTINCT user_pseudo_id) as users
          FROM `bonsai-outlet.analytics_250808038.events_*`
          WHERE _TABLE_SUFFIX >= '20250101'
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
        weeks AS (
          SELECT week_start, year FROM bonsai_weekly
          UNION DISTINCT
          SELECT week_start, year FROM amazon_weekly
          UNION DISTINCT
          SELECT week_start, year FROM amazon_traffic_weekly
          UNION DISTINCT
          SELECT week_start, year FROM ga4_traffic
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
          COALESCE(g.sessions, 0) as bonsai_sessions,
          COALESCE(g.users, 0) as bonsai_users,
          ROUND(SAFE_DIVIDE(COALESCE(b.total_orders, 0), COALESCE(g.sessions, 0)) * 100, 2) as bonsai_cvr,
          COALESCE(a.total_units, 0) as amazon_units,
          COALESCE(a.total_sales, 0) as amazon_revenue,
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
        LEFT JOIN amazon_weekly a ON w.week_start = a.week_start AND w.year = a.year
        LEFT JOIN amazon_traffic_weekly t ON w.week_start = t.week_start AND w.year = t.year
        LEFT JOIN ga4_traffic g ON w.week_start = g.week_start AND w.year = g.year
        LEFT JOIN wholesale_weekly wh ON w.week_start = wh.week_start AND w.year = wh.year
        ORDER BY w.week_start DESC
        LIMIT 100
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        # Convert to list of dicts
        data = []
        for row in results:
            data.append(dict(row))
        
        return jsonify({
            'success': True,
            'data': data,
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
