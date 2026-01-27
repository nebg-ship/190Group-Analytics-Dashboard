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
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(total_items) as total_items
          FROM `bonsai-outlet.sales.bc_order`
          WHERE DATE(order_created_date_time) >= '2025-01-01'
            -- Exclude: 0 (Incomplete), 3 (Partially Shipped), 5 (Cancelled), 6 (Declined)
            AND order_status_id NOT IN (0, 3, 5, 6)
          GROUP BY week_start, year
        ),
        amazon_weekly AS (
          SELECT 
            -- Align report_start_date (usually Wed/Thu) to the following Monday, but split at year start
            GREATEST(DATE_ADD(DATE_TRUNC(DATE(report_start_date), WEEK(MONDAY)), INTERVAL 7 DAY), DATE_TRUNC(DATE(report_start_date), YEAR)) as week_start,
            EXTRACT(YEAR FROM DATE(report_start_date)) as year,
            ROUND(SUM(COALESCE(sales_29_95, 0) + COALESCE(sales_59_92, 0) + COALESCE(sales_103_35, 0) + 
                COALESCE(sales_119_1, 0) + COALESCE(sales_138_32, 0) + COALESCE(sales_164_55, 0)), 2) as total_sales,
            SUM(COALESCE(units_sold_2, 0) + COALESCE(units_sold_4, 0) + COALESCE(units_sold_7, 0) + 
                COALESCE(units_sold_8, 0) + COALESCE(units_sold_9, 0) + COALESCE(units_sold_11, 0)) as total_units,
            ROUND(SUM(COALESCE(net_proceeds_total_6_64, 0) + COALESCE(net_proceeds_total_11_95, 0) + 
                COALESCE(net_proceeds_total_17_68, 0) + COALESCE(net_proceeds_total_28_12, 0) +
                COALESCE(net_proceeds_total_33_81, 0) + COALESCE(net_proceeds_total_49_74, 0)), 2) as net_proceeds
          FROM `bonsai-outlet.amazon_weekly_economics.weekly_sku_economics`
          GROUP BY 1, 2
        )
        SELECT 
          FORMAT_DATE('%Y-%m-%d', b.week_start) as week_start,
          b.year,
          b.total_orders as bonsai_orders,
          b.total_revenue as bonsai_revenue,
          b.avg_order_value as bonsai_aov,
          b.unique_customers as bonsai_customers,
          COALESCE(a.total_units, 0) as amazon_units,
          COALESCE(a.total_sales, 0) as amazon_revenue,
          COALESCE(a.net_proceeds, 0) as amazon_net_proceeds,
          ROUND((COALESCE(a.net_proceeds, 0) / NULLIF(COALESCE(a.total_sales, 0), 0)) * 100, 2) as amazon_margin_pct,
          ROUND(b.total_revenue + COALESCE(a.total_sales, 0), 2) as total_company_revenue,
          ROUND(b.total_revenue + COALESCE(a.net_proceeds, 0), 2) as estimated_company_profit
        FROM bonsai_weekly b
        LEFT JOIN amazon_weekly a ON b.week_start = a.week_start AND b.year = a.year
        ORDER BY b.week_start DESC
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
