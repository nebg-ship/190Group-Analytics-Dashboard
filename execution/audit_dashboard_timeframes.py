"""
Audit dashboard date-window calculations against daily dashboard rows.

This script imports the Flask app directly, so it checks the current code without
requiring a running local server.
"""
import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "api"))

import dashboard_data  # noqa: E402


RANGE_ORDER = ["today", "last7", "last30", "mtd", "qtd", "ytd"]


def parse_date(value):
    return datetime.strptime(value, "%Y-%m-%d").date()


def money(value):
    return f"${value:,.2f}"


def range_bounds(range_name, as_of):
    if range_name == "today":
        return as_of, as_of
    if range_name == "last7":
        return as_of - timedelta(days=6), as_of
    if range_name == "last30":
        return as_of - timedelta(days=29), as_of
    if range_name == "mtd":
        return as_of.replace(day=1), as_of
    if range_name == "qtd":
        quarter_start_month = ((as_of.month - 1) // 3) * 3 + 1
        return date(as_of.year, quarter_start_month, 1), as_of
    if range_name == "ytd":
        return date(as_of.year, 1, 1), as_of
    raise ValueError(f"Unsupported range: {range_name}")


def start_of_week(value):
    return value - timedelta(days=value.weekday())


def filter_daily(rows, start, end, trim_current_week=False, as_of=None):
    if trim_current_week:
        cutoff = start_of_week(as_of or end)
        end = min(end, cutoff - timedelta(days=1))
    return [row for row in rows if start.isoformat() <= row["week_start"] <= end.isoformat()]


def sum_field(rows, field):
    return sum(float(row.get(field) or 0) for row in rows)


def fetch_dashboard_payload():
    with dashboard_data.app.app_context():
        response = dashboard_data.get_dashboard_data()
        if isinstance(response, tuple):
            flask_response, status_code = response
        else:
            flask_response, status_code = response, response.status_code

        payload = flask_response.get_json()
        if status_code != 200 or not payload.get("success"):
            raise RuntimeError(payload.get("error") or f"Dashboard endpoint returned {status_code}")
        return payload


def fetch_amazon_source_totals(start, end):
    client = dashboard_data.get_bigquery_client()
    project = dashboard_data.PROJECT_ID
    dataset = dashboard_data.AMAZON_ECON_DATASET
    econ_table = f"{project}.{dataset}.fact_sku_day_us"
    reports_table = f"{project}.{dataset}.fact_business_reports_us"

    query = f"""
    WITH economics AS (
      SELECT
        ROUND(SUM(CAST(gross_sales AS FLOAT64)), 2) AS gross_sales,
        SUM(units) AS units
      FROM `{econ_table}`
      WHERE business_date BETWEEN @start_date AND @end_date
    ),
    business_reports AS (
      SELECT
        ROUND(SUM(CAST(ordered_product_sales AS FLOAT64)), 2) AS ordered_product_sales,
        SUM(units_ordered) AS units
      FROM `{reports_table}`
      WHERE report_date BETWEEN @start_date AND @end_date
    )
    SELECT
      economics.gross_sales AS economics_gross_sales,
      business_reports.ordered_product_sales AS business_report_sales,
      economics.units AS economics_units,
      business_reports.units AS business_report_units
    FROM economics, business_reports
    """
    job_config = dashboard_data.bigquery.QueryJobConfig(
        query_parameters=[
            dashboard_data.bigquery.ScalarQueryParameter("start_date", "DATE", start.isoformat()),
            dashboard_data.bigquery.ScalarQueryParameter("end_date", "DATE", end.isoformat()),
        ]
    )
    return dict(next(iter(client.query(query, job_config=job_config).result())))


def print_range_table(daily_rows, as_of):
    header = (
        "range        dates                    amazon gross     bonsai revenue   "
        "wholesale rev    total company"
    )
    print(header)
    print("-" * len(header))
    for range_name in RANGE_ORDER:
        start, end = range_bounds(range_name, as_of)
        trim = range_name in {"mtd", "qtd", "ytd"}
        rows = filter_daily(daily_rows, start, end, trim_current_week=trim, as_of=as_of)
        effective_end = rows[0]["week_start"] if rows else "no data"
        effective_start = rows[-1]["week_start"] if rows else "no data"
        print(
            f"{range_name:<12} {effective_start}..{effective_end}  "
            f"{money(sum_field(rows, 'amazon_revenue')):>14}  "
            f"{money(sum_field(rows, 'bonsai_revenue')):>15}  "
            f"{money(sum_field(rows, 'wholesale_revenue')):>14}  "
            f"{money(sum_field(rows, 'total_company_revenue')):>15}"
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", default=date.today().isoformat(), help="YYYY-MM-DD, default today")
    args = parser.parse_args()
    as_of = parse_date(args.as_of)

    payload = fetch_dashboard_payload()
    weekly_rows = payload.get("data") or []
    daily_rows = payload.get("daily_data") or []

    print(f"Dashboard payload: {len(weekly_rows)} weekly rows, {len(daily_rows)} daily rows")
    print(f"As of: {as_of.isoformat()}")
    print()

    print_range_table(daily_rows, as_of)
    print()

    old_last30 = weekly_rows[:4]
    old_start = old_last30[-1]["week_start"] if old_last30 else "no data"
    old_end = old_last30[0]["week_start"] if old_last30 else "no data"
    print("Old last30 bucket-slice check:")
    print(
        f"  first 4 weekly buckets {old_start}..{old_end}: "
        f"{money(sum_field(old_last30, 'amazon_revenue'))} Amazon gross"
    )

    start, end = range_bounds("last30", as_of)
    source = fetch_amazon_source_totals(start, end)
    print()
    print(f"Amazon source comparison for exact last30 ({start}..{end}):")
    print(f"  Economics gross sales:  {money(float(source.get('economics_gross_sales') or 0))}")
    print(f"  Business report sales: {money(float(source.get('business_report_sales') or 0))}")
    print(f"  Economics units:       {int(source.get('economics_units') or 0):,}")
    print(f"  Business report units: {int(source.get('business_report_units') or 0):,}")

    amazon_nonzero = [row for row in daily_rows if float(row.get("amazon_revenue") or 0) > 0]
    if amazon_nonzero:
        print()
        print(f"Latest non-zero Amazon economics date: {amazon_nonzero[0]['week_start']}")


if __name__ == "__main__":
    main()
