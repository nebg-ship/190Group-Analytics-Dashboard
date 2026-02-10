
import os
import datetime
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

PROPERTY_ID = "250808038"
SERVICE_ACCOUNT_FILE = "service-account.json"

def get_client():
    return BetaAnalyticsDataClient()

def test_ga4():
    client = get_client()
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="sessions")],
        date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
    )
    response = client.run_report(request)
    print("GA4 Test Success!")
    for row in response.rows:
        print(f"Date: {row.dimension_values[0].value}, Sessions: {row.metric_values[0].value}")

if __name__ == "__main__":
    try:
        test_ga4()
    except Exception as e:
        print(f"GA4 Test Failed: {e}")
