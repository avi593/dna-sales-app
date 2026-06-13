"""
דנא ציוד אינסטלציה — Backend מודיעין עסקי
Google Cloud Function שמושך נתונים מ-Google Analytics 4 (ובהמשך Google Ads)
ומחזיר אותם לאפליקציית הדשבורד.

Entry point: analytics
Runtime: Python 3.12
משתני סביבה נדרשים:
  GA4_PROPERTY_ID  — מזהה ה-Property של GA4 (מספר בלבד, לדוגמה 123456789)
  ALLOWED_ORIGIN   — אופציונלי. דומיין שמותר לו לקרוא (ברירת מחדל: *)
"""

import os
import functions_framework
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    OrderBy,
    RunReportRequest,
)

PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")

CORS = {
    "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _int(metric_values, i):
    try:
        return int(float(metric_values[i].value or 0))
    except Exception:
        return 0


def _float(metric_values, i):
    try:
        return round(float(metric_values[i].value or 0), 2)
    except Exception:
        return 0.0


@functions_framework.http
def analytics(request):
    # CORS preflight
    if request.method == "OPTIONS":
        return ("", 204, CORS)

    if not PROPERTY_ID:
        return ({"error": "GA4_PROPERTY_ID לא הוגדר במשתני הסביבה"}, 500, CORS)

    prop = f"properties/{PROPERTY_ID}"
    client = BetaAnalyticsDataClient()
    last_28 = [DateRange(start_date="28daysAgo", end_date="today")]
    result = {"range": "28 ימים אחרונים"}

    # ── סיכום KPIs ──
    try:
        r = client.run_report(RunReportRequest(
            property=prop,
            date_ranges=last_28,
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="sessions"),
                Metric(name="screenPageViews"),
                Metric(name="conversions"),
                Metric(name="totalRevenue"),
            ],
        ))
        mv = r.rows[0].metric_values if r.rows else []
        result["summary"] = {
            "users": _int(mv, 0),
            "sessions": _int(mv, 1),
            "pageviews": _int(mv, 2),
            "conversions": _int(mv, 3),
            "revenue": _float(mv, 4),
        }
    except Exception as e:
        result["summary"] = {"error": str(e)}

    # ── מקורות תנועה (ערוצים) ──
    try:
        r = client.run_report(RunReportRequest(
            property=prop,
            date_ranges=last_28,
            dimensions=[Dimension(name="sessionDefaultChannelGroup")],
            metrics=[Metric(name="sessions"), Metric(name="activeUsers")],
            order_bys=[OrderBy(
                metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True
            )],
            limit=8,
        ))
        result["channels"] = [
            {
                "name": row.dimension_values[0].value,
                "sessions": _int(row.metric_values, 0),
                "users": _int(row.metric_values, 1),
            }
            for row in r.rows
        ]
    except Exception as e:
        result["channels"] = {"error": str(e)}

    # ── עמודים מובילים ──
    try:
        r = client.run_report(RunReportRequest(
            property=prop,
            date_ranges=last_28,
            dimensions=[Dimension(name="pagePath")],
            metrics=[Metric(name="screenPageViews")],
            order_bys=[OrderBy(
                metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True
            )],
            limit=10,
        ))
        result["topPages"] = [
            {
                "path": row.dimension_values[0].value,
                "views": _int(row.metric_values, 0),
            }
            for row in r.rows
        ]
    except Exception as e:
        result["topPages"] = {"error": str(e)}

    # ── מגמה יומית (משתמשים) ──
    try:
        r = client.run_report(RunReportRequest(
            property=prop,
            date_ranges=last_28,
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="activeUsers")],
            order_bys=[OrderBy(
                dimension=OrderBy.DimensionOrderBy(dimension_name="date")
            )],
        ))
        result["daily"] = [
            {
                "date": row.dimension_values[0].value,
                "users": _int(row.metric_values, 0),
            }
            for row in r.rows
        ]
    except Exception as e:
        result["daily"] = {"error": str(e)}

    return (result, 200, CORS)
