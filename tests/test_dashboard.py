"""
test_dashboard.py – Modular page, API & Prometheus /metrics tests using FastAPI TestClient.
Mocks DB queries and Spark progress file so tests run without Postgres or Spark.
"""
from datetime import datetime, timezone
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.dashboard.app import app

client = TestClient(app)


def test_pages_serve_separate_html_files():
    overview = client.get("/")
    assert overview.status_code == 200
    assert "StayStream Live" in overview.text
    assert "/static/js/overview.js" in overview.text

    cities = client.get("/cities")
    assert cities.status_code == 200
    assert "City Analytics" in cities.text
    assert "/static/js/cities.js" in cities.text

    dlq = client.get("/dlq")
    assert dlq.status_code == 200
    assert "DLQ Inspector" in dlq.text
    assert "/static/js/dlq.js" in dlq.text

    js = client.get("/static/js/common.js")
    assert js.status_code == 200
    assert "updateHealthBanner" in js.text


@patch("src.dashboard.routes.api.read_spark_progress")
@patch("src.dashboard.routes.api.query_one")
def test_api_summary_and_prometheus_metrics(mock_one, mock_spark):
    mock_one.side_effect = [
        {
            "total_bookings": 250,
            "total_cancellations": 12,
            "total_revenue": 1250000.0,
            "avg_latency_ms": 42.5,
            "freshness_seconds": 3.2,
        },
        {"dlq_count": 5},
    ]
    mock_spark.return_value = {
        "num_input_rows": 40,
        "input_rows_per_second": 20.0,
        "processed_rows_per_second": 850.5,
        "batch_duration_ms": 310,
        "updated_at": "2026-11-27T15:00:00Z",
    }

    resp = client.get("/api/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_bookings"] == 250
    assert data["total_cancellations"] == 12
    assert data["dlq_count"] == 5
    assert data["spark_progress"]["processed_rows_per_second"] == 850.5

    mock_one.side_effect = [
        {
            "total_bookings": 250,
            "total_cancellations": 12,
            "total_revenue": 1250000.0,
            "avg_latency_ms": 42.5,
            "freshness_seconds": 3.2,
        },
        {"dlq_count": 5},
    ]
    m_resp = client.get("/metrics")
    assert m_resp.status_code == 200
    assert "staystream_bookings_total 250" in m_resp.text
    assert "staystream_spark_processed_rows_per_second 850.50" in m_resp.text


@patch("src.dashboard.routes.api.query_all")
def test_api_cities_and_timeseries(mock_all):
    mock_all.return_value = [
        {"city": "Goa", "bookings": 100, "cancellations": 5, "revenue": 500000.0, "avg_latency_ms": 35.0}
    ]
    c_resp = client.get("/api/cities")
    assert c_resp.status_code == 200
    assert c_resp.json()[0]["city"] == "Goa"

    ts = datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc)
    mock_all.return_value = [
        {"window_start": ts, "bookings": 20, "cancellations": 1, "revenue": 80000.0}
    ]
    t_resp = client.get("/api/timeseries")
    assert t_resp.status_code == 200
    assert t_resp.json()[0]["bookings"] == 20


@patch("src.dashboard.routes.api.query_all")
def test_api_dlq_with_filter(mock_all):
    ts = datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc)
    mock_all.side_effect = [
        [{"id": 1, "raw_value": "{bad", "reason": "invalid_json", "received_at": ts}],
        [{"reason": "invalid_json", "count": 1}],
    ]
    resp = client.get("/api/dlq?reason=invalid_json")
    assert resp.status_code == 200
    body = resp.json()
    assert body["items"][0]["reason"] == "invalid_json"
    assert body["breakdown"][0]["count"] == 1
