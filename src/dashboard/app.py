"""
app.py – FastAPI real-time dashboard & Prometheus /metrics server for StayStream.

Run with:
    uvicorn src.dashboard.app:app --host 0.0.0.0 --port 8000 --reload
"""
import json
import os
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse

from src.common import config

app = FastAPI(title="StayStream Live Dashboard", version="1.0.0")

STATIC_INDEX = Path(__file__).parent / "static" / "index.html"


def _query_all(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    with psycopg2.connect(config.DB_DSN, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def _query_one(sql: str, params: tuple = ()) -> dict[str, Any]:
    rows = _query_all(sql, params)
    return rows[0] if rows else {}


def _read_spark_progress() -> dict[str, Any]:
    try:
        if os.path.exists(config.SPARK_METRICS_FILE):
            with open(config.SPARK_METRICS_FILE, encoding="utf-8") as f:
                return json.load(f)
    except (OSError, json.JSONDecodeError):
        pass
    return {
        "num_input_rows": 0,
        "input_rows_per_second": 0.0,
        "processed_rows_per_second": 0.0,
        "batch_duration_ms": 0,
        "updated_at": None,
    }


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(STATIC_INDEX.read_text(encoding="utf-8"))


@app.get("/api/summary")
def api_summary() -> dict[str, Any]:
    metrics = _query_one(
        """
        SELECT
            COALESCE(SUM(bookings), 0)::int             AS total_bookings,
            COALESCE(SUM(cancellations), 0)::int        AS total_cancellations,
            COALESCE(SUM(revenue), 0)::float            AS total_revenue,
            COALESCE(AVG(avg_latency_ms), 0)::float     AS avg_latency_ms,
            EXTRACT(EPOCH FROM (now() - MAX(updated_at)))::float AS freshness_seconds
        FROM booking_metrics_per_minute;
        """
    )
    dlq = _query_one("SELECT COUNT(*)::int AS dlq_count FROM booking_events_dlq;")
    return {
        **metrics,
        "dlq_count": dlq.get("dlq_count", 0),
        "spark_progress": _read_spark_progress(),
    }


@app.get("/api/cities")
def api_cities() -> list[dict[str, Any]]:
    return _query_all(
        """
        SELECT
            city,
            SUM(bookings)::int              AS bookings,
            SUM(cancellations)::int         AS cancellations,
            SUM(revenue)::float             AS revenue,
            ROUND(AVG(avg_latency_ms)::numeric, 1)::float AS avg_latency_ms
        FROM booking_metrics_per_minute
        GROUP BY city
        ORDER BY revenue DESC;
        """
    )


@app.get("/api/timeseries")
def api_timeseries(limit: int = 30) -> list[dict[str, Any]]:
    rows = _query_all(
        """
        SELECT
            window_start,
            SUM(bookings)::int      AS bookings,
            SUM(cancellations)::int AS cancellations,
            SUM(revenue)::float     AS revenue
        FROM booking_metrics_per_minute
        GROUP BY window_start
        ORDER BY window_start DESC
        LIMIT %s;
        """,
        (limit,),
    )
    rows.reverse()
    for r in rows:
        if r.get("window_start") is not None:
            r["window_start"] = r["window_start"].isoformat()
    return rows


@app.get("/api/dlq")
def api_dlq(limit: int = 25) -> dict[str, Any]:
    items = _query_all(
        """
        SELECT id, raw_value, reason, received_at
        FROM booking_events_dlq
        ORDER BY id DESC
        LIMIT %s;
        """,
        (limit,),
    )
    for item in items:
        if item.get("received_at") is not None:
            item["received_at"] = item["received_at"].isoformat()

    breakdown = _query_all(
        """
        SELECT reason, COUNT(*)::int AS count
        FROM booking_events_dlq
        GROUP BY reason
        ORDER BY count DESC;
        """
    )
    return {"items": items, "breakdown": breakdown}


@app.get("/metrics", response_class=PlainTextResponse)
def prometheus_metrics() -> PlainTextResponse:
    summary = api_summary()
    sp = summary["spark_progress"]
    lines = [
        "# HELP staystream_bookings_total Total valid bookings aggregated",
        "# TYPE staystream_bookings_total counter",
        f"staystream_bookings_total {summary.get('total_bookings', 0)}",
        "# HELP staystream_cancellations_total Total cancellations aggregated",
        "# TYPE staystream_cancellations_total counter",
        f"staystream_cancellations_total {summary.get('total_cancellations', 0)}",
        "# HELP staystream_revenue_total Net revenue aggregated",
        "# TYPE staystream_revenue_total gauge",
        f"staystream_revenue_total {summary.get('total_revenue', 0.0)}",
        "# HELP staystream_dlq_total Total malformed events routed to DLQ",
        "# TYPE staystream_dlq_total counter",
        f"staystream_dlq_total {summary.get('dlq_count', 0)}",
        "# HELP staystream_avg_latency_ms Average end-to-end event latency in ms",
        "# TYPE staystream_avg_latency_ms gauge",
        f"staystream_avg_latency_ms {summary.get('avg_latency_ms', 0.0):.2f}",
        "# HELP staystream_spark_input_rows_per_second Latest Spark input rate",
        "# TYPE staystream_spark_input_rows_per_second gauge",
        f"staystream_spark_input_rows_per_second {sp.get('input_rows_per_second', 0.0):.2f}",
        "# HELP staystream_spark_processed_rows_per_second Latest Spark processed rate",
        "# TYPE staystream_spark_processed_rows_per_second gauge",
        f"staystream_spark_processed_rows_per_second {sp.get('processed_rows_per_second', 0.0):.2f}",
        "# HELP staystream_spark_batch_duration_ms Latest Spark micro-batch duration in ms",
        "# TYPE staystream_spark_batch_duration_ms gauge",
        f"staystream_spark_batch_duration_ms {sp.get('batch_duration_ms', 0)}",
    ]
    return PlainTextResponse("\n".join(lines) + "\n")
