"""
routes/api.py – JSON API endpoints for the StayStream dashboard.
"""
from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Query

from src.dashboard.db import query_all, query_one, read_spark_progress

router = APIRouter(prefix="/api", tags=["api"])


@router.get("/summary")
def api_summary() -> dict[str, Any]:
    metrics = query_one(
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
    dlq = query_one("SELECT COUNT(*)::int AS dlq_count FROM booking_events_dlq;")
    alerts = query_one(
        "SELECT COUNT(*)::int AS active_alerts FROM booking_alerts WHERE resolved = false;"
    )
    return {
        **metrics,
        "dlq_count": dlq.get("dlq_count", 0),
        "active_alerts": alerts.get("active_alerts", 0),
        "spark_progress": read_spark_progress(),
    }


@router.get("/cities")
def api_cities() -> list[dict[str, Any]]:
    return query_all(
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


@router.get("/timeseries")
def api_timeseries(limit: int = 30) -> list[dict[str, Any]]:
    rows = query_all(
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


@router.get("/dlq")
def api_dlq(
    limit: int = 50,
    reason: Optional[str] = Query(default=None),
) -> dict[str, Any]:
    if reason:
        items = query_all(
            """
            SELECT id, raw_value, reason, received_at
            FROM booking_events_dlq
            WHERE reason = %s
            ORDER BY id DESC
            LIMIT %s;
            """,
            (reason, limit),
        )
    else:
        items = query_all(
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

    breakdown = query_all(
        """
        SELECT reason, COUNT(*)::int AS count
        FROM booking_events_dlq
        GROUP BY reason
        ORDER BY count DESC;
        """
    )
    return {"items": items, "breakdown": breakdown}


@router.get("/alerts")
def api_alerts(
    limit: int = 50,
    active_only: bool = True,
) -> dict[str, Any]:
    if active_only:
        items = query_all(
            """
            SELECT id, alert_type, city, window_start, window_end, details, created_at
            FROM booking_alerts
            WHERE resolved = false
            ORDER BY created_at DESC
            LIMIT %s;
            """,
            (limit,),
        )
    else:
        items = query_all(
            """
            SELECT id, alert_type, city, window_start, window_end, details, resolved, created_at
            FROM booking_alerts
            ORDER BY created_at DESC
            LIMIT %s;
            """,
            (limit,),
        )

    for item in items:
        for ts_key in ("window_start", "window_end", "created_at"):
            if item.get(ts_key) is not None:
                item[ts_key] = item[ts_key].isoformat()

    summary = query_all(
        """
        SELECT alert_type, COUNT(*)::int AS count
        FROM booking_alerts
        WHERE resolved = false
        GROUP BY alert_type
        ORDER BY count DESC;
        """
    )
    return {"items": items, "summary": summary}
