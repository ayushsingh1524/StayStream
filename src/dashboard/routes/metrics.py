"""
routes/metrics.py – Prometheus /metrics endpoint.
"""
from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from src.dashboard.routes.api import api_summary

router = APIRouter(tags=["observability"])


@router.get("/metrics", response_class=PlainTextResponse)
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
