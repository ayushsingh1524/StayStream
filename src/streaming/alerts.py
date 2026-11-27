"""
alerts.py – Real-time anomaly detection on streaming micro-batches.

Runs inside foreachBatch after metrics are upserted.  Detects:
  • high_cancellation_rate – city cancel rate exceeds threshold in a window.

A per-city cooldown prevents alert storms.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import psycopg2

from src.common import config

logger = logging.getLogger(__name__)

# In-memory cooldown tracker: { (alert_type, city): last_alert_utc }
_cooldowns: Dict[Tuple[str, str], datetime] = {}


def _past_cooldown(alert_type: str, city: str) -> bool:
    key = (alert_type, city)
    last = _cooldowns.get(key)
    if last is None:
        return True
    elapsed = (datetime.now(timezone.utc) - last).total_seconds()
    return elapsed >= config.ALERT_COOLDOWN_MINUTES * 60


def _record_cooldown(alert_type: str, city: str) -> None:
    _cooldowns[(alert_type, city)] = datetime.now(timezone.utc)


def _write_alerts(alerts: List[tuple]) -> None:
    """Insert alert rows into the booking_alerts table."""
    if not alerts:
        return
    sql = """
    INSERT INTO booking_alerts (alert_type, city, window_start, window_end, details)
    VALUES (%s, %s, %s, %s, %s);
    """
    try:
        with psycopg2.connect(config.DB_DSN) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, alerts)
            conn.commit()
    except Exception as exc:
        logger.error("Failed to write alerts: %s", exc)


def detect_anomalies(batch_rows: list) -> None:
    """
    Scan a collected micro-batch for anomalies and write alerts.

    Each row is a dict with keys:
        window (struct: start, end), city, bookings, cancellations, revenue, avg_latency_ms
    """
    alerts_to_write: List[tuple] = []

    for row in batch_rows:
        city = row["city"]
        bookings = row["bookings"] or 0
        cancellations = row["cancellations"] or 0
        total = bookings + cancellations

        if total == 0:
            continue

        cancel_rate = cancellations / total

        # ── High cancellation rate alert ──────────────────────────────────
        if cancel_rate >= config.ALERT_CANCEL_RATE_THRESHOLD:
            if _past_cooldown("high_cancellation_rate", city):
                details = json.dumps({
                    "cancel_rate": round(cancel_rate, 4),
                    "threshold": config.ALERT_CANCEL_RATE_THRESHOLD,
                    "bookings": bookings,
                    "cancellations": cancellations,
                    "revenue": float(row.get("revenue", 0)),
                })
                alerts_to_write.append((
                    "high_cancellation_rate",
                    city,
                    row["window"]["start"],
                    row["window"]["end"],
                    details,
                ))
                _record_cooldown("high_cancellation_rate", city)
                logger.warning(
                    "ALERT: %s cancel rate %.0f%% in %s (window %s)",
                    city, cancel_rate * 100, city, row["window"]["start"],
                )

    if alerts_to_write:
        _write_alerts(alerts_to_write)
        logger.info("Wrote %d anomaly alert(s)", len(alerts_to_write))
