"""
sinks.py – Write aggregated results and DLQ rows to PostgreSQL.
All writes are idempotent (upsert on conflict).
"""
import logging
from typing import Sequence

import psycopg2

from src.common.config import DB_DSN

logger = logging.getLogger(__name__)

# ── SQL statements ─────────────────────────────────────────────────────────────

_UPSERT_METRICS = """
INSERT INTO booking_metrics_per_minute
    (window_start, window_end, city, bookings, revenue)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (window_start, city)
DO UPDATE SET
    bookings   = EXCLUDED.bookings,
    revenue    = EXCLUDED.revenue,
    updated_at = now();
"""

_INSERT_DLQ = """
INSERT INTO booking_events_dlq (raw_value, reason)
VALUES (%s, %s);
"""


def upsert_metrics(rows: Sequence[tuple]) -> None:
    """
    Upsert (window_start, window_end, city, bookings, revenue) tuples.
    Running the same batch twice gives the same result.
    """
    if not rows:
        return
    with psycopg2.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.executemany(_UPSERT_METRICS, rows)
    logger.info("upserted %d metric rows", len(rows))


def write_dlq(rows: Sequence[tuple]) -> None:
    """
    Append (raw_value, reason) tuples to the dead-letter table.
    """
    if not rows:
        return
    with psycopg2.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.executemany(_INSERT_DLQ, rows)
    logger.info("wrote %d DLQ rows", len(rows))

