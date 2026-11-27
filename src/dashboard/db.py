"""
db.py – Database query helpers and Spark progress snapshot reader for the dashboard.
"""
import json
import os
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from src.common import config


def query_all(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    with psycopg2.connect(config.DB_DSN, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def query_one(sql: str, params: tuple = ()) -> dict[str, Any]:
    rows = query_all(sql, params)
    return rows[0] if rows else {}


def read_spark_progress() -> dict[str, Any]:
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
