"""
test_pipeline_local.py – Light integration: parse → aggregate → collect in batch mode.
No Kafka, no Postgres. Validates the full data flow end-to-end.
"""
import json
import pytest
from pyspark.sql import SparkSession

from src.streaming.transforms import parse_events, aggregate_per_minute


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("StayStream-integration-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


BASE = {
    "event_type":  "BOOKING_CREATED",
    "booking_id":  999,
    "hotel_id":    1,
    "room_type":   "STANDARD",
    "nights":      1,
    "user_id":     "u-test",
    "produced_at": "2026-11-22T10:00:01Z",
}


def make_payload(**overrides) -> bytes:
    return json.dumps({**BASE, **overrides}).encode()


def raw_df(spark, payloads):
    return spark.createDataFrame([(p,) for p in payloads], ["value"])


class TestEndToEnd:

    def test_valid_pipeline_produces_correct_aggregates(self, spark):
        payloads = [
            make_payload(event_id="p-001", city="Delhi", total_price=5000.0, event_time="2026-11-22T10:00:00Z"),
            make_payload(event_id="p-002", city="Delhi", total_price=3000.0, event_time="2026-11-22T10:00:30Z"),
            make_payload(event_id="p-003", city="Goa",   total_price=8000.0, event_time="2026-11-22T10:00:45Z"),
        ]
        raw = raw_df(spark, payloads)
        valid, invalid = parse_events(raw)
        agg = aggregate_per_minute(valid)
        result = {r["city"]: r for r in agg.collect()}

        assert invalid.count() == 0
        assert result["Delhi"]["bookings"] == 2
        assert float(result["Delhi"]["revenue"]) == 8000.0
        assert result["Goa"]["bookings"] == 1
        assert float(result["Goa"]["revenue"]) == 8000.0

    def test_mixed_valid_invalid(self, spark):
        payloads = [
            make_payload(event_id="ok-1", city="Mumbai", total_price=2000.0, event_time="2026-11-22T10:00:00Z"),
            b'{"broken": json',                                           # malformed
            make_payload(event_id=None, city="Mumbai", total_price=1000.0, event_time="2026-11-22T10:00:00Z"),
        ]
        raw = raw_df(spark, payloads)
        valid, invalid = parse_events(raw)
        assert valid.count() == 1
        assert invalid.count() == 2

    def test_deduplication_in_pipeline(self, spark):
        payload = make_payload(
            event_id="dedup-99", city="Jaipur",
            total_price=4000.0, event_time="2026-11-22T10:00:00Z"
        )
        raw = raw_df(spark, [payload, payload, payload])
        valid, _ = parse_events(raw)
        agg = aggregate_per_minute(valid)
        rows = agg.collect()
        assert rows[0]["bookings"] == 1
        assert float(rows[0]["revenue"]) == 4000.0

