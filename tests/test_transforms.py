"""
test_transforms.py – Unit tests for pure transform functions.
No Kafka, no Postgres. Uses a local SparkSession in batch mode.
"""
import pytest
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.streaming.transforms import parse_events, aggregate_per_minute


# ── Shared SparkSession (module-scoped for speed) ──────────────────────────────

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("StayStream-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _raw(spark, payloads: list[str]):
    """Wrap JSON strings into a DataFrame with a 'value' binary column."""
    rows = [(p.encode(),) for p in payloads]
    return spark.createDataFrame(rows, ["value"])


GOOD_EVENT = {
    "event_id":    "evt-001",
    "event_type":  "BOOKING_CREATED",
    "booking_id":  1,
    "hotel_id":    10,
    "city":        "Goa",
    "room_type":   "DELUXE",
    "nights":      2,
    "total_price": 10000.0,
    "user_id":     "u1",
    "event_time":  "2026-01-01T10:00:00Z",
    "produced_at": "2026-01-01T10:00:01Z",
}


def _json(**overrides) -> str:
    import json
    event = {**GOOD_EVENT, **overrides}
    return json.dumps(event)


# ── parse_events tests ─────────────────────────────────────────────────────────

class TestParseEvents:

    def test_valid_row_passes_through(self, spark):
        raw = _raw(spark, [_json()])
        valid, invalid = parse_events(raw)
        assert valid.count() == 1
        assert invalid.count() == 0

    def test_null_city_goes_to_invalid(self, spark):
        raw = _raw(spark, [_json(city=None), _json()])
        valid, invalid = parse_events(raw)
        assert valid.count() == 1
        assert invalid.count() == 1

    def test_null_event_id_goes_to_invalid(self, spark):
        raw = _raw(spark, [_json(event_id=None)])
        valid, invalid = parse_events(raw)
        assert valid.count() == 0
        assert invalid.count() == 1

    def test_zero_price_goes_to_invalid(self, spark):
        raw = _raw(spark, [_json(total_price=0)])
        valid, invalid = parse_events(raw)
        assert valid.count() == 0
        assert invalid.count() == 1

    def test_malformed_json_goes_to_invalid(self, spark):
        raw = _raw(spark, ['{"BROKEN": true, bad json'])
        valid, invalid = parse_events(raw)
        assert valid.count() == 0
        assert invalid.count() == 1

    def test_event_time_parsed_to_timestamp(self, spark):
        raw = _raw(spark, [_json()])
        valid, _ = parse_events(raw)
        from pyspark.sql.types import TimestampType
        assert dict(valid.dtypes)["event_time"] == "timestamp"

    def test_multiple_valid_rows(self, spark):
        events = [_json(event_id=f"evt-{i:03d}", city="Goa") for i in range(5)]
        raw = _raw(spark, events)
        valid, invalid = parse_events(raw)
        assert valid.count() == 5
        assert invalid.count() == 0


# ── aggregate_per_minute tests (batch mode) ────────────────────────────────────

def _valid_df(spark, rows: list[dict]):
    """Build a valid_df from dicts, event_time already as timestamp."""
    import json
    payloads = [json.dumps(r) for r in rows]
    raw = _raw(spark, payloads)
    valid, _ = parse_events(raw)
    return valid


class TestAggregatePerMinute:

    def test_single_event_sums_correctly(self, spark):
        raw = _raw(spark, [_json()])
        valid, _ = parse_events(raw)
        agg = aggregate_per_minute(valid)
        rows = agg.collect()
        assert len(rows) == 1
        assert rows[0]["bookings"] == 1
        assert float(rows[0]["revenue"]) == 10000.0

    def test_duplicate_event_id_counted_once(self, spark):
        # Same event_id sent twice → bookings=1
        event = _json(event_id="dup-001", total_price=5000.0)
        raw = _raw(spark, [event, event])
        valid, _ = parse_events(raw)
        agg = aggregate_per_minute(valid)
        rows = agg.collect()
        assert rows[0]["bookings"] == 1
        assert float(rows[0]["revenue"]) == 5000.0

    def test_revenue_sums_across_events(self, spark):
        events = [
            _json(event_id="e1", total_price=1000.0, city="Goa"),
            _json(event_id="e2", total_price=2000.0, city="Goa"),
            _json(event_id="e3", total_price=4000.0, city="Goa"),
        ]
        raw = _raw(spark, events)
        valid, _ = parse_events(raw)
        agg = aggregate_per_minute(valid)
        rows = {r["city"]: r for r in agg.collect()}
        assert rows["Goa"]["bookings"] == 3
        assert float(rows["Goa"]["revenue"]) == 7000.0

    def test_two_cities_stay_separate(self, spark):
        events = [
            _json(event_id="g1", city="Goa",     total_price=1000.0),
            _json(event_id="m1", city="Mumbai",   total_price=2000.0),
            _json(event_id="m2", city="Mumbai",   total_price=3000.0),
        ]
        raw = _raw(spark, events)
        valid, _ = parse_events(raw)
        agg = aggregate_per_minute(valid)
        rows = {r["city"]: r for r in agg.collect()}
        assert rows["Goa"]["bookings"]    == 1
        assert rows["Mumbai"]["bookings"] == 2
        assert float(rows["Mumbai"]["revenue"]) == 5000.0

    def test_events_in_different_minutes_in_different_windows(self, spark):
        events = [
            _json(event_id="a1", event_time="2026-01-01T10:00:30Z"),
            _json(event_id="a2", event_time="2026-01-01T10:01:30Z"),   # next minute
        ]
        raw = _raw(spark, events)
        valid, _ = parse_events(raw)
        agg = aggregate_per_minute(valid)
        windows = agg.collect()
        # Must produce 2 distinct windows
        starts = {str(r["window"]["start"]) for r in windows}
        assert len(starts) == 2

    def test_exactly_100_events_one_city(self, spark):
        import json
        events = [
            json.dumps({**GOOD_EVENT, "event_id": f"bulk-{i:04d}", "total_price": 100.0})
            for i in range(100)
        ]
        raw = _raw(spark, events)
        valid, _ = parse_events(raw)
        agg = aggregate_per_minute(valid)
        rows = agg.collect()
        assert rows[0]["bookings"] == 100
        assert abs(float(rows[0]["revenue"]) - 10000.0) < 0.01

