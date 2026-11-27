"""
transforms.py – Pure transformation functions (no Kafka, no DB).
Testable in both batch and streaming mode.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.streaming.schema import EVENT_SCHEMA


def parse_events(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Parse raw Kafka bytes → (valid_df, invalid_df).

    valid_df  : properly structured rows with event_time cast to Timestamp.
    invalid_df: rows with raw JSON text and a specific failure reason for the DLQ.
    """
    parsed = raw_df.select(
        F.col("value").cast("string").alias("raw"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
    )

    is_valid = (
        F.col("e").isNotNull()
        & F.col("e.event_id").isNotNull()
        & F.col("e.city").isNotNull()
        & F.col("e.event_time").isNotNull()
        & (F.col("e.total_price") > 0)
    )

    valid = (
        parsed.where(is_valid)
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn(
            "event_type",
            F.coalesce(F.col("event_type"), F.lit("BOOKING_CREATED")),
        )
    )

    all_fields_null = (
        F.col("e").isNull()
        | (
            F.col("e.event_id").isNull()
            & F.col("e.city").isNull()
            & F.col("e.event_time").isNull()
            & F.col("e.total_price").isNull()
        )
    )

    reason = (
        F.when(all_fields_null, F.lit("invalid_json"))
        .when(
            F.col("e.event_id").isNull()
            | F.col("e.city").isNull()
            | F.col("e.event_time").isNull(),
            F.lit("missing_required_field"),
        )
        .when(
            F.col("e.total_price").isNull() | (F.col("e.total_price") <= 0),
            F.lit("non_positive_price"),
        )
        .otherwise(F.lit("failed_validation"))
    )

    invalid = parsed.where(~is_valid).select(
        F.col("raw").alias("raw_value"),
        reason.alias("reason"),
    )

    return valid, invalid


def aggregate_per_minute(
    valid_df: DataFrame,
    watermark: str = "2 minutes",
    window: str = "1 minute",
) -> DataFrame:
    """
    Deduplicates by event_id then computes per-city tumbling-window aggregates.

    Handles BOOKING_CREATED (adds to bookings & revenue) and
    BOOKING_CANCELLED (adds to cancellations & subtracts from net revenue).

    Returns a streaming or batch DataFrame with columns:
        window (struct: start, end), city, bookings, cancellations, revenue
    """
    is_cancel = F.col("event_type") == "BOOKING_CANCELLED"

    return (
        valid_df
        .withWatermark("event_time", watermark)
        .dropDuplicates(["event_id"])
        .groupBy(F.window("event_time", window), "city")
        .agg(
            F.sum(F.when(~is_cancel, 1).otherwise(0)).cast("int").alias("bookings"),
            F.sum(F.when(is_cancel, 1).otherwise(0)).cast("int").alias("cancellations"),
            F.sum(
                F.when(is_cancel, -F.col("total_price")).otherwise(F.col("total_price"))
            ).alias("revenue"),
            F.avg(F.unix_timestamp(F.to_timestamp("produced_at"))).alias("_avg_produced_ts"),
        )
        .withColumn(
            "avg_latency_ms",
            F.coalesce(
                F.greatest(
                    F.lit(0.0),
                    (F.unix_timestamp(F.current_timestamp()) - F.col("_avg_produced_ts")) * 1000.0,
                ),
                F.lit(0.0),
            ),
        )
        .drop("_avg_produced_ts")
    )
