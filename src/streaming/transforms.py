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
    invalid_df: rows with raw JSON text for the DLQ.
    """
    parsed = raw_df.select(
        F.col("value").cast("string").alias("raw"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
    )

    # Validity: mandatory fields present and total_price > 0
    is_valid = (
        F.col("e.event_id").isNotNull()
        & F.col("e.city").isNotNull()
        & F.col("e.event_time").isNotNull()
        & (F.col("e.total_price") > 0)
    )

    valid = (
        parsed.where(is_valid)
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
    )

    invalid = parsed.where(~is_valid).select(
        F.col("raw").alias("raw_value"),
        F.lit("failed_validation").alias("reason"),
    )

    return valid, invalid


def aggregate_per_minute(
    valid_df: DataFrame,
    watermark: str = "2 minutes",
    window: str = "1 minute",
) -> DataFrame:
    """
    Deduplicates by event_id then computes per-city tumbling-window aggregates.

    Returns a streaming or batch DataFrame with columns:
        window (struct: start, end), city, bookings, revenue
    """
    return (
        valid_df
        .withWatermark("event_time", watermark)
        .dropDuplicates(["event_id"])
        .groupBy(F.window("event_time", window), "city")
        .agg(
            F.count("*").alias("bookings"),
            F.sum("total_price").alias("revenue"),
        )
    )

