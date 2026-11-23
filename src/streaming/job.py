"""
job.py – Entry point for the PySpark Structured Streaming job.

Run with:
    python -m src.streaming.job

Or, to override config:
    KAFKA_BOOTSTRAP=localhost:9092 TRIGGER_INTERVAL="10 seconds" \\
        python -m src.streaming.job
"""
import logging
import os
import sys

# Allow running from repo root without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession

from src.common import config
from src.streaming.schema import EVENT_SCHEMA
from src.streaming.transforms import aggregate_per_minute, parse_events
from src.streaming.sinks import upsert_metrics, write_dlq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("staystream.job")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StayStream")
        .config("spark.jars.packages", config.KAFKA_CONNECTOR_PACKAGE)
        .config("spark.sql.shuffle.partitions", str(config.SHUFFLE_PARTITIONS))
        # Silence noisy Spark logs (keep WARN and above)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def write_batch(batch_df, batch_id: int) -> None:
    """
    foreachBatch handler: collect aggregated rows, upsert to Postgres.
    Aggregates are small (one row per city per minute window), so collect() is safe.
    """
    rows = [
        (r["window"]["start"], r["window"]["end"], r["city"], r["bookings"], float(r["revenue"]))
        for r in batch_df.collect()
    ]
    if rows:
        logger.info("batch %d: upserting %d metric rows", batch_id, len(rows))
        upsert_metrics(rows)
    else:
        logger.debug("batch %d: no rows to upsert", batch_id)


def write_dlq_batch(batch_df, batch_id: int) -> None:
    rows = [(r["raw_value"], r["reason"]) for r in batch_df.collect()]
    if rows:
        logger.warning("batch %d: writing %d DLQ rows", batch_id, len(rows))
        write_dlq(rows)


def log_progress(query) -> None:
    p = query.lastProgress
    if p:
        logger.info(
            "inputRows=%s  inputRate=%.1f/s  processedRate=%.1f/s  batchDuration=%sms",
            p.get("numInputRows", "?"),
            p.get("inputRowsPerSecond", 0),
            p.get("processedRowsPerSecond", 0),
            p.get("durationMs", {}).get("triggerExecution", "?"),
        )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading from Kafka: %s / %s", config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    valid_df, invalid_df = parse_events(raw)
    agg_df = aggregate_per_minute(
        valid_df,
        watermark=config.WATERMARK_DURATION,
        window=config.WINDOW_DURATION,
    )

    # ── Metrics sink ───────────────────────────────────────────────────────────
    metrics_query = (
        agg_df.writeStream
        .outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", config.CHECKPOINT_DIR)
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .start()
    )

    # ── DLQ sink (separate query, no watermark needed) ─────────────────────────
    dlq_query = (
        invalid_df.writeStream
        .outputMode("append")
        .foreachBatch(write_dlq_batch)
        .option("checkpointLocation", config.CHECKPOINT_DIR + "_dlq")
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .start()
    )

    logger.info("Streaming queries started. Awaiting termination…")

    try:
        while metrics_query.isActive:
            metrics_query.awaitTermination(timeout=10)
            log_progress(metrics_query)
    except KeyboardInterrupt:
        logger.info("Shutting down…")
    finally:
        metrics_query.stop()
        dlq_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

