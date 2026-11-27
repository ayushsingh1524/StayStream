"""
config.py – All tunables read from environment variables with safe defaults.
Override with a .env file (python-dotenv) or export in your shell before running.
"""
import os


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


# ── Kafka ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = _env("KAFKA_TOPIC", "booking-events")

# ── Postgres ───────────────────────────────────────────────────────────────────
DB_HOST     = _env("DB_HOST", "localhost")
DB_PORT     = int(_env("DB_PORT", "5432"))
DB_NAME     = _env("DB_NAME", "staystream")
DB_USER     = _env("DB_USER", "stream")
DB_PASSWORD = _env("DB_PASSWORD", "stream_dev_pw")

DB_DSN = (
    f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
    f"user={DB_USER} password={DB_PASSWORD}"
)

# ── Streaming tuning ───────────────────────────────────────────────────────────
WATERMARK_DURATION   = _env("WATERMARK", "2 minutes")
WINDOW_DURATION      = _env("WINDOW", "1 minute")
TRIGGER_INTERVAL     = _env("TRIGGER_INTERVAL", "5 seconds")
SHUFFLE_PARTITIONS   = int(_env("SHUFFLE_PARTITIONS", "4"))
CHECKPOINT_DIR       = _env("CHECKPOINT_DIR", "./checkpoints/metrics")

# ── Kafka connector package (must match PySpark version) ──────────────────────
PYSPARK_VERSION         = _env("PYSPARK_VERSION", "3.5.3")
KAFKA_CONNECTOR_PACKAGE = (
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{PYSPARK_VERSION}"
)

