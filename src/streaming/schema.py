"""
schema.py – Spark StructType for a booking event JSON payload.
"""
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType,
)

EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    nullable=False),
    StructField("event_type",  StringType(),    nullable=True),
    StructField("booking_id",  LongType(),      nullable=True),
    StructField("hotel_id",    LongType(),      nullable=True),
    StructField("city",        StringType(),    nullable=False),
    StructField("room_type",   StringType(),    nullable=True),
    StructField("nights",      LongType(),      nullable=True),
    StructField("total_price", DoubleType(),    nullable=False),
    StructField("user_id",     StringType(),    nullable=True),
    # Stored as string in JSON; parsed to timestamp in transforms.py
    StructField("event_time",  StringType(),    nullable=False),
    StructField("produced_at", StringType(),    nullable=True),
])

