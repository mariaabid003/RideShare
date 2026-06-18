"""
Phase 2 - Spark Structured Streaming -> Bronze layer.

Continuously reads ride events from Kafka and lands them as Parquet in the
Bronze layer. The Bronze layer keeps the data raw but cleaned:

  * drop invalid events (missing ride_id / event_type / event_time)
  * tag every record with a standardized city
  * surface ride duration (from ride_ended events)
  * add ingestion metadata (ingest_time, source_topic)

Output is partitioned by event_date + event_type so downstream batch jobs
(Phase 3) can scan only what they need.

Run via docker compose (see docker-compose.yml). Configurable with env vars:
  KAFKA_BOOTSTRAP, KAFKA_TOPIC, BRONZE_PATH, CHECKPOINT_PATH
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ride-events")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/datalake/bronze")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/opt/datalake/_checkpoints/bronze")

VALID_EVENT_TYPES = [
    "ride_requested",
    "ride_accepted",
    "ride_started",
    "ride_ended",
    "payment_completed",
    "ride_cancelled",
]
VALID_CITIES = ["Karachi"]

# --------------------------------------------------------------------------- #
# Schema of the JSON we put on Kafka in Phase 1.
# It's a superset of every event type; fields not present on a given event
# simply parse as null.
# --------------------------------------------------------------------------- #
POINT = StructType([
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
])

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("ride_id", StringType()),
    StructField("city", StringType()),
    StructField("rider_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("pickup", POINT),
    StructField("dropoff", POINT),
    StructField("duration_min", DoubleType()),
    StructField("distance_km", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_method", StringType()),
    StructField("status", StringType()),
    StructField("cancelled_by", StringType()),
    StructField("reason", StringType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("rideshare-bronze-stream")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def transform(raw):
    """Parse the Kafka value JSON, clean it, and add Bronze metadata."""
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_time")
        .withColumn("e", F.from_json("json_str", EVENT_SCHEMA))
        .select("e.*", "kafka_time")
    )

    cleaned = (
        parsed
        # real event timestamp from the payload
        .withColumn("event_timestamp", F.to_timestamp("event_time"))
        # ---- FILTER invalid events ----
        .filter(F.col("ride_id").isNotNull())
        .filter(F.col("event_type").isin(VALID_EVENT_TYPES))
        .filter(F.col("event_timestamp").isNotNull())
        # ---- TAG city (standardize, default to Karachi) ----
        .withColumn("city", F.coalesce(F.col("city"), F.lit("Karachi")))
        .withColumn("is_valid_city", F.col("city").isin(VALID_CITIES))
        # ---- Surface RIDE DURATION (present on ride_ended events) ----
        .withColumn(
            "ride_duration_min",
            F.when(F.col("event_type") == "ride_ended", F.col("duration_min")),
        )
        # ---- Ingestion metadata ----
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn("source_topic", F.lit(KAFKA_TOPIC))
        # ---- Partition key ----
        .withColumn("event_date", F.to_date("event_timestamp"))
    )
    return cleaned


def write_batch(batch_df, batch_id: int) -> None:
    """Persist one micro-batch to Bronze and log a quick summary."""
    if batch_df.rdd.isEmpty():
        return
    batch_df.persist()
    (
        batch_df.write
        .mode("append")
        .partitionBy("event_date", "event_type")
        .parquet(BRONZE_PATH)
    )
    total = batch_df.count()
    print(f"\n=== batch {batch_id}: wrote {total} events to Bronze ===")
    (batch_df.groupBy("event_type").count().orderBy("event_type").show(truncate=False))
    batch_df.unpersist()


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")  # read everything already in the topic
        .load()
    )

    bronze = transform(raw)

    query = (
        bronze.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"[INFO] streaming '{KAFKA_TOPIC}' from {KAFKA_BOOTSTRAP} -> {BRONZE_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
