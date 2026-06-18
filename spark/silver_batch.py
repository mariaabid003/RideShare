"""
Phase 3 - Spark batch job -> Silver layer.

Runs daily (scheduled by Airflow in Phase 7). It reads the raw Bronze layer
and computes business insights, writing them as the Silver layer:

  * daily_city_metrics : total rides, avg ride duration, revenue per city/day
  * hourly_demand      : ride requests per hour, with the peak hour flagged

By default it processes EVERY date found in Bronze (handy for testing).
Set PROCESS_DATE=YYYY-MM-DD to process a single day (what Airflow will pass,
typically "yesterday").

Env vars: BRONZE_PATH, SILVER_PATH, PROCESS_DATE
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/datalake/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "/opt/datalake/silver")
PROCESS_DATE = os.getenv("PROCESS_DATE")  # optional "YYYY-MM-DD"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("rideshare-silver-batch")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        # only overwrite the date partitions we actually process
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze = spark.read.parquet(BRONZE_PATH)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] could not read Bronze at {BRONZE_PATH}: {exc}")
        print("[HINT] run the Phase 2 streaming job first so Bronze has data.")
        sys.exit(1)

    if PROCESS_DATE:
        bronze = bronze.filter(F.col("event_date") == F.lit(PROCESS_DATE))
        print(f"[INFO] processing single date: {PROCESS_DATE}")
    else:
        print("[INFO] processing ALL dates found in Bronze")

    if bronze.rdd.isEmpty():
        print("[WARN] no Bronze rows to process. Nothing written.")
        spark.stop()
        return

    # --- slice Bronze by event type ---
    ended = bronze.filter(F.col("event_type") == "ride_ended")
    requested = bronze.filter(F.col("event_type") == "ride_requested")
    payments = bronze.filter(F.col("event_type") == "payment_completed")

    # ----------------------------------------------------------------- #
    # 1) Daily metrics per city: rides, avg duration, revenue
    # ----------------------------------------------------------------- #
    rides = ended.groupBy("event_date", "city").agg(
        F.countDistinct("ride_id").alias("total_rides"),
        F.round(F.avg("ride_duration_min"), 2).alias("avg_duration_min"),
    )

    revenue = payments.groupBy("event_date", "city").agg(
        F.round(F.sum("amount"), 2).alias("revenue_pkr"),
        F.countDistinct("ride_id").alias("paid_rides"),
    )

    daily_city_metrics = (
        rides.join(revenue, ["event_date", "city"], "outer")
        .na.fill(0)
        .orderBy("event_date", "city")
    )

    # ----------------------------------------------------------------- #
    # 2) Hourly demand + peak hour per city/day
    # ----------------------------------------------------------------- #
    hourly = (
        requested
        .withColumn("hour", F.hour("event_timestamp"))
        .groupBy("event_date", "city", "hour")
        .agg(F.countDistinct("ride_id").alias("ride_requests"))
    )

    # rank hours within each city/day; rank 1 = peak demand hour
    w = Window.partitionBy("event_date", "city").orderBy(F.col("ride_requests").desc())
    hourly_demand = (
        hourly
        .withColumn("is_peak_hour", F.row_number().over(w) == 1)
        .orderBy("event_date", "city", "hour")
    )

    # ----------------------------------------------------------------- #
    # Write Silver (partitioned by date) + log a preview
    # ----------------------------------------------------------------- #
    (
        daily_city_metrics.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{SILVER_PATH}/daily_city_metrics")
    )
    (
        hourly_demand.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{SILVER_PATH}/hourly_demand")
    )

    print("\n=== Silver: daily_city_metrics ===")
    daily_city_metrics.show(truncate=False)

    print("=== Silver: peak demand hour per city/day ===")
    hourly_demand.filter(F.col("is_peak_hour")).show(truncate=False)

    print(f"[INFO] Silver written under {SILVER_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
