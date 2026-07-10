"""
Quick post-load verification for the RideShare Snowflake warehouse.

Connects using warehouse/.env and prints:
  * row counts for every RAW and GOLD table
  * a referential-integrity check (fct_rides drivers/riders that don't join)
  * a small preview of the headline fct_rides model

Run:
    python warehouse/verify_load.py
"""

import os
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

DB = os.getenv("SNOWFLAKE_DATABASE", "RIDESHARE")

# dbt concatenates the profile schema (GOLD) with each model's custom +schema,
# so the Gold marts land in GOLD_GOLD (and staging views in GOLD_STAGING).
GOLD_SCHEMA = "GOLD_GOLD"

RAW_TABLES = ["RIDES", "SILVER_DAILY_CITY_METRICS", "SILVER_HOURLY_DEMAND"]
GOLD_TABLES = ["DIM_DRIVER", "DIM_RIDER", "FCT_RIDES",
               "FCT_DAILY_CITY_METRICS", "FCT_HOURLY_DEMAND"]


def conn():
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        sys.exit(f"[ERROR] missing env vars: {', '.join(missing)} (check warehouse/.env)")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )


def count(cur, schema, table):
    cur.execute(f"SELECT COUNT(*) FROM {DB}.{schema}.{table}")
    return cur.fetchone()[0]


def main():
    cx = conn()
    cur = cx.cursor()

    print(f"\nConnected to {DB} as {os.getenv('SNOWFLAKE_USER')}\n")

    print("RAW schema (loaded by load_to_snowflake.py)")
    for t in RAW_TABLES:
        print(f"  RAW.{t:<28} {count(cur, 'RAW', t):>8,} rows")

    print(f"\n{GOLD_SCHEMA} schema (built by dbt)")
    for t in GOLD_TABLES:
        print(f"  {GOLD_SCHEMA}.{t:<28} {count(cur, GOLD_SCHEMA, t):>8,} rows")

    # Referential integrity: every fct_rides row should join to a driver & rider.
    cur.execute(f"""
        SELECT
          COUNT_IF(driver_name IS NULL) AS orphan_drivers,
          COUNT_IF(rider_name  IS NULL) AS orphan_riders,
          COUNT(*)                       AS total_rides
        FROM {DB}.{GOLD_SCHEMA}.FCT_RIDES
    """)
    od, orr, total = cur.fetchone()
    print(f"\nReferential integrity ({GOLD_SCHEMA}.FCT_RIDES)")
    print(f"  total rides           {total:>8,}")
    print(f"  rides w/o driver match{od:>8,}")
    print(f"  rides w/o rider match {orr:>8,}")
    print("  -> OK" if od == 0 and orr == 0 else "  -> WARNING: unmatched rows found")

    # Preview
    print(f"\nSample {GOLD_SCHEMA}.FCT_RIDES rows:")
    cur.execute(f"""
        SELECT ride_id, ride_date, vehicle_type, rider_segment,
               driver_rating, fare_amount, revenue, payment_method
        FROM {DB}.{GOLD_SCHEMA}.FCT_RIDES
        LIMIT 5
    """)
    cols = [c[0] for c in cur.description]
    print("  " + " | ".join(cols))
    for row in cur.fetchall():
        print("  " + " | ".join(str(v) for v in row))

    cur.close()
    cx.close()
    print("\n[DONE] verification complete.")


if __name__ == "__main__":
    main()
