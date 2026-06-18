"""
Phase 4 (step 1) - Load the local Silver/Bronze data into Snowflake RAW schema.

dbt then models these RAW tables into the clean Gold layer.

Loads three RAW tables:
  RAW.RIDES                     - one row per completed+paid ride (built from Bronze)
  RAW.SILVER_DAILY_CITY_METRICS - the Phase 3 daily aggregates
  RAW.SILVER_HOURLY_DEMAND      - the Phase 3 hourly demand table

Credentials are read automatically from warehouse/.env (python-dotenv), or from
real environment variables if those are already set.

Run:
    pip install -r warehouse/requirements.txt
    python warehouse/load_to_snowflake.py
"""

import os
import sys
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Auto-load credentials from warehouse/.env so you never have to set them by hand.
load_dotenv(Path(__file__).resolve().parent / ".env")

# --------------------------------------------------------------------------- #
# Paths (host-side data lake produced by Phases 2 & 3)
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parents[1]
BRONZE = ROOT / "datalake" / "bronze"
SILVER = ROOT / "datalake" / "silver"
RAW_SCHEMA = "RAW"


def conn():
    """Open a Snowflake connection from environment variables."""
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        sys.exit(f"[ERROR] missing env vars: {', '.join(missing)} "
                 f"(check warehouse/.env)")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )


def build_rides() -> pd.DataFrame:
    """Build a ride-grain table from Bronze: ride_ended joined to its payment."""
    bronze = pd.read_parquet(BRONZE)  # reads all partitions
    ended = bronze[bronze["event_type"] == "ride_ended"][[
        "ride_id", "rider_id", "driver_id", "vehicle_type", "city",
        "event_date", "ride_duration_min", "distance_km", "fare_amount",
    ]]
    pay = bronze[bronze["event_type"] == "payment_completed"][[
        "ride_id", "amount", "payment_method",
    ]].rename(columns={"amount": "paid_amount"})
    rides = ended.merge(pay, on="ride_id", how="left")
    return rides


def load_parquet_dir(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def upper_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Snowflake stores unquoted identifiers in upper case; match that."""
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]
    return df


def load_table(cx, df: pd.DataFrame, table: str) -> None:
    df = upper_cols(df)
    write_pandas(
        cx, df, table_name=table, schema=RAW_SCHEMA,
        auto_create_table=True, overwrite=True,  # truncate+reload -> idempotent
    )
    print(f"[OK] loaded {len(df):>6} rows -> {RAW_SCHEMA}.{table}")


def main() -> None:
    cx = conn()
    cur = cx.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
    cur.execute(f"USE SCHEMA {RAW_SCHEMA}")
    cur.close()

    load_table(cx, build_rides(), "RIDES")
    load_table(cx, load_parquet_dir(SILVER / "daily_city_metrics"),
               "SILVER_DAILY_CITY_METRICS")
    load_table(cx, load_parquet_dir(SILVER / "hourly_demand"),
               "SILVER_HOURLY_DEMAND")

    cx.close()
    print("[DONE] RAW schema loaded. Now run: .\\rundbt.ps1 run")


if __name__ == "__main__":
    main()
