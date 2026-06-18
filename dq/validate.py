"""
Phase 5 - Great Expectations data-quality gate.

Validates the data lake layers BEFORE they flow downstream. Checks:
  * no null ride IDs
  * ride duration is not negative
  * city names are valid
  * revenue / fare values are positive

If ANY check fails the script prints an alert and exits with code 1, so the
orchestrator (Airflow, Phase 7) stops the pipeline instead of publishing bad
data. All checks pass -> exit 0.

Run:
    pip install -r dq/requirements.txt
    python dq/validate.py
"""

import os
# Silence GE's tqdm progress bars (cosmetic noise on the console).
os.environ.setdefault("TQDM_DISABLE", "1")

import sys
from pathlib import Path

import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe

ROOT = Path(__file__).resolve().parents[1]
BRONZE = Path(os.getenv("BRONZE_PATH", ROOT / "datalake" / "bronze"))
SILVER = Path(os.getenv("SILVER_PATH", ROOT / "datalake" / "silver"))
VALID_CITIES = ["Karachi"]


def validate(df: pd.DataFrame, suite_name: str, expectations: list):
    """Run a list of expectations against an in-memory DataFrame."""
    ctx = gx.get_context(mode="ephemeral")
    asset = ctx.data_sources.add_pandas("dq").add_dataframe_asset("asset")
    batch = (
        asset.add_batch_definition_whole_dataframe("bd")
        .get_batch(batch_parameters={"dataframe": df})
    )
    suite = gx.ExpectationSuite(name=suite_name)
    for exp in expectations:
        suite.add_expectation(exp)
    return batch.validate(suite)


def report(layer: str, res) -> bool:
    """Pretty-print one layer's result; return True if it passed."""
    failed = [r for r in res.results if not r.success]
    if res.success:
        print(f"[PASS] {layer}: all {len(res.results)} checks passed")
        return True
    print(f"[FAIL] {layer}: {len(failed)} of {len(res.results)} checks FAILED")
    for r in failed:
        cfg = r.expectation_config
        col = cfg.kwargs.get("column")
        bad = r.result.get("unexpected_count", "?")
        print(f"   - {cfg.type} on '{col}': {bad} bad value(s)")
    return False


def main() -> None:
    try:
        bronze = pd.read_parquet(BRONZE)
        rides = bronze[bronze["event_type"] == "ride_ended"]
        metrics = pd.read_parquet(SILVER / "daily_city_metrics")
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] could not read the data lake: {exc}")
        print("[HINT] run Phases 2 & 3 first so Bronze/Silver exist.")
        sys.exit(1)

    ok = True

    # --- Bronze: ride-level facts ---
    ok &= report("Bronze (rides)", validate(rides, "bronze_rides", [
        gxe.ExpectColumnValuesToNotBeNull(column="ride_id"),
        gxe.ExpectColumnValuesToBeBetween(column="ride_duration_min", min_value=0),
        gxe.ExpectColumnValuesToBeInSet(column="city", value_set=VALID_CITIES),
        gxe.ExpectColumnValuesToBeBetween(column="fare_amount", min_value=0, strict_min=True),
    ]))

    # --- Silver: daily city metrics ---
    ok &= report("Silver (daily_city_metrics)", validate(metrics, "silver_daily", [
        gxe.ExpectColumnValuesToNotBeNull(column="city"),
        gxe.ExpectColumnValuesToBeInSet(column="city", value_set=VALID_CITIES),
        gxe.ExpectColumnValuesToBeBetween(column="revenue_pkr", min_value=0, strict_min=True),
        gxe.ExpectColumnValuesToBeBetween(column="total_rides", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="avg_duration_min", min_value=0),
    ]))

    print()
    if not ok:
        print("*** DATA QUALITY GATE FAILED -- pipeline should STOP (exit 1) ***")
        sys.exit(1)
    print(">>> DATA QUALITY GATE PASSED -- safe to proceed (exit 0)")
    sys.exit(0)


if __name__ == "__main__":
    main()
