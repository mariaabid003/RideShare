"""
Phase 7 - Airflow nightly orchestration for the RideShare pipeline.

Runs once per day and chains the batch side of the pipeline:

    silver_batch  ->  dq_gate  ->  load_raw  ->  dbt_build

  1. silver_batch : Spark job #2 (spark/silver_batch.py) aggregates Bronze into
                    the Silver layer for the run's logical date.
  2. dq_gate      : Great Expectations checks (dq/validate.py). Exits non-zero on
                    failure, so Airflow stops the run here and fires the alert.
  3. load_raw     : lands the local data lake into Snowflake RAW
                    (warehouse/load_to_snowflake.py).
  4. dbt_build    : builds + tests the Gold layer (dbt build).

Each step runs inside its own isolated virtualenv baked into the Airflow image
(see airflow/Dockerfile), so dependency sets never clash. The whole project is
mounted read/write at /opt/airflow/project.

If any task fails (including the data-quality gate), `alert_on_failure` runs -
wire it to email/Slack where noted.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# --------------------------------------------------------------------------- #
# Paths INSIDE the Airflow container (project is bind-mounted here)
# --------------------------------------------------------------------------- #
PROJECT = "/opt/airflow/project"
BRONZE = f"{PROJECT}/datalake/bronze"
SILVER = f"{PROJECT}/datalake/silver"

# Per-step virtualenvs baked into the image (airflow/Dockerfile)
PY_SPARK = "/opt/venv/spark/bin/python"       # pyspark 3.5.1
PY_DQ = "/opt/venv/dq/bin/python"             # great-expectations
PY_WH = "/opt/venv/warehouse/bin/python"      # snowflake-connector + pandas
DBT = "/opt/venv/warehouse/bin/dbt"           # dbt-snowflake (same venv)


def alert_on_failure(context) -> None:
    """Fire when any task fails. Replace the print with a real notifier."""
    ti = context["task_instance"]
    print(
        f"[ALERT] RideShare pipeline FAILED\n"
        f"  dag={ti.dag_id} task={ti.task_id}\n"
        f"  logical_date={context['logical_date']}\n"
        f"  log_url={ti.log_url}"
    )
    # To notify for real, add one of:
    #   from airflow.providers.slack.notifications.slack import send_slack_notification
    #   from airflow.utils.email import send_email
    # and call it here (or set default_args["email"] + email_on_failure=True).


default_args = {
    "owner": "maria",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="rideshare_daily",
    description="Nightly Silver batch -> data-quality gate -> load -> dbt Gold build",
    default_args=default_args,
    start_date=datetime(2026, 6, 18),
    schedule="0 2 * * *",          # once a day at 02:00 (server time)
    catchup=False,
    max_active_runs=1,
    tags=["rideshare", "phase7"],
) as dag:

    # 1) Spark job #2: Bronze -> Silver for this run's date.
    #    {{ ds }} is the run's logical date (YYYY-MM-DD). To (re)process EVERY
    #    date currently in Bronze instead - handy for a first backfill/demo -
    #    remove the PROCESS_DATE line below.
    silver_batch = BashOperator(
        task_id="silver_batch",
        bash_command=f"{PY_SPARK} {PROJECT}/spark/silver_batch.py",
        append_env=True,
        env={
            "BRONZE_PATH": BRONZE,
            "SILVER_PATH": SILVER,
            "PROCESS_DATE": "{{ ds }}",
        },
    )

    # 2) Data-quality gate. Non-zero exit stops the run + triggers the alert.
    dq_gate = BashOperator(
        task_id="dq_gate",
        bash_command=f"{PY_DQ} {PROJECT}/dq/validate.py",
        append_env=True,
        env={"BRONZE_PATH": BRONZE, "SILVER_PATH": SILVER},
    )

    # 3) Land the local data lake into Snowflake RAW.
    #    The loader self-loads warehouse/.env via python-dotenv.
    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=f"{PY_WH} {PROJECT}/warehouse/load_to_snowflake.py",
    )

    # 4) Build + test the Gold layer. Source .env so dbt's env_var() resolves.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"set -a && . {PROJECT}/warehouse/.env && set +a && "
            f"cd {PROJECT}/dbt && "
            f"{DBT} build --profiles-dir . --project-dir ."
        ),
    )

    silver_batch >> dq_gate >> load_raw >> dbt_build
