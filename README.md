# Real-Time Ride Sharing Pipeline (Careem Clone)

End-to-end data pipeline over a simulated ride-sharing service in Karachi.

```
Python (fake events) → Kafka → Spark Streaming → S3 (Bronze)
                                        → Spark Batch → S3 (Silver)
                                              → dbt → Snowflake (Gold) → Power BI
Great Expectations  → data quality after each layer
Airflow             → schedules & monitors the batch side
```

## Phase 1 — Event Simulator (done)

Generates Careem-style events and pushes them into a real Kafka topic. Each ride
flows through its full lifecycle:

```
ride_requested → ride_accepted → ride_started → ride_ended → payment_completed
```

(~8% of rides are cancelled after the request to create messy/incomplete data
for Spark to filter in Phase 2.)

### Run it

1. **Start Kafka** (needs Docker):

   ```bash
   docker compose up -d
   ```

   Optional: open the Kafka UI at http://localhost:8080 to watch messages arrive.

2. **Install the Python dependency:**

   ```bash
   pip install -r producer/requirements.txt
   ```

3. **Run the simulator:**

   ```bash
   python producer/event_simulator.py
   ```

   You'll see events streaming to the console as they're produced.

### Configuration (environment variables)

| Variable          | Default          | Meaning                                   |
| ----------------- | ---------------- | ----------------------------------------- |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address                      |
| `KAFKA_TOPIC`     | `ride-events`    | Topic events are written to               |
| `EVENTS_PER_MIN`  | `30`             | New rides started per minute              |
| `MAX_RIDES`       | `0`              | Stop after N rides (`0` = run forever)    |

Example — a quick 10-ride test run:

```bash
MAX_RIDES=10 EVENTS_PER_MIN=120 python producer/event_simulator.py
```

### Inspect the topic from the CLI (optional)

```bash
docker exec -it rideshare-kafka \
  kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic ride-events --from-beginning
```

## Phase 2 — Spark Streaming → Bronze (done)

A Spark Structured Streaming job (`spark/bronze_stream.py`) reads the
`ride-events` topic continuously and lands a cleaned **Bronze** layer as Parquet:

* filters invalid events (missing `ride_id` / `event_type` / `event_time`)
* tags every record with a standardized `city`
* surfaces `ride_duration_min` (from `ride_ended` events)
* adds ingestion metadata (`ingest_time`, `source_topic`)
* writes Parquet partitioned by `event_date` / `event_type`

Spark runs in its own container and reaches Kafka over the internal
`kafka:29092` listener. Bronze output lands on your host at `./datalake/bronze`.

### Run it

1. **Rebuild the stack** (Kafka now exposes an internal listener, and the Spark
   service is new, so a rebuild is required):

   ```bash
   docker compose down
   docker compose up -d --build
   ```

   The first build downloads Java + PySpark and the Kafka connector jar, so give
   it a few minutes.

2. **Make sure events are flowing** (in a separate terminal, on the host):

   ```bash
   python producer/event_simulator.py
   ```

3. **Watch the Bronze job process micro-batches:**

   ```bash
   docker compose logs -f spark
   ```

   Every 10 seconds it prints how many events it wrote and a per-event-type
   breakdown.

4. **Inspect the Bronze layer** on your host — you'll see a partitioned tree:

   ```
   datalake/bronze/event_date=2026-06-18/event_type=ride_ended/part-*.parquet
   ```

### Config (env vars on the `spark` service)

| Variable          | Default                          | Meaning                       |
| ----------------- | -------------------------------- | ----------------------------- |
| `KAFKA_BOOTSTRAP` | `kafka:29092`                    | Broker (internal listener)    |
| `KAFKA_TOPIC`     | `ride-events`                    | Topic to consume              |
| `BRONZE_PATH`     | `/opt/datalake/bronze`           | Bronze output (host volume)   |
| `CHECKPOINT_PATH` | `/opt/datalake/_checkpoints/...` | Streaming checkpoint location  |

> Note on duration: the simulator emits events seconds apart for pacing, so the
> wall-clock gap between `ride_started` and `ride_ended` is *not* the real ride
> length. The true duration is the `duration_min` field on `ride_ended`, which is
> what Bronze surfaces. Phase 3 aggregates these into daily Silver insights.

## Phase 3 — Spark batch → Silver (done)

A daily Spark batch job (`spark/silver_batch.py`) reads the Bronze layer and
computes business insights into the **Silver** layer:

* `silver/daily_city_metrics` — total rides, avg ride duration, revenue per city/day
* `silver/hourly_demand` — ride requests per hour, with the peak hour flagged

It runs as a **one-off** container (it finishes and exits), so it's defined under
a `batch` compose profile and won't auto-start with `docker compose up`.

### Run it

Make sure Bronze has data first (Phase 2 must have processed some events). Then:

```bash
# process every date currently in Bronze
docker compose run --rm spark-batch

# or process a single day (what Airflow will pass in Phase 7)
docker compose run --rm -e PROCESS_DATE=2026-06-19 spark-batch
```

The job prints the daily metrics table and the peak hour per city to the console,
and writes Parquet to `./datalake/silver/...` on your host.

### Config (env vars on the `spark-batch` service)

| Variable       | Default                 | Meaning                                   |
| -------------- | ----------------------- | ----------------------------------------- |
| `BRONZE_PATH`  | `/opt/datalake/bronze`  | Input Bronze layer                        |
| `SILVER_PATH`  | `/opt/datalake/silver`  | Output Silver layer                       |
| `PROCESS_DATE` | *(unset)*               | `YYYY-MM-DD` to process one day; unset = all |

> Writes use dynamic partition overwrite, so re-running for a date replaces only
> that day's Silver partition — safe to re-run.

## Phase 4 — dbt → Snowflake Gold (done)

Two steps: a Python loader lands the local data lake in Snowflake `RAW`, then
dbt models it into the clean **Gold** layer.

```
datalake (Silver/Bronze parquet)
        │  load_to_snowflake.py
        ▼
   RAW.RIDES, RAW.SILVER_DAILY_CITY_METRICS, RAW.SILVER_HOURLY_DEMAND
        │  dbt (staging views → Gold tables) + seeds (driver/rider master)
        ▼
   GOLD.fct_rides, GOLD.fct_daily_city_metrics, GOLD.fct_hourly_demand,
   GOLD.dim_driver, GOLD.dim_rider
```

`fct_rides` is the headline model: it joins ride facts to the `dim_driver` and
`dim_rider` dimensions (seeded master data) so Power BI can slice revenue by
driver rating, rider segment, vehicle type, etc.

### One-time setup

1. **Create the warehouse/db/schemas** — run `warehouse/setup_snowflake.sql` in a
   Snowflake worksheet.
2. **Install deps** (in your conda env or a fresh venv):

   ```bash
   pip install -r warehouse/requirements.txt
   ```

3. **Add credentials** — copy `warehouse/.env.example` to `warehouse/.env`, fill
   it in, then load it into your shell (see comments at the top of that file).

### Run it

```bash
# 1. land RAW tables from the local data lake
python warehouse/load_to_snowflake.py

# 2. build the Gold layer with dbt
cd dbt
dbt deps          # (no-op unless you add packages)
dbt seed --profiles-dir .     # loads driver/rider master data
dbt run  --profiles-dir .     # builds staging views + Gold tables
dbt test --profiles-dir .     # runs the schema tests
```

After this, the Gold tables live in `RIDESHARE.GOLD` — ready for Power BI in
Phase 6.

> The loader uses `overwrite=True` (truncate + reload) and dbt models are rebuilt
> each run, so the whole step is safely re-runnable. Secrets stay out of the repo:
> both the loader and dbt read Snowflake credentials from environment variables.

## Phase 5 — Great Expectations data-quality gate (done)

`dq/validate.py` runs Great Expectations checks on the data lake and acts as a
**gate**: if any check fails it prints an alert and exits with code `1`, so the
orchestrator (Airflow, Phase 7) stops the pipeline instead of shipping bad data.

Checks (straight from the roadmap):

* **Bronze (rides):** no null `ride_id`; `ride_duration_min` not negative; `city`
  in the valid set; `fare_amount` positive.
* **Silver (daily_city_metrics):** `city` not null and valid; `revenue_pkr`
  positive; `total_rides` and `avg_duration_min` not negative.

### Run it

```bash
pip install -r dq/requirements.txt
python dq/validate.py
```

Output is a per-layer `[PASS]`/`[FAIL]` report. On failure it lists exactly which
column broke and how many bad values there were, then exits non-zero.

```
[PASS] Bronze (rides): all 4 checks passed
[PASS] Silver (daily_city_metrics): all 5 checks passed
>>> DATA QUALITY GATE PASSED -- safe to proceed (exit 0)
```

### Why the exit code matters

Airflow treats a non-zero exit as a failed task. So in Phase 7 the DAG runs
`validate.py` right after the Silver batch; if it exits `1`, the downstream dbt /
publish steps never run and Airflow fires the failure alert. That's the
"if checks fail → pipeline stops and alerts" requirement, enforced for real.

> The script reads `BRONZE_PATH` / `SILVER_PATH` env vars (defaulting to the local
> data lake), so the same code validates whatever paths Airflow passes it.

## Next: Phase 6 — connect Power BI to the Snowflake Gold tables and build the
dashboards (live demand map, revenue trends, peak hours, driver performance).

## Phase 7 — Airflow orchestrates the nightly batch: Silver job -> data-quality
gate -> dbt -> alert on failure.
