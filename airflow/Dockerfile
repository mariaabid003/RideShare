# Airflow image for the RideShare Phase-7 orchestration.
#
# Build context is the PROJECT ROOT (see docker-compose.airflow.yml) so we can
# reuse the repo's own requirement files. Each pipeline step gets an isolated
# virtualenv, so their dependency sets never collide with Airflow's own pins:
#
#   /opt/venv/spark      -> pyspark 3.5.1        (spark/silver_batch.py)
#   /opt/venv/dq         -> great-expectations   (dq/validate.py)
#   /opt/venv/warehouse  -> snowflake + dbt      (load_to_snowflake.py, dbt build)
FROM apache/airflow:2.9.3-python3.11

# --- Java 17 for PySpark (needs root) ---
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Repo requirement files (copied from the project-root build context)
COPY warehouse/requirements.txt /tmp/warehouse-req.txt
COPY dq/requirements.txt        /tmp/dq-req.txt

# /opt is root-owned; create the venv root and hand it to the airflow user
# so the non-root build steps below can write into it.
RUN mkdir -p /opt/venv && chown -R airflow: /opt/venv

# --- Isolated virtualenvs (as the airflow user) ---
USER airflow
RUN python -m venv /opt/venv/spark \
    && /opt/venv/spark/bin/pip install --no-cache-dir \
       pyspark==3.5.1 pandas==2.2.2 pyarrow==16.1.0

RUN python -m venv /opt/venv/warehouse \
    && /opt/venv/warehouse/bin/pip install --no-cache-dir -r /tmp/warehouse-req.txt

RUN python -m venv /opt/venv/dq \
    && /opt/venv/dq/bin/pip install --no-cache-dir -r /tmp/dq-req.txt
