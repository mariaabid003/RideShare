# Minimal, reliable PySpark image (avoids vendor image tag churn).
# Java 17 + Python 3.11 + pyspark 3.5.1, which matches the Kafka connector
# package pinned in docker-compose.yml.
# Pin to Debian 12 (bookworm): it still ships Java 17, which Spark 3.5 supports.
# (Newer slim tags moved to Debian 13/trixie, which only offers Java 21.)
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN pip install --no-cache-dir pyspark==3.5.1

WORKDIR /opt/app
