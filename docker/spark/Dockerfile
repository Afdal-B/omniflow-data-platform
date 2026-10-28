FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.15 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_NO_CACHE=1 \
    PATH=/opt/venv/bin:$PATH

WORKDIR /app

COPY pyproject.toml uv.lock .python-version README.md ./
RUN uv sync --frozen --no-dev --group spark --no-install-project

RUN useradd --create-home --uid 10001 spark && install -d -o spark /opt/ivy

ENV SPARK_PACKAGES="org.apache.hadoop:hadoop-aws:3.5.0,org.postgresql:postgresql:42.7.14,org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1" \
    SPARK_JARS_IVY=/opt/ivy
USER spark
RUN python -c "import os; from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]') \
.config('spark.jars.packages', os.environ['SPARK_PACKAGES']) \
.config('spark.jars.ivy', os.environ['SPARK_JARS_IVY']) \
.getOrCreate().stop()"

USER root
COPY src ./src
RUN uv sync --frozen --no-dev --group spark --no-editable
USER spark
