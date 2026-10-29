"""Spark session configured from the environment."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    """Spark session with the S3A and package settings of the environment."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if packages := os.environ.get("SPARK_PACKAGES"):
        builder = builder.config("spark.jars.packages", packages)
    if ivy_dir := os.environ.get("SPARK_JARS_IVY"):
        builder = builder.config("spark.jars.ivy", ivy_dir)

    if endpoint := os.environ.get("S3_ENDPOINT_URL"):
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config(
                "spark.hadoop.fs.s3a.endpoint.region",
                os.environ.get("AWS_REGION", "us-east-1"),
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(endpoint.startswith("https")),
            )
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
