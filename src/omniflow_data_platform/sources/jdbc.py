"""Ingestion of a relational table into bronze."""

from __future__ import annotations

import logging
import os

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from omniflow_data_platform.config import JdbcSource, PipelineConfig
from omniflow_data_platform.extraction import (
    PipelineState,
    extract_query,
    lower_bound_for,
)
from omniflow_data_platform.lake import Lake
from omniflow_data_platform.lake_fs import delete_path, path_exists
from omniflow_data_platform.quality import profile, split, with_quality_flags

log = logging.getLogger(__name__)

STATE_SCHEMA = "last_batch_id string, lower_bound string, high_watermark string"


def read_state(spark: SparkSession, path: str) -> PipelineState | None:
    """Incremental state of the pipeline, if any."""
    if not path_exists(spark, path):
        return None
    row = spark.read.schema(STATE_SCHEMA).json(path).first()
    return PipelineState(
        row["last_batch_id"], row["lower_bound"], row["high_watermark"]
    )


def write_state(spark: SparkSession, path: str, state: PipelineState) -> None:
    """Persist the incremental state."""
    spark.createDataFrame([state.__dict__], STATE_SCHEMA).coalesce(1).write.mode(
        "overwrite"
    ).json(path)


def read_table(spark: SparkSession, source: JdbcSource, query: str) -> DataFrame:
    """Read the query result over JDBC."""
    env, prefix = os.environ, source.connection
    url = (
        f"jdbc:postgresql://{env[f'{prefix}_HOST']}:{env.get(f'{prefix}_PORT', '5432')}"
        f"/{env[f'{prefix}_NAME']}"
    )
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", query)
        .option("user", env[f"{prefix}_USER"])
        .option("password", env[f"{prefix}_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", source.fetch_size)
        .load()
    )


def run(
    spark: SparkSession,
    pipeline: PipelineConfig,
    lake: Lake,
    batch_id: str,
    full_refresh: bool,
) -> dict:
    """Extract, check and write one batch to bronze and the DLQ."""
    source, target = pipeline.source, pipeline.target
    assert isinstance(source, JdbcSource)
    incremental = source.mode == "incremental"
    state_path = lake.state_path(pipeline.name)

    state = read_state(spark, state_path) if incremental else None
    lower_bound = lower_bound_for(source, state, batch_id, full_refresh)
    query = extract_query(source, lower_bound)
    log.info("Extracting: %s", query)

    rows = read_table(spark, source, query).select(
        "*",
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(f"{target.source}.{source.table}").alias("_source"),
    )
    flagged = with_quality_flags(rows, pipeline.quality).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    rows_valid, rows_rejected, reasons = profile(flagged)
    valid, invalid = split(flagged)

    bronze = lake.bronze_path(target.source, target.dataset, batch_id)
    dlq = lake.dlq_path(target.source, target.dataset, batch_id)
    valid.write.mode("overwrite").parquet(bronze)
    if rows_rejected:
        invalid.write.mode("overwrite").json(dlq)
    elif path_exists(spark, dlq):
        delete_path(spark, dlq)

    if incremental:
        new_max = flagged.select(
            F.date_format(
                F.max(source.watermark_column), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
            )
        ).first()[0]
        high_watermark = new_max or (state.high_watermark if state else None)
        write_state(
            spark, state_path, PipelineState(batch_id, lower_bound, high_watermark)
        )
    flagged.unpersist()

    return {
        "mode": "full" if lower_bound is None else f"incremental (> {lower_bound})",
        "rows_read": rows_valid + rows_rejected,
        "rows_valid": rows_valid,
        "rows_rejected": rows_rejected,
        "rejection_reasons": dict(reasons),
    }
