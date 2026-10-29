"""Ingestion of a Kafka topic into bronze with Structured Streaming."""

from __future__ import annotations

import logging
import os
from collections import Counter

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from omniflow_data_platform.config import KafkaSource, PipelineConfig, WindowAggregation
from omniflow_data_platform.lake import Lake
from omniflow_data_platform.lake_fs import delete_path
from omniflow_data_platform.quality import (
    CORRUPT_RECORD_COL,
    is_valid,
    profile,
    split,
    with_quality_flags,
)

log = logging.getLogger(__name__)

KAFKA_COLUMNS = ["_kafka_partition", "_kafka_offset", "_kafka_ts"]
METADATA_COLUMNS = ["_batch_id", "_ingested_at", "_source"]


def read_events(spark: SparkSession, source: KafkaSource) -> DataFrame:
    """Stream of the topic events, parsed from JSON."""
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .option("subscribe", source.topic)
        .option("startingOffsets", source.starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if source.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", source.max_offsets_per_trigger)

    raw = reader.load().select(
        F.col("value").cast("string").alias("_raw_payload"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_ts"),
    )
    return parse_payload(raw, source)


def parse_payload(raw: DataFrame, source: KafkaSource) -> DataFrame:
    """Parse the JSON payload, keeping unreadable messages."""
    schema = f"{source.schema_ddl}, {CORRUPT_RECORD_COL} string"
    parse_options = {
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": CORRUPT_RECORD_COL,
    }
    return (
        raw.withColumn("_parsed", F.from_json("_raw_payload", schema, parse_options))
        .select("*", "_parsed.*")
        .drop("_parsed")
    )


def with_metadata(df: DataFrame, batch_id: str, source_name: str) -> DataFrame:
    """Add the batch and lineage columns."""
    return df.select(
        "*",
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(source_name).alias("_source"),
    )


def window_counts(valid_events: DataFrame, agg: WindowAggregation) -> DataFrame:
    """Event counts per time window, with a watermark."""
    measures = {"event_count": F.count(F.lit(1))}
    if agg.distinct_count_column:
        measures[f"unique_{agg.distinct_count_column}"] = F.approx_count_distinct(
            agg.distinct_count_column
        )
    return (
        valid_events.withWatermark(agg.time_column, agg.watermark)
        .groupBy(F.window(agg.time_column, agg.window), *agg.group_by)
        .agg(*(expr.alias(name) for name, expr in measures.items()))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            *agg.group_by,
            *measures,
        )
    )


def run(
    spark: SparkSession,
    pipeline: PipelineConfig,
    lake: Lake,
    batch_id: str,
    full_refresh: bool,
) -> dict:
    """Process the available messages into bronze, the DLQ and aggregates."""
    source, target = pipeline.source, pipeline.target
    assert isinstance(source, KafkaSource)
    streams = [target.dataset, *(agg.dataset for agg in pipeline.aggregations)]

    if full_refresh:
        log.warning(
            "Full refresh: checkpoints deleted, the topic is read from the start"
        )
        for stream in streams:
            delete_path(spark, lake.checkpoint_path(pipeline.name, stream))

    source_name = f"{target.source}.{source.topic}"
    events = with_quality_flags(
        with_metadata(read_events(spark, source), batch_id, source_name),
        pipeline.quality,
        check_parsing=True,
    )
    bronze = lake.bronze_path(target.source, target.dataset, batch_id)
    dlq = lake.dlq_path(target.source, target.dataset, batch_id)
    bronze_columns = [f.name for f in source.fields] + KAFKA_COLUMNS + METADATA_COLUMNS
    totals = {"micro_batches": 0, "rows_valid": 0, "rows_rejected": 0}
    reasons_total: Counter[str] = Counter()

    def write_micro_batch(batch: DataFrame, epoch_id: int) -> None:
        batch.persist()
        rows_valid, rows_rejected, reasons = profile(batch)
        valid, invalid = split(batch)
        if rows_valid:
            if target.deduplicate_on:
                valid = valid.dropDuplicates(target.deduplicate_on)
            valid.select(*bronze_columns).write.mode("append").parquet(bronze)
        if rows_rejected:
            invalid.write.mode("append").json(dlq)
        batch.unpersist()
        totals["micro_batches"] += 1
        totals["rows_valid"] += rows_valid
        totals["rows_rejected"] += rows_rejected
        reasons_total.update(reasons)
        log.info(
            "micro-batch %s: %s valid, %s rejected", epoch_id, rows_valid, rows_rejected
        )

    queries = [
        events.writeStream.foreachBatch(write_micro_batch)
        .option(
            "checkpointLocation", lake.checkpoint_path(pipeline.name, target.dataset)
        )
        .trigger(availableNow=True)
        .start()
    ]

    valid_events = events.filter(is_valid())
    for agg in pipeline.aggregations:
        agg_path = lake.bronze_path(target.source, agg.dataset, batch_id)

        def write_windows(
            batch: DataFrame, epoch_id: int, path: str = agg_path
        ) -> None:
            if not batch.isEmpty():
                with_metadata(batch, batch_id, source_name).write.mode(
                    "append"
                ).parquet(path)

        queries.append(
            window_counts(valid_events, agg)
            .writeStream.outputMode("append")
            .foreachBatch(write_windows)
            .option(
                "checkpointLocation", lake.checkpoint_path(pipeline.name, agg.dataset)
            )
            .trigger(availableNow=True)
            .start()
        )

    for query in queries:
        query.awaitTermination()

    return {
        "mode": "stream (available now)",
        **totals,
        "rows_read": totals["rows_valid"] + totals["rows_rejected"],
        "rejection_reasons": dict(reasons_total),
    }
