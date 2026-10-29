"""Job loading bronze batches into the warehouse raw schema."""

from __future__ import annotations

import argparse
import json
import logging
import os
import time

from pyspark.sql import SparkSession

from omniflow_data_platform.config import load_pipeline, raw_table_name
from omniflow_data_platform.lake import Lake
from omniflow_data_platform.lake_fs import path_exists, write_text
from omniflow_data_platform.run_reports import run_step
from omniflow_data_platform.spark_session import build_spark
from omniflow_data_platform.warehouse import (
    WarehouseConnection,
    publish,
    transit_table_name,
)

log = logging.getLogger("load_to_warehouse")

PARTITION_COLUMNS = ("ingestion_date", "batch_id")


def load_dataset(
    spark: SparkSession,
    lake: Lake,
    warehouse: WarehouseConnection,
    source: str,
    dataset: str,
    batch_id: str | None,
) -> int | None:
    """Load one dataset into its raw table; None when nothing to load."""
    if batch_id is None:
        path = lake.dataset_path("bronze", source, dataset)
    else:
        path = lake.bronze_path(source, dataset, batch_id)
    if not path_exists(spark, path):
        return None

    rows = spark.read.parquet(path)
    rows = rows.drop(*(c for c in PARTITION_COLUMNS if c in rows.columns))

    table = raw_table_name(source, dataset)
    rows.write.jdbc(
        url=warehouse.jdbc_url,
        table=f"raw.{transit_table_name(table)}",
        mode="overwrite",
        properties={
            "user": warehouse.user,
            "password": warehouse.password,
            "driver": "org.postgresql.Driver",
        },
    )
    with warehouse.connect() as conn:
        return publish(conn, table, batch_id)


def main(argv: list[str] | None = None) -> None:
    """Load a pipeline batch, or its whole history, into the warehouse."""
    parser = argparse.ArgumentParser(description="Load bronze data into the warehouse.")
    parser.add_argument(
        "--pipeline", required=True, help="config/pipelines/<name>.yaml"
    )
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--batch-id", help="load this batch only")
    scope.add_argument(
        "--all-batches", action="store_true", help="reload the whole bronze history"
    )
    args = parser.parse_args(argv)

    pipeline = load_pipeline(args.pipeline)
    lake = Lake(os.environ["LAKE_BUCKET"])
    warehouse = WarehouseConnection.from_env()
    spark = build_spark(f"load-{pipeline.name}")
    started = time.monotonic()

    def load_all() -> dict:
        loaded = {}
        for dataset in pipeline.datasets:
            table = f"raw.{raw_table_name(pipeline.target.source, dataset)}"
            loaded[table] = load_dataset(
                spark, lake, warehouse, pipeline.target.source, dataset, args.batch_id
            )
        return {"rows_loaded": loaded}

    try:
        if args.batch_id is None:
            details = load_all()
        else:
            details = run_step(
                "load",
                pipeline.name,
                args.batch_id,
                work=load_all,
                save=lambda report: write_text(
                    spark,
                    lake.run_report_path(pipeline.name, args.batch_id, "load"),
                    report.to_json(),
                ),
            )
    finally:
        spark.stop()
    summary = {
        "pipeline": pipeline.name,
        "batch_id": args.batch_id or "all",
        **details,
        "duration_s": round(time.monotonic() - started, 1),
    }
    log.info("Load summary: %s", json.dumps(summary))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    main()
