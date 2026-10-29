"""Job ingesting one pipeline batch into the bronze zone."""

from __future__ import annotations

import argparse
import json
import logging
import os
import time

from omniflow_data_platform.config import load_pipeline
from omniflow_data_platform.lake import Lake, new_batch_id
from omniflow_data_platform.lake_fs import write_text
from omniflow_data_platform.run_reports import run_step
from omniflow_data_platform.sources import jdbc, kafka
from omniflow_data_platform.spark_session import build_spark

log = logging.getLogger("run_pipeline")

RUNNERS = {"jdbc": jdbc.run, "kafka": kafka.run}


def main(argv: list[str] | None = None) -> None:
    """Run the ingest step of a pipeline."""
    parser = argparse.ArgumentParser(description="Run one ingestion pipeline.")
    parser.add_argument(
        "--pipeline", required=True, help="config/pipelines/<name>.yaml"
    )
    parser.add_argument(
        "--batch-id", default=None, help="defaults to the current UTC time"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="ignore the watermark / Kafka checkpoints and read everything again",
    )
    args = parser.parse_args(argv)

    pipeline = load_pipeline(args.pipeline)
    batch_id = args.batch_id or new_batch_id()
    lake = Lake(os.environ["LAKE_BUCKET"])

    spark = build_spark(f"pipeline-{pipeline.name}")
    started = time.monotonic()
    run = RUNNERS[pipeline.source.type]
    try:
        details = run_step(
            "ingest",
            pipeline.name,
            batch_id,
            work=lambda: run(spark, pipeline, lake, batch_id, args.full_refresh),
            save=lambda report: write_text(
                spark,
                lake.run_report_path(pipeline.name, batch_id, "ingest"),
                report.to_json(),
            ),
        )
    finally:
        spark.stop()
    summary = {
        "pipeline": pipeline.name,
        "batch_id": batch_id,
        **details,
        "duration_s": round(time.monotonic() - started, 1),
    }
    log.info("Run summary: %s", json.dumps(summary))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    main()
