from __future__ import annotations

import argparse
import json
import logging
import os

from omniflow_data_platform.config import load_pipeline
from omniflow_data_platform.lake import Lake
from omniflow_data_platform.lake_fs import read_text
from omniflow_data_platform.run_reports import STEPS, merge_reports
from omniflow_data_platform.spark_session import build_spark
from omniflow_data_platform.warehouse import WarehouseConnection, record_pipeline_run

log = logging.getLogger("record_run")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Record a pipeline run for monitoring."
    )
    parser.add_argument(
        "--pipeline", required=True, help="config/pipelines/<name>.yaml"
    )
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args(argv)

    pipeline = load_pipeline(args.pipeline)
    lake = Lake(os.environ["LAKE_BUCKET"])
    spark = build_spark(f"record-{pipeline.name}")
    try:
        reports = {}
        for step in STEPS:
            text = read_text(
                spark, lake.run_report_path(pipeline.name, args.batch_id, step)
            )
            reports[step] = json.loads(text) if text else None
    finally:
        spark.stop()

    run = merge_reports(
        pipeline.name, args.batch_id, reports["ingest"], reports["load"]
    )
    with WarehouseConnection.from_env().connect() as conn:
        record_pipeline_run(conn, run)
    log.info("Recorded run: %s", json.dumps(run))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    main()
