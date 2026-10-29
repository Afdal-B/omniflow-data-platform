"""Tests of the generated Airflow DAGs."""

from itertools import pairwise

import pytest

pytest.importorskip("airflow.sdk")

from airflow.dag_processing.dagbag import DagBag

from omniflow_data_platform.config import load_all_pipelines
from omniflow_data_platform.orchestration.dag_factory import (
    TRANSFORM_DAG_ID,
    build_all_dags,
)


@pytest.fixture(scope="module")
def dags():
    return {dag.dag_id: dag for dag in build_all_dags()}


def test_one_dag_per_pipeline_yaml_plus_dbt(dags):
    expected = {f"ingest__{p.name}" for p in load_all_pipelines()} | {TRANSFORM_DAG_ID}
    assert set(dags) == expected


def test_schedules_come_from_the_yaml(dags):
    assert dags["ingest__clickstream_events"].schedule == "*/15 * * * *"
    assert dags["ingest__oltp_orders"].schedule == "@hourly"


def test_runs_of_a_pipeline_never_overlap(dags):
    assert all(dag.max_active_runs == 1 and not dag.catchup for dag in dags.values())


def test_ingestion_then_load_in_the_spark_image(dags):
    dag = dags["ingest__oltp_orders"]
    ingest, load = dag.get_task("ingest_to_bronze"), dag.get_task("load_to_warehouse")
    assert load.task_id in ingest.downstream_task_ids
    for task in (ingest, load):
        assert task.image == "stratum/spark-jobs:latest"
        assert "--pipeline oltp_orders --batch-id {{" in task.command
        assert task.pool == "spark"
        assert task.retries == 2


def test_load_tasks_announce_the_raw_tables_they_update(dags):
    load = dags["ingest__clickstream_events"].get_task("load_to_warehouse")
    assert {asset.name for asset in load.outlets} == {
        "warehouse.raw.events_clickstream",
        "warehouse.raw.events_clickstream_traffic_5min",
    }


def test_airflow_parses_the_dags_folder_without_errors():
    bag = DagBag(dag_folder="airflow/dags")
    assert bag.import_errors == {}
    assert len(bag.dag_ids) == len(load_all_pipelines()) + 1


def test_dbt_runs_when_every_raw_table_was_updated(dags):
    dag = dags[TRANSFORM_DAG_ID]
    produced = {
        asset.name
        for name, d in dags.items()
        if name.startswith("ingest__")
        for asset in d.get_task("load_to_warehouse").outlets
    }
    assert {asset.name for asset in dag.schedule} == produced


def test_dbt_layers_run_in_order(dags):
    dag = dags[TRANSFORM_DAG_ID]
    order = ["source_freshness", "build_staging", "build_intermediate", "build_marts"]
    for upstream, downstream in pairwise(order):
        assert downstream in dag.get_task(upstream).downstream_task_ids


def test_record_run_is_a_teardown_after_ingest_and_load(dags):
    dag = dags["ingest__oltp_orders"]
    record = dag.get_task("record_run")
    assert record.is_teardown
    assert record.upstream_task_ids == {"load_to_warehouse"}
