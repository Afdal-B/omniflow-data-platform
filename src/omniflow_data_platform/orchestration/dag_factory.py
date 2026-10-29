"""Airflow DAGs generated from the pipeline configurations."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Asset, Param, chain
from docker.types import Mount

from omniflow_data_platform.config import (
    PipelineConfig,
    load_all_pipelines,
    raw_table_name,
)

SPARK_IMAGE = os.environ.get("STRATUM_SPARK_IMAGE", "stratum/spark-jobs:latest")
DBT_IMAGE = os.environ.get("STRATUM_DBT_IMAGE", "stratum/dbt:latest")
TRANSFORM_DAG_ID = "transform__dbt"
DOCKER_NETWORK = os.environ.get("STRATUM_DOCKER_NETWORK", "stratum_default")
HOST_PROJECT_DIR = os.environ.get("STRATUM_HOST_DIR", "")
SPARK_POOL = "spark"

JOB_ENV_VARS = (
    "OLTP_DB_HOST",
    "OLTP_DB_PORT",
    "OLTP_DB_NAME",
    "OLTP_DB_USER",
    "OLTP_DB_PASSWORD",
    "LAKE_BUCKET",
    "S3_ENDPOINT_URL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "KAFKA_BOOTSTRAP_SERVERS",
    "SPARK_DRIVER_MEMORY",
    "WAREHOUSE_DB_HOST",
    "WAREHOUSE_DB_PORT",
    "WAREHOUSE_DB_NAME",
    "WAREHOUSE_DB_USER",
    "WAREHOUSE_DB_PASSWORD",
)

BATCH_ID = "{{ dag_run.run_after.strftime('%Y%m%dT%H%M%SZ') }}"


def raw_table_asset(source: str, dataset: str) -> Asset:
    """Airflow asset of a warehouse raw table."""
    return Asset(name=f"warehouse.raw.{raw_table_name(source, dataset)}")


def spark_job(task_id: str, command: str, **kwargs) -> DockerOperator:
    """Task running a Spark job in its container."""
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=command,
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        private_environment={
            name: os.environ[name] for name in JOB_ENV_VARS if name in os.environ
        },
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_DIR}/config",
                target="/app/config",
                type="bind",
                read_only=True,
            )
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        pool=SPARK_POOL,
        execution_timeout=timedelta(minutes=30),
        **kwargs,
    )


def _doc(pipeline: PipelineConfig) -> str:
    """Markdown documentation of an ingestion DAG."""
    rules = "\n".join(f"- `{r.check}` on `{r.column}`" for r in pipeline.quality)
    location = getattr(pipeline.source, "table", None) or pipeline.source.topic
    return (
        f"### {pipeline.name}\n\n{pipeline.description}\n\n"
        f"**Source**: `{pipeline.source.type}` `{location}` -> "
        f"**bronze**: `{pipeline.target.source}/{pipeline.target.dataset}`\n\n"
        f"**Quality rules** (violations go to the DLQ):\n{rules or '- none'}\n\n"
        f"_Generated from `config/pipelines/{pipeline.name}.yaml`: "
        "edit the YAML, not the DAG._"
    )


def build_dag(pipeline: PipelineConfig) -> DAG:
    """Ingestion DAG of a pipeline: ingest, load, then record the run."""
    orchestration = pipeline.orchestration
    with DAG(
        dag_id=f"ingest__{pipeline.name}",
        description=pipeline.description,
        doc_md=_doc(pipeline),
        schedule=orchestration.schedule,
        start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=["stratum", "ingestion", pipeline.source.type, *orchestration.tags],
        default_args={
            "owner": orchestration.owner,
            "retries": orchestration.retries,
            "retry_delay": timedelta(minutes=2),
            "retry_exponential_backoff": True,
        },
        params={
            "full_refresh": Param(
                False,
                type="boolean",
                description="Ignore the watermark / Kafka checkpoints and read everything.",
            )
        },
    ) as dag:
        ingest = spark_job(
            "ingest_to_bronze",
            "python -m omniflow_data_platform.jobs.run_pipeline"
            f" --pipeline {pipeline.name} --batch-id {BATCH_ID}"
            "{% if params.full_refresh %} --full-refresh{% endif %}",
        )
        load = spark_job(
            "load_to_warehouse",
            "python -m omniflow_data_platform.jobs.load_to_warehouse"
            f" --pipeline {pipeline.name} --batch-id {BATCH_ID}",
            outlets=[
                raw_table_asset(pipeline.target.source, dataset)
                for dataset in pipeline.datasets
            ],
        )
        record = spark_job(
            "record_run",
            "python -m omniflow_data_platform.jobs.record_run"
            f" --pipeline {pipeline.name} --batch-id {BATCH_ID}",
            retries=1,
        )
        ingest >> load >> record.as_teardown()
    return dag


def dbt_task(task_id: str, command: str) -> DockerOperator:
    """Task running a dbt command in its container."""
    env = os.environ
    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        command=command,
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        private_environment={
            "WAREHOUSE_DB_HOST": env.get("WAREHOUSE_DB_HOST", ""),
            "WAREHOUSE_DB_PORT": env.get("WAREHOUSE_DB_PORT", "5432"),
            "WAREHOUSE_DB_NAME": env.get("WAREHOUSE_DB_NAME", ""),
            "WAREHOUSE_DB_USER": env.get("WAREHOUSE_DB_USER", ""),
            "DBT_ENV_SECRET_WAREHOUSE_PASSWORD": env.get("WAREHOUSE_DB_PASSWORD", ""),
        },
        mount_tmp_dir=False,
        auto_remove="success",
        execution_timeout=timedelta(minutes=30),
    )


def build_transform_dag(pipelines: list[PipelineConfig]) -> DAG:
    """dbt DAG triggered by the update of the raw tables."""
    with DAG(
        dag_id=TRANSFORM_DAG_ID,
        description="dbt: staging -> intermediate (silver) -> marts (gold), tested layer by layer.",
        doc_md=(
            "### dbt transformations\n\n"
            "Triggered when **every** raw table has received a new batch (Airflow Assets "
            "emitted by the `load_to_warehouse` tasks of the `ingest__*` DAGs).\n\n"
            "Each layer runs `dbt build` (models, then their data tests and unit tests): a "
            "failing test stops the run before the next layer is built on wrong data."
        ),
        schedule=[
            raw_table_asset(pipeline.target.source, dataset)
            for pipeline in pipelines
            for dataset in pipeline.datasets
        ],
        start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=["stratum", "transformation", "dbt"],
        default_args={
            "owner": "analytics-engineering",
            "retries": 1,
            "retry_delay": timedelta(minutes=2),
        },
    ) as dag:
        chain(
            dbt_task("source_freshness", "source freshness"),
            *(
                dbt_task(f"build_{layer}", f"build --select path:models/{layer}")
                for layer in ("staging", "intermediate", "marts")
            ),
        )
    return dag


def build_all_dags() -> list[DAG]:
    """Every ingestion DAG and the transformation DAG."""
    pipelines = load_all_pipelines()
    return [
        *(build_dag(pipeline) for pipeline in pipelines),
        build_transform_dag(pipelines),
    ]
