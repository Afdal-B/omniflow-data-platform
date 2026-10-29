"""Airflow entry point exposing the DAGs generated from config/pipelines."""

from airflow.sdk import DAG

from omniflow_data_platform.orchestration.dag_factory import build_all_dags

dags: list[DAG] = build_all_dags()

for _dag in dags:
    globals()[_dag.dag_id] = _dag
