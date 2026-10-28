from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from omniflow_data_platform.config import (
    PIPELINES_DIR,
    PipelineConfig,
    load_all_pipelines,
    load_pipeline,
)


def pipeline_dict(**overrides) -> dict:
    pipeline = {
        "name": "demo",
        "description": "demo",
        "orchestration": {"schedule": "@daily"},
        "source": {
            "type": "jdbc",
            "connection": "OLTP_DB",
            "table": "shop.demo",
            "mode": "full",
        },
        "target": {"source": "oltp", "dataset": "demo"},
    }
    return pipeline | overrides


def test_every_pipeline_in_the_repo_is_valid():
    pipelines = load_all_pipelines(PIPELINES_DIR)
    assert {p.name for p in pipelines} == {
        "clickstream_events",
        "oltp_customers",
        "oltp_products",
        "oltp_orders",
        "oltp_order_items",
    }


def test_unknown_keys_are_rejected():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        PipelineConfig.model_validate(pipeline_dict(descripton="typo"))


@pytest.mark.parametrize("table", ["shop.demo; DROP TABLE x", "Shop.Demo", "demo"])
def test_table_names_are_constrained(table):
    source = pipeline_dict()["source"] | {"table": table}
    with pytest.raises(ValidationError):
        PipelineConfig.model_validate(pipeline_dict(source=source))


def test_incremental_mode_requires_a_watermark_column():
    source = pipeline_dict()["source"] | {"mode": "incremental"}
    with pytest.raises(ValidationError, match="watermark_column"):
        PipelineConfig.model_validate(pipeline_dict(source=source))


def test_unknown_check_is_rejected():
    rules = [{"check": "is_positive", "column": "price"}]
    with pytest.raises(ValidationError):
        PipelineConfig.model_validate(pipeline_dict(quality=rules))


def test_range_rule_needs_a_bound():
    rules = [{"check": "range", "column": "price"}]
    with pytest.raises(ValidationError, match="min"):
        PipelineConfig.model_validate(pipeline_dict(quality=rules))


def test_file_name_must_match_pipeline_name(tmp_path: Path):
    (tmp_path / "other.yaml").write_text(yaml.safe_dump(pipeline_dict()))
    with pytest.raises(ValueError, match="'name' must be 'other'"):
        load_pipeline("other", tmp_path)


def kafka_source(**overrides) -> dict:
    return {
        "type": "kafka",
        "topic": "shop.clickstream.v1",
        "fields": [{"name": "event_id", "type": "string"}],
    } | overrides


def test_kafka_source_builds_the_spark_schema():
    source = kafka_source(
        fields=[
            {"name": "event_id", "type": "string"},
            {"name": "event_ts", "type": "timestamp"},
        ]
    )
    pipeline = PipelineConfig.model_validate(pipeline_dict(source=source))
    assert pipeline.source.schema_ddl == "event_id string, event_ts timestamp"


def test_unknown_field_type_is_rejected():
    source = kafka_source(fields=[{"name": "event_id", "type": "varchar"}])
    with pytest.raises(ValidationError):
        PipelineConfig.model_validate(pipeline_dict(source=source))


def test_aggregations_are_rejected_on_batch_sources():
    agg = {
        "dataset": "x_5min",
        "time_column": "ts",
        "window": "5 minutes",
        "watermark": "1 minute",
    }
    with pytest.raises(ValidationError, match="only supported on kafka"):
        PipelineConfig.model_validate(pipeline_dict(aggregations=[agg]))


@pytest.mark.parametrize("interval", ["5 min", "five minutes", "5minutes"])
def test_intervals_must_be_spark_durations(interval):
    rule = {
        "check": "timestamp_window",
        "column": "event_ts",
        "reference": "_kafka_ts",
        "max_lag": interval,
        "max_lead": "5 minutes",
    }
    with pytest.raises(ValidationError):
        PipelineConfig.model_validate(
            pipeline_dict(source=kafka_source(), quality=[rule])
        )
