"""Pipeline configuration contract, validated from the YAML files."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

PIPELINES_DIR = Path(os.environ.get("STRATUM_CONFIG_DIR", "config")) / "pipelines"

Identifier = Annotated[str, Field(pattern=r"^[a-z_][a-z0-9_]*$")]
QualifiedTable = Annotated[str, Field(pattern=r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")]
Interval = Annotated[str, Field(pattern=r"^\d+ (second|minute|hour|day)s?$")]
SparkType = Annotated[
    str,
    Field(
        pattern=r"^(string|boolean|int|bigint|double|timestamp|date|decimal\(\d+,\d+\))$"
    ),
]


class StrictModel(BaseModel):
    """Immutable model rejecting unknown keys."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class NotNullRule(StrictModel):
    """Rejects rows where the column is null."""

    check: Literal["not_null"]
    column: Identifier


class AcceptedValuesRule(StrictModel):
    """Rejects rows whose value is not in the allowed list."""

    check: Literal["accepted_values"]
    column: Identifier
    values: list[str | int] = Field(min_length=1)


class RegexRule(StrictModel):
    """Rejects rows whose value does not match the pattern."""

    check: Literal["regex"]
    column: Identifier
    pattern: str


class RangeRule(StrictModel):
    """Rejects rows whose value is outside [min, max]."""

    check: Literal["range"]
    column: Identifier
    min: float | None = None
    max: float | None = None

    @model_validator(mode="after")
    def _has_a_bound(self) -> RangeRule:
        if self.min is None and self.max is None:
            raise ValueError("a range rule needs 'min' and/or 'max'")
        return self


class TimestampWindowRule(StrictModel):
    """Rejects timestamps too far from a reference column."""

    check: Literal["timestamp_window"]
    column: Identifier
    reference: Identifier
    max_lag: Interval
    max_lead: Interval


QualityRule = Annotated[
    NotNullRule | AcceptedValuesRule | RegexRule | RangeRule | TimestampWindowRule,
    Field(discriminator="check"),
]


class JdbcSource(StrictModel):
    """Relational table read over JDBC, fully or incrementally."""

    type: Literal["jdbc"]
    connection: Annotated[str, Field(pattern=r"^[A-Z][A-Z0-9_]*$")]
    table: QualifiedTable
    mode: Literal["full", "incremental"]
    watermark_column: Identifier | None = None
    lookback_minutes: int = Field(default=0, ge=0)
    fetch_size: int = Field(default=10_000, gt=0)

    @model_validator(mode="after")
    def _incremental_needs_a_watermark(self) -> JdbcSource:
        if self.mode == "incremental" and self.watermark_column is None:
            raise ValueError("incremental mode requires 'watermark_column'")
        return self


class FieldSpec(StrictModel):
    """Name and Spark type of a field in a JSON payload."""

    name: Identifier
    type: SparkType


class KafkaSource(StrictModel):
    """Kafka topic carrying JSON events."""

    type: Literal["kafka"]
    topic: Annotated[str, Field(pattern=r"^[A-Za-z0-9._-]+$")]
    starting_offsets: Literal["earliest", "latest"] = "earliest"
    max_offsets_per_trigger: int | None = Field(default=None, gt=0)
    fields: list[FieldSpec] = Field(min_length=1)

    @property
    def schema_ddl(self) -> str:
        """Spark DDL schema of the payload."""
        return ", ".join(f"{f.name} {f.type}" for f in self.fields)


Source = Annotated[JdbcSource | KafkaSource, Field(discriminator="type")]


class WindowAggregation(StrictModel):
    """Windowed count computed on the stream."""

    dataset: Identifier
    time_column: Identifier
    window: Interval
    watermark: Interval
    group_by: list[Identifier] = []
    distinct_count_column: Identifier | None = None


class Target(StrictModel):
    """Bronze dataset written by the pipeline."""

    source: Identifier
    dataset: Identifier
    deduplicate_on: list[Identifier] = []


class Orchestration(StrictModel):
    """Airflow scheduling settings of the pipeline."""

    schedule: str
    retries: int = Field(default=2, ge=0)
    owner: str = "data-engineering"
    tags: list[str] = []


class PipelineConfig(StrictModel):
    """Full definition of one ingestion pipeline."""

    name: Identifier
    description: str
    orchestration: Orchestration
    source: Source
    quality: list[QualityRule] = []
    target: Target
    aggregations: list[WindowAggregation] = []

    @property
    def datasets(self) -> list[str]:
        """Bronze datasets produced: the target, then aggregations."""
        return [self.target.dataset, *(agg.dataset for agg in self.aggregations)]

    @model_validator(mode="after")
    def _aggregations_need_a_stream(self) -> PipelineConfig:
        if self.aggregations and self.source.type != "kafka":
            raise ValueError("window aggregations are only supported on kafka sources")
        return self


def raw_table_name(source: str, dataset: str) -> str:
    """Warehouse raw table name of a bronze dataset."""
    return f"{source}_{dataset}"


def load_pipeline(name: str, pipelines_dir: Path = PIPELINES_DIR) -> PipelineConfig:
    """Load and validate config/pipelines/<name>.yaml."""
    path = pipelines_dir / f"{name}.yaml"
    if not path.is_file():
        raise FileNotFoundError(f"no pipeline configuration at {path}")
    pipeline = PipelineConfig.model_validate(yaml.safe_load(path.read_text()))
    if pipeline.name != path.stem:
        raise ValueError(
            f"{path.name}: 'name' must be '{path.stem}', got '{pipeline.name}'"
        )
    return pipeline


def load_all_pipelines(pipelines_dir: Path = PIPELINES_DIR) -> list[PipelineConfig]:
    """Load every pipeline configuration, sorted by name."""
    return [
        load_pipeline(path.stem, pipelines_dir)
        for path in sorted(pipelines_dir.glob("*.yaml"))
    ]
