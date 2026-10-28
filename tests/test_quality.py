import subprocess

import pytest
from pydantic import TypeAdapter

from omniflow_data_platform.config import QualityRule


def java_available() -> bool:
    try:
        return (
            subprocess.run(
                ["java", "-version"], check=False, capture_output=True
            ).returncode
            == 0
        )
    except FileNotFoundError:
        return False


if not java_available():
    pytest.skip("Spark tests need Java", allow_module_level=True)

from pyspark.sql import SparkSession

from omniflow_data_platform.quality import (
    REASONS_COL,
    profile,
    split,
    with_quality_flags,
)

RULES = TypeAdapter(list[QualityRule]).validate_python(
    [
        {"check": "not_null", "column": "id"},
        {
            "check": "accepted_values",
            "column": "segment",
            "values": ["consumer", "business"],
        },
        {
            "check": "regex",
            "column": "email",
            "pattern": r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$",
        },
        {"check": "range", "column": "price", "min": 0, "max": 1000},
    ]
)
ROWS = [
    (1, "consumer", "a@b.fr", 10.0),
    (2, None, None, None),
    (None, "consumer", "a@b.fr", 10.0),
    (4, "enterprise", "nope", -5.0),
]


@pytest.fixture(scope="module")
def flagged():
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    df = spark.createDataFrame(
        ROWS, "id long, segment string, email string, price double"
    )
    yield with_quality_flags(df, RULES)
    spark.stop()


def reasons_by_id(df) -> dict:
    return {row["id"]: row[REASONS_COL] for row in df.collect()}


def test_valid_rows_have_no_reason(flagged):
    reasons = reasons_by_id(flagged)
    assert reasons[1] == []
    assert reasons[2] == []


def test_every_broken_rule_is_reported(flagged):
    reasons = reasons_by_id(flagged)
    assert reasons[None] == ["not_null:id"]
    assert reasons[4] == ["accepted_values:segment", "regex:email", "range:price"]


def test_split_and_profile(flagged):
    valid, invalid = split(flagged)
    assert REASONS_COL not in valid.columns
    assert (valid.count(), invalid.count()) == (2, 2)
    rows_valid, rows_rejected, reasons = profile(flagged)
    assert (rows_valid, rows_rejected) == (2, 2)
    assert reasons == {
        "not_null:id": 1,
        "accepted_values:segment": 1,
        "regex:email": 1,
        "range:price": 1,
    }


def test_unparseable_and_mistyped_payloads_are_rejected_as_malformed(flagged):
    from omniflow_data_platform.config import KafkaSource
    from omniflow_data_platform.sources.kafka import parse_payload

    spark = flagged.sparkSession
    source = KafkaSource(
        type="kafka",
        topic="t",
        fields=[
            {"name": "event_id", "type": "string"},
            {"name": "customer_id", "type": "bigint"},
        ],
    )
    raw = spark.createDataFrame(
        [
            ('{"event_id": "ok", "customer_id": 1}',),
            ('{"event_id": "truncated...',),
            ('{"event_id": "typed", "customer_id": "not-a-number"}',),
        ],
        "_raw_payload string",
    )
    checked = with_quality_flags(parse_payload(raw, source), [], check_parsing=True)
    reasons = [row[REASONS_COL] for row in checked.collect()]
    assert reasons == [[], ["malformed_payload"], ["malformed_payload"]]


def test_events_too_late_or_in_the_future_are_rejected(flagged):
    from datetime import datetime

    spark = flagged.sparkSession
    rule = TypeAdapter(QualityRule).validate_python(
        {
            "check": "timestamp_window",
            "column": "event_ts",
            "reference": "kafka_ts",
            "max_lag": "3 days",
            "max_lead": "5 minutes",
        }
    )
    arrived = datetime(2026, 10, 28, 12, 0)
    df = spark.createDataFrame(
        [
            ("on time", datetime(2026, 10, 28, 11, 58), arrived),
            ("10 days late", datetime(2026, 10, 18, 12, 0), arrived),
            ("2 days ahead", datetime(2026, 10, 30, 12, 0), arrived),
        ],
        "label string, event_ts timestamp, kafka_ts timestamp",
    )
    rows = with_quality_flags(df, [rule]).collect()
    assert {r["label"]: r[REASONS_COL] for r in rows} == {
        "on time": [],
        "10 days late": ["timestamp_window:event_ts"],
        "2 days ahead": ["timestamp_window:event_ts"],
    }
