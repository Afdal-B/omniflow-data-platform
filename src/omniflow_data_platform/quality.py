"""Data quality rules applied before writing to the lake."""

from __future__ import annotations

from collections import Counter

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from omniflow_data_platform.config import (
    AcceptedValuesRule,
    NotNullRule,
    QualityRule,
    RangeRule,
    RegexRule,
    TimestampWindowRule,
)

REASONS_COL = "_dlq_reasons"
CORRUPT_RECORD_COL = "_corrupt_record"
MALFORMED_PAYLOAD = "malformed_payload"


def rule_label(rule: QualityRule) -> str:
    """Label of a rule, as reported in the DLQ."""
    return f"{rule.check}:{rule.column}"


def violation(rule: QualityRule) -> Column:
    """Condition true when a row breaks the rule."""
    value = F.col(rule.column)
    match rule:
        case NotNullRule():
            return value.isNull()
        case AcceptedValuesRule(values=values):
            broken = ~value.isin(values)
        case RegexRule(pattern=pattern):
            broken = ~value.rlike(pattern)
        case RangeRule(min=low, max=high):
            broken = F.lit(False)
            if low is not None:
                broken = broken | (value < low)
            if high is not None:
                broken = broken | (value > high)
        case TimestampWindowRule(reference=reference, max_lag=lag, max_lead=lead):
            earliest = F.col(reference) - F.expr(f"INTERVAL {lag}")
            latest = F.col(reference) + F.expr(f"INTERVAL {lead}")
            broken = (value < earliest) | (value > latest)
        case _:
            raise TypeError(f"unsupported rule: {rule!r}")
    return value.isNotNull() & broken


def with_quality_flags(
    df: DataFrame, rules: list[QualityRule], check_parsing: bool = False
) -> DataFrame:
    """Add the list of broken rules to each row."""
    checks = [(rule_label(rule), violation(rule)) for rule in rules]
    if check_parsing:
        checks.insert(0, (MALFORMED_PAYLOAD, F.col(CORRUPT_RECORD_COL).isNotNull()))
    if not checks:
        return df.withColumn(REASONS_COL, F.array().cast("array<string>"))
    labels = [F.when(broken, F.lit(label)) for label, broken in checks]
    return df.withColumn(
        REASONS_COL, F.filter(F.array(*labels), lambda r: r.isNotNull())
    )


def is_valid() -> Column:
    """Condition true when a row broke no rule."""
    return F.size(REASONS_COL) == 0


def split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split flagged rows into valid and rejected."""
    return df.filter(is_valid()).drop(REASONS_COL), df.filter(~is_valid())


def profile(df: DataFrame) -> tuple[int, int, Counter[str]]:
    """Counts of valid and rejected rows, and rejections per rule."""
    counts = {
        row["valid"]: row["count"]
        for row in df.groupBy(is_valid().alias("valid")).count().collect()
    }
    reasons = Counter(
        {
            row["reason"]: row["count"]
            for row in df.select(F.explode(REASONS_COL).alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        }
    )
    return counts.get(True, 0), counts.get(False, 0), reasons
