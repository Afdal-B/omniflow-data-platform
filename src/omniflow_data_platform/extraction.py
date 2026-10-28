from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from omniflow_data_platform.config import JdbcSource


@dataclass(frozen=True)
class PipelineState:
    last_batch_id: str
    lower_bound: str | None
    high_watermark: str | None


def lower_bound_for(
    source: JdbcSource,
    state: PipelineState | None,
    batch_id: str,
    full_refresh: bool,
) -> str | None:
    if source.mode == "full" or full_refresh or state is None:
        return None
    if state.last_batch_id == batch_id:
        return state.lower_bound
    if state.high_watermark is None:
        return None
    bound = datetime.fromisoformat(state.high_watermark) - timedelta(
        minutes=source.lookback_minutes
    )
    return bound.isoformat()


def extract_query(source: JdbcSource, lower_bound: str | None) -> str:
    if lower_bound is None:
        return source.table
    bound = datetime.fromisoformat(lower_bound).isoformat()
    return (
        f"(SELECT * FROM {source.table} "
        f"WHERE {source.watermark_column} > '{bound}'::timestamptz) AS incremental_extract"
    )
