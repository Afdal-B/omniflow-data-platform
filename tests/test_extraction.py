"""Tests of the incremental extraction logic."""

from omniflow_data_platform.config import JdbcSource
from omniflow_data_platform.extraction import (
    PipelineState,
    extract_query,
    lower_bound_for,
)

INCREMENTAL = JdbcSource(
    type="jdbc",
    connection="OLTP_DB",
    table="shop.orders",
    mode="incremental",
    watermark_column="updated_at",
    lookback_minutes=10,
)
FULL = JdbcSource(type="jdbc", connection="OLTP_DB", table="shop.products", mode="full")
STATE = PipelineState(
    last_batch_id="20261028T100000Z",
    lower_bound="2026-10-28T08:50:00+00:00",
    high_watermark="2026-10-28T09:59:30+00:00",
)


def test_first_run_extracts_everything():
    assert lower_bound_for(INCREMENTAL, None, "20261028T110000Z", False) is None


def test_next_run_starts_at_the_watermark_minus_the_lookback():
    bound = lower_bound_for(INCREMENTAL, STATE, "20261028T110000Z", False)
    assert bound == "2026-10-28T09:49:30+00:00"


def test_rerun_of_the_same_batch_reuses_its_bound():
    bound = lower_bound_for(INCREMENTAL, STATE, STATE.last_batch_id, False)
    assert bound == STATE.lower_bound


def test_full_refresh_and_full_mode_ignore_the_state():
    assert lower_bound_for(INCREMENTAL, STATE, "20261028T110000Z", True) is None
    assert lower_bound_for(FULL, STATE, "20261028T110000Z", False) is None


def test_query_without_bound_reads_the_table():
    assert extract_query(INCREMENTAL, None) == "shop.orders"


def test_query_with_bound_filters_on_the_watermark():
    query = extract_query(INCREMENTAL, "2026-10-28T09:49:30+00:00")
    assert query == (
        "(SELECT * FROM shop.orders WHERE updated_at > "
        "'2026-10-28T09:49:30+00:00'::timestamptz) AS incremental_extract"
    )
