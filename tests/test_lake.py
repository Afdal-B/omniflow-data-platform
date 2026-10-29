"""Tests of the lake path layout."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from omniflow_data_platform.lake import Lake, new_batch_id


def test_batch_id_is_the_utc_time_of_the_run():
    paris = timezone(timedelta(hours=1))
    assert (
        new_batch_id(datetime(2026, 10, 28, 11, 15, 0, tzinfo=paris))
        == "20261028T101500Z"
    )


def test_batch_ids_sort_chronologically():
    first = new_batch_id(datetime(2026, 10, 28, 9, 59, 59, tzinfo=UTC))
    second = new_batch_id(datetime(2026, 10, 28, 10, 0, 0, tzinfo=UTC))
    assert sorted([second, first]) == [first, second]


def test_bronze_path_layout():
    path = Lake("stratum-lake").bronze_path("oltp", "customers", "20261028T101500Z")
    assert path == (
        "s3a://stratum-lake/bronze/oltp/customers"
        "/ingestion_date=2026-10-28/batch_id=20261028T101500Z"
    )


@pytest.mark.parametrize(
    ("source", "dataset", "batch_id"),
    [
        ("oltp", "../customers", "20261028T101500Z"),
        ("OLTP", "customers", "20261028T101500Z"),
        ("oltp", "customers", "2026-10-28"),
    ],
)
def test_bronze_path_rejects_unsafe_values(source, dataset, batch_id):
    with pytest.raises(ValueError):
        Lake("stratum-lake").bronze_path(source, dataset, batch_id)


def test_dlq_and_state_paths():
    lake = Lake("stratum-lake")
    assert lake.dlq_path("oltp", "customers", "20261028T101500Z") == (
        "s3a://stratum-lake/dlq/oltp/customers"
        "/ingestion_date=2026-10-28/batch_id=20261028T101500Z"
    )
    assert (
        lake.state_path("oltp_customers") == "s3a://stratum-lake/_state/oltp_customers"
    )


def test_checkpoint_path():
    assert Lake("stratum-lake").checkpoint_path(
        "clickstream_events", "clickstream"
    ) == ("s3a://stratum-lake/_checkpoints/clickstream_events/clickstream")


def test_dataset_path_is_the_root_of_all_batches():
    assert Lake("stratum-lake").dataset_path("bronze", "oltp", "orders") == (
        "s3a://stratum-lake/bronze/oltp/orders"
    )


def test_run_report_path():
    assert Lake("stratum-lake").run_report_path(
        "oltp_orders", "20261028T190000Z", "ingest"
    ) == ("s3a://stratum-lake/_runs/oltp_orders/batch_id=20261028T190000Z/ingest.json")
