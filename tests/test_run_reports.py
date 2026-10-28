import json

import pytest

from omniflow_data_platform.run_reports import (
    StepReport,
    merge_reports,
    run_step,
    summarize_error,
)


def report(
    step, status="success", details=None, error=None, start="10:00:00", end="10:00:30"
):
    return {
        "pipeline": "oltp_orders",
        "batch_id": "20261028T100000Z",
        "step": step,
        "started_at": f"2026-10-28T{start}+00:00",
        "finished_at": f"2026-10-28T{end}+00:00",
        "status": status,
        "details": details or {},
        "error": error,
    }


INGEST_OK = report(
    "ingest",
    details={
        "mode": "incremental",
        "rows_read": 200,
        "rows_valid": 190,
        "rows_rejected": 10,
        "rejection_reasons": {"regex:email": 10},
    },
)
LOAD_OK = report(
    "load",
    details={"rows_loaded": {"raw.a": 150, "raw.b": 40, "raw.c": None}},
    start="10:00:31",
    end="10:00:45",
)


def test_successful_run():
    run = merge_reports("oltp_orders", "20261028T100000Z", INGEST_OK, LOAD_OK)
    assert run["status"] == "success"
    assert (run["rows_read"], run["rows_rejected"], run["dlq_rate"]) == (200, 10, 0.05)
    assert run["rows_loaded"] == 190
    assert run["duration_s"] == 45.0
    assert run["rejection_reasons"] == {"regex:email": 10}


def test_failed_ingestion_skips_the_load():
    failed = report("ingest", status="failed", error="ConnectionError: refused")
    run = merge_reports("oltp_orders", "20261028T100000Z", failed, None)
    assert (run["status"], run["ingest_status"], run["load_status"]) == (
        "failed",
        "failed",
        "skipped",
    )
    assert run["error"] == "ConnectionError: refused"


def test_a_step_that_died_without_report_is_a_failure():
    run = merge_reports("oltp_orders", "20261028T100000Z", INGEST_OK, None)
    assert (run["status"], run["load_status"]) == ("failed", "no_report")
    run = merge_reports("oltp_orders", "20261028T100000Z", None, None)
    assert (run["status"], run["ingest_status"]) == ("failed", "no_report")


def test_run_step_saves_the_report_and_reraises_on_failure():
    saved: list[StepReport] = []

    def boom() -> dict:
        raise ValueError("bad data")

    with pytest.raises(ValueError):
        run_step("ingest", "p", "20261028T100000Z", work=boom, save=saved.append)
    assert saved[0].status == "failed"
    assert saved[0].error == "ValueError: bad data"
    assert json.loads(saved[0].to_json())["finished_at"] is not None


def test_run_step_returns_the_details_on_success():
    saved: list[StepReport] = []
    details = run_step(
        "load", "p", "20261028T100000Z", work=lambda: {"n": 1}, save=saved.append
    )
    assert details == {"n": 1}
    assert (saved[0].status, saved[0].details) == ("success", {"n": 1})


def test_java_stack_frames_are_removed_from_errors():
    java_error = RuntimeError(
        "An error occurred while calling o60.load.\n"
        ": org.apache.spark.SparkException: [JDBC] syntax error\n"
        "\tat org.apache.spark.sql.JDBCRDD.resolveTable(JDBCRDD.scala:89)\n"
        "\t... 12 more\n"
        "Caused by: org.postgresql.util.PSQLException: relation does not exist\n"
        "\tat org.postgresql.core.v3.QueryExecutorImpl.receive(QueryExecutorImpl.java:2733)"
    )
    assert summarize_error(java_error) == (
        "RuntimeError: An error occurred while calling o60.load. | "
        ": org.apache.spark.SparkException: [JDBC] syntax error | "
        "Caused by: org.postgresql.util.PSQLException: relation does not exist"
    )
