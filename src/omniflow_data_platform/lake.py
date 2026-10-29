"""Path layout of the S3 data lake."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

_BATCH_ID = re.compile(r"^\d{8}T\d{6}Z$")
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def new_batch_id(now: datetime | None = None) -> str:
    """Batch identifier derived from a UTC timestamp."""
    return (now or datetime.now(UTC)).astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def _check_names(*names: str) -> None:
    """Reject names that are unsafe in a lake path."""
    for name in names:
        if not _NAME.match(name):
            raise ValueError(f"invalid name for a lake path: {name!r}")


@dataclass(frozen=True)
class Lake:
    """Builds every path of the lake bucket."""

    bucket: str

    def batch_path(
        self, zone: Literal["bronze", "dlq"], source: str, dataset: str, batch_id: str
    ) -> str:
        """Partition of one batch in the bronze or DLQ zone."""
        _check_names(source, dataset)
        if not _BATCH_ID.match(batch_id):
            raise ValueError(f"invalid batch id: {batch_id!r}")
        ingestion_date = f"{batch_id[:4]}-{batch_id[4:6]}-{batch_id[6:8]}"
        return (
            f"s3a://{self.bucket}/{zone}/{source}/{dataset}"
            f"/ingestion_date={ingestion_date}/batch_id={batch_id}"
        )

    def dataset_path(
        self, zone: Literal["bronze", "dlq"], source: str, dataset: str
    ) -> str:
        """Root of a dataset in the bronze or DLQ zone."""
        _check_names(source, dataset)
        return f"s3a://{self.bucket}/{zone}/{source}/{dataset}"

    def bronze_path(self, source: str, dataset: str, batch_id: str) -> str:
        """Bronze partition of one batch."""
        return self.batch_path("bronze", source, dataset, batch_id)

    def dlq_path(self, source: str, dataset: str, batch_id: str) -> str:
        """Dead letter queue partition of one batch."""
        return self.batch_path("dlq", source, dataset, batch_id)

    def state_path(self, pipeline: str) -> str:
        """Incremental state of a pipeline."""
        _check_names(pipeline)
        return f"s3a://{self.bucket}/_state/{pipeline}"

    def checkpoint_path(self, pipeline: str, stream: str) -> str:
        """Structured Streaming checkpoint of a stream."""
        _check_names(pipeline, stream)
        return f"s3a://{self.bucket}/_checkpoints/{pipeline}/{stream}"

    def run_report_path(self, pipeline: str, batch_id: str, step: str) -> str:
        """JSON report of one step of a run."""
        _check_names(pipeline, step)
        if not _BATCH_ID.match(batch_id):
            raise ValueError(f"invalid batch id: {batch_id!r}")
        return f"s3a://{self.bucket}/_runs/{pipeline}/batch_id={batch_id}/{step}.json"
