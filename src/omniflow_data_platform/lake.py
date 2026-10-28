from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

_BATCH_ID = re.compile(r"^\d{8}T\d{6}Z$")
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def new_batch_id(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def _check_names(*names: str) -> None:
    for name in names:
        if not _NAME.match(name):
            raise ValueError(f"invalid name for a lake path: {name!r}")


@dataclass(frozen=True)
class Lake:
    bucket: str

    def batch_path(
        self, zone: Literal["bronze", "dlq"], source: str, dataset: str, batch_id: str
    ) -> str:
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
        _check_names(source, dataset)
        return f"s3a://{self.bucket}/{zone}/{source}/{dataset}"

    def bronze_path(self, source: str, dataset: str, batch_id: str) -> str:
        return self.batch_path("bronze", source, dataset, batch_id)

    def dlq_path(self, source: str, dataset: str, batch_id: str) -> str:
        return self.batch_path("dlq", source, dataset, batch_id)

    def state_path(self, pipeline: str) -> str:
        _check_names(pipeline)
        return f"s3a://{self.bucket}/_state/{pipeline}"

    def checkpoint_path(self, pipeline: str, stream: str) -> str:
        _check_names(pipeline, stream)
        return f"s3a://{self.bucket}/_checkpoints/{pipeline}/{stream}"

    def run_report_path(self, pipeline: str, batch_id: str, step: str) -> str:
        _check_names(pipeline, step)
        if not _BATCH_ID.match(batch_id):
            raise ValueError(f"invalid batch id: {batch_id!r}")
        return f"s3a://{self.bucket}/_runs/{pipeline}/batch_id={batch_id}/{step}.json"
