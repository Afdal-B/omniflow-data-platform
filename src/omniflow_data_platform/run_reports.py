from __future__ import annotations

import json
import logging
import traceback
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger(__name__)

STEPS = ("ingest", "load")


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass
class StepReport:
    pipeline: str
    batch_id: str
    step: str
    started_at: str = field(default_factory=now_iso)
    finished_at: str | None = None
    status: str = "running"
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def finish(
        self, details: dict[str, Any] | None = None, error: str | None = None
    ) -> None:
        self.finished_at = now_iso()
        self.status = "failed" if error else "success"
        self.details = details or {}
        self.error = error

    def to_json(self) -> str:
        return json.dumps(asdict(self))


def summarize_error(exc: BaseException, max_length: int = 1000) -> str:
    text = "".join(traceback.format_exception_only(exc))
    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith(("at ", "..."))
    ]
    return " | ".join(lines)[:max_length]


def run_step(
    step: str,
    pipeline: str,
    batch_id: str,
    work: Callable[[], dict[str, Any]],
    save: Callable[[StepReport], None],
) -> dict[str, Any]:
    report = StepReport(pipeline=pipeline, batch_id=batch_id, step=step)
    try:
        details = work()
    except Exception as exc:
        report.finish(error=summarize_error(exc))
        raise
    else:
        report.finish(details=details)
        return details
    finally:
        try:
            save(report)
        except Exception:
            log.exception("Could not save the run report")


def _duration_s(started_at: str, finished_at: str | None) -> float | None:
    if finished_at is None:
        return None
    delta = datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)
    return round(delta.total_seconds(), 1)


def merge_reports(
    pipeline: str,
    batch_id: str,
    ingest: dict[str, Any] | None,
    load: dict[str, Any] | None,
) -> dict[str, Any]:
    ingest_status = ingest["status"] if ingest else "no_report"
    if load:
        load_status = load["status"]
    else:
        load_status = "skipped" if ingest_status != "success" else "no_report"
    details = (ingest or {}).get("details", {})
    rows_read = details.get("rows_read")
    rows_rejected = details.get("rows_rejected")
    loaded = [
        n for n in (load or {}).get("details", {}).get("rows_loaded", {}).values() if n
    ]
    reports = [r for r in (ingest, load) if r]
    finished = [r["finished_at"] for r in reports if r.get("finished_at")]
    started_at = reports[0]["started_at"] if reports else None
    finished_at = max(finished) if finished else None

    return {
        "pipeline": pipeline,
        "batch_id": batch_id,
        "status": "success"
        if (ingest_status, load_status) == ("success",) * 2
        else "failed",
        "ingest_status": ingest_status,
        "load_status": load_status,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_s": _duration_s(started_at, finished_at) if started_at else None,
        "mode": details.get("mode"),
        "rows_read": rows_read,
        "rows_valid": details.get("rows_valid"),
        "rows_rejected": rows_rejected,
        "dlq_rate": round(rows_rejected / rows_read, 4) if rows_read else None,
        "rejection_reasons": details.get("rejection_reasons", {}),
        "rows_loaded": sum(loaded) if load else None,
        "error": next((r["error"] for r in reports if r.get("error")), None),
    }
