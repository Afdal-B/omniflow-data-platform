from __future__ import annotations

import json
import random
from collections.abc import Callable
from datetime import datetime, timedelta

from omniflow_data_platform.generator.visits import Event


def _truncated_json(event: Event) -> bytes:
    return json.dumps(event).encode()[:40]


def _missing_event_id(event: Event) -> Event:
    return {k: v for k, v in event.items() if k != "event_id"}


def _unknown_event_type(event: Event) -> Event:
    return {**event, "event_type": "teleport"}


def _wrong_type(event: Event) -> Event:
    return {**event, "customer_id": "not-a-number"}


def _future_timestamp(event: Event) -> Event:
    ts = datetime.fromisoformat(event["event_ts"]) + timedelta(days=2)
    return {**event, "event_ts": ts.isoformat(timespec="milliseconds")}


def _very_late_event(event: Event) -> Event:
    ts = datetime.fromisoformat(event["event_ts"]) - timedelta(days=10)
    return {**event, "event_ts": ts.isoformat(timespec="milliseconds")}


def _unknown_device(event: Event) -> Event:
    return {**event, "device": "smart-fridge"}


CORRUPTIONS: dict[str, Callable[[Event], Event | bytes]] = {
    "truncated_json": _truncated_json,
    "missing_event_id": _missing_event_id,
    "unknown_event_type": _unknown_event_type,
    "wrong_type": _wrong_type,
    "future_timestamp": _future_timestamp,
    "very_late_event": _very_late_event,
    "unknown_device": _unknown_device,
}


def corrupt(event: Event, rng: random.Random) -> tuple[str, bytes]:
    kind = rng.choice(list(CORRUPTIONS))
    corrupted = CORRUPTIONS[kind](event)
    if isinstance(corrupted, bytes):
        return kind, corrupted
    return kind, json.dumps(corrupted).encode()
