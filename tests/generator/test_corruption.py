import json
import random
from datetime import datetime

import pytest

from omniflow_data_platform.generator.corruption import CORRUPTIONS, corrupt

EVENT = {
    "event_id": "6f1c2a7e-0b8e-4c5e-9a51-0d2f3b1a9c11",
    "event_type": "page_view",
    "event_ts": "2026-10-27T12:00:00.000+00:00",
    "anonymous_id": "0b9d9f2c-8d1e-4a3b-9f6e-2c4d5e6f7a8b",
    "customer_id": None,
    "page_url": "/",
    "product_id": None,
    "order_id": None,
    "device": "mobile",
    "country_code": "FR",
}


def test_every_kind_of_corruption_is_reachable():
    rng = random.Random(0)
    kinds = {corrupt(EVENT, rng)[0] for _ in range(500)}
    assert kinds == set(CORRUPTIONS)


@pytest.mark.parametrize("kind", sorted(set(CORRUPTIONS) - {"truncated_json"}))
def test_corrupted_payloads_differ_from_the_original(kind):
    payload = CORRUPTIONS[kind](EVENT)
    assert payload != EVENT


def test_truncated_json_is_not_parseable():
    with pytest.raises(json.JSONDecodeError):
        json.loads(CORRUPTIONS["truncated_json"](EVENT))


def test_timestamp_corruptions_move_the_event_in_time():
    original = datetime.fromisoformat(EVENT["event_ts"])
    assert (
        datetime.fromisoformat(CORRUPTIONS["future_timestamp"](EVENT)["event_ts"])
        > original
    )
    assert (
        datetime.fromisoformat(CORRUPTIONS["very_late_event"](EVENT)["event_ts"])
        < original
    )


def test_the_original_event_is_never_mutated():
    snapshot = dict(EVENT)
    rng = random.Random(1)
    for _ in range(100):
        corrupt(EVENT, rng)
    assert snapshot == EVENT
