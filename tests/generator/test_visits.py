"""Tests of the visit simulation."""

import random
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from omniflow_data_platform.generator.visits import EVENT_TYPES, Product, simulate_visit

NOW = datetime(2026, 10, 27, 12, 0, tzinfo=UTC)
EXPECTED_KEYS = {
    "event_id",
    "event_type",
    "event_ts",
    "anonymous_id",
    "customer_id",
    "page_url",
    "product_id",
    "order_id",
    "device",
    "country_code",
}


class FakeShop:
    """In-memory shop recording the orders it receives."""

    def __init__(self) -> None:
        self.orders: list[tuple[int, list[Product], str]] = []
        self.new_customers = 0

    def random_customer(self) -> int | None:
        return 42

    def random_products(self, n: int) -> list[Product]:
        return [
            Product(product_id=i, unit_price=Decimal("10.00")) for i in range(1, n + 1)
        ]

    def create_customer(self) -> int:
        self.new_customers += 1
        return 1000 + self.new_customers

    def create_order(self, customer_id: int, cart: list[Product], channel: str) -> int:
        self.orders.append((customer_id, cart, channel))
        return 5000 + len(self.orders)


@pytest.fixture(scope="module")
def visits():
    shop, rng = FakeShop(), random.Random(7)
    return shop, [simulate_visit(shop, rng, NOW) for _ in range(2000)]


def test_every_event_has_the_contract_fields(visits):
    _, all_visits = visits
    for events in all_visits:
        for event in events:
            assert set(event) == EXPECTED_KEYS
            assert event["event_type"] in EVENT_TYPES


def test_a_visit_starts_with_a_page_view_and_is_chronological(visits):
    _, all_visits = visits
    for events in all_visits:
        assert events[0]["event_type"] == "page_view"
        timestamps = [datetime.fromisoformat(e["event_ts"]) for e in events]
        assert timestamps == sorted(timestamps)
        assert timestamps[-1] <= NOW


def test_one_visitor_per_visit(visits):
    _, all_visits = visits
    for events in all_visits:
        assert len({e["anonymous_id"] for e in events}) == 1


def test_purchases_create_orders_and_reference_them(visits):
    shop, all_visits = visits
    purchases = [
        e for events in all_visits for e in events if e["event_type"] == "purchase"
    ]
    assert len(purchases) == len(shop.orders)
    assert {p["order_id"] for p in purchases} == {
        5000 + i for i in range(1, len(shop.orders) + 1)
    }
    assert all(p["customer_id"] is not None for p in purchases)


def test_conversion_rate_is_realistic(visits):
    shop, all_visits = visits
    conversion = len(shop.orders) / len(all_visits)
    assert 0.02 <= conversion <= 0.05


def test_same_seed_same_visits():
    first = simulate_visit(FakeShop(), random.Random(1), NOW)
    second = simulate_visit(FakeShop(), random.Random(1), NOW)
    assert first == second
