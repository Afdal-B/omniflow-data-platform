from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

EVENT_TYPES = (
    "page_view",
    "search",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "begin_checkout",
    "purchase",
)
DEVICES = ("desktop", "mobile", "tablet")
DEVICE_WEIGHTS = (2, 3, 1)
COUNTRIES = ("FR", "FR", "FR", "BE", "CH", "DE", "ES", "IT", "GB", "NL")
SEARCH_TERMS = (
    "headphones",
    "jacket",
    "lamp",
    "sneakers",
    "serum",
    "backpack",
    "watch",
    "yoga mat",
)

P_KNOWN_CUSTOMER = 0.4
P_SEARCH = 0.3
P_ADD_TO_CART = 0.08
P_REMOVE_FROM_CART = 0.15
P_CHECKOUT = 0.25
P_PURCHASE = 0.6


@dataclass(frozen=True)
class Product:
    product_id: int
    unit_price: Decimal


class Shop(Protocol):
    def random_customer(self) -> int | None: ...

    def random_products(self, n: int) -> list[Product]: ...

    def create_customer(self) -> int: ...

    def create_order(
        self, customer_id: int, cart: list[Product], channel: str
    ) -> int: ...


Event = dict[str, Any]


def simulate_visit(shop: Shop, rng: random.Random, now: datetime) -> list[Event]:
    customer_id = shop.random_customer() if rng.random() < P_KNOWN_CUSTOMER else None
    device = rng.choices(DEVICES, DEVICE_WEIGHTS)[0]
    visitor = {
        "anonymous_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "device": device,
        "country_code": rng.choice(COUNTRIES),
    }
    clock = now - timedelta(minutes=rng.uniform(2, 8))
    events: list[Event] = []

    def emit(event_type: str, page_url: str, **ids: int | None) -> None:
        nonlocal clock
        clock = min(clock + timedelta(seconds=rng.uniform(3, 60)), now)
        events.append(
            {
                "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                "event_type": event_type,
                "event_ts": clock.isoformat(timespec="milliseconds"),
                **visitor,
                "customer_id": customer_id,
                "page_url": page_url,
                "product_id": ids.get("product_id"),
                "order_id": ids.get("order_id"),
            }
        )

    emit("page_view", "/")
    if rng.random() < P_SEARCH:
        emit("search", f"/search?q={rng.choice(SEARCH_TERMS).replace(' ', '+')}")

    viewed = shop.random_products(rng.randint(1, 5))
    for product in viewed:
        emit(
            "product_view",
            f"/products/{product.product_id}",
            product_id=product.product_id,
        )

    cart = [product for product in viewed if rng.random() < P_ADD_TO_CART]
    for product in cart:
        emit("add_to_cart", "/cart", product_id=product.product_id)
    if cart and rng.random() < P_REMOVE_FROM_CART:
        removed = cart.pop()
        emit("remove_from_cart", "/cart", product_id=removed.product_id)

    if not cart or rng.random() >= P_CHECKOUT:
        return events
    emit("begin_checkout", "/checkout")
    if rng.random() >= P_PURCHASE:
        return events

    if customer_id is None:
        customer_id = shop.create_customer()
    channel = "mobile" if device == "mobile" else "web"
    order_id = shop.create_order(customer_id, cart, channel)
    emit("purchase", "/checkout/confirmation", order_id=order_id)
    return events
