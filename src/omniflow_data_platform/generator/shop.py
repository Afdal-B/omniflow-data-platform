from __future__ import annotations

import random
from decimal import Decimal

import psycopg
from faker import Faker

from omniflow_data_platform.generator.visits import COUNTRIES, Product

VALID_EMAIL = r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"


class PostgresShop:
    def __init__(self, conninfo: str, rng: random.Random) -> None:
        self.conn = psycopg.connect(conninfo, autocommit=True)
        self.rng = rng
        self.fake = Faker("fr_FR")
        self.fake.seed_instance(rng.random())

    def close(self) -> None:
        self.conn.close()

    def random_customer(self) -> int | None:
        row = self.conn.execute(
            "SELECT customer_id FROM shop.customers WHERE email ~ %s ORDER BY random() LIMIT 1",
            (VALID_EMAIL,),
        ).fetchone()
        return row[0] if row else None

    def random_products(self, n: int) -> list[Product]:
        rows = self.conn.execute(
            "SELECT product_id, unit_price FROM shop.products WHERE is_active ORDER BY random() LIMIT %s",
            (n,),
        ).fetchall()
        return [Product(product_id, unit_price) for product_id, unit_price in rows]

    def create_customer(self) -> int:
        first_name, last_name = self.fake.first_name(), self.fake.last_name()
        local_part = (
            f"{first_name}.{last_name}.{self.rng.randint(1, 999_999)}".lower().replace(
                " ", ""
            )
        )
        row = self.conn.execute(
            "INSERT INTO shop.customers (email, first_name, last_name, country_code, segment) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING customer_id",
            (
                f"{local_part}@example.com",
                first_name,
                last_name,
                self.rng.choice(COUNTRIES),
                "business" if self.rng.random() < 0.15 else "consumer",
            ),
        ).fetchone()
        if row is None:
            raise RuntimeError("Customer insert did not return a customer ID")
        return row[0]

    def create_order(self, customer_id: int, cart: list[Product], channel: str) -> int:
        with self.conn.transaction():
            row = self.conn.execute(
                "INSERT INTO shop.orders (customer_id, status, channel) "
                "VALUES (%s, 'pending', %s) RETURNING order_id",
                (customer_id, channel),
            ).fetchone()
            if row is None:
                raise RuntimeError("Order insert did not return an order ID")
            order_id = row[0]
            for product in cart:
                discount = (
                    round(product.unit_price * Decimal("0.1"), 2)
                    if self.rng.random() < 0.2
                    else 0
                )
                self.conn.execute(
                    "INSERT INTO shop.order_items "
                    "(order_id, product_id, quantity, unit_price, discount_amount) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (
                        order_id,
                        product.product_id,
                        self.rng.randint(1, 3),
                        product.unit_price,
                        discount,
                    ),
                )
        return order_id

    def advance_orders(self, limit: int = 50) -> int:
        result = self.conn.execute(
            """
            UPDATE shop.orders
            SET status = CASE status
                WHEN 'pending' THEN CASE WHEN random() < 0.9 THEN 'paid' ELSE 'cancelled' END
                WHEN 'paid' THEN 'shipped'
                WHEN 'shipped' THEN 'delivered'
            END
            WHERE order_id IN (
                SELECT order_id FROM shop.orders
                WHERE (status = 'pending' AND updated_at < now() - interval '1 minute')
                   OR (status = 'paid' AND updated_at < now() - interval '5 minutes')
                   OR (status = 'shipped' AND updated_at < now() - interval '15 minutes')
                ORDER BY updated_at
                LIMIT %s
            )
            """,
            (limit,),
        )
        return result.rowcount

    def reprice_product(self) -> None:
        self.conn.execute(
            "UPDATE shop.products SET unit_price = round((unit_price * (0.9 + random() * 0.2))::numeric, 2) "
            "WHERE product_id = (SELECT product_id FROM shop.products ORDER BY random() LIMIT 1)"
        )
