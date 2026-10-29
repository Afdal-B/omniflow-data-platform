"""Entry point of the generator: publishes visits and updates the shop."""

from __future__ import annotations

import json
import logging
import random
import signal
import threading
import time
from collections import Counter
from datetime import UTC, datetime

import psycopg
from confluent_kafka import KafkaError, Message, Producer

from omniflow_data_platform.generator.corruption import corrupt
from omniflow_data_platform.generator.settings import Settings
from omniflow_data_platform.generator.shop import PostgresShop
from omniflow_data_platform.generator.visits import simulate_visit

log = logging.getLogger("generator")


def connect_with_retry(
    settings: Settings, rng: random.Random, attempts: int = 30
) -> PostgresShop:
    """Connect to the shop database, waiting for it to start."""
    for attempt in range(1, attempts + 1):
        try:
            return PostgresShop(settings.oltp_conninfo, rng)
        except psycopg.OperationalError as exc:
            log.warning("Postgres not ready (%s/%s): %s", attempt, attempts, exc)
            time.sleep(2)
    raise RuntimeError("could not connect to Postgres")


def build_producer(settings: Settings) -> Producer:
    """Idempotent Kafka producer."""
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "stratum-generator",
            "enable.idempotence": True,
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )


def main() -> None:
    """Generate traffic until stopped."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    settings = Settings.from_env()
    rng = random.Random(settings.seed)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    shop = connect_with_retry(settings, rng)
    producer = build_producer(settings)
    delivery_errors = Counter()

    def on_delivery(error: KafkaError | None, _message: Message) -> None:
        if error is not None:
            delivery_errors[error.name()] += 1

    log.info(
        "Publishing to %s every %ss", settings.clickstream_topic, settings.tick_seconds
    )
    while not stop.is_set():
        published, corruptions = 0, Counter()
        for _ in range(rng.randint(1, settings.max_visits_per_tick)):
            for event in simulate_visit(shop, rng, datetime.now(UTC)):
                if rng.random() < settings.corruption_rate:
                    kind, payload = corrupt(event, rng)
                    corruptions[kind] += 1
                else:
                    payload = json.dumps(event).encode()
                producer.produce(
                    settings.clickstream_topic,
                    key=event["anonymous_id"].encode(),
                    value=payload,
                    on_delivery=on_delivery,
                )
                published += 1
            producer.poll(0)

        shop.advance_orders()
        if rng.random() < 0.1:
            shop.create_customer()
        if rng.random() < 0.02:
            shop.reprice_product()

        log.info(
            "tick: %s events published, corrupted: %s",
            published,
            dict(corruptions) or "none",
        )
        if delivery_errors:
            log.error("Kafka delivery errors so far: %s", dict(delivery_errors))
        stop.wait(settings.tick_seconds)

    log.info("Stopping: flushing pending messages")
    producer.flush(10)
    shop.close()


if __name__ == "__main__":
    main()
