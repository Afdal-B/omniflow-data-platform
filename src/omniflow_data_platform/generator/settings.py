from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    clickstream_topic: str
    oltp_conninfo: str
    tick_seconds: float
    max_visits_per_tick: int
    corruption_rate: float
    seed: int | None

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        seed = env.get("GENERATOR_SEED")
        return cls(
            kafka_bootstrap_servers=env["KAFKA_BOOTSTRAP_SERVERS"],
            clickstream_topic=env.get("CLICKSTREAM_TOPIC", "shop.clickstream.v1"),
            oltp_conninfo=(
                f"host={env['OLTP_DB_HOST']} port={env.get('OLTP_DB_PORT', '5432')} "
                f"dbname={env['OLTP_DB_NAME']} user={env['OLTP_DB_USER']} "
                f"password={env['OLTP_DB_PASSWORD']}"
            ),
            tick_seconds=float(env.get("TICK_SECONDS", "2")),
            max_visits_per_tick=int(env.get("MAX_VISITS_PER_TICK", "5")),
            corruption_rate=float(env.get("CORRUPTION_RATE", "0.03")),
            seed=int(seed) if seed else None,
        )
