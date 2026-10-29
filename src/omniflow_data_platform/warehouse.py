"""Writes to the PostgreSQL warehouse."""

from __future__ import annotations

import os
from dataclasses import dataclass

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class WarehouseConnection:
    """Connection settings of the warehouse."""

    host: str
    port: str
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls, prefix: str = "WAREHOUSE_DB") -> WarehouseConnection:
        """Settings read from <prefix>_* variables."""
        env = os.environ
        return cls(
            host=env[f"{prefix}_HOST"],
            port=env.get(f"{prefix}_PORT", "5432"),
            dbname=env[f"{prefix}_NAME"],
            user=env[f"{prefix}_USER"],
            password=env[f"{prefix}_PASSWORD"],
        )

    @property
    def jdbc_url(self) -> str:
        """JDBC URL used by Spark."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}"

    def connect(self) -> psycopg.Connection:
        """Open a psycopg connection in UTC."""
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            options="-c timezone=UTC",
        )


def transit_table_name(table: str) -> str:
    """Name of the transit table used during a load."""
    return f"_load_{table}"


def publish(
    conn: psycopg.Connection,
    table: str,
    batch_id: str | None,
) -> int:
    """Move a transit table into its raw table in one transaction."""
    target = sql.Identifier("raw", table)
    transit = sql.Identifier("raw", transit_table_name(table))
    with conn.transaction():
        conn.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {} (LIKE {})").format(target, transit)
        )
        _add_missing_columns(conn, table)

        if batch_id is None:
            conn.execute(sql.SQL("TRUNCATE {}").format(target))
        else:
            conn.execute(
                sql.SQL("DELETE FROM {} WHERE _batch_id = %s").format(target),
                (batch_id,),
            )
        columns = sql.SQL(", ").join(
            sql.Identifier(name) for name in _columns(conn, transit_table_name(table))
        )
        inserted = conn.execute(
            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                target, columns, columns, transit
            )
        ).rowcount
        conn.execute(sql.SQL("DROP TABLE {}").format(transit))
    return inserted


def _columns(conn: psycopg.Connection, table: str) -> list[str]:
    """Columns of a raw table, in order."""
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = %s ORDER BY ordinal_position",
        (table,),
    ).fetchall()
    return [name for (name,) in rows]


def _add_missing_columns(conn: psycopg.Connection, table: str) -> None:
    """Add to the raw table the columns new in the transit table."""
    existing = set(_columns(conn, table))
    new_columns = conn.execute(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) "
        "FROM pg_attribute a "
        "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY a.attnum",
        (f"raw.{transit_table_name(table)}",),
    ).fetchall()
    for name, data_type in new_columns:
        if name not in existing:
            conn.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                    sql.Identifier("raw", table),
                    sql.Identifier(name),
                    sql.SQL(data_type),
                )
            )


PIPELINE_RUNS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    pipeline           text        NOT NULL,
    batch_id           text        NOT NULL,
    status             text        NOT NULL,
    ingest_status      text        NOT NULL,
    load_status        text        NOT NULL,
    started_at         timestamptz,
    finished_at        timestamptz,
    duration_s         double precision,
    mode               text,
    rows_read          bigint,
    rows_valid         bigint,
    rows_rejected      bigint,
    dlq_rate           double precision,
    rejection_reasons  jsonb       NOT NULL DEFAULT '{}',
    rows_loaded        bigint,
    error              text,
    recorded_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, batch_id)
)
"""

_RUN_COLUMNS = (
    "pipeline",
    "batch_id",
    "status",
    "ingest_status",
    "load_status",
    "started_at",
    "finished_at",
    "duration_s",
    "mode",
    "rows_read",
    "rows_valid",
    "rows_rejected",
    "dlq_rate",
    "rejection_reasons",
    "rows_loaded",
    "error",
)


def record_pipeline_run(conn: psycopg.Connection, run: dict) -> None:
    """Insert or update a run in ops.pipeline_runs."""
    columns = sql.SQL(", ").join(sql.Identifier(c) for c in _RUN_COLUMNS)
    updates = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in _RUN_COLUMNS
        if c not in ("pipeline", "batch_id")
    )
    values = [
        Jsonb(run[c]) if c == "rejection_reasons" else run[c] for c in _RUN_COLUMNS
    ]
    with conn.transaction():
        conn.execute(PIPELINE_RUNS_DDL)
        conn.execute(
            sql.SQL(
                "INSERT INTO ops.pipeline_runs ({}, recorded_at) VALUES ({}, now()) "
                "ON CONFLICT (pipeline, batch_id) DO UPDATE SET {}, recorded_at = now()"
            ).format(
                columns, sql.SQL(", ").join(sql.Placeholder() * len(values)), updates
            ),
            values,
        )
