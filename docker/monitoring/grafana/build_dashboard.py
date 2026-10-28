from __future__ import annotations

import json
from pathlib import Path

OUTPUT = Path(__file__).parent / "dashboards" / "stratum-platform.json"
PROMETHEUS = {"type": "prometheus", "uid": "prometheus"}
WAREHOUSE = {"type": "grafana-postgresql-datasource", "uid": "warehouse"}

panels: list[dict] = []


def _next_id() -> int:
    return len(panels) + 1


def row(title: str, y: int) -> None:
    panels.append(
        {
            "id": _next_id(),
            "type": "row",
            "title": title,
            "collapsed": False,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
            "panels": [],
        }
    )


def panel(
    title: str,
    kind: str,
    targets: list[dict],
    grid: tuple[int, int, int, int],
    unit: str | None = None,
    description: str | None = None,
    thresholds: list[tuple[str, float | None]] | None = None,
    options: dict | None = None,
    field_config: dict | None = None,
) -> None:
    x, y, w, h = grid
    defaults: dict = {}
    if unit:
        defaults["unit"] = unit
    if thresholds:
        defaults["color"] = {"mode": "thresholds"}
        defaults["thresholds"] = {
            "mode": "absolute",
            "steps": [{"color": color, "value": value} for color, value in thresholds],
        }
    defaults.update(field_config or {})
    panels.append(
        {
            "id": _next_id(),
            "type": kind,
            "title": title,
            "description": description or "",
            "datasource": targets[0]["datasource"],
            "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "targets": targets,
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": options or {},
        }
    )


def promql(expr: str, legend: str = "", ref: str = "A") -> dict:
    return {
        "refId": ref,
        "datasource": PROMETHEUS,
        "expr": expr,
        "legendFormat": legend,
        "range": True,
    }


def sql(query: str, fmt: str = "table", ref: str = "A") -> dict:
    return {
        "refId": ref,
        "datasource": WAREHOUSE,
        "editorMode": "code",
        "rawQuery": True,
        "format": fmt,
        "rawSql": query.strip(),
    }


RUNS_24H = "FROM ops.pipeline_runs WHERE finished_at > now() - interval '24 hours'"
GREEN, ORANGE, RED = "green", "orange", "red"

row("Pipelines — runs recorded in ops.pipeline_runs", 0)
panel(
    "Runs (24 h)",
    "stat",
    [sql(f"SELECT count(*) AS runs {RUNS_24H}")],
    (0, 1, 6, 4),
)
panel(
    "Failed runs (24 h)",
    "stat",
    [sql(f"SELECT count(*) FILTER (WHERE status = 'failed') AS failed {RUNS_24H}")],
    (6, 1, 6, 4),
    thresholds=[(GREEN, None), (RED, 1)],
    description="Runs whose ingestion or load failed. Details in the table below.",
)
panel(
    "Rows ingested (24 h)",
    "stat",
    [sql(f"SELECT coalesce(sum(rows_valid), 0) AS rows {RUNS_24H}")],
    (12, 1, 6, 4),
    unit="short",
)
panel(
    "DLQ rate (24 h)",
    "stat",
    [
        sql(
            "SELECT coalesce(sum(rows_rejected)::float / nullif(sum(rows_read), 0), 0) "
            f"AS dlq_rate {RUNS_24H}"
        )
    ],
    (18, 1, 6, 4),
    unit="percentunit",
    thresholds=[(GREEN, None), (ORANGE, 0.02), (RED, 0.05)],
    description="Share of the rows read that broke a quality rule (sent to the DLQ).",
)
panel(
    "DLQ rate by pipeline",
    "timeseries",
    [
        sql(
            """
SELECT finished_at AS time, pipeline AS metric, dlq_rate
FROM ops.pipeline_runs
WHERE $__timeFilter(finished_at) AND rows_read > 0
ORDER BY 1
""",
            fmt="time_series",
        )
    ],
    (0, 5, 12, 8),
    unit="percentunit",
    field_config={"custom": {"drawStyle": "points", "pointSize": 6}},
)
panel(
    "Rows read per hour",
    "timeseries",
    [
        sql(
            """
SELECT $__timeGroupAlias(finished_at, '1h'), pipeline AS metric, sum(rows_read) AS rows
FROM ops.pipeline_runs
WHERE $__timeFilter(finished_at)
GROUP BY 1, 2
ORDER BY 1
""",
            fmt="time_series",
        )
    ],
    (12, 5, 12, 8),
    unit="short",
    field_config={
        "custom": {
            "drawStyle": "bars",
            "fillOpacity": 60,
            "stacking": {"mode": "normal"},
        }
    },
)
panel(
    "Top rejection reasons (24 h)",
    "bargauge",
    [
        sql(
            f"""
SELECT r.pipeline || ' / ' || kv.key AS reason, sum(kv.value::int) AS rows
FROM (SELECT pipeline, rejection_reasons {RUNS_24H}) AS r
CROSS JOIN LATERAL jsonb_each_text(r.rejection_reasons) AS kv
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
"""
        )
    ],
    (0, 13, 9, 9),
    options={
        "orientation": "horizontal",
        "displayMode": "gradient",
        "reduceOptions": {"values": True, "calcs": []},
    },
)
panel(
    "Latest runs",
    "table",
    [
        sql(
            """
SELECT finished_at, pipeline, status, ingest_status, load_status, rows_read, rows_rejected,
       round(dlq_rate::numeric, 4) AS dlq_rate, rows_loaded, round(duration_s::numeric) AS duration_s,
       error
FROM ops.pipeline_runs
WHERE $__timeFilter(finished_at)
ORDER BY finished_at DESC
LIMIT 50
"""
        )
    ],
    (9, 13, 15, 9),
    field_config={"custom": {"filterable": True}},
)

row("Streaming — Kafka", 22)
panel(
    "Messages published per second",
    "timeseries",
    [
        promql(
            'sum by (topic) (rate(kafka_topic_partition_current_offset{topic!~"__.*"}[5m]))',
            "{{topic}}",
        )
    ],
    (0, 23, 16, 7),
    unit="ops",
    description="Throughput of each topic, from the growth of the partitions' last offset.",
)
panel(
    "Messages retained",
    "stat",
    [
        promql(
            'sum by (topic) (kafka_topic_partition_current_offset{topic!~"__.*"}'
            ' - kafka_topic_partition_oldest_offset{topic!~"__.*"})',
            "{{topic}}",
        )
    ],
    (16, 23, 8, 7),
    unit="short",
    description="Messages still in Kafka (7-day retention): what could be replayed.",
)

row("Orchestration — Airflow", 30)
panel(
    "Failed tasks (24 h)",
    "bargauge",
    [
        promql(
            'sum by (dag_id) (increase(airflow_task_finished_total{state="failed"}[24h]))',
            "{{dag_id}}",
        )
    ],
    (0, 31, 8, 8),
    thresholds=[(GREEN, None), (RED, 1)],
    field_config={"noValue": "0"},
    options={"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True},
)
panel(
    "DAG run duration (median)",
    "timeseries",
    [
        promql(
            'max by (dag_id) (airflow_dagrun_duration_seconds{quantile="0.5", state="success"})',
            "{{dag_id}}",
        )
    ],
    (8, 31, 10, 8),
    unit="s",
)
panel(
    "Spark pool: running / queued",
    "timeseries",
    [
        promql('airflow_pool_running_slots{pool="spark"}', "running"),
        promql('airflow_pool_queued_slots{pool="spark"}', "queued", "B"),
    ],
    (18, 31, 6, 8),
    unit="short",
    description="2 Spark containers at most. Tasks queued for long: the platform lacks capacity.",
)

row("Source — OLTP Postgres", 39)
panel(
    "OLTP database",
    "stat",
    [promql("max(pg_up)")],
    (0, 40, 6, 6),
    thresholds=[(RED, None), (GREEN, 1)],
    options={"colorMode": "background"},
    field_config={
        "mappings": [
            {"type": "value", "options": {"0": {"text": "DOWN"}, "1": {"text": "UP"}}}
        ]
    },
)
panel(
    "Connections by state",
    "timeseries",
    [promql('sum by (state) (pg_stat_activity_count{datname="shop"})', "{{state}}")],
    (6, 40, 18, 6),
    unit="short",
)

dashboard = {
    "uid": "stratum-platform",
    "title": "Stratum — Platform Health",
    "tags": ["stratum"],
    "timezone": "browser",
    "schemaVersion": 41,
    "version": 1,
    "refresh": "1m",
    "time": {"from": "now-24h", "to": "now"},
    "graphTooltip": 1,
    "editable": True,
    "panels": panels,
    "templating": {"list": []},
    "annotations": {"list": []},
}

if __name__ == "__main__":
    OUTPUT.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n")
    print(f"{OUTPUT}: {sum(p['type'] != 'row' for p in panels)} panels")
