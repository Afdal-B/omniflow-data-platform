import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "docker/monitoring/grafana/build_dashboard.py"


def load_builder():
    spec = importlib.util.spec_from_file_location("build_dashboard", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dashboard_json_is_up_to_date():
    builder = load_builder()
    committed = json.loads(builder.OUTPUT.read_text())
    assert committed == builder.dashboard, (
        "Run: uv run python docker/monitoring/grafana/build_dashboard.py"
    )


def test_every_panel_uses_a_provisioned_datasource():
    builder = load_builder()
    uids = {
        p["datasource"]["uid"]
        for p in builder.dashboard["panels"]
        if p["type"] != "row"
    }
    assert uids == {"prometheus", "warehouse"}
