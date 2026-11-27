from __future__ import annotations

import argparse
import os
from pathlib import Path

import pytest

from repark_parity.scoreboard.encoding import write_lines
from repark_parity.scoreboard.launch import import_path
from repark_parity.scoreboard.settings import MissingSettingError, Settings, resolve_settings
from repark_parity.scoreboard.summary import (
    build_scoreboard,
    compare_scoreboards,
    flaky_cells,
    read_json,
    render_comparison,
    render_scoreboard,
    write_json,
)

META = {"label": "fixture", "revision": "abc", "measured": "2026-11-27", "spark": "4.1.2"}


def write_lane(
    root: Path, lane: str, rows: list[tuple[str, bool, str, dict]], value: int = 1
) -> None:
    folder = root / lane
    folder.mkdir(parents=True)
    cases = [{"id": key, "ansi": ansi, "zone": "UTC", **extra} for key, ansi, _, extra in rows]
    write_lines(folder / "cases.jsonl", cases)
    write_lines(folder / "diff.jsonl", [{"id": key, "class": label} for key, _, label, _ in rows])
    answers = [
        {"id": key, "outcome": "answered", "value": [[value]], "binary": {"sha256": "f00d"}}
        for key, _, _, _ in rows
    ]
    write_lines(folder / "repark.jsonl", answers)


def base_run(root: Path) -> Path:
    write_lane(
        root,
        "numbers",
        [
            ("n1", True, "EQUAL", {"rule_id": "SIM-CONST-CAST"}),
            ("n2", True, "WRONG-VALUE", {"rule_id": "SIM-CONST-CAST"}),
            ("n3", False, "EQUAL", {}),
            ("n4", False, "EXCLUDED", {}),
        ],
    )
    write_lane(
        root,
        "dates",
        [
            ("d1", True, "WRONG-TYPE", {"variant": "hour-dst"}),
            ("d2", False, "REFUSED-WHERE-SPARK-ANSWERS", {"record_ids": ["CARD-1", "CARD-2"]}),
            ("d3", True, "EQUAL", {}),
        ],
    )
    return root


def head_run(root: Path) -> Path:
    write_lane(
        root,
        "numbers",
        [
            ("n1", True, "WRONG-VALUE", {"rule_id": "SIM-CONST-CAST"}),
            ("n2", True, "EQUAL", {"rule_id": "SIM-CONST-CAST"}),
            ("n3", False, "EQUAL", {}),
            ("n4", False, "EXCLUDED", {}),
        ],
    )
    write_lane(
        root,
        "dates",
        [
            ("d1", True, "NULLABILITY-ONLY", {"variant": "hour-dst"}),
            ("d2", False, "REFUSED-WHERE-SPARK-ANSWERS", {"record_ids": ["CARD-1", "CARD-2"]}),
            ("d3", True, "EQUAL", {}),
        ],
    )
    return root


def test_scoreboard_counts_classes_lanes_ansi_and_families(tmp_path: Path) -> None:
    board = build_scoreboard(base_run(tmp_path / "base"), {"d3": "clock"}, set(), META)
    totals = board["totals"]
    assert totals["definitions"] == 7
    assert totals["comparable"] == 5
    assert totals["equal"] == 2
    assert totals["equal_share"] == 0.4
    assert totals["recorded_exclusions"] == 1
    assert totals["excluded_at_run"] == 1
    assert board["ansi"] == {
        "on": {"EQUAL": 1, "WRONG-VALUE": 1, "WRONG-TYPE": 1},
        "off": {"EQUAL": 1, "REFUSED-WHERE-SPARK-ANSWERS": 1},
    }
    assert board["lanes"]["numbers"] == {
        "comparable": 3,
        "equal": 2,
        "classes": {"EQUAL": 2, "WRONG-VALUE": 1, "EXCLUDED": 1},
    }
    assert board["families"]["rule_id:SIM-CONST-CAST"] == {"EQUAL": 1, "WRONG-VALUE": 1}
    assert board["families"]["record_ids:CARD-2"] == {"REFUSED-WHERE-SPARK-ANSWERS": 1}
    assert board["cells"]["d3"] == ["dates", "RECORDED-EXCLUSION", True]
    assert board["meta"]["native_sha256"] == ["f00d"]
    assert "| EQUAL | 2 | 1 | 1 |" in render_scoreboard(board)


def test_flaky_cells_leave_the_count(tmp_path: Path) -> None:
    board = build_scoreboard(base_run(tmp_path / "base"), {}, {"n2"}, META)
    assert board["totals"]["flaky"] == 1
    assert board["totals"]["comparable"] == 5
    assert board["cells"]["n2"][1] == "FLAKY"


def test_comparison_lists_moves_by_lane_and_id(tmp_path: Path) -> None:
    base = build_scoreboard(base_run(tmp_path / "base"), {"d3": "clock"}, set(), META)
    head = build_scoreboard(head_run(tmp_path / "head"), {"d3": "clock"}, set(), META)
    write_json(tmp_path / "base.json.gz", base)
    report = compare_scoreboards(read_json(tmp_path / "base.json.gz"), head)
    assert report["lanes"]["numbers"] == {
        "moved_off": [{"id": "n1", "to": "WRONG-VALUE"}],
        "moved_on": [{"id": "n2", "from": "WRONG-VALUE"}],
    }
    assert report["lanes"]["dates"] == {
        "changed_unlike": [{"id": "d1", "from": "WRONG-TYPE", "to": "NULLABILITY-ONLY"}]
    }
    totals = report["totals"]
    assert (totals["moved_on"], totals["moved_off"], totals["changed_unlike"]) == (1, 1, 1)
    assert totals["uncounted"] == 0
    assert "| numbers | 1 | 1 | 0 |" in render_comparison(report)


def test_a_cell_flaky_on_one_side_is_uncounted(tmp_path: Path) -> None:
    base = build_scoreboard(base_run(tmp_path / "base"), {}, set(), META)
    head = build_scoreboard(head_run(tmp_path / "head"), {}, {"n1"}, META)
    report = compare_scoreboards(base, head)
    assert report["totals"]["moved_off"] == 0
    assert report["lanes"]["numbers"]["uncounted"] == [{"id": "n1", "from": "EQUAL", "to": "FLAKY"}]


def test_flaky_detection_compares_answers_across_runs(tmp_path: Path) -> None:
    rows = [("n1", True, "EQUAL", {}), ("n2", True, "EQUAL", {})]
    write_lane(tmp_path / "one", "numbers", rows)
    write_lane(tmp_path / "two", "numbers", rows)
    assert flaky_cells([tmp_path / "one", tmp_path / "two"]) == {}
    write_lane(tmp_path / "three", "numbers", rows, value=2)
    found = flaky_cells([tmp_path / "one", tmp_path / "three"])
    assert found == {"n1": ["value"], "n2": ["value"]}


def test_a_missing_setting_names_its_flag_and_variable() -> None:
    settings = resolve_settings(argparse.Namespace(cpus=None), {})
    with pytest.raises(MissingSettingError, match=r"--cpus .*SCOREBOARD_CPUS"):
        settings.require("cpus")
    with pytest.raises(MissingSettingError, match="--memory-floor-mib"):
        settings.require("memory_floor_mib")


def test_a_flag_wins_over_the_environment() -> None:
    environ = {"SCOREBOARD_CPUS": "0-3", "SCOREBOARD_SPARK_DRIVERS": "2", "JAVA_HOME": "jdk"}
    settings = resolve_settings(argparse.Namespace(cpus="4-7"), environ)
    assert settings.require("cpus") == "4-7"
    assert settings.require("spark_drivers") == 2
    assert str(settings.require("java_home")) == "jdk"


def test_the_repark_path_puts_the_package_before_its_dependencies(tmp_path: Path) -> None:
    settings = Settings(package=tmp_path / "pkg", dependency_path="deps")
    entries = import_path("repark", settings).split(os.pathsep)
    assert entries[:2] == [str((tmp_path / "pkg").resolve()), "deps"]
    assert entries[-1].endswith("cellpath")
    assert len(import_path("spark", settings).split(os.pathsep)) == 2
