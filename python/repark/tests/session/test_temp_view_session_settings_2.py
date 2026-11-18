from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import _temp_view_session_settings_2_cells as cells
import pytest
from test_temp_view_session_settings_1 import assert_answer, build, same_as_spark

from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("temp_view_session_settings_2")
HERE = Path(__file__)
ORACLE = json.loads(HERE.with_name("temp_view_session_settings_2_spark_oracle.json").read_text())
PLANS = json.loads(HERE.with_name("temp_view_session_settings_2_plans.json").read_text())
STANDING = json.loads(HERE.with_name("temp_view_session_settings_2_standing.json").read_text())
KEYS = sorted(ORACLE["true"])
UNSET_KEYS = sorted(ORACLE["unset"])


def standing(key: str, mode: str) -> tuple[str, dict[str, Any]] | None:
    held = STANDING[mode].get(key)
    if held is None:
        return None
    return STANDING["classes"][held["class"]], held["answer"]


def measure(root: Path, mode: str, what: str) -> Any:
    session = build(root, mode)
    table = cells.Cells(session, str(root), mode)
    found = table.run() if what == "cells" else table.explain()
    session.stop()
    _reset_active_session_for_tests()
    return found


@pytest.fixture(scope="module", params=["true", "false"])
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, Any]]]:
    root = tmp_path_factory.mktemp(f"tvs2-{request.param}")
    yield request.param, measure(root, request.param, "cells")


@pytest.fixture(scope="module")
def unset_measured(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return measure(tmp_path_factory.mktemp("tvs2-unset"), "unset", "cells")


def assert_standing(found: dict[str, Any], want: dict[str, Any], key: str, reason: str) -> None:
    if "error" in want:
        assert "error" in found, (key, reason, found)
        assert found["error"] == want["error"], (key, reason, found)
        return
    assert "error" not in found, (key, reason, found)
    assert found["rows"] == want["rows"], (key, reason, found)


@pytest.mark.parametrize("key", KEYS)
def test_a_dataframe_door_cell_answers_as_spark(
    measured: tuple[str, dict[str, Any]], key: str
) -> None:
    mode, found = measured
    held = standing(key, mode)
    if held is None:
        assert_answer(found[key], ORACLE[mode][key], key)
        return
    reason, want = held
    assert not same_as_spark(ORACLE[mode][key], found[key]), (key, reason)
    assert_standing(found[key], want, key, reason)


@pytest.mark.parametrize("key", UNSET_KEYS)
def test_an_unset_flag_is_captured_from_the_frame(unset_measured: dict[str, Any], key: str) -> None:
    assert_answer(unset_measured[key], ORACLE["unset"][key], key)


def test_matching_settings_keep_todays_plans(tmp_path: Path) -> None:
    plans = measure(tmp_path, "true", "explain")
    assert plans == PLANS["true"]
