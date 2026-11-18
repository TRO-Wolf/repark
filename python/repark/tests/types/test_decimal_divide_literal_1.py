from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import _decimal_divide_literal_1_cells as cells
import pytest

from repark import ReparkSession
from repark import functions as sf
from repark.spark.session import _reset_active_session_for_tests

ORACLE = json.loads(
    Path(__file__).with_name("decimal_divide_literal_1_spark_oracle.json").read_text()
)
KEYS = sorted(ORACLE["cells"]["false"])
PICK_KEYS = sorted(ORACLE["pick_false"]["false"])
CONDITION = re.compile(r"\[([A-Z_]+)")
PICK_KEY = "spark.sql.legacy.literal.pickMinimumPrecision"


def session_for(mode: str, root: Path, extra: dict[str, str]) -> Any:
    _reset_active_session_for_tests()
    builder = (
        ReparkSession.builder.appName("test-decimal-divide-literal-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "wh"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", mode)
    )
    for key, value in extra.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


@pytest.fixture(scope="module", params=["false", "true"])
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, Any]]]:
    root = tmp_path_factory.mktemp(f"ddl-{request.param}")
    session = session_for(request.param, root, {})
    cells.make_tables(session, str(root))
    found = cells.measure_all(session, sf, "lc")
    session.stop()
    _reset_active_session_for_tests()
    yield request.param, found


@pytest.fixture(scope="module", params=["false", "true"])
def pick_false(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, Any]]]:
    root = tmp_path_factory.mktemp(f"ddl-pmp-{request.param}")
    session = session_for(request.param, root, {PICK_KEY: "false"})
    cells.make_tables(session, str(root))
    found = cells.measure_pick_false(session, sf)
    session.stop()
    _reset_active_session_for_tests()
    yield request.param, found


def condition(value: str) -> str | None:
    match = CONDITION.search(value)
    return match.group(1) if match else None


def assert_as_spark(found: Any, want: Any, key: str) -> None:
    if isinstance(want, str):
        assert isinstance(found, str), (key, found, want)
        assert condition(found) == condition(want), (key, found, want)
        return
    if isinstance(want, dict) and "statement" in want:
        assert isinstance(found, dict), (key, found)
        assert found["statement"] == want["statement"] == "ok", (key, found, want)
        assert found["schema"] == want["schema"], (key, found, want)
        assert cells.same_answer(found["rows"], want["rows"]), (key, found, want)
        return
    assert cells.same_answer(found, want), (key, found, want)


@pytest.mark.parametrize("key", KEYS)
def test_a_cell_answers_as_spark(measured: tuple[str, dict[str, Any]], key: str) -> None:
    mode, found = measured
    assert_as_spark(found[key], ORACLE["cells"][mode][key], key)


@pytest.mark.parametrize("key", PICK_KEYS)
def test_a_pick_minimum_precision_false_cell_answers_as_spark(
    pick_false: tuple[str, dict[str, Any]], key: str
) -> None:
    mode, found = pick_false
    assert_as_spark(found[key], ORACLE["pick_false"][mode][key], key)
