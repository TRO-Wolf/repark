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
PMOD_KEYS = sorted(ORACLE["pmod"]["false"])
SET_KEYS = sorted(
    (built, key) for built in ("default", "false") for key in ORACLE["runtime_set"][built]["false"]
)
CONDITION = re.compile(r"\[([A-Z_]+)")
PICK_KEY = "spark.sql.legacy.literal.pickMinimumPrecision"
CARDED_CONDITIONS = {"REMAINDER_BY_ZERO": "DIVIDE_BY_ZERO"}
OVERFLOW_CLASS = "an overflowing pmod() raises Arrow's error, not NUMERIC_VALUE_OUT_OF_RANGE"
FPMOD_TYPE = "F.pmod answers its formula's type, not Pmod's (proposed card)"
GAPS = {
    **{
        ("true", f"pm/{door}/{p}/left_neg"): OVERFLOW_CLASS
        for door in ("sql", "expr", "select_expr")
        for p in cells.PRECISIONS
    },
    **{
        (mode, f"pm/F/{p}/left_neg"): FPMOD_TYPE
        for mode in ("false", "true")
        for p in cells.PRECISIONS
    },
    ("false", "pm/F/d10_2/int_column"): FPMOD_TYPE,
    ("true", "pm/F/d10_2/int_column"): FPMOD_TYPE,
    ("true", "pm/F/i/left_neg"): "F.pmod's integer sum overflows with ANSI on (proposed card)",
    ("false", "rs/set_false/view"): "TEMP-VIEW-SESSION-SETTINGS-1 does not capture the key",
    ("true", "rs/set_false/view"): "TEMP-VIEW-SESSION-SETTINGS-1 does not capture the key",
}


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


@pytest.fixture(scope="module", params=["false", "true"])
def pmod_measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, Any]]]:
    root = tmp_path_factory.mktemp(f"ddl-pmod-{request.param}")
    session = session_for(request.param, root, {})
    cells.make_pmod_table(session, str(root))
    found = cells.measure_pmod(session, sf)
    session.stop()
    _reset_active_session_for_tests()
    yield request.param, found


@pytest.fixture(scope="module", params=["false", "true"])
def runtime_set(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, dict[str, Any]]]]:
    found: dict[str, dict[str, Any]] = {}
    for built, extra in (("default", {}), ("false", {PICK_KEY: "false"})):
        root = tmp_path_factory.mktemp(f"ddl-set-{built}-{request.param}")
        session = session_for(request.param, root, extra)
        cells.make_tables(session, str(root))
        found[built] = cells.measure_runtime_set(session, built == "false")
        session.stop()
        _reset_active_session_for_tests()
    yield request.param, found


def condition(value: str) -> str | None:
    match = CONDITION.search(value)
    return match.group(1) if match else None


def assert_as_spark(found: Any, want: Any, key: str) -> None:
    if isinstance(want, str):
        assert isinstance(found, str), (key, found, want)
        spark = condition(want)
        assert condition(found) == CARDED_CONDITIONS.get(spark, spark), (key, found, want)
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


def mark_gap(request: pytest.FixtureRequest, mode: str, key: str) -> None:
    reason = GAPS.get((mode, key))
    if reason is not None:
        request.applymarker(pytest.mark.xfail(reason=reason, strict=True))


@pytest.mark.parametrize("key", PMOD_KEYS)
def test_a_pmod_cell_answers_as_spark(
    pmod_measured: tuple[str, dict[str, Any]], key: str, request: pytest.FixtureRequest
) -> None:
    mode, found = pmod_measured
    mark_gap(request, mode, key)
    assert_as_spark(found[key], ORACLE["pmod"][mode][key], key)


@pytest.mark.parametrize(("built", "key"), SET_KEYS)
def test_a_runtime_set_cell_answers_as_spark(
    runtime_set: tuple[str, dict[str, dict[str, Any]]],
    built: str,
    key: str,
    request: pytest.FixtureRequest,
) -> None:
    mode, found = runtime_set
    mark_gap(request, mode, key)
    assert_as_spark(found[built][key], ORACLE["runtime_set"][built][mode][key], key)
