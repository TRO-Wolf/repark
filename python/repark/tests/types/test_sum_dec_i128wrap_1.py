from __future__ import annotations

import json
import re
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import _sum_dec_i128wrap_1_cells as cells
import pytest

from repark import ReparkSession
from repark import functions as sf
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.window import Window

ORACLE = json.loads(Path(__file__).with_name("sum_dec_i128wrap_1_spark_oracle.json").read_text())
KEYS = sorted(ORACLE["cells"]["false"])
REPEAT_KEYS = sorted(ORACLE["repeats"]["false"])
CONDITION = re.compile(r"\[([A-Z_]+)")
WRAP = 2**128


@pytest.fixture(scope="module", params=["false", "true"])
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, Any], Path]]:
    root = tmp_path_factory.mktemp(f"sum-{request.param}")
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-sum-dec-i128wrap-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "wh"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    cells.make_tables(session, str(root))
    found = cells.measure_all(session, sf, Window, "lc", "repark")
    found["repeats"] = cells.measure_repeats(session, cells.REPEATS)
    session.stop()
    _reset_active_session_for_tests()
    yield request.param, found, root / "wh"


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
        if want["statement"] == "ok":
            assert found["statement"] == "ok", (key, found, want)
        else:
            assert condition(found["statement"]) == condition(want["statement"]), (key, found)
        if isinstance(want["rows"], str) and condition(want["rows"]) == "TABLE_OR_VIEW_NOT_FOUND":
            assert isinstance(found["rows"], str), (key, found)
            assert "not found" in found["rows"], (key, found)
            return
        assert_as_spark(found["rows"], want["rows"], key)
        return
    assert cells.same_answer(found, want), (key, found, want)


@pytest.mark.parametrize("key", KEYS)
def test_a_cell_answers_as_spark(measured: tuple[str, dict[str, Any], Path], key: str) -> None:
    mode, found, _ = measured
    assert_as_spark(found[key], ORACLE["cells"][mode][key], key)


def test_no_data_file_holds_a_sum_past_38_digits_or_its_wrap(
    measured: tuple[str, dict[str, Any], Path],
) -> None:
    _, _, warehouse = measured
    values = cells.data_values(warehouse)
    assert values, "the fitting groups were written"
    for value in values:
        sign, digits, exponent = value.as_tuple()
        unscaled = int("".join(map(str, digits))) * (-1 if sign else 1)
        assert abs(unscaled) < 10**38, value
        assert -exponent in (0, 38), value
    allowed = {Decimal(350), Decimal("3.50E-36")}
    assert {value for value in values if value != 0} <= allowed, values


def drops_the_total(answer: Any, mode: str, function: str, g: str) -> bool:
    if mode == "true" and function == "sum":
        return isinstance(answer, str) and condition(answer) == "ARITHMETIC_OVERFLOW"
    return isinstance(answer, dict) and answer["rows"] == [f"{g}|null"]


@pytest.mark.parametrize("key", REPEAT_KEYS)
def test_a_multi_partial_query_gives_one_answer_on_every_run(
    measured: tuple[str, dict[str, Any], Path], key: str
) -> None:
    mode, found, _ = measured
    runs = found["repeats"][key]
    seen = {json.dumps(run, sort_keys=True) for run in runs}
    assert len(runs) == cells.REPEATS, key
    assert len(seen) == 1, (key, seen)
    spark_runs = ORACLE["repeats"][mode][key]
    assert len({json.dumps(run, sort_keys=True) for run in spark_runs}) == 1, key
    _, function, group, column = key.split("/")
    g = group[1:]
    if function == "distinct":
        layout = drops_the_total(runs[0], mode, "sum", g)
        assert layout or cells.same_answer(runs[0], spark_runs[0]), (key, runs[0])
        return
    if g in cells.MERGE_TOTAL_CLASS:
        assert drops_the_total(spark_runs[0], mode, function, g), (key, spark_runs[0])
        scale = column.split("_")[1]
        exact = {
            "rows": [cells.exact_row(int(g), column)],
            "types": ["int", f"decimal(38,{scale})"],
        }
        assert cells.same_answer(runs[0], exact), (key, runs[0], exact)
        return
    assert_as_spark(runs[0], spark_runs[0], key)
