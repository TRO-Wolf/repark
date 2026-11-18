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
SPREAD = cells.spread_sql()
SPREAD_SETS = sorted({(key.split("/")[0], key.split("/")[-1]) for key in SPREAD})
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


def drops(answer: Any) -> bool:
    if isinstance(answer, str):
        return condition(answer) is not None
    return all(row == "null" or row.endswith("|null") for row in answer["rows"])


@pytest.mark.parametrize("key", KEYS, ids=[f"{cells.class_of(key)}:{key}" for key in KEYS])
def test_a_cell_answers_as_spark_or_as_its_named_class(
    measured: tuple[str, dict[str, Any], Path], key: str
) -> None:
    mode, found, _ = measured
    want = ORACLE["cells"][mode][key]
    if key in cells.KEPT_DIVERGENCE:
        assert drops(want), (key, want)
        exact = cells.kept_exact(key, ORACLE["cells"]["false"][key]["types"])
        assert cells.same_answer(found[key], exact), (key, found[key], exact)
        return
    if key in cells.CONDITION_CLASS and mode == "true":
        assert condition(want) == cells.CONDITION_CLASS[key], (key, want)
        assert isinstance(found[key], str), (key, found[key])
        assert condition(found[key]) == "ARITHMETIC_OVERFLOW", (key, found[key])
        return
    assert_as_spark(found[key], want, key)


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


@pytest.mark.parametrize("mode", ["false", "true"])
def test_spark_drops_the_total_of_the_kept_divergence_group_on_every_run(mode: str) -> None:
    for key, runs in ORACLE["repeats"][mode].items():
        _, function, group, _ = key.split("/")
        if group[1:] in cells.MERGE_TOTAL_CLASS:
            assert len({json.dumps(run, sort_keys=True) for run in runs}) == 1, key
            assert drops_the_total(runs[0], mode, function, group[1:]), (key, runs[0])


@pytest.fixture(scope="module")
def spread_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("sum-spread")
    cells.write_spread_tables(root)
    return root


def spread_runs(root: Path, mode: str, partitions: int, keys: list[str]) -> dict[str, list[Any]]:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-sum-dec-i128wrap-1-spread")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", mode)
        .getOrCreate()
    )
    try:
        session.sql(f"SET datafusion.execution.target_partitions = {partitions}").collect()
        cells.register_spread_tables(session, root)
        return {
            key: [
                cells.attempt(lambda key=key: cells.answer_of(session.sql(SPREAD[key])))
                for _ in range(cells.REPEATS)
            ]
            for key in keys
        }
    finally:
        session.stop()
        _reset_active_session_for_tests()


@pytest.mark.parametrize("mode", ["false", "true"], ids=["ansi_off", "ansi_on"])
@pytest.mark.parametrize("partitions", cells.TARGET_PARTITIONS, ids=lambda n: f"tp{n}")
@pytest.mark.parametrize(("table", "column"), SPREAD_SETS, ids=[f"{t}-{c}" for t, c in SPREAD_SETS])
def test_a_multi_partial_total_is_the_exact_total_on_every_run_at_2_4_and_8_partitions(
    spread_root: Path, mode: str, partitions: int, table: str, column: str
) -> None:
    keys = [
        key for key in sorted(SPREAD) if key.startswith(f"{table}/") and key.endswith(f"/{column}")
    ]
    for key, runs in spread_runs(spread_root, mode, partitions, keys).items():
        seen = {json.dumps(run, sort_keys=True) for run in runs}
        assert len(runs) == cells.REPEATS, key
        assert len(seen) == 1, (key, len(seen), sorted(text[:160] for text in seen))
        want = cells.spread_exact(key, mode)
        if isinstance(want, str):
            assert isinstance(runs[0], str), (key, str(runs[0])[:300])
            assert condition(runs[0]) == want, (key, runs[0][:300])
        else:
            assert cells.same_answer(runs[0], want), (key, str(runs[0])[:300], str(want)[:300])
