from __future__ import annotations

import json
import re
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import _avg_decimal_round_1_cells as cells
import pytest

from repark import ReparkSession
from repark import functions as sf
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.window import Window

ORACLE = json.loads(Path(__file__).with_name("avg_decimal_round_1_spark_oracle.json").read_text())
KEYS = sorted(ORACLE["cells"]["false"])
CONDITION = re.compile(r"\[([A-Z_]+)")
MIXED = {
    "avg": "SELECT g, avg(DISTINCT c10_2) AS m, count(c10_2) AS n FROM t GROUP BY g",
    "mean": "SELECT g, mean(DISTINCT c10_2) AS m, count(c10_2) AS n FROM t GROUP BY g",
}


def ruled(session: Any, root: str) -> dict[str, Any]:
    found = {
        f"mixed/{f}": cells.attempt(lambda q=q: session.sql(q).collect()) for f, q in MIXED.items()
    }
    frame = session.table("t").where("g = 9")
    found["fexpr/mean"] = cells.attempt(lambda: frame.select(sf.expr("mean(c10_2)")).collect())
    path = f"{root}/pq_two"
    source = session.table("s04")
    source.where("k < 2").write.parquet(f"{path}/a")
    source.where("k >= 2").write.parquet(f"{path}/b")
    two = session.read.parquet(f"{path}/a").unionByName(session.read.parquet(f"{path}/b"))
    two.createOrReplaceTempView("two")
    for f in cells.FUNCTIONS:
        for name in ("two", "s04"):
            found[f"{name}/{f}"] = cells.attempt(
                lambda f=f, name=name: cells.answer_of(
                    session.sql(f"SELECT g, {f}(x) AS m FROM {name} GROUP BY g")
                )
            )
    return found


@pytest.fixture(scope="module", params=["false", "true"])
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, Any]]]:
    root = tmp_path_factory.mktemp(f"avg-{request.param}")
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-avg-decimal-round-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "wh"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    cells.make_tables(session, str(root))
    found = cells.measure_all(session, sf, Window, "lc")
    found.update(ruled(session, str(root)))
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


@pytest.mark.parametrize("function", cells.FUNCTIONS)
def test_distinct_beside_another_aggregate_refuses(
    measured: tuple[str, dict[str, Any]], function: str
) -> None:
    _, found = measured
    refusal = found[f"mixed/{function}"]
    assert isinstance(refusal, str), refusal
    assert "DistinctAvgAccumulator" in refusal, refusal


def test_bare_fexpr_mean_reaches_the_spark_averager(measured: tuple[str, dict[str, Any]]) -> None:
    _, found = measured
    refusal = found["fexpr/mean"]
    assert isinstance(refusal, str), refusal
    assert "SparkAvgWithRetract" in refusal, refusal


@pytest.mark.parametrize("function", cells.FUNCTIONS)
def test_a_partial_sum_past_the_sum_type_is_sparks_null_sum(
    measured: tuple[str, dict[str, Any]], function: str
) -> None:
    mode, found = measured
    answer = found[f"two/{function}"]
    if mode == "true":
        assert isinstance(answer, str), answer
        assert "[ARITHMETIC_OVERFLOW] Overflow in sum of decimals" in answer, answer
    else:
        assert answer == {"rows": ["1|null"], "types": ["int", "decimal(38,4)"]}, answer


@pytest.mark.parametrize("function", cells.FUNCTIONS)
def test_a_grouped_sum_past_38_digits_comes_back_inside_one_partial(
    measured: tuple[str, dict[str, Any]], function: str
) -> None:
    _, found = measured
    answer = found[f"s04/{function}"]
    assert isinstance(answer, dict), answer
    assert answer["types"] == ["int", "decimal(38,4)"], answer
    assert [Decimal(row.split("|")[1]) for row in answer["rows"]] == [Decimal("1.0000")], answer
