from __future__ import annotations

import json
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import _live_parity as lp
import pytest

import repark

PARTITION_COUNTS = (1, 2, 16)
ROW_COUNT = 5000
VIEW = "offset_nested_sort_1"
NATIVE_TABLE = "offset_nested_sort_1_native"
ORACLE = json.loads(
    (Path(__file__).parent / "offset_nested_sort_1_spark_oracle.json").read_text(encoding="utf-8")
)
SQL_CELLS: dict[str, str] = ORACLE["statements"]
SQL_ROWS: dict[str, dict[str, Any]] = ORACLE["sql"]
DATAFRAME_ROWS: dict[str, dict[str, Any]] = ORACLE["dataframe"]
REPORTED = "SELECT v FROM (SELECT v FROM t ORDER BY v LIMIT 5 OFFSET 4990) ORDER BY v"


def _source_rows() -> list[tuple[int, int]]:
    values = list(range(1, ROW_COUNT + 1))
    random.Random(7).shuffle(values)
    return [(value, value % 7) for value in values]


def _statement(name: str, relation: str) -> str:
    return SQL_CELLS[name].replace(" t ", f" {relation} ").replace(" t)", f" {relation})")


def _dataframe_cells(frame: Any, functions: Any) -> dict[str, Callable[[], Any]]:
    v_desc = functions.col("v").desc()
    v_plus_one = (functions.col("v") + 1).alias("w")
    values = frame.select("v")
    window = values.orderBy("v").offset(4990).limit(5)
    return {
        "orderby_offset_limit_orderby": lambda: window.orderBy("v"),
        "sort_limit_offset_orderby": lambda: values.sort("v").limit(4995).offset(4990).orderBy("v"),
        "orderby_offset_orderby": lambda: values.orderBy("v").offset(4990).orderBy("v"),
        "orderby_limit_orderby": lambda: values.orderBy("v").limit(5).orderBy("v"),
        "orderby_offset_limit": lambda: window,
        "orderby_offset_limit_orderby_desc": lambda: window.orderBy(v_desc),
        "orderby_offset_limit_orderby_k_v": lambda: (
            frame.orderBy("v").offset(4990).limit(5).orderBy("k", "v")
        ),
        "orderby_offset_limit_orderby_v_k": lambda: (
            frame.orderBy("v").offset(4990).limit(5).orderBy("v", "k")
        ),
        "orderby_offset_past_the_end_orderby": lambda: (
            values.orderBy("v").offset(6000).limit(5).orderBy("v")
        ),
        "orderby_offset_limit_filter_orderby": lambda: window.where("v > 0").orderBy("v"),
        "orderby_offset_limit_select_orderby": lambda: window.select(v_plus_one).orderBy("w"),
        "orderby_offset_limit_orderby_count": lambda: window.orderBy("v").groupBy().count(),
        "orderby_offset_orderby_desc": lambda: values.orderBy("v").offset(4990).orderBy(v_desc),
        "orderby_offset_orderby_k_v": lambda: frame.orderBy("v").offset(4990).orderBy("k", "v"),
        "sort_offset_sort_desc_limit": lambda: values.sort("v").offset(4990).sort(v_desc).limit(3),
    }


def _rows(table: Any) -> list[list[int]]:
    return [
        list(row) for row in zip(*(column.to_pylist() for column in table.columns), strict=True)
    ]


def _answer(table: Any) -> dict[str, Any]:
    return {"types": [str(kind) for kind in table.schema.types], "rows": _rows(table)}


def _repark_engine(partitions: int) -> lp.Engine:
    engine = lp.build_repark_engine((("spark.sql.shuffle.partitions", str(partitions)),))
    engine.session.createDataFrame(_source_rows(), "v long, k long").createOrReplaceTempView(VIEW)
    return engine


def test_oracle_carries_the_reported_statement_and_every_dataframe_shape() -> None:
    assert SQL_CELLS["reported"] == REPORTED
    assert SQL_ROWS["reported"] == {
        "types": ["int64"],
        "rows": [[4991], [4992], [4993], [4994], [4995]],
    }
    assert sorted(SQL_CELLS) == sorted(SQL_ROWS)
    engine = _repark_engine(1)
    cells = _dataframe_cells(engine.session.table(VIEW), engine.functions)
    assert sorted(cells) == sorted(DATAFRAME_ROWS)


@pytest.mark.parametrize("partitions", PARTITION_COUNTS)
def test_spark_sql_door_answers_spark_rows(partitions: int) -> None:
    engine = _repark_engine(partitions)
    wrong = {}
    for name in SQL_CELLS:
        got = _answer(engine.arrow_of(engine.session.sql(_statement(name, VIEW))))
        if got != SQL_ROWS[name]:
            wrong[name] = got
    assert wrong == {}


@pytest.mark.parametrize("partitions", PARTITION_COUNTS)
def test_dataframe_door_answers_spark_rows(partitions: int) -> None:
    engine = _repark_engine(partitions)
    cells = _dataframe_cells(engine.session.table(VIEW), engine.functions)
    wrong = {}
    for name, build in cells.items():
        got = _answer(engine.arrow_of(build()))
        if got != DATAFRAME_ROWS[name]:
            wrong[name] = got
    assert wrong == {}


@pytest.mark.parametrize("partitions", PARTITION_COUNTS)
def test_native_sql_door_answers_spark_rows(partitions: int) -> None:
    repark.sql(
        f"CREATE TABLE IF NOT EXISTS {NATIVE_TABLE} AS "
        "SELECT value AS v, value % 7 AS k FROM generate_series(1, 5000)"
    ).to_arrow()
    repark.sql(f"SET datafusion.execution.target_partitions = {partitions}").to_arrow()
    wrong = {}
    for name in SQL_CELLS:
        got = _rows(repark.sql(_statement(name, NATIVE_TABLE)).to_arrow())
        if got != SQL_ROWS[name]["rows"]:
            wrong[name] = got
    assert wrong == {}


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_spark_answers_every_recorded_cell(spark_engine: lp.Engine) -> None:
    oracle_view = f"{VIEW}_oracle"
    frame = spark_engine.session.createDataFrame(_source_rows(), "v long, k long")
    frame.createOrReplaceTempView(oracle_view)
    for name in SQL_CELLS:
        table = spark_engine.arrow_of(spark_engine.session.sql(_statement(name, oracle_view)))
        assert _answer(table) == SQL_ROWS[name], name
    cells = _dataframe_cells(spark_engine.session.table(oracle_view), spark_engine.functions)
    for name, build in cells.items():
        assert _answer(spark_engine.arrow_of(build())) == DATAFRAME_ROWS[name], name
