"""DECIMAL-CACHE-1 pins: every oracle decimal cell on both doors and every action.

pins: decimal-cache-1/C-005, C-007, C-012
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.dataframe.core import DataFrame

_ORACLE_PATH = Path(__file__).with_name("decimal_cache_1_oracle.json")


def _oracle_cells() -> list[dict[str, str]]:
    """Read the copied live-PySpark oracle cells."""
    raw: Any = json.loads(_ORACLE_PATH.read_text())
    cells: list[dict[str, str]] = raw
    return cells


def _cell_id(cell: dict[str, str]) -> str:
    """Pytest id for one oracle cell."""
    return cell["input"] + cell["op"]


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """One session per test, stopped on teardown."""
    session = ReparkSession.builder.appName("pytest-decimal-cache-1").getOrCreate()
    yield session
    session.stop()


def _base_frame(
    spark: ReparkSession, decimal_input: str, price: Any = Decimal("176.56")
) -> DataFrame:
    """Two-column frame with one price row at the oracle input type."""
    return spark.createDataFrame([(1, price)], f"id LONG, price {decimal_input.upper()}")


def _cell_base_frame(spark: ReparkSession, cell: dict[str, str]) -> DataFrame:
    """Base frame for one cell: null-scalar cells carry a null price row."""
    if "-null" in cell["op"]:
        return _base_frame(spark, cell["input"], None)
    return _base_frame(spark, cell["input"])


def _facade_expression(operation: str) -> Any:
    """Facade spelling of one oracle operation."""
    if operation == "*5":
        return F.col("price") * 5
    if operation == "+1":
        return F.col("price") + 1
    if operation == "-1":
        return F.col("price") - 1
    if operation == "*col":
        return F.col("price") * F.col("price")
    if operation == "-p":
        return -F.col("price")
    if operation == "-null-dec":
        return -(F.lit(None).cast("decimal(10,2)"))
    if operation == "-null-int":
        return -(F.lit(None).cast("int"))
    if operation == "-null-bigint":
        return -(F.lit(None).cast("bigint"))
    if operation == "-null-double":
        return -(F.lit(None).cast("double"))
    return F.col("price") * F.lit(5).cast("decimal(1,0)")


def _assert_type(frame: DataFrame, expected_type: str) -> None:
    """Assert the analyzed facade type of column n."""
    assert frame.schema["n"].dataType.simpleString() == expected_type


def _assert_rows(rows: list[Any], expected_value: str) -> None:
    """Assert one collected row carries the oracle decimal digits."""
    assert len(rows) == 1
    assert str(rows[0]["n"]) == expected_value


def _assert_every_action(
    frame: DataFrame, expected_type: str, expected_value: str, cache_value: str
) -> None:
    """Assert type and value through eager, cache, persist, and collect."""
    _assert_type(frame, expected_type)
    eager = frame.eager()
    _assert_type(eager, expected_type)
    _assert_rows(eager.collect(), expected_value)
    cached = frame.cache()
    _assert_type(cached, expected_type)
    _assert_rows(cached.collect(), cache_value)
    persisted = frame.persist()
    _assert_type(persisted, expected_type)
    _assert_rows(persisted.collect(), expected_value)
    _assert_rows(frame.collect(), expected_value)


@pytest.mark.parametrize("cell", _oracle_cells(), ids=_cell_id)
def test_oracle_cell_on_facade_through_every_action(
    spark: ReparkSession, cell: dict[str, str]
) -> None:
    """C-005: one oracle cell via withColumns through every materializing action."""
    base = _cell_base_frame(spark, cell)
    frame = base.withColumns({"n": _facade_expression(cell["op"])})
    _assert_every_action(frame, cell["facade_type"], cell["facade_value"], cell["cache_value"])


@pytest.mark.parametrize("cell", _oracle_cells(), ids=_cell_id)
def test_oracle_cell_on_sql_door_through_every_action(
    spark: ReparkSession, cell: dict[str, str]
) -> None:
    """C-005: one oracle cell via spark.sql through every materializing action."""
    base = _cell_base_frame(spark, cell)
    base.createOrReplaceTempView("v")
    frame = spark.sql(f"SELECT {cell['sql']} AS n FROM v")
    _assert_every_action(frame, cell["sql_type"], cell["sql_value"], cell["cache_value"])


def test_original_report_shape_eager(spark: ReparkSession) -> None:
    """C-005: the reported withColumns times-five eager on a DECIMAL(38,10) frame."""
    frame = _base_frame(spark, "decimal(38,10)").withColumns({"new_price": F.col("price") * 5})
    assert frame.schema["new_price"].dataType.simpleString() == "decimal(38,8)"
    rows = frame.eager().collect()
    assert len(rows) == 1
    assert str(rows[0]["new_price"]) == "882.80000000"


_NULL_COLUMN_TYPES: list[tuple[str, str]] = [
    ("decimal(10,2)", "DECIMAL(10,2)"),
    ("int", "INT"),
    ("bigint", "BIGINT"),
    ("double", "DOUBLE"),
]


@pytest.mark.parametrize(("simple_type", "ddl_type"), _NULL_COLUMN_TYPES)
def test_neg_null_column_rows_stay_null(
    spark: ReparkSession, simple_type: str, ddl_type: str
) -> None:
    """C-012: negating an all-null column keeps every row null with the child type."""
    inferred = spark.createDataFrame([(1, None), (2, None)], "id LONG, price DOUBLE")
    base = inferred.withColumns({"price": F.col("price").cast(ddl_type)})
    frame = base.withColumns({"n": -F.col("price")})
    assert frame.schema["n"].dataType.simpleString() == simple_type
    for rows in (frame.collect(), frame.eager().collect()):
        assert [row["n"] for row in rows] == [None, None]
    base.createOrReplaceTempView("v_null")
    sql_frame = spark.sql("SELECT -price AS n FROM v_null")
    assert sql_frame.schema["n"].dataType.simpleString() == simple_type
    for rows in (sql_frame.collect(), sql_frame.eager().collect()):
        assert [row["n"] for row in rows] == [None, None]
