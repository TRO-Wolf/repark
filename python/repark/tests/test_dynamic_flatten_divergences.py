"""Measured dynamicFlatten divergences from Spark; cited in map.md."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.types import LongType, StructField, StructType


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-dynflatten-divergences").getOrCreate()
    try:
        yield session
    finally:
        session.stop()
        _reset_active_session_for_tests()


def _nested_struct(depth: int) -> StructType:
    inner = StructType([StructField("Val", LongType(), True)])
    for level in range(depth - 1, 0, -1):
        inner = StructType([StructField(f"L{level}", inner, True)])
    return inner


def _nested_value(depth: int) -> dict[str, Any]:
    value: dict[str, Any] = {"Val": 9}
    for level in range(depth - 1, 0, -1):
        value = {f"L{level}": value}
    return value


def _frame(spark: ReparkSession, depth: int, keep: str | None) -> Any:
    fields = [] if keep is None else [StructField(keep, LongType(), False)]
    fields.append(StructField("Payload", _nested_struct(depth), True))
    row: dict[str, Any] = {"Payload": _nested_value(depth)}
    if keep is not None:
        row[keep] = 1
    return spark.createDataFrame([row], schema=StructType(fields))


@pytest.mark.parametrize("depth", [1, 2, 3, 4])
@pytest.mark.parametrize("keep", [None, "id", "k"])
def test_keep_column_beside_any_struct_depth_collects_the_spark_row(
    spark: ReparkSession, depth: int, keep: str | None
) -> None:
    """Every cell of the keep x depth matrix collects the row live Spark returns."""
    table = _frame(spark, depth, keep).dynamicFlatten().to_arrow()
    leaf = "Payload_" + "_".join([f"L{index}" for index in range(1, depth)] + ["Val"])
    assert table.column_names == ([keep, leaf] if keep is not None else [leaf])
    expected = {leaf: 9} if keep is None else {keep: 1, leaf: 9}
    assert table.to_pylist() == [expected]
