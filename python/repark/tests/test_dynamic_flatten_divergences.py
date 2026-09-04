"""Measured dynamicFlatten divergences from Spark; cited in map.md."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
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


@pytest.mark.parametrize("keep", ["id", "k"])
def test_keep_column_at_depth_two_is_ambiguous_qualified_vs_unqualified(
    spark: ReparkSession, keep: str
) -> None:
    """Depth 2 beside any keep column fails on qualified vs unqualified, not on duplication."""
    ambiguous = r"\bqualified field name\b.*which would be ambiguous"
    with pytest.raises(AnalysisException, match=ambiguous):
        _frame(spark, 2, keep).dynamicFlatten().to_arrow()


@pytest.mark.parametrize("depth", [3, 4])
@pytest.mark.parametrize("keep", ["id", "k"])
def test_keep_column_at_depth_three_and_deeper_duplicates_the_unqualified_name(
    spark: ReparkSession, depth: int, keep: str
) -> None:
    """Depth 3 and deeper beside a keep column fail on duplication, a different message."""
    with pytest.raises(AnalysisException, match=r"duplicate unqualified field name"):
        _frame(spark, depth, keep).dynamicFlatten().to_arrow()


@pytest.mark.parametrize(("depth", "keep"), [(1, "id"), (1, "k"), (3, None), (4, None)])
def test_depth_one_with_keep_and_any_depth_without_keep_still_collect(
    spark: ReparkSession, depth: int, keep: str | None
) -> None:
    """Control: the clash needs both a sibling keep column and depth 2 or deeper."""
    table = _frame(spark, depth, keep).dynamicFlatten().to_arrow()
    assert table.num_rows == 1
    expected = ["Payload_" + "_".join([f"L{i}" for i in range(1, depth)] + ["Val"])]
    assert table.column_names == ([keep, *expected] if keep is not None else expected)
