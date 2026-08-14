"""T7 / census r6 — isStreaming, Column.substr, array_contains facade pins."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

from repark import SparkSession
from repark.spark.functions import array_contains, col, lit


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    session = SparkSession.builder.master("local[1]").appName("test-t7-census-r6").getOrCreate()
    yield session
    session.stop()


def _rows(table: pa.Table) -> list[dict[str, Any]]:
    return table.to_pylist()


def test_is_streaming_always_false(spark: SparkSession) -> None:
    """Batch-only: DataFrame.isStreaming / is_streaming is False (classic FAIL-MISSING wall)."""
    frame = spark.createDataFrame([(1,)], "a long")
    assert frame.isStreaming is False
    assert frame.is_streaming is False


def test_column_substr_values(spark: SparkSession) -> None:
    """Column.substr uses Spark substring UDF (pos 0 ≡ 1)."""
    frame = spark.createDataFrame([("hello",), ("world",)], "name string")
    out = frame.select(col("name").substr(1, 2).alias("s")).to_arrow()
    assert _rows(out) == [{"s": "he"}, {"s": "wo"}]
    # pos 0 ≡ 1 (Spark)
    out0 = frame.select(col("name").substr(0, 3).alias("s")).to_arrow()
    assert _rows(out0) == [{"s": "hel"}, {"s": "wor"}]


def test_column_substr_column_args(spark: SparkSession) -> None:
    """Column.substr accepts Column start/length (classic enum-literal style)."""
    frame = spark.createDataFrame([("abcd", 1, 2)], "name string, start int, length int")
    out = frame.select(col("name").substr(col("start"), col("length")).alias("s")).to_arrow()
    assert _rows(out) == [{"s": "ab"}]


def test_array_contains_values(spark: SparkSession) -> None:
    """functions.array_contains Column builder (was FAIL-MISSING refuse)."""
    frame = spark.createDataFrame([([1, 2, 3],), ([4, 5],)], "arr array<long>")
    out = frame.select(array_contains(col("arr"), lit(2)).alias("hit")).to_arrow()
    rows = _rows(out)
    assert rows[0]["hit"] is True
    assert rows[1]["hit"] is False
