"""PERF-UNPIVOT-1 step 1 — native stack() unpivot.

pins: perf-unpivot-1/C-001, perf-unpivot-1/C-002, perf-unpivot-1/C-003, perf-unpivot-1/C-004,
perf-unpivot-1/C-005
"""

from __future__ import annotations

import math
import time
from collections.abc import Iterator

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-perf-unpivot-1").getOrCreate()
    yield session
    session.stop()


def test_stack_sql_two_by_two_matches_spark(spark: ReparkSession) -> None:
    """SQL stack(2, 1, 2, 3, 4) projects col0/col1 over two rows.

    pins: perf-unpivot-1/C-001, perf-unpivot-1/C-004
    """
    table = spark.sql("SELECT stack(2, 1, 2, 3, 4)").to_arrow()
    assert table.column_names == ["col0", "col1"]
    assert table.to_pylist() == [{"col0": 1, "col1": 2}, {"col0": 3, "col1": 4}]


def test_stack_sql_pads_a_short_last_row(spark: ReparkSession) -> None:
    """A remainder column is NULL-filled on the last row.

    pins: perf-unpivot-1/C-001
    """
    rows = spark.sql("SELECT stack(2, 1, 2, 3)").to_arrow().to_pylist()
    assert rows == [{"col0": 1, "col1": 2}, {"col0": 3, "col1": None}]


def test_stack_sql_n_out_of_range_is_loud(spark: ReparkSession) -> None:
    """n must be in (0, 2147483647].

    pins: perf-unpivot-1/C-001
    """
    with pytest.raises(Exception, match="VALUE_OUT_OF_RANGE"):
        spark.sql("SELECT stack(0, 1, 2)").collect()
    with pytest.raises(Exception, match="VALUE_OUT_OF_RANGE"):
        spark.sql("SELECT stack(-1, 1, 2)").collect()


def test_stack_facade_with_passthrough_and_alias(spark: ReparkSession) -> None:
    """F.stack is reachable, aliases output names, and repeats siblings.

    pins: perf-unpivot-1/C-004
    """
    frame = spark.createDataFrame([(1, 10, 20, 30, 40)], ["id", "a", "b", "c", "d"])
    table = frame.select("id", F.stack(F.lit(2), "a", "b", "c", "d")).to_arrow()
    assert table.column_names == ["id", "col0", "col1"]
    assert table.to_pylist() == [
        {"id": 1, "col0": 10, "col1": 20},
        {"id": 1, "col0": 30, "col1": 40},
    ]
    aliased = (
        spark.range(1)
        .select(F.stack(F.lit(2), F.lit(1), F.lit(2), F.lit(3), F.lit(4)).alias("x", "y"))
        .to_arrow()
    )
    assert aliased.column_names == ["x", "y"]
    assert aliased.to_pylist() == [{"x": 1, "y": 2}, {"x": 3, "y": 4}]


def test_stack_independent_column_types(spark: ReparkSession) -> None:
    """Each output column keeps its own type; mixed columns in one field refuse.

    pins: perf-unpivot-1/C-001
    """
    table = spark.sql("SELECT stack(2, 1, 'a', 2, 'b')").to_arrow()
    assert table.schema.field("col0").type in {pa.int32(), pa.int64()}
    assert pa.types.is_string(table.schema.field("col1").type)
    assert [row["col1"] for row in table.to_pylist()] == ["a", "b"]
    with pytest.raises(Exception, match="STACK_COLUMN_DIFF_TYPES"):
        spark.sql("SELECT stack(2, 1, 2, 'a', 'b')").collect()


def test_stack_plan_is_scan_aggregate_unpivot(spark: ReparkSession) -> None:
    """EXPLAIN shows one scan, one aggregate, one unpivot, no per-cell projection.

    pins: perf-unpivot-1/C-003
    """
    frame = spark.range(1000).select(F.col("id").alias("k"))
    aggregated = frame.agg(
        F.sum("k").alias("c0"),
        F.min("k").alias("c1"),
        F.max("k").alias("c2"),
        F.count("k").alias("c3"),
        F.count("k").alias("c4"),
    )
    stacked = aggregated.select(F.stack(5, *aggregated.columns))
    explained = stacked._explain_text(extended=True)
    assert "UnpivotExec" in explained, explained
    assert explained.count("TableScan") == 1, explained
    assert "AggregateExec" in explained
    assert "mapInArrow" not in explained
    rows = stacked.to_arrow().to_pylist()
    assert len(rows) == 5
    assert list(stacked.columns) == ["col0"]


def _median(samples: list[float]) -> float:
    ordered = sorted(samples)
    return ordered[len(ordered) // 2]


def _time_stack(spark: ReparkSession, width: int) -> float:
    columns = [F.lit(index).alias(f"c{index}") for index in range(width)]
    frame = spark.range(1).select(*columns)
    stacked = frame.select(F.stack(5, *frame.columns))
    start = time.perf_counter()
    table = stacked.to_arrow()
    elapsed = time.perf_counter() - start
    assert table.num_rows == 5
    return elapsed


def test_stack_is_linear_in_columns(spark: ReparkSession) -> None:
    """stack over a one-row 50/250/500-column frame fits exponent <= 1.1.

    pins: perf-unpivot-1/C-002
    """
    widths = (50, 250, 500)
    medians: dict[int, float] = {}
    for width in widths:
        _time_stack(spark, width)
        samples = [_time_stack(spark, width) for _ in range(3)]
        medians[width] = _median(samples)
    log_x = [math.log(width) for width in widths]
    log_y = [math.log(max(medians[width], 1e-9)) for width in widths]
    x_mean = sum(log_x) / 3
    y_mean = sum(log_y) / 3
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(log_x, log_y, strict=True))
    denominator = sum((x - x_mean) ** 2 for x in log_x)
    exponent = numerator / denominator
    assert exponent <= 1.1, (
        f"stack wall must be linear in columns; exponent={exponent:.3f} "
        f"medians={medians} (bridge shape was 21.5s / 0.7s call at 500 columns)"
    )
    assert medians[500] < 21.5


def test_stack_name_is_exported(spark: ReparkSession) -> None:
    """F.stack is a public name.

    pins: perf-unpivot-1/C-005
    """
    assert hasattr(F, "stack")
    assert "stack" in F.__all__
    _ = spark
