"""Pins the Spark-equal array answers FN-FIX-1 closed for the four EX-8 rows (registry §7)."""

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def test_array_position_not_found_returns_zero() -> None:
    """FN-ARRAYPOS-1: not-found is 0. pins: fn-fix-1-registry-rows/C-003"""
    repark = ReparkSession.builder.appName("fn-arraypos").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([([1, 2, 3], 2), ([1, 2, 3], 9), (None, 1)], ["a", "x"])
        values = [
            row["p"]
            for row in frame.select(F.array_position(F.col("a"), F.col("x")).alias("p")).collect()
        ]
        assert values == [2, 0, None]
        empty = repark.createDataFrame([([], 1)], ["a", "x"])
        empty_values = [
            row["p"]
            for row in empty.select(F.array_position(F.col("a"), F.col("x")).alias("p")).collect()
        ]
        assert empty_values == [0]
        sql_pos = [
            row["p"]
            for row in repark.sql("SELECT array(1, 1, 2, 3) AS a")
            .select(F.array_position(F.col("a"), F.lit(2)).alias("p"))
            .collect()
        ]
        assert sql_pos == [3]
    finally:
        repark.stop()


def test_array_sort_nulls_last() -> None:
    """FN-ARRAYSORT-1: array_sort NULLs last. pins: fn-fix-1-registry-rows/C-003"""
    repark = ReparkSession.builder.appName("fn-arraysort").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([([3, 1, 2],), ([2, None, 1],), (None,)], ["a"])
        values = [row["s"] for row in frame.select(F.array_sort(F.col("a")).alias("s")).collect()]
        assert values == [[1, 2, 3], [1, 2, None], None]
        sort_asc = [row["s"] for row in frame.select(F.sort_array(F.col("a")).alias("s")).collect()]
        assert sort_asc == [[1, 2, 3], [None, 1, 2], None]
        sort_desc = [
            row["s"] for row in frame.select(F.sort_array(F.col("a"), False).alias("s")).collect()
        ]
        assert sort_desc == [[3, 2, 1], [2, 1, None], None]
    finally:
        repark.stop()


def test_arrays_overlap_three_valued() -> None:
    """FN-ARRAYSOVERLAP-1: three-valued overlap. pins: fn-fix-1-registry-rows/C-003"""
    repark = ReparkSession.builder.appName("fn-arrays-overlap").master("local[1]").getOrCreate()
    try:
        data = [
            ([1, None], [2, None]),
            ([1, 2], [3, 4]),
            ([None], [1]),
            ([1, 2], [2, 3]),
            ([1, None], [1]),
            ([None], [None]),
        ]
        frame = repark.createDataFrame(data, ["a", "b"])
        values = [
            row["o"]
            for row in frame.select(F.arrays_overlap(F.col("a"), F.col("b")).alias("o")).collect()
        ]
        assert values == [None, False, None, True, True, None]
    finally:
        repark.stop()


def test_flatten_null_subarray_makes_row_null() -> None:
    """FN-FLATTEN-1: a NULL sub-array makes the row NULL. pins: fn-fix-1-registry-rows/C-003"""
    repark = ReparkSession.builder.appName("fn-flatten").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [([[1, 2], [3]],), ([[1], None],), ([[None, 1, 2]],), (None,)], ["a"]
        )
        values = [row["f"] for row in frame.select(F.flatten(F.col("a")).alias("f")).collect()]
        assert values == [[1, 2, 3], None, [None, 1, 2], None]
    finally:
        repark.stop()
