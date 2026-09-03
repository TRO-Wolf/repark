"""Pins the four silent array divergences the EX-8 remediation filed (registry §7)."""

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def test_array_position_not_found_returns_null() -> None:
    """FN-ARRAYPOS-1 today: not-found is NULL; Spark 4.1.2 answers 0."""
    repark = ReparkSession.builder.appName("fn-arraypos").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([([1, 2, 3], 2), ([1, 2, 3], 9), (None, 1)], ["a", "x"])
        values = [
            row["p"]
            for row in frame.select(F.array_position(F.col("a"), F.col("x")).alias("p")).collect()
        ]
        assert values == [2, None, None]
    finally:
        repark.stop()


def test_array_sort_nulls_first() -> None:
    """FN-ARRAYSORT-1 today: array_sort orders NULLs first; Spark 4.1.2 orders them last."""
    repark = ReparkSession.builder.appName("fn-arraysort").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([([3, 1, 2],), ([2, None, 1],), (None,)], ["a"])
        values = [row["s"] for row in frame.select(F.array_sort(F.col("a")).alias("s")).collect()]
        assert values == [[1, 2, 3], [None, 1, 2], None]
    finally:
        repark.stop()


def test_arrays_overlap_null_block_returns_false() -> None:
    """FN-ARRAYSOVERLAP-1 today: a NULL element answers False; Spark 4.1.2 answers NULL."""
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
        assert values == [False, False, False, True, True, False]
    finally:
        repark.stop()


def test_flatten_drops_null_subarray() -> None:
    """FN-FLATTEN-1 today: a NULL sub-array is dropped; Spark 4.1.2 answers NULL."""
    repark = ReparkSession.builder.appName("fn-flatten").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [([[1, 2], [3]],), ([[1], None],), ([[None, 1, 2]],), (None,)], ["a"]
        )
        values = [row["f"] for row in frame.select(F.flatten(F.col("a")).alias("f")).collect()]
        assert values == [[1, 2, 3], [1], [None, 1, 2], None]
    finally:
        repark.stop()
