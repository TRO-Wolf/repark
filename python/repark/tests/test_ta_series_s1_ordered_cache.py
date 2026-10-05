"""TA-SERIES S1 pins: a sorted single-partition cache keeps its order on plain reads.

pins: ta-series-s1/P-S1-1, P-S1-2, P-S1-5, P-S1-6
"""

from __future__ import annotations

import itertools
from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.dataframe.core import DataFrame
from repark.spark.window import Window

ROWS = 200_000
RUNS = 3


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = (
        ReparkSession.builder.appName("pytest-ta-series-s1")
        .config("datafusion.execution.target_partitions", "16")
        .config("datafusion.optimizer.repartition_file_scans", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


def _permutation_frame(spark: ReparkSession) -> DataFrame:
    shuffled = (F.col("id") * 7_919 + 13) % ROWS
    return spark.range(0, ROWS).withColumn("key", shuffled)


def _nullable_key_frame(spark: ReparkSession) -> DataFrame:
    shuffled = (F.col("id") * 7_919 + 13) % ROWS
    key = F.when(F.col("id") % 997 == 0, F.lit(None)).otherwise(shuffled)
    return spark.range(0, ROWS).withColumn("key", key)


def _materialize(frame: DataFrame, door: str) -> DataFrame:
    if door == "cache":
        frame.cache()
        frame.count()
        return frame
    if door == "eager":
        return frame.eager()
    return frame.localCheckpoint()


def _keys(frame: DataFrame) -> list[int | None]:
    return frame.select("key").toArrow().column("key").to_pylist()


def _compare(first: int | None, second: int | None, descending: bool, nulls_first: bool) -> int:
    if first is None and second is None:
        return 0
    if first is None:
        return -1 if nulls_first else 1
    if second is None:
        return 1 if nulls_first else -1
    if first == second:
        return 0
    ordered = first < second
    if descending:
        ordered = not ordered
    return -1 if ordered else 1


def _is_ordered(values: list[int | None], descending: bool, nulls_first: bool) -> bool:
    for position in range(len(values) - 1):
        if _compare(values[position], values[position + 1], descending, nulls_first) > 0:
            return False
    return True


def test_sorted_eager_reads_back_in_order(spark: ReparkSession) -> None:
    eager = _permutation_frame(spark).sort("key").eager()
    for _ in range(RUNS):
        assert _is_ordered(_keys(eager), False, True)
        assert _is_ordered(_keys(eager.withColumn("one", F.lit(1))), False, True)


@pytest.mark.parametrize("door", ["cache", "eager", "checkpoint"])
def test_sorted_cache_keeps_order_on_plain_reads(spark: ReparkSession, door: str) -> None:
    cached = _materialize(_permutation_frame(spark).sort("key"), door)
    shaped = _materialize(
        _permutation_frame(spark)
        .filter(F.col("id") % 2 == 0)
        .withColumn("doubled", F.col("key") * 2)
        .sort("key"),
        door,
    )
    for _ in range(RUNS):
        assert _is_ordered(_keys(cached), False, True)
        assert _is_ordered(_keys(cached.withColumn("one", F.lit(1))), False, True)
        assert _is_ordered(_keys(cached.select("*")), False, True)
        assert _is_ordered(_keys(shaped), False, True)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "CACHE-ORDER-READ-1: BatchSplitStream splits the single batch at execution, "
        "RoundRobinBatch(16)-on-top spreads the slices, CoalescePartitionsExec merges "
        "in completion order"
    ),
)
@pytest.mark.parametrize("door", ["cache", "eager", "checkpoint"])
def test_sorted_cache_filter_read_order_follow_up(spark: ReparkSession, door: str) -> None:
    cached = _materialize(_permutation_frame(spark).sort("key"), door)
    late = _materialize(
        _permutation_frame(spark)
        .sort("key")
        .filter(F.col("id") % 2 == 0)
        .withColumn("doubled", F.col("key") * 2),
        door,
    )
    for _ in range(RUNS):
        assert _is_ordered(_keys(cached.filter(F.col("id") % 3 == 0)), False, True)
        assert _is_ordered(_keys(late), False, True)


def test_descending_and_nulls_first_order_carried(spark: ReparkSession) -> None:
    frame = _nullable_key_frame(spark)
    desc_first = _materialize(frame.sort(F.desc_nulls_first("key")), "eager")
    desc_last = _materialize(frame.sort(F.desc_nulls_last("key")), "eager")
    asc_last = _materialize(frame.sort(F.asc_nulls_last("key")), "eager")
    asc_first = _materialize(frame.sort(F.asc_nulls_first("key")), "eager")
    for _ in range(RUNS):
        assert _is_ordered(_keys(desc_first), True, True)
        assert _is_ordered(_keys(desc_last), True, False)
        assert _is_ordered(_keys(asc_last), False, False)
        assert _is_ordered(_keys(asc_first), False, True)
    resorted = _permutation_frame(spark).sort(F.desc("key")).eager().sort("key")
    for _ in range(RUNS):
        assert _is_ordered(_keys(resorted), False, True)


def test_sort_filter_cache_multi_partition_not_declared(spark: ReparkSession) -> None:
    late = _materialize(
        _permutation_frame(spark)
        .sort("key")
        .filter(F.col("id") % 2 == 0)
        .withColumn("doubled", F.col("key") * 2),
        "cache",
    )
    ranked = late.select(
        "key", F.row_number().over(Window.orderBy(F.asc_nulls_first("key"))).alias("rank")
    )
    table = ranked.toArrow()
    pairs = sorted(
        zip(
            table.column("key").to_pylist(),
            table.column("rank").to_pylist(),
            strict=True,
        )
    )
    for (_, first), (_, second) in itertools.pairwise(pairs):
        assert first < second
