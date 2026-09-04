"""Divergence pins for the EX-16 DataFrame-b example batch (registry §7 EX-DF-7..10)."""

from __future__ import annotations

import contextlib
import io
from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark.errors import UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-ex16-dataframe-b").getOrCreate()
    yield session
    session.stop()


def test_intersect_all_divergence(spark: ReparkSession) -> None:
    """intersectAll / intersect_all refuse; Spark answers the multiset intersect (EX-DF-7)."""
    left = spark.createDataFrame([(1,), (1,), (2,)], ["n"])
    right = spark.createDataFrame([(1,), (1,), (3,)], ["n"])
    with pytest.raises(UnsupportedOperationException, match="intersectAll"):
        left.intersectAll(right)
    with pytest.raises(UnsupportedOperationException, match="intersectAll"):
        left.intersect_all(right)


def test_grouping_sets_divergence(spark: ReparkSession) -> None:
    """groupingSets is one set per column plus the grand total; Spark's shape differs (EX-DF-8)."""
    frame = spark.createDataFrame([("a", 1), ("a", 2), ("b", 3)], ["g", "k"])
    counted = frame.groupingSets("g", "k").count()
    assert counted.columns == ["g", "k", "count"]
    assert set(counted.collect()) == {
        ("a", None, 2),
        ("b", None, 1),
        (None, 1, 1),
        (None, 2, 1),
        (None, 3, 1),
        (None, None, 3),
    }
    snake_counted = frame.grouping_sets("g", "k").count()
    assert snake_counted.columns == ["g", "k", "count"]
    with pytest.raises(AttributeError):
        frame.groupingSets([("g", "k"), ("g",), ()], "g", "k")


def test_merge_into_divergence(spark: ReparkSession) -> None:
    """mergeInto's sugar key and target./source. qualifiers work; Spark refuses both (EX-DF-9)."""
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).write.saveAsTable("ex16_b_mrg")
    source = spark.createDataFrame([(1, "A"), (3, "c")], ["id", "name"])
    (
        source.mergeInto("ex16_b_mrg", "id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()
    )
    rows = {tuple(row) for row in spark.sql("SELECT id, name FROM ex16_b_mrg").collect()}
    assert rows == {(1, "A"), (2, "b"), (3, "c")}
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).write.saveAsTable("ex16_b_mrg2")
    snake_source = spark.createDataFrame([(1, "A"), (3, "c")], ["id", "name"])
    (
        snake_source.merge_into("ex16_b_mrg2", F.col("target.id") == F.col("source.id"))
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()
    )
    snake_rows = {tuple(row) for row in spark.sql("SELECT id, name FROM ex16_b_mrg2").collect()}
    assert snake_rows == {(1, "A"), (2, "b"), (3, "c")}


def test_print_schema_stdout_matches_spark(spark: ReparkSession) -> None:
    """printSchema's stdout is byte-identical to Spark's capture (EX-DF-10 FIXED)."""
    frame = spark.createDataFrame([("a", 1, 10.0)], ["g", "k", "v"])
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        frame.printSchema()
    assert printed.getvalue() == (
        "root\n"
        " |-- g: string (nullable = true)\n"
        " |-- k: long (nullable = true)\n"
        " |-- v: double (nullable = true)\n"
        "\n"
    )
