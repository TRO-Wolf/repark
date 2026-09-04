"""Divergence pins for the EX-15 DataFrame-a example batch (registry §7 EX-DF-1..6)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException
from repark.spark.types import DoubleType, StructField, StructType


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-ex15-dataframe-a").getOrCreate()
    yield session
    session.stop()


def test_colregex_spelling_divergence(spark: ReparkSession) -> None:
    """colRegex compiles the raw string: plain regex matches, backticked raises (EX-DF-1)."""
    frame = spark.createDataFrame([("a", 1), ("b", 2)], ["g", "k"])
    assert frame.select(frame.colRegex("^(k)$")).columns == ["k"]
    assert frame.select(frame.col_regex("^(k)$")).columns == ["k"]
    with pytest.raises(AnalysisException):
        frame.colRegex("`^(k)$`")
    with pytest.raises(AnalysisException):
        frame.col_regex("`^(k)$`")


def test_global_temp_view_divergence(spark: ReparkSession) -> None:
    """All three global-temp-view spellings refuse loudly; Spark registers views (EX-DF-2)."""
    frame = spark.createDataFrame([(1,)], ["k"])
    with pytest.raises(UnsupportedOperationException, match="createGlobalTempView"):
        frame.createGlobalTempView("gt_ex15")
    with pytest.raises(UnsupportedOperationException, match="createGlobalTempView"):
        frame.createOrReplaceGlobalTempView("gt_ex15")
    with pytest.raises(UnsupportedOperationException, match="createGlobalTempView"):
        frame.create_global_temp_view("gt_ex15")


def test_except_all_divergence(spark: ReparkSession) -> None:
    """exceptAll / except_all refuse loudly; Spark answers the multiset difference (EX-DF-3)."""
    left = spark.createDataFrame([(1,), (1,), (2,)], ["n"])
    right = spark.createDataFrame([(1,)], ["n"])
    with pytest.raises(UnsupportedOperationException, match="exceptAll"):
        left.exceptAll(right)
    with pytest.raises(UnsupportedOperationException, match="exceptAll"):
        left.except_all(right)


def test_describe_row_order_divergence(spark: ReparkSession) -> None:
    """describe rows are unordered in repark; Spark prints count/mean/stddev/min/max (EX-DF-4)."""
    frame = spark.createDataFrame(
        [(1, 10.0), (2, 20.0), (2, 30.0), (3, 40.0), (1, 50.0), (2, None)],
        ["k", "v"],
    )
    described = frame.describe("k", "v")
    assert described.columns == ["summary", "k", "v"]
    rows = {row["summary"]: (row["k"], row["v"]) for row in described.collect()}
    assert rows == {
        "count": ("6", "5"),
        "mean": ("1.8333333333333333", "30.0"),
        "stddev": ("0.752772652709081", "15.811388300841896"),
        "min": ("1", "10.0"),
        "max": ("3", "50.0"),
    }


def test_corr_cov_null_pair_divergence(spark: ReparkSession) -> None:
    """corr/cov skip the NULL pair; Spark 4.1.2 answers the NULL as 0.0 (EX-DF-5)."""
    schema = StructType(
        [
            StructField("u", DoubleType(), True),
            StructField("v", DoubleType(), True),
        ]
    )
    frame = spark.createDataFrame(
        [(1.0, 10.0), (2.0, 20.0), (2.0, 30.0), (3.0, 40.0), (1.0, 50.0), (2.0, None)],
        schema,
    )
    assert frame.corr("u", "v") == 0.18898223650461363
    assert frame.cov("u", "v") == 2.5


def test_create_temp_view_replaces_silently(spark: ReparkSession) -> None:
    """createTempView replaces an existing name silently; Spark refuses the duplicate (EX-DF-6)."""
    first = spark.createDataFrame([(7,)], ["k"])
    first.createTempView("tv_dup_ex15")
    assert spark.sql("SELECT k FROM tv_dup_ex15").collect() == [(7,)]
    second = spark.createDataFrame([(8,)], ["k"])
    second.createTempView("tv_dup_ex15")
    assert spark.sql("SELECT k FROM tv_dup_ex15").collect() == [(8,)]
