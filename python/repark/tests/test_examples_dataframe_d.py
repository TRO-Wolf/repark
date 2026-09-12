"""Divergence pins for the EX-19 DataFrame-d and EX-29 class-remainder batches.

Registry §7 rows EX-DF-18/19, EX-ROW-1 (EX-19), the EX-DF-1 arm extension (EX-29), and the
EX-DF-4 describe/string-column pins flipped to Spark's answer by DF-DESCRIBE-STR-1.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark import functions as F  # noqa: N812


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-ex19-dataframe-d").getOrCreate()
    yield session
    session.stop()


def test_with_columns_renamed_duplicate_names_divergence(spark: ReparkSession) -> None:
    """withColumnsRenamed refuses duplicate names; Spark answers ['k', 'k', 'v'] (EX-DF-18)."""
    frame = spark.createDataFrame([("a", 1, 10.0)], ["g", "k", "v"])
    with pytest.raises(AnalysisException, match="duplicate column names"):
        frame.withColumnsRenamed({"g": "k", "k": "k"})
    with pytest.raises(AnalysisException, match="duplicate column names"):
        frame.with_columns_renamed({"g": "k", "k": "k"})


def test_stat_freq_items_refuses(spark: ReparkSession) -> None:
    """stat.freqItems refuses loudly; Spark answers the frequent-item table (EX-DF-19)."""
    frame = spark.createDataFrame([(1, 10.0), (2, 20.0), (3, 40.0)], ["k", "v"])
    with pytest.raises(UnsupportedOperationException, match="freqItems"):
        frame.stat.freqItems(["k", "v"])


def test_row_asdict_recursive_false_struct_divergence(spark: ReparkSession) -> None:
    """asDict(False) flattens a struct field to a dict; Spark keeps the nested Row (EX-ROW-1)."""
    frame = spark.createDataFrame([("a", 1)], ["g", "k"])
    row = frame.select(F.struct("g", "k").alias("s")).first()
    assert row.asDict() == {"s": {"g": "a", "k": 1}}
    assert row.as_dict() == {"s": {"g": "a", "k": 1}}
    assert row.asDict(True) == {"s": {"g": "a", "k": 1}}


def test_describe_string_column_null_stats(spark: ReparkSession) -> None:
    """describe over a string column answers NULL mean/stddev cells like Spark (EX-DF-4)."""
    frame = spark.createDataFrame([("a", 1), ("b", 2)], ["g", "k"])
    described = frame.describe("g")
    assert described.columns == ["summary", "g"]
    assert [(row["summary"], row["g"]) for row in described.collect()] == [
        ("count", "2"),
        ("mean", None),
        ("stddev", None),
        ("min", "a"),
        ("max", "b"),
    ]
    assert [(row["summary"], row["g"], row["k"]) for row in frame.describe().collect()] == [
        ("count", "2", "2"),
        ("mean", None, "1.5"),
        ("stddev", None, "0.7071067811865476"),
        ("min", "a", "1"),
        ("max", "b", "2"),
    ]
    numeric_strings = spark.createDataFrame([("10",), ("2",), ("a",)], ["s"])
    assert [(row["summary"], row["s"]) for row in numeric_strings.describe("s").collect()] == [
        ("count", "3"),
        ("mean", "6.0"),
        ("stddev", "5.656854249492381"),
        ("min", "10"),
        ("max", "a"),
    ]


def test_describe_non_describable_column_arms(spark: ReparkSession) -> None:
    """describe/summary skip non-numeric non-string columns; naming one raises (EX-DF-4)."""
    frame = spark.createDataFrame([(True, 1), (False, 2)], ["b", "k"])
    assert frame.describe().columns == ["summary", "k"]
    assert frame.summary("count", "mean", "stddev", "min", "max").columns == [
        "summary",
        "k",
    ]
    with pytest.raises(PySparkValueError):
        frame.describe("b").collect()


def test_colregex_multi_match_first_match(spark: ReparkSession) -> None:
    """colRegex answers the first match only; Spark expands all matches (EX-DF-1)."""
    frame = spark.createDataFrame([("a", 1, 10.0)], ["g", "k", "v"])
    assert frame.select(frame.colRegex("^(g|k)$")).columns == ["g"]
    assert frame.select(frame.col_regex("^(g|k)$")).columns == ["g"]
