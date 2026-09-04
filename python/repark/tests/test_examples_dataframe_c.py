"""Divergence pins for the EX-18 DataFrame-c example batch (registry §7 EX-DF-10..16)."""

from __future__ import annotations

import contextlib
import io
from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException, UnsupportedOperationException

SIX_ROWS = [
    ("a", 1, 10.0),
    ("a", 2, 20.0),
    ("a", 2, 30.0),
    ("a", 3, 40.0),
    ("b", 1, 50.0),
    ("b", 2, None),
]


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-ex18-dataframe-c").getOrCreate()
    yield session
    session.stop()


def test_same_semantics_alias_divergence(spark: ReparkSession) -> None:
    """sameSemantics answers handle identity: True on self, False on the aliased twin (EX-DF-10)."""
    frame = spark.createDataFrame(SIX_ROWS, ["g", "k", "v"])
    assert frame.sameSemantics(frame) is True
    assert frame.same_semantics(frame) is True
    assert frame.sameSemantics(frame.alias("x")) is False


def test_replace_unsubset_arms(spark: ReparkSession) -> None:
    """replace without subset casts or raises; Spark replaces typed cells per column (EX-DF-11)."""
    frame = spark.createDataFrame([(1, "x"), (2, "y")], ["n", "s"])
    with pytest.raises(PySparkException, match="Cast error"):
        frame.replace("x", "xx").collect()
    numeric = spark.createDataFrame([(1, 10.0), (2, 20.0)], ["k", "v"])
    replaced = numeric.replace(20.0, 99.0)
    assert set(replaced.collect()) == {(1.0, 10.0), (2.0, 99.0)}


def test_sample_plan_seed_stable(spark: ReparkSession) -> None:
    """sample(0.5, seed=1) bakes a plan seed: two collects answer one stable multiset (EX-DF-12)."""
    frame = spark.createDataFrame(SIX_ROWS, ["g", "k", "v"])
    sampled = frame.sample(0.5, seed=1)
    first = set(sampled.collect())
    assert first == {("a", 1, 10.0), ("a", 2, 30.0), ("b", 1, 50.0)}
    assert set(sampled.collect()) == first
    assert frame.sample(fraction=1.0, seed=1).count() == 6


def test_sampleby_seeded_fraction_divergence(spark: ReparkSession) -> None:
    """sampleBy 0.5/0.5 at seed 0 keeps three rows where Spark keeps two (EX-DF-13)."""
    frame = spark.createDataFrame(SIX_ROWS, ["g", "k", "v"])
    sampled = frame.sampleBy("g", {"a": 0.5, "b": 0.5}, seed=0)
    assert set(sampled.collect()) == {("a", 2, 30.0), ("a", 3, 40.0), ("b", 2, None)}


def test_summary_divergent_arms(spark: ReparkSession) -> None:
    """Bare summary() and string-column mean raise; Spark answers ordered rows (EX-DF-14)."""
    frame = spark.createDataFrame(SIX_ROWS, ["g", "k", "v"])
    with pytest.raises(UnsupportedOperationException, match="not Spark-shaped"):
        frame.summary()
    with pytest.raises(AnalysisException):
        frame.summary("count", "mean", "stddev")
    cells = set(frame.summary("count", "min", "max").collect())
    assert cells == {
        ("count", "6", "6", "5"),
        ("min", "a", "1", "10.0"),
        ("max", "b", "3", "50.0"),
    }
    stats = spark.createDataFrame(
        [(1, 10.0), (2, 20.0), (2, 30.0), (3, 40.0), (1, 50.0)],
        ["k", "v"],
    )
    assert stats.summary("count").collect() == [("count", "5", "5")]


def test_show_rendering_divergence(spark: ReparkSession) -> None:
    """show(3) renders the repark grid without Spark's 'only showing top' trailer (EX-DF-15)."""
    frame = spark.createDataFrame(
        [("a", 1, 10.0), ("a", 2, 20.0), ("a", 2, 30.0)],
        ["g", "k", "v"],
    )
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        frame.show(3)
    assert printed.getvalue() == (
        "+---+---+------+\n"
        "| g | k | v    |\n"
        "+---+---+------+\n"
        "| a | 1 | 10.0 |\n"
        "| a | 2 | 20.0 |\n"
        "| a | 2 | 30.0 |\n"
        "+---+---+------+\n"
    )


def test_tojson_refuses(spark: ReparkSession) -> None:
    """toJSON refuses loudly; Spark answers one JSON object string per row (EX-DF-16)."""
    frame = spark.createDataFrame([("a", 1)], ["g", "k"])
    with pytest.raises(UnsupportedOperationException, match="toJSON"):
        frame.toJSON()
