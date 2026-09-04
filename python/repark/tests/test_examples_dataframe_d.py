"""Divergence pins for the EX-19 DataFrame-d example batch (registry §7 EX-DF-18/19, EX-ROW-1)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException
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
