"""Divergence pins for the EX-17 Column-a example batch (registry §7 EX-COL-1..2)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812
from repark.spark.types import DoubleType, StringType, StructField, StructType


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-ex17-column-a").getOrCreate()
    yield session
    session.stop()


def test_col_cast_qualified_projection_name(spark: ReparkSession) -> None:
    """Bare F.col cast select names the CDF-qualified column; Spark answers v (EX-COL-1)."""
    frame = spark.createDataFrame([(10.0,)], ["v"])
    name = frame.select(F.col("v").cast("double")).columns[0]
    assert name.startswith("datafusion.public.__repark_cdf_") and name.endswith(".v")


def test_get_field_bare_projection_name(spark: ReparkSession) -> None:
    """Unaliased getField projects r['a']; Spark 4.1.2 answers r.a (EX-COL-2)."""
    schema = StructType(
        [
            StructField(
                "r",
                StructType([StructField("a", StringType()), StructField("b", DoubleType())]),
            )
        ]
    )
    frame = spark.createDataFrame([(("x", 2.0),)], schema)
    assert frame.select(frame.r.getField("a")).columns == ["r['a']"]
