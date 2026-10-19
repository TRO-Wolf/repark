"""TA-SERIES-S0b P-S0b-1 — ANSI division by negative zero matches Spark 4.1.2.

Recorded Spark 4.1.2 cells (ANSI on, ``results_spark_order.json``; no live Spark needed):

* ``SELECT 1.0D / -0.0D`` answers ``[DIVIDE_BY_ZERO] Division by zero. ...``
* ``SELECT a / b FROM VALUES (1.0D, -0.0D) AS t(a, b)`` answers ``[DIVIDE_BY_ZERO] ...``
* ``SELECT 1.0D / CAST('NaN' AS DOUBLE)`` answers ``nan``

RePark at S0 answered ``-inf`` for the first two cells; S0b raises like Spark.
"""

from __future__ import annotations

import math

import pyarrow as pa
import pytest

from repark import ReparkSession, functions
from repark.errors import PySparkException


@pytest.fixture
def spark() -> ReparkSession:
    """Build one test session."""
    session = ReparkSession.builder.appName("pytest-ansi-neg-zero").getOrCreate()
    yield session
    session.stop()


def test_ansi_divide_scalar_negative_zero_matches_spark(spark: ReparkSession) -> None:
    """Pin P-S0b-1: scalar ``1.0D / -0.0D`` raises ``DIVIDE_BY_ZERO``."""
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT 1.0D / -0.0D AS q").to_arrow()


def test_ansi_divide_column_negative_zero_matches_spark(spark: ReparkSession) -> None:
    """Pin P-S0b-1: column-valued ``-0.0`` raises ``DIVIDE_BY_ZERO`` on both doors."""
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT a / b AS q FROM VALUES (1.0D, -0.0D) AS t(a, b)").to_arrow()
    frame = spark.createDataFrame([(1.0, -0.0)], ["a", "b"])
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        frame.select((functions.col("a") / functions.col("b")).alias("q")).to_arrow()


def test_ansi_divide_nan_passes(spark: ReparkSession) -> None:
    """Pin P-S0b-1: division by NaN still answers ``nan``."""
    table = spark.sql("SELECT 1.0D / CAST('NaN' AS DOUBLE) AS q").to_arrow()
    assert table.schema.field("q").type == pa.float64()
    assert math.isnan(table.column("q")[0].as_py())
