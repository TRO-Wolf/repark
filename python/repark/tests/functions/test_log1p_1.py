"""LOG1P-1 — precise ``log1p`` / ``expm1`` kernels, three doors, Spark-equal.

pins: log1p-1-precise-kernels/C-001, C-002, C-004
"""

from __future__ import annotations

import math

import pyarrow as pa
import pytest

from repark.spark import SparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

LOG1P_1E16 = 1e-16
LOG1P_1E10 = 9.999999999500001e-11
LOG1P_NEG_1E10 = -1.00000000005e-10
LOG1P_1E5 = 9.999950000333332e-06
LOG1P_ONE = 0.6931471805599453
EXPM1_1E10 = 1.00000000005e-10
EXPM1_NEG_1E10 = -9.999999999500001e-11
EXPM1_1E5 = 1.0000050000166668e-05
EXPM1_ONE = 1.718281828459045
EXPM1_NEG_ONE = -0.6321205588285577
EXPM1_700 = 1.0142320547350045e304
LN_8 = 2.0794415416798357
LOG10_8 = 0.9030899869919434


def _spark() -> SparkSession:
    return SparkSession.builder.appName("log1p-1").getOrCreate()


def _sql_arrow(sql: str) -> pa.Table:
    return _spark().sql(sql).toArrow()


def _ansi_arrow(sql: str) -> pa.Table:
    import repark

    return repark.sql(sql).to_arrow()


def _assert_double(table: pa.Table, want: float | None) -> None:
    column = table.column("r")
    assert str(table.schema.field("r").type) == "double"
    got = column.to_pylist()[0]
    if want is None:
        assert got is None
        return
    assert got is not None
    if isinstance(want, float) and math.isnan(want):
        assert isinstance(got, float) and math.isnan(got)
        return
    assert got == want


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        ("SELECT log1p(CAST(1e-16 AS DOUBLE)) AS r", LOG1P_1E16),
        ("SELECT log1p(CAST(1e-10 AS DOUBLE)) AS r", LOG1P_1E10),
        ("SELECT log1p(CAST(-1e-10 AS DOUBLE)) AS r", LOG1P_NEG_1E10),
        ("SELECT log1p(CAST(1e-5 AS DOUBLE)) AS r", LOG1P_1E5),
        ("SELECT log1p(CAST(0.0 AS DOUBLE)) AS r", 0.0),
        ("SELECT log1p(CAST(1.0 AS DOUBLE)) AS r", LOG1P_ONE),
        ("SELECT log1p(CAST(-1.0 AS DOUBLE)) AS r", None),
        ("SELECT log1p(CAST(-2.0 AS DOUBLE)) AS r", None),
        ("SELECT log1p(CAST(NULL AS DOUBLE)) AS r", None),
        ("SELECT log1p(CAST('NaN' AS DOUBLE)) AS r", float("nan")),
        ("SELECT log1p(CAST(0 AS INT)) AS r", 0.0),
        ("SELECT log1p(CAST(1 AS INT)) AS r", LOG1P_ONE),
        ("SELECT log1p(CAST(1 AS DECIMAL(10,0))) AS r", LOG1P_ONE),
        ("SELECT log1p(CAST('0.0000000000000001' AS DECIMAL(38,16))) AS r", LOG1P_1E16),
        ("SELECT expm1(CAST(1e-16 AS DOUBLE)) AS r", LOG1P_1E16),
        ("SELECT expm1(CAST(1e-10 AS DOUBLE)) AS r", EXPM1_1E10),
        ("SELECT expm1(CAST(-1e-10 AS DOUBLE)) AS r", EXPM1_NEG_1E10),
        ("SELECT expm1(CAST(1e-5 AS DOUBLE)) AS r", EXPM1_1E5),
        ("SELECT expm1(CAST(0.0 AS DOUBLE)) AS r", 0.0),
        ("SELECT expm1(CAST(1.0 AS DOUBLE)) AS r", EXPM1_ONE),
        ("SELECT expm1(CAST(-1.0 AS DOUBLE)) AS r", EXPM1_NEG_ONE),
        ("SELECT expm1(CAST(700.0 AS DOUBLE)) AS r", EXPM1_700),
        ("SELECT expm1(CAST(710.0 AS DOUBLE)) AS r", float("inf")),
        ("SELECT expm1(CAST(NULL AS DOUBLE)) AS r", None),
        ("SELECT expm1(CAST('NaN' AS DOUBLE)) AS r", float("nan")),
        ("SELECT expm1(CAST(0 AS INT)) AS r", 0.0),
        ("SELECT expm1(CAST(1 AS INT)) AS r", EXPM1_ONE),
        ("SELECT expm1(CAST(1 AS DECIMAL(10,0))) AS r", EXPM1_ONE),
        ("SELECT expm1(CAST('0.0000000000000001' AS DECIMAL(38,16))) AS r", LOG1P_1E16),
    ],
)
def test_spark_sql_and_ansi_doors_match_spark(sql: str, want: float | None) -> None:
    """pins: log1p-1-precise-kernels/C-001, C-002, C-004"""
    spark_table = _sql_arrow(sql)
    ansi_table = _ansi_arrow(sql)
    _assert_double(spark_table, want)
    _assert_double(ansi_table, want)
    spark_val = spark_table.column("r").to_pylist()[0]
    ansi_val = ansi_table.column("r").to_pylist()[0]
    if spark_val is None:
        assert ansi_val is None
    elif isinstance(spark_val, float) and math.isnan(spark_val):
        assert isinstance(ansi_val, float) and math.isnan(ansi_val)
    else:
        assert spark_val == ansi_val


def test_facade_tiny_argument_is_not_the_composed_form() -> None:
    """pins: log1p-1-precise-kernels/C-004"""
    session = _spark()
    log1p = session.range(1).select(F.log1p(F.lit(1e-16)).alias("r")).toArrow()
    expm1 = session.range(1).select(F.expm1(F.lit(1e-16)).alias("r")).toArrow()
    log1p_val = log1p.column("r").to_pylist()[0]
    expm1_val = expm1.column("r").to_pylist()[0]
    assert log1p_val == LOG1P_1E16
    assert expm1_val == LOG1P_1E16
    assert log1p_val != math.log(1.0 + 1e-16)
    assert expm1_val != math.exp(1e-16) - 1.0
    assert str(log1p.schema.field("r").type) == "double"
    assert str(expm1.schema.field("r").type) == "double"


def test_facade_matches_sql_on_the_measured_grid() -> None:
    """pins: log1p-1-precise-kernels/C-001, C-004"""
    session = _spark()
    rows = [
        (1e-16, LOG1P_1E16, LOG1P_1E16),
        (1e-10, LOG1P_1E10, EXPM1_1E10),
        (-1e-10, LOG1P_NEG_1E10, EXPM1_NEG_1E10),
        (1e-5, LOG1P_1E5, EXPM1_1E5),
        (0.0, 0.0, 0.0),
        (1.0, LOG1P_ONE, EXPM1_ONE),
        (-1.0, None, EXPM1_NEG_ONE),
        (-2.0, None, -0.8646647167633873),
        (700.0, 6.55250788703459, EXPM1_700),
        (710.0, 6.566672429803241, float("inf")),
    ]
    for value, log1p_want, expm1_want in rows:
        frame = session.range(1).select(
            F.log1p(F.lit(value)).alias("l1"),
            F.expm1(F.lit(value)).alias("em"),
        )
        table = frame.toArrow()
        got_l1 = table.column("l1").to_pylist()[0]
        got_em = table.column("em").to_pylist()[0]
        if log1p_want is None:
            assert got_l1 is None
        else:
            assert got_l1 == log1p_want
        assert got_em == expm1_want


def test_sem1_incidentals_stay_green() -> None:
    """pins: log1p-1-precise-kernels/C-004"""
    session = _spark()
    frame = session.createDataFrame([(8.0,), (0.0,), (-1.0,)], "x double")
    log2 = frame.select(F.log2("x").alias("r")).toArrow()
    assert log2.column("r").to_pylist() == [3.0, None, None]
    log1p = frame.select(F.log1p("x").alias("r")).toArrow()
    assert log1p.column("r").to_pylist()[0] == math.log1p(8.0)
    assert log1p.column("r").to_pylist()[1] == 0.0
    assert log1p.column("r").to_pylist()[2] is None
    ln = session.range(1).select(F.ln(F.lit(8.0)).alias("r")).toArrow()
    assert ln.column("r").to_pylist() == [LN_8]
    import repark

    ansi_log = repark.sql("SELECT log(8) AS r").to_arrow()
    assert ansi_log.column("r").to_pylist()[0] == LOG10_8
    spark_log = _sql_arrow("SELECT log(8) AS r")
    assert spark_log.column("r").to_pylist()[0] == LN_8
    exp = session.range(1).select(F.exp(F.lit(1.0)).alias("r")).toArrow()
    assert exp.column("r").to_pylist()[0] == math.exp(1.0)


def test_facade_source_is_the_kernel_not_composition() -> None:
    """pins: log1p-1-precise-kernels/C-004"""
    from pathlib import Path

    text = (
        Path(__file__)
        .resolve()
        .parents[1]
        .joinpath("src/repark/spark/functions_expr.py")
        .read_text(encoding="utf-8")
    )
    assert 'return _scalar("log1p", col)' in text
    assert 'return _scalar("expm1", col)' in text
    assert "return log(lit(1) + _as_column_arg(col, as_lit=False))" not in text
    assert "return exp(col) - lit(1)" not in text
