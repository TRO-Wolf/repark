"""SEM-1 LOG-1 — Spark-door ``log`` is the natural log, dual-arity, null-guarded.

Oracle: live PySpark 4.1.2 (2026-08-31). Value AND Arrow type on ``toArrow()``.
Reachable Spark doors: Spark SQL and the facade Column API. Native ANSI
``repark.sql()`` keeps DataFusion base-10 ``log``.
"""

from __future__ import annotations

import math

from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _spark():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("sem1-spark-log").getOrCreate()


def _sql_arrow(sql: str):
    return _spark().sql(sql).toArrow()


def _double_null(sql: str) -> None:
    table = _sql_arrow(sql)
    assert table.column("r").to_pylist() == [None], sql
    assert str(table.schema.field("r").type) == "double", sql


def test_sql_one_arg_is_natural_log() -> None:
    """pins: sem-1-spark-answer-parity/C-004"""
    table = _sql_arrow("SELECT log(8) AS r")
    assert table.column("r").to_pylist() == [2.0794415416798357]
    assert str(table.schema.field("r").type) == "double"


def test_sql_two_arg_positive() -> None:
    """pins: sem-1-spark-answer-parity/C-004"""
    table = _sql_arrow("SELECT log(2, 8) AS r")
    assert table.column("r").to_pylist() == [3.0]
    assert str(table.schema.field("r").type) == "double"


def test_sql_domain_edges_are_null() -> None:
    """pins: sem-1-spark-answer-parity/C-004, C-010"""
    for sql in (
        "SELECT log(0) AS r",
        "SELECT log(-1) AS r",
        "SELECT log(CAST(NULL AS DOUBLE)) AS r",
        "SELECT log(0, 8) AS r",
        "SELECT log(-2, 8) AS r",
        "SELECT log(10, 0) AS r",
        "SELECT log(10, -1) AS r",
        "SELECT log(CAST(NULL AS DOUBLE), 8) AS r",
        "SELECT log(10, CAST(NULL AS DOUBLE)) AS r",
        "SELECT log(CAST('-Infinity' AS DOUBLE)) AS r",
    ):
        _double_null(sql)


def test_sql_base_one_is_ieee() -> None:
    """pins: sem-1-spark-answer-parity/C-004, C-010"""
    inf = _sql_arrow("SELECT log(1, 8) AS r").column("r").to_pylist()[0]
    assert inf == float("inf")
    nan = _sql_arrow("SELECT log(1, 1) AS r").column("r").to_pylist()[0]
    assert isinstance(nan, float) and math.isnan(nan)


def test_facade_two_arg_form() -> None:
    """pins: sem-1-spark-answer-parity/C-006"""
    table = _spark().range(1).select(F.log(2.0, F.lit(8.0)).alias("r")).toArrow()
    assert table.column("r").to_pylist() == [3.0]
    assert str(table.schema.field("r").type) == "double"
    edges = (
        _spark()
        .createDataFrame([(8.0,), (0.0,), (-1.0,)], "x double")
        .select(F.log(2.0, "x").alias("r"))
        .toArrow()
    )
    assert edges.column("r").to_pylist() == [3.0, None, None]


def test_facade_one_arg_matches_sql() -> None:
    """pins: sem-1-spark-answer-parity/C-006"""
    frame = _spark().range(1)
    facade = frame.select(F.log(F.lit(8.0)).alias("r")).toArrow()
    door = _sql_arrow("SELECT log(8) AS r")
    assert facade.column("r").to_pylist() == door.column("r").to_pylist()
    assert str(facade.schema.field("r").type) == "double"


def test_ln_stays_natural_log() -> None:
    """pins: sem-1-spark-answer-parity/C-006"""
    table = _spark().range(1).select(F.ln(F.lit(8.0)).alias("r")).toArrow()
    assert table.column("r").to_pylist() == [2.0794415416798357]


def test_log2_and_log1p_incidental() -> None:
    """pins: sem-1-spark-answer-parity/C-010"""
    frame = _spark().createDataFrame([(8.0,), (0.0,), (-1.0,)], "x double")
    log2 = frame.select(F.log2("x").alias("r")).toArrow()
    assert log2.column("r").to_pylist() == [3.0, None, None]
    log1p = frame.select(F.log1p("x").alias("r")).toArrow()
    assert log1p.column("r").to_pylist()[0] == math.log1p(8.0)
    assert log1p.column("r").to_pylist()[1] == 0.0
    assert log1p.column("r").to_pylist()[2] is None


def test_native_ansi_log_stays_base_ten() -> None:
    """pins: sem-1-spark-answer-parity/C-007"""
    import repark

    table = repark.sql("SELECT log(8) AS r").to_arrow()
    assert table.column("r").to_pylist()[0] == 0.9030899869919434
