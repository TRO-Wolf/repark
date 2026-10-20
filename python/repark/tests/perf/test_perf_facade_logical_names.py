"""PERF-FACADE-WITHCOLUMN-1 — logical-schema column names are byte-equal to the analyzed names.

Oracle: ``PyDataFrame.column_names`` (the analyzed schema, still analyzer-backed) against
``_native.logical_column_names`` (the plan schema the facade now reads). The repark analyzer
rules all rewrite through ``NamePreserver``; this pin holds that invariant at the boundary
``DataFrame.columns`` depends on.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from repark import ReparkSession, _native
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark(tmp_path: Path) -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-perf-facade-names").getOrCreate()
    session.createDataFrame([(index,) for index in range(8)], ["id"]).createOrReplaceTempView(
        "nums"
    )
    yield session
    session.stop()


def _assert_names_agree(frame: object, label: str) -> list[str]:
    inner = frame._inner
    analyzed = list(inner.column_names())
    logical = list(_native.logical_column_names(inner))
    assert logical == analyzed, label
    return logical


STATEMENTS: tuple[tuple[str, str], ...] = (
    ("star", "SELECT * FROM nums"),
    ("alias", "SELECT id AS Renamed, id + 1 AS `two words` FROM nums"),
    (
        "unaliased_arith",
        "SELECT id + 1, id * 2, id / 3, id % 2, -id, CAST(id AS DOUBLE) FROM nums",
    ),
    (
        "decimal_arith",
        "SELECT CAST(id AS DECIMAL(10,2)) + CAST(id AS DECIMAL(6,4)), "
        "CAST(id AS DECIMAL(10,2)) / CAST(2 AS DECIMAL(3,1)) FROM nums",
    ),
    (
        "mixed_int_widths",
        "SELECT CAST(id AS TINYINT) + CAST(id AS BIGINT), CAST(id AS SMALLINT) * 2 FROM nums",
    ),
    (
        "strings",
        "SELECT concat(CAST(id AS STRING), 'x'), substr(CAST(id AS STRING), 1, 1), "
        "upper(CAST(id AS STRING)) LIKE '%1%' FROM nums",
    ),
    (
        "nested",
        "SELECT named_struct('A', id, 'b', CAST(id AS STRING)) AS S, array(id, id + 1) AS arr, "
        "map('k', id) AS m FROM nums",
    ),
    (
        "nested_field",
        "SELECT t.s.a FROM (SELECT named_struct('a', id, 'b', id + 1) AS s FROM nums) t",
    ),
    ("case_preserved", "SELECT id AS MixedCase, id AS UPPER, id AS lower FROM nums"),
    ("aggregate", "SELECT count(*), sum(id), avg(id), max(id) FROM nums"),
    (
        "group_by",
        "SELECT id % 2 AS bucket, count(*) AS n, avg(id) FROM nums GROUP BY id % 2",
    ),
    (
        "window",
        "SELECT id, row_number() OVER (ORDER BY id), sum(id) OVER (ORDER BY id) FROM nums",
    ),
    (
        "join_star",
        "SELECT * FROM nums a JOIN nums b ON a.id = b.id",
    ),
    ("subquery_star", "SELECT * FROM (SELECT id AS Kept FROM nums) t"),
    ("union", "SELECT id FROM nums UNION ALL SELECT id + 1 FROM nums"),
    ("case_when", "SELECT CASE WHEN id > 1 THEN id ELSE 0 END, id IS NULL FROM nums"),
    ("cast_timestamp", "SELECT CAST(id AS TIMESTAMP) FROM nums"),
    ("cast_date", "SELECT CAST('2020-01-05' AS DATE) FROM nums"),
    ("cast_float", "SELECT CAST(id AS FLOAT) FROM nums"),
    ("cast_string", "SELECT CAST(id AS STRING) FROM nums"),
)


@pytest.mark.parametrize(("label", "sql"), STATEMENTS, ids=[name for name, _ in STATEMENTS])
def test_logical_names_equal_analyzed_names_for_sql(
    spark: ReparkSession, label: str, sql: str
) -> None:
    """Each planned statement reports the same field names before and after analysis."""
    _assert_names_agree(spark.sql(sql), label)


def test_logical_names_equal_analyzed_names_across_a_withcolumn_chain(
    spark: ReparkSession,
) -> None:
    """Every frame in a dependent withColumn chain keeps analyzed-equal names."""
    frame = spark.sql("SELECT id, CAST(id AS DOUBLE) AS v FROM nums")
    _assert_names_agree(frame, "chain-0")
    for depth in range(12):
        frame = frame.withColumn(f"c{depth}", F.col("v") * depth + F.col("id"))
        _assert_names_agree(frame, f"chain-{depth + 1}")
    assert frame.columns == ["id", "v", *[f"c{depth}" for depth in range(12)]]


def test_logical_names_equal_analyzed_names_for_dataframe_transforms(
    spark: ReparkSession,
) -> None:
    """select / filter / drop / withColumnRenamed keep analyzed-equal names."""
    base = spark.sql("SELECT id AS Id, CAST(id AS STRING) AS S FROM nums")
    _assert_names_agree(base.select("Id"), "select")
    _assert_names_agree(base.filter(F.col("Id") > 1), "filter")
    _assert_names_agree(base.drop("S"), "drop")
    _assert_names_agree(base.withColumnRenamed("S", "Renamed"), "renamed")
    _assert_names_agree(base.select(F.col("Id") + 1), "select-expr")
    _assert_names_agree(base.select(F.lit(1), F.lit("x"), F.lit(None)), "select-literals")
    _assert_names_agree(base.orderBy("Id").limit(2), "order-limit")
    _assert_names_agree(base.groupBy("Id").agg(F.count("*")), "grouped")


def test_columns_property_matches_the_analyzed_names(spark: ReparkSession) -> None:
    """``DataFrame.columns`` still answers with the analyzed field names."""
    frame = spark.sql("SELECT id AS MixedCase, id + 1, CAST(id AS DECIMAL(9,2)) FROM nums")
    assert frame.columns == list(frame._inner.column_names())
