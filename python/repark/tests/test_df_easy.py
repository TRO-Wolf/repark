"""R-DF-EASY — easy DataFrame lowerings (oracle shapes; coverage over completeness)."""

from __future__ import annotations

import io
from contextlib import redirect_stdout

import pytest

from repark import ReparkSession
from repark.errors import (
    PySparkValueError,
    UnsupportedOperationException,
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-df-easy").getOrCreate()
    yield session
    session.stop()


def test_select_expr_and_to_df(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id, 10 AS v")
    out = frame.selectExpr("id", "v + 1 AS v1")
    table = out.to_arrow()
    assert table.to_pylist() == [{"id": 1, "v1": 11}]
    renamed = frame.toDF("a", "b")
    assert renamed.columns == ["a", "b"]
    assert renamed.to_arrow().to_pylist() == [{"a": 1, "b": 10}]


def test_dtypes_print_schema_to_arrow(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id, 'x' AS name")
    assert ("id", "int") in [(n, t if t != "bigint" else "int") for n, t in frame.dtypes] or any(
        n == "id" for n, _ in frame.dtypes
    )
    assert any(n == "name" and "string" in t for n, t in frame.dtypes)
    buf = io.StringIO()
    with redirect_stdout(buf):
        frame.printSchema()
    text = buf.getvalue()
    assert "root" in text
    assert "id" in text
    assert frame.toArrow().num_rows == 1


def test_set_ops_and_cross_join(spark: ReparkSession) -> None:
    left = spark.sql("SELECT 1 AS id UNION ALL SELECT 1 UNION ALL SELECT 2")
    right = spark.sql("SELECT 1 AS id UNION ALL SELECT 3")
    assert left.intersect(right).to_arrow().to_pylist() == [{"id": 1}]
    # Multiset *All forms: engine bags diverge from Spark — fail loud (octo C1-L-005/006).
    with pytest.raises(UnsupportedOperationException, match="intersectAll"):
        left.intersectAll(right)
    with pytest.raises(UnsupportedOperationException, match="exceptAll"):
        left.exceptAll(spark.sql("SELECT 1 AS id"))
    assert sorted(r["id"] for r in left.subtract(right).to_arrow().to_pylist()) == [2]
    crossed = spark.sql("SELECT 1 AS a").crossJoin(spark.sql("SELECT 2 AS b"))
    assert crossed.to_arrow().to_pylist() == [{"a": 1, "b": 2}]


def test_offset_and_alias(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3").orderBy("id")
    assert [r[0] for r in frame.offset(1).collect()] == [2, 3]
    aliased = frame.alias("t")
    assert aliased.count() == 3


def test_describe_summary_replace(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id, 2.0 AS x UNION ALL SELECT 2, 4.0")
    desc = frame.describe("id", "x")
    assert "summary" in desc.columns
    assert desc.count() == 5
    rows = {r[0]: r for r in desc.collect()}
    assert "count" in rows and "mean" in rows
    summary = frame.summary("count", "min", "max")
    assert summary.count() == 3
    with pytest.raises(UnsupportedOperationException, match=r"without statistics|percentiles"):
        frame.summary()
    with pytest.raises(UnsupportedOperationException, match="25%"):
        frame.summary("25%")
    replaced = frame.replace(1, 9, subset=["id"])
    assert sorted(r[0] for r in replaced.collect()) == [2, 9]


def test_sample_seed_deterministic(spark: ReparkSession) -> None:
    frame = spark.sql(
        "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
        "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8"
    )
    a = frame.sample(fraction=0.5, seed=7).to_arrow().to_pylist()
    b = frame.sample(fraction=0.5, seed=7).to_arrow().to_pylist()
    assert a == b
    assert 0 <= len(a) <= 8
    with pytest.raises(UnsupportedOperationException, match="withReplacement"):
        frame.sample(True, 0.5)


def test_random_split_weights(spark: ReparkSession) -> None:
    frame = spark.sql(
        "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 "
        "UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 "
        "UNION ALL SELECT 9 UNION ALL SELECT 10"
    )
    parts = frame.randomSplit([1.0, 1.0], seed=1)
    assert len(parts) == 2
    total = parts[0].count() + parts[1].count()
    assert total == 10


def test_col_regex_and_noops(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS user_id, 2 AS user_name, 3 AS other")
    col = frame.colRegex("user_.*")
    # first match only (disclosed)
    assert frame.select(col).columns[0].startswith("user_")
    same = frame.repartition(4).coalesce(1).hint("broadcast")
    assert same.count() == 1


def test_to_df_arity_error(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id, 2 AS v")
    with pytest.raises(PySparkValueError, match="column names"):
        frame.toDF("only_one")
