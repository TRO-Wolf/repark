"""R-DF-BATCH2 — cube/rollup/unpivot/explain/views + loud census."""

from __future__ import annotations

import pytest

from repark import ReparkSession
from repark import functions as functions_api
from repark.errors import UnsupportedOperationException


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-df-batch2").getOrCreate()
    yield session
    session.stop()


def test_cube_rollup_count(spark: ReparkSession) -> None:
    """CUBE/ROLLUP agg AS-aliases Spark / user names.

    Pin column ``c``, not just row count.
    """
    frame = spark.sql("SELECT * FROM (VALUES (1,'a'),(1,'b'),(2,'a')) t(g,v)")
    cube_table = frame.cube("g").agg(functions_api.count("*").alias("c")).to_arrow()
    assert "c" in cube_table.column_names
    cube_rows = cube_table.to_pylist()
    assert len(cube_rows) >= 2
    by_g = {row["g"]: row["c"] for row in cube_rows}
    assert by_g[1] == 2
    assert by_g[2] == 1
    assert by_g[None] == 3
    rollup_table = frame.rollup("g").agg(functions_api.count("*").alias("c")).to_arrow()
    assert "c" in rollup_table.column_names
    rollup_rows = rollup_table.to_pylist()
    assert len(rollup_rows) >= 2
    # Default Spark name when no alias (projection / agg_name path).
    default_table = frame.cube("g").agg(functions_api.count("*")).to_arrow()
    assert any(name in default_table.column_names for name in ("count(1)", "count", "count(*)"))
    # GroupedData.count shortcut also structural (no Int64 rewrite).
    shortcut = frame.cube("g").count().to_arrow()
    assert "count" in shortcut.column_names
    by_count = {row["g"]: row["count"] for row in shortcut.to_pylist()}
    assert by_count[1] == 2
    assert by_count[2] == 1
    assert by_count[None] == 3


def test_cube_first_hostile_count_lit_uncorrupted(spark: ReparkSession) -> None:
    """CUBE free-SQL must not substring-rewrite count(Int64(1)) inside AF args.

    Select-global-agg is already structural count(*); cube/rollup must match.
    """
    frame = spark.sql("SELECT * FROM (VALUES (1,'a'),(1,'b'),(2,'a')) t(g,v)")
    hostile = "count(Int64(1))"
    table = (
        frame.cube("g")
        .agg(functions_api.first(functions_api.lit(hostile)).alias("token"))
        .to_arrow()
    )
    tokens = {row["token"] for row in table.to_pylist()}
    assert tokens == {hostile}
    # GroupBy native path (no free-SQL rewrite) is the oracle for the same expression.
    gb = (
        frame.groupBy("g")
        .agg(functions_api.first(functions_api.lit(hostile)).alias("token"))
        .to_arrow()
    )
    assert {row["token"] for row in gb.to_pylist()} == {hostile}


def test_unpivot(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id, 10 AS a, 20 AS b")
    out = frame.unpivot("id", ["a", "b"], "variable", "value").to_arrow().to_pylist()
    assert len(out) == 2
    variables = {row["variable"] for row in out}
    assert variables == {"a", "b"}
    by_var = {row["variable"]: row["value"] for row in out}
    assert by_var == {"a": 10, "b": 20}


def test_unpivot_quotes_hostile_names_and_labels(spark: ReparkSession) -> None:
    """unpivot quotes idents + string-literal labels.

    Hostile labels / output names must not retarget FROM or break string literals; reserved id
    column ``order`` still works when quoted.
    """
    from repark.errors import AnalysisException
    from repark.spark._idents import quote_ident as _quote_ident
    from repark.spark.dataframe import _sql_string_literal

    frame = spark.sql('SELECT 1 AS "order", 10 AS a, 20 AS b')
    out = frame.unpivot("order", ["a", "b"], "variable", "value").to_arrow().to_pylist()
    assert len(out) == 2
    assert {row["variable"] for row in out} == {"a", "b"}
    assert {row["order"] for row in out} == {1}
    # Escaping helpers (mutation-proof vs naive f-string quoting).
    hostile_label = "a' FROM other --"
    assert _sql_string_literal(hostile_label) == "'a'' FROM other --'"
    assert _sql_string_literal(hostile_label) != f"'{hostile_label}'"
    hostile_out = 'x" AS y, 1 AS z --'
    assert _quote_ident(hostile_out) == f'"{hostile_out.replace(chr(34), chr(34) * 2)}"'
    assert _quote_ident(hostile_out) != f'"{hostile_out}"'
    # Missing hostile value column → schema analysis error (quoted), not free-SQL inject.
    with pytest.raises(AnalysisException, match=r"No field named|Schema error"):
        frame.unpivot("order", [hostile_label], "variable", "value").collect()
    # Hostile output names stay *one* quoted identifier, not extra columns or a reshaped SELECT.
    var_table = frame.unpivot("order", ["a"], hostile_out, "value").to_arrow()
    assert list(var_table.column_names) == ["order", hostile_out, "value"]
    assert "y" not in var_table.column_names
    assert "z" not in var_table.column_names
    assert var_table.to_pylist() == [{"order": 1, hostile_out: "a", "value": 10}]
    val_table = frame.unpivot("order", ["a"], "variable", hostile_out).to_arrow()
    assert list(val_table.column_names) == ["order", "variable", hostile_out]
    assert "y" not in val_table.column_names
    assert val_table.to_pylist() == [{"order": 1, "variable": "a", hostile_out: 10}]


def test_create_temp_view_and_explain(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS x")
    frame.createTempView("tv_batch2")
    assert spark.sql("SELECT * FROM tv_batch2").collect()[0][0] == 1
    frame.explain()
    frame.explain(True)


def test_tojson_and_global_temp_loud(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS x")
    with pytest.raises(UnsupportedOperationException, match="toJSON"):
        frame.toJSON()
    with pytest.raises(UnsupportedOperationException, match="createGlobalTempView"):
        frame.createGlobalTempView("g")
    # R-PIVOT: pivot returns GroupedData (W5 done-signal).
    pivoted = frame.groupBy("x").pivot("x", [1])
    assert pivoted is not None
    # G1: approxQuantile / stat.corr are live (property form). Loud residual is freqItems.
    quantiles = frame.approxQuantile("x", [0.5], 0.0)
    assert isinstance(quantiles, list) and len(quantiles) == 1
    corr_frame = spark.range(5).selectExpr("id AS a", "id * 2 AS b")
    assert abs(corr_frame.stat.corr("a", "b") - 1.0) < 1e-9
    with pytest.raises(UnsupportedOperationException, match="freqItems"):
        frame.stat.freqItems(["x"])
