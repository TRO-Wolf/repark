"""COLUMN-PARITY-1 critic-round pins — sequential ``UpdateFields`` and L-005..L-010.

pins: column-parity-1/C-008
"""

from __future__ import annotations

from typing import Any

import pytest

from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark import ReparkSession

BASE_SCHEMA = "i int, d double, s string, st struct<a:int,b:string>"
BASE_ROWS = [(1, 1.0, "a", (1, "x")), (2, float("nan"), None, None), (None, None, "c", (3, None))]
NESTED_SCHEMA = "st struct<a:int, inner:struct<x:int,y:int>>"
NESTED_ROWS = [((1, (2, 3)),), (None,)]


@pytest.fixture
def spark() -> ReparkSession:
    """A default session for the critic-round pins."""
    return ReparkSession.builder.appName("pytest-column-parity-1-critic").getOrCreate()


def _base_df(spark: ReparkSession) -> Any:
    """The ``base_df`` oracle fixture."""
    return spark.createDataFrame(BASE_ROWS, BASE_SCHEMA)


def _nested_df(spark: ReparkSession) -> Any:
    """The ``nested_df`` oracle fixture."""
    return spark.createDataFrame(NESTED_ROWS, NESTED_SCHEMA)


def _assert_frame(frame: Any, columns: list[str], schema: str, rows: list[dict[str, Any]]) -> None:
    """Assert a DataFrame answer against recorded columns, simpleString, and row dicts."""
    assert frame.columns == columns
    assert frame.schema.simpleString() == schema
    assert frame.to_arrow().to_pylist() == rows


def test_withfield_chain_replaces_just_added(spark: ReparkSession) -> None:
    """``withField("c", 1).withField("c", 2)`` keeps one ``c = 2`` (L-001).

    Sequential ``UpdateFields`` semantics per Spark docs; UNMEASURED live.

    pins: column-parity-1/C-008 (L-001)
    """
    out = _base_df(spark).select(
        F.col("st").withField("c", F.lit(1)).withField("c", F.lit(2)).alias("r")
    )
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:int,b:string,c:int>>",
        [
            {"r": {"a": 1, "b": "x", "c": 2}},
            {"r": None},
            {"r": {"a": 3, "b": None, "c": 2}},
        ],
    )


def test_withfield_nested_chain_replaces_just_added(spark: ReparkSession) -> None:
    """``withField("inner.z", 1).withField("inner.z", 2)`` keeps one ``z = 2`` (L-001).

    Sequential ``UpdateFields`` semantics per Spark docs; UNMEASURED live.

    pins: column-parity-1/C-008 (L-001)
    """
    out = _nested_df(spark).select(
        F.col("st").withField("inner.z", F.lit(1)).withField("inner.z", F.lit(2)).alias("r")
    )
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:int,inner:struct<x:int,y:int,z:int>>>",
        [
            {"r": {"a": 1, "inner": {"x": 2, "y": 3, "z": 2}}},
            {"r": None},
        ],
    )


def test_withfield_then_drop_just_added(spark: ReparkSession) -> None:
    """``withField("c", 9).dropFields("c")`` removes the new field (L-002).

    Sequential ``UpdateFields`` semantics per Spark docs; UNMEASURED live.

    pins: column-parity-1/C-008 (L-002)
    """
    out = _base_df(spark).select(F.col("st").withField("c", F.lit(9)).dropFields("c").alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:int,b:string>>",
        [
            {"r": {"a": 1, "b": "x"}},
            {"r": None},
            {"r": {"a": 3, "b": None}},
        ],
    )


def test_dropfields_then_with_appends(spark: ReparkSession) -> None:
    """``dropFields("a").withField("a", 9)`` appends ``a`` after ``b`` (L-003).

    Sequential ``UpdateFields`` semantics per Spark docs; UNMEASURED live.

    pins: column-parity-1/C-008 (L-003)
    """
    out = _base_df(spark).select(F.col("st").dropFields("a").withField("a", F.lit(9)).alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<b:string,a:int>>",
        [
            {"r": {"b": "x", "a": 9}},
            {"r": None},
            {"r": {"b": None, "a": 9}},
        ],
    )


def test_struct_edit_in_filter_eq(spark: ReparkSession) -> None:
    """``filter(st.withField("c", 9).getField("c") == 9)`` keeps non-NULL ``st`` (L-004).

    Sequential ``UpdateFields`` semantics per Spark docs; UNMEASURED live.

    pins: column-parity-1/C-008 (L-004)
    """
    out = (
        _base_df(spark).filter(F.col("st").withField("c", F.lit(9)).getField("c") == 9).select("i")
    )
    _assert_frame(out, ["i"], "struct<i:int>", [{"i": 1}, {"i": None}])


def test_struct_edit_in_filter_drop_and_null(spark: ReparkSession) -> None:
    """``filter`` of a dropFields/getField predicate and an isNotNull predicate (L-004).

    Sequential ``UpdateFields`` semantics per Spark docs; UNMEASURED live.

    pins: column-parity-1/C-008 (L-004)
    """
    out = _base_df(spark).filter(F.col("st").dropFields("b").getField("a") == 1).select("i")
    _assert_frame(out, ["i"], "struct<i:int>", [{"i": 1}])
    out = (
        _base_df(spark)
        .filter(F.col("st").withField("c", F.lit(9)).getField("c").isNotNull())
        .select("i")
    )
    _assert_frame(out, ["i"], "struct<i:int>", [{"i": 1}, {"i": None}])


def test_struct_edit_in_select_expr_and_when(spark: ReparkSession) -> None:
    """``withField`` values compose inside ``+`` and ``when`` (L-004).

    Sequential ``UpdateFields`` semantics per Spark docs; UNMEASURED live.

    pins: column-parity-1/C-008 (L-004)
    """
    out = _base_df(spark).select(
        (F.col("st").withField("c", F.lit(9)).getField("c") + 1).alias("r")
    )
    _assert_frame(out, ["r"], "struct<r:int>", [{"r": 10}, {"r": None}, {"r": 10}])
    out = _base_df(spark).select(
        F.when(F.col("st").withField("c", F.lit(9)).getField("c") == 9, 1).otherwise(0).alias("r")
    )
    _assert_frame(out, ["r"], "struct<r:int>", [{"r": 1}, {"r": 0}, {"r": 1}])


def test_struct_edit_in_orderby_groupby(spark: ReparkSession) -> None:
    """``orderBy`` / ``groupBy`` consume a ``withField`` field (L-004).

    Sequential ``UpdateFields`` semantics per Spark docs; UNMEASURED live.

    pins: column-parity-1/C-008 (L-004)
    """
    out = (
        _base_df(spark)
        .orderBy(F.col("st").withField("c", F.col("i")).getField("c"), F.col("i"))
        .select("i")
    )
    _assert_frame(out, ["i"], "struct<i:int>", [{"i": None}, {"i": 2}, {"i": 1}])
    out = (
        _base_df(spark)
        .groupBy(F.col("st").withField("c", F.lit(9)).getField("c"))
        .count()
        .orderBy(F.col("count").desc())
    )
    pairs = [
        (
            next(value for key, value in row.items() if key != "count"),
            row["count"],
        )
        for row in out.to_arrow().to_pylist()
    ]
    assert pairs == [(9, 2), (None, 1)]


def test_struct_edit_in_join_and_nested_value(spark: ReparkSession) -> None:
    """Join-on and nested ``withField`` value positions answer (L-004).

    Sequential ``UpdateFields`` semantics per Spark docs; UNMEASURED live.

    pins: column-parity-1/C-008 (L-004)
    """
    left = _base_df(spark)
    right = spark.createDataFrame([(9, "k9"), (3, "k3")], "k int, v string")
    out = left.join(right, left["st"].withField("c", F.lit(9)).getField("c") == right["k"]).select(
        "i", "v"
    )
    assert out.to_arrow().to_pylist() == [
        {"i": 1, "v": "k9"},
        {"i": None, "v": "k9"},
    ]
    out = _base_df(spark).select(
        F.col("st").withField("c", F.col("st").withField("d", F.lit(1)).getField("a")).alias("r")
    )
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:int,b:string,c:int>>",
        [
            {"r": {"a": 1, "b": "x", "c": 1}},
            {"r": None},
            {"r": {"a": 3, "b": None, "c": 3}},
        ],
    )


def test_isnan_non_float_types(spark: ReparkSession) -> None:
    """``isNaN`` on date/timestamp/boolean answers false for every row (L-007).

    Spark's ``isnan`` only has a NaN for floating types; UNMEASURED live on
    date/timestamp/boolean.

    pins: column-parity-1/C-008 (L-007)
    """
    import datetime

    frame = spark.createDataFrame([(datetime.date(2020, 1, 1),), (None,)], "dt date")
    out = frame.select(F.col("dt").isNaN().alias("r"))
    _assert_frame(out, ["r"], "struct<r:boolean>", [{"r": False}, {"r": False}])
    frame = spark.createDataFrame([(datetime.datetime(2020, 1, 1, 12),), (None,)], "ts timestamp")
    out = frame.select(F.col("ts").isNaN().alias("r"))
    _assert_frame(out, ["r"], "struct<r:boolean>", [{"r": False}, {"r": False}])
    frame = spark.createDataFrame([(True,), (None,)], "b boolean")
    out = frame.select(F.col("b").isNaN().alias("r"))
    _assert_frame(out, ["r"], "struct<r:boolean>", [{"r": False}, {"r": False}])


def test_name_later_alias_drops_metadata(spark: ReparkSession) -> None:
    """``name("y")`` after ``name("x", metadata=)`` drops the metadata (L-009).

    Spark builds a new alias without the previous metadata; UNMEASURED live.

    pins: column-parity-1/C-008 (L-009)
    """
    frame = _base_df(spark).select(F.col("i").name("x", metadata={"m": 1}).name("y"))
    assert frame.schema["y"].metadata == {}
    frame = _base_df(spark).select(F.col("i").name("x", metadata={"m": 1}).alias("y"))
    assert frame.schema["y"].metadata == {}


def test_withfield_case_conf_no_effect(spark: ReparkSession) -> None:
    """``spark.sql.caseSensitive`` is not wired; matching stays case-insensitive (L-010).

    The conf is stored but read by no engine path, so ``withField("A", v)`` still
    replaces every case-insensitive match.

    pins: column-parity-1/C-008 (L-010)
    """
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = spark.createDataFrame([((1, 2),)], "st struct<a:int,A:int>")
    out = frame.select(F.col("st").withField("A", F.lit(9)).alias("r"))
    assert out.schema.simpleString() == "struct<r:struct<A:int,A:int>>"


def test_isin_wide_literal_list(spark: ReparkSession) -> None:
    """10 000-literal ``isin`` answers the same rows as the small equivalent.

    pins: column-parity-1/C-008
    """
    frame = spark.range(100_000)
    wide = frame.filter(F.col("id").isin(*range(10_000))).count()
    narrow = frame.filter(F.col("id") < 10_000).count()
    assert wide == narrow == 10_000
