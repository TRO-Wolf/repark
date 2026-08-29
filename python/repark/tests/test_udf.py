"""U8 classic scalar Python udf: per-row facade projection-rewrite over mapInArrow."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

from repark import SparkSession
from repark.errors import (
    AnalysisException,
    PySparkException,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark.functions import (
    PythonUDFColumn,
    UserDefinedFunction,
    col,
    explode,
    lit,
    pandas_udf,
    udf,
    years,
)
from repark.spark.functions import (
    sum as f_sum,
)
from repark.spark.types import IntegerType, LongType


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    session = SparkSession.builder.master("local[1]").appName("test-python-udf").getOrCreate()
    yield session
    session.stop()


def _rows(table: pa.Table) -> list[dict[str, Any]]:
    return table.to_pylist()


def _multiset(rows: list[dict[str, Any]]) -> list[tuple[tuple[str, Any], ...]]:
    def cell(value: Any) -> Any:
        if value is None:
            return ("null",)
        if isinstance(value, float) and value != value:  # NaN
            return ("nan",)
        return ("v", value)

    packed = [tuple(sorted((key, cell(val)) for key, val in row.items())) for row in rows]
    return sorted(packed)


@udf("long")
def double_long(value: int | None) -> int | None:
    if value is None:
        return None
    return int(value) * 2


@udf(LongType())
def add_long(left: int | None, right: int | None) -> int | None:
    if left is None or right is None:
        return None
    return int(left) + int(right)


@udf("string")
def upper_str(value: str | None) -> str | None:
    if value is None:
        return None
    return str(value).upper()


def test_udf_select_values(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,), (2,), (3,)], "a long")
    out = frame.select(double_long("a").alias("b")).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"b": 2}, {"b": 4}, {"b": 6}])
    assert out.schema.field("b").type == pa.int64()


def test_udf_nulls(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,), (None,), (3,)], "a long")
    out = frame.select(double_long(col("a")).alias("b")).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"b": 2}, {"b": None}, {"b": 6}])


def test_udf_type_coercion_long(spark: SparkSession) -> None:
    """Return Python int coerced to declared long (Arrow int64)."""

    @udf("long")
    def as_long(value: object) -> object:
        return value  # may be int from long column

    frame = spark.createDataFrame([(10,), (20,)], "a long")
    out = frame.select(as_long("a").alias("b")).to_arrow()
    assert out.schema.field("b").type == pa.int64()
    assert _rows(out) == [{"b": 10}, {"b": 20}]


def test_udf_multi_arg(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 10), (2, 20)], "a long, b long")
    out = frame.select(add_long("a", "b").alias("s")).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"s": 11}, {"s": 22}])


def test_udf_string(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("ab",), ("Cd",)], "s string")
    out = frame.select(upper_str("s").alias("u")).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"u": "AB"}, {"u": "CD"}])


def test_udf_with_column(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    out = frame.withColumn("b", double_long(col("a"))).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 1, "b": 2}, {"a": 2, "b": 4}])


def test_udf_pass_through_sibling(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, "x"), (2, "y")], "a long, s string")
    out = frame.select(double_long("a").alias("b"), col("s")).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"b": 2, "s": "x"}, {"b": 4, "s": "y"}])


def test_udf_error_surfacing(spark: SparkSession) -> None:
    @udf("long")
    def boom(_value: object) -> None:
        raise ValueError("oracle-udf-boom")

    frame = spark.createDataFrame([(1,)], "a long")
    with pytest.raises(PySparkException, match="oracle-udf-boom"):
        frame.select(boom("a")).collect()


def test_udf_empty_input(spark: SparkSession) -> None:
    calls = {"n": 0}

    @udf("long")
    def counted(value: int | None) -> int | None:
        calls["n"] += 1
        return value

    frame = spark.createDataFrame([], "a long")
    out = frame.select(counted("a").alias("b")).to_arrow()
    assert out.num_rows == 0
    assert calls["n"] == 0


def test_udf_lazy_until_action(spark: SparkSession) -> None:
    calls = {"n": 0}

    @udf("long")
    def counted(value: int | None) -> int | None:
        calls["n"] += 1
        return None if value is None else int(value) * 2

    frame = spark.createDataFrame([(1,), (2,)], "a long")
    planned = frame.withColumn("b", counted(col("a")))
    # schema/columns must not run the UDF
    _ = planned.schema
    _ = planned.columns
    assert calls["n"] == 0
    _ = planned.to_arrow()
    assert calls["n"] == 2


def test_udf_composition_refused(spark: SparkSession) -> None:
    marker = double_long(col("a"))
    assert isinstance(marker, PythonUDFColumn)
    with pytest.raises(UnsupportedOperationException, match="arithmetic"):
        _ = marker + 1
    with pytest.raises(UnsupportedOperationException, match="cast"):
        marker.cast("long")
    with pytest.raises(UnsupportedOperationException, match="window"):
        marker.over("w")  # type: ignore[arg-type]
    with pytest.raises(PySparkValueError, match="bool"):
        bool(marker)


def test_udf_decorator_forms() -> None:
    @udf
    def default_string(value: object) -> object:
        return value

    assert isinstance(default_string, UserDefinedFunction)
    assert default_string._return_type_sql == "string"

    @udf(returnType=IntegerType())
    def as_int(value: object) -> object:
        return value

    assert as_int._return_type_sql == "int"

    direct = udf(lambda x: x, "long")
    assert isinstance(direct, UserDefinedFunction)
    # Spark simpleString for long is "bigint" (DataType.fromDDL("long") identity).
    assert direct._return_type_sql in {"long", "bigint"}


def test_udf_return_type_refuse_variant() -> None:
    with pytest.raises(PySparkTypeError, match=r"variant|concrete|supported"):
        udf(lambda x: x, "variant")


def test_udf_partition_transform_input_refused(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,)], "a long")
    with pytest.raises(AnalysisException, match="PARTITION_TRANSFORM"):
        frame.select(double_long(years("a"))).collect()


def test_udf_aggregate_input_refused(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    with pytest.raises(AnalysisException, match="aggregate"):
        frame.select(double_long(f_sum("a"))).collect()


def test_udf_generator_mix_refused(spark: SparkSession) -> None:
    frame = spark.createDataFrame([([1, 2],)], "a array<long>")
    with pytest.raises(AnalysisException, match=r"explode|generator"):
        frame.select(explode("a"), double_long(lit(1))).collect()


def test_udf_mix_with_pandas_udf_refused(spark: SparkSession) -> None:
    @pandas_udf("long")
    def double_series(series: Any) -> Any:
        return series

    frame = spark.createDataFrame([(1,)], "a long")
    with pytest.raises(UnsupportedOperationException, match="mix classic udf and pandas_udf"):
        frame.select(double_long("a"), double_series("a")).collect()


def test_spark_udf_register_returns_callable(spark: SparkSession) -> None:
    registered = spark.udf.register("my_double", double_long._user_func, "long")
    assert callable(registered)
    assert isinstance(registered, UserDefinedFunction)
    frame = spark.createDataFrame([(3,)], "a long")
    out = frame.select(registered("a").alias("b")).to_arrow()
    assert _rows(out) == [{"b": 6}]


def test_spark_udf_register_java_loud(spark: SparkSession) -> None:
    with pytest.raises(UnsupportedOperationException, match=r"registerJavaFunction|no JVM"):
        spark.udf.registerJavaFunction("j", "com.example.UDF")


def test_sql_udf_select_list_rewrite(spark: SparkSession) -> None:
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    frame.createOrReplaceTempView("t_udf")
    out = spark.sql("SELECT my_double(a) AS b FROM t_udf").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"b": 2}, {"b": 4}])


def test_sql_udf_with_pass_through(spark: SparkSession) -> None:
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1, "x"), (2, "y")], "a long, s string")
    frame.createOrReplaceTempView("t_udf2")
    out = spark.sql("SELECT my_double(a) AS b, s FROM t_udf2").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"b": 2, "s": "x"}, {"b": 4, "s": "y"}])


def test_sql_udf_in_where_rewrites(spark: SparkSession) -> None:
    """U8 residual / U10: WHERE scalar UDF rewrites."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    frame.createOrReplaceTempView("t_udf3")
    out = spark.sql("SELECT a FROM t_udf3 WHERE my_double(a) > 0").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 1}, {"a": 2}])


def test_sql_udf_unregistered_name_passes_to_engine(spark: SparkSession) -> None:
    """Generic ident( must NOT be treated as UDF — only registry names."""
    frame = spark.createDataFrame([(1,)], "a long")
    frame.createOrReplaceTempView("t_udf4")
    # abs is an engine function, not a registered Python UDF.
    out = spark.sql("SELECT abs(a) AS b FROM t_udf4").to_arrow()
    assert _rows(out) == [{"b": 1}]


def test_udf_per_row_cost_documented(spark: SparkSession) -> None:
    """Honest per-row cost: N rows → N Python invocations (not vectorized)."""
    calls = {"n": 0}

    @udf("long")
    def counted(value: int | None) -> int | None:
        calls["n"] += 1
        return value

    n_rows = 50
    frame = spark.createDataFrame([(i,) for i in range(n_rows)], "a long")
    _ = frame.select(counted("a")).to_arrow()
    assert calls["n"] == n_rows


def test_udf_as_nondeterministic() -> None:
    fn = udf(lambda x: x, "long").asNondeterministic()
    assert isinstance(fn, UserDefinedFunction)


def test_sql_udf_name_in_string_literal_not_scanned(spark: SparkSession) -> None:
    """Registry scan must ignore UDF name text inside SQL string literals (octo C1-L-001)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,)], "a long")
    frame.createOrReplaceTempView("t_udf_str")
    out = spark.sql("SELECT 'my_double(a)' AS s FROM t_udf_str").to_arrow()
    assert _rows(out) == [{"s": "my_double(a)"}]


def test_sql_udf_name_in_comment_not_scanned(spark: SparkSession) -> None:
    """Registry scan must ignore UDF name text inside SQL comments (octo C1-L-001)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(7,)], "a long")
    frame.createOrReplaceTempView("t_udf_cmt")
    out = spark.sql("SELECT a /* my_double(x) */ FROM t_udf_cmt").to_arrow()
    assert _rows(out) == [{"a": 7}]
    out_line = spark.sql("SELECT a -- my_double(x)\nFROM t_udf_cmt").to_arrow()
    assert _rows(out_line) == [{"a": 7}]


def test_sql_udf_name_in_where_string_not_scanned(spark: SparkSession) -> None:
    """WHERE predicate comparing to a string that looks like a UDF call is not a UDF use."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1, "my_double(1)"), (2, "x")], "a long, s string")
    frame.createOrReplaceTempView("t_udf_ws")
    out = spark.sql("SELECT a FROM t_udf_ws WHERE s = 'my_double(1)'").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 1}])


def test_spark_udf_register_name_must_be_simple_ident(spark: SparkSession) -> None:
    """Register names are simple SQL identifiers (octo C1-Q-002)."""
    with pytest.raises(PySparkTypeError, match=r"simple SQL identifier"):
        spark.udf.register("my-udf", lambda x: x, "long")
    with pytest.raises(PySparkTypeError, match=r"simple SQL identifier"):
        spark.udf.register("a.b", lambda x: x, "long")
    with pytest.raises(PySparkTypeError, match=r"simple SQL identifier"):
        spark.udf.register("1x", lambda x: x, "long")


def test_sql_udf_qualified_column_arg(spark: SparkSession) -> None:
    """SELECT-list simple form accepts qualified column args ``t.a`` (octo C2-L-001)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    frame.createOrReplaceTempView("t_udf_qual")
    out = spark.sql("SELECT my_double(t_udf_qual.a) AS b FROM t_udf_qual").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"b": 2}, {"b": 4}])
    out_alias = spark.sql("SELECT my_double(x.a) AS b FROM t_udf_qual x").to_arrow()
    assert _multiset(_rows(out_alias)) == _multiset([{"b": 2}, {"b": 4}])


def test_sql_udf_nested_subquery_message(spark: SparkSession) -> None:
    """Nested UDF refuse names nested subquery (not 'outside SELECT list') — octo C2-Q-001."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,)], "a long")
    frame.createOrReplaceTempView("t_udf_nest")
    with pytest.raises(UnsupportedOperationException, match=r"nested subquery"):
        spark.sql("SELECT * FROM (SELECT my_double(a) AS b FROM t_udf_nest)").collect()


def test_udf_decimal_float_coercion(spark: SparkSession) -> None:
    """Python float/int coerce into declared decimal returnType (octo C2-L-002)."""

    @udf("decimal(10,2)")
    def as_dec(value: object) -> object:
        if value is None:
            return None
        return float(value) + 0.5  # Spark Python UDF accepts float → decimal

    frame = spark.createDataFrame([(1,), (2,)], "a long")
    out = frame.select(as_dec("a").alias("d")).to_arrow()
    rows = _rows(out)
    assert len(rows) == 2
    assert float(rows[0]["d"]) == 1.5
    assert float(rows[1]["d"]) == 2.5


def test_spark_udf_register_case_insensitive_overwrite(spark: SparkSession) -> None:
    """Second register differing only by case replaces the first (octo C3-L-001)."""
    spark.udf.register("CamelCase", lambda x: int(x) + 1 if x is not None else None, "long")
    spark.udf.register("camelcase", lambda x: 0 if x is not None else None, "long")
    registry = spark._udf_registry()
    assert "camelcase" in registry
    assert "CamelCase" not in registry
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    frame.createOrReplaceTempView("t_udf_case")
    # Both casings resolve to the latest registration (returns 0).
    out = spark.sql("SELECT CamelCase(a) AS b FROM t_udf_case").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"b": 0}, {"b": 0}])
    out2 = spark.sql("SELECT camelcase(a) AS b FROM t_udf_case").to_arrow()
    assert _multiset(_rows(out2)) == _multiset([{"b": 0}, {"b": 0}])


def test_sql_udf_plan_captures_function_at_sql_time(spark: SparkSession) -> None:
    """SQL rewrite binds the UDF callable at ``spark.sql`` time (not action time)."""
    spark.udf.register("snap_dbl", lambda x: int(x) * 2 if x is not None else None, "long")
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    frame.createOrReplaceTempView("t_udf_snap")
    planned = spark.sql("SELECT snap_dbl(a) AS b FROM t_udf_snap")
    spark.udf.register("snap_dbl", lambda x: int(x) * 100 if x is not None else None, "long")
    # Already-planned frame keeps the pre-overwrite function.
    assert _multiset(_rows(planned.to_arrow())) == _multiset([{"b": 2}, {"b": 4}])
    # New sql() sees the overwrite.
    assert _multiset(_rows(spark.sql("SELECT snap_dbl(a) AS b FROM t_udf_snap").to_arrow())) == (
        _multiset([{"b": 100}, {"b": 200}])
    )


def test_sql_udf_comment_between_name_and_paren(spark: SparkSession) -> None:
    """``udf /*c*/ (col)`` still rewrites (octo C4-L-001)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(3,)], "a long")
    frame.createOrReplaceTempView("t_udf_cmt2")
    out = spark.sql("SELECT my_double /*c*/ (a) AS b FROM t_udf_cmt2").to_arrow()
    assert _rows(out) == [{"b": 6}]
    out2 = spark.sql("SELECT my_double/*c*/(a) AS b FROM t_udf_cmt2").to_arrow()
    assert _rows(out2) == [{"b": 6}]


def test_udf_join_union_after_projection(spark: SparkSession) -> None:
    """Materialized udf columns compose with join/union (octo C4 composition pin)."""

    @udf("long")
    def double_long(value: int | None) -> int | None:
        return None if value is None else int(value) * 2

    left = spark.createDataFrame([(1,), (2,)], "a long").select(double_long("a").alias("k"))
    right = spark.createDataFrame([(2, "x"), (4, "y")], "k long, s string")
    joined = left.join(right, on="k").to_arrow()
    assert _multiset(_rows(joined)) == _multiset([{"k": 2, "s": "x"}, {"k": 4, "s": "y"}])
    u1 = spark.createDataFrame([(1,)], "a long").select(double_long("a").alias("b"))
    u2 = spark.createDataFrame([(2,)], "a long").select(double_long("a").alias("b"))
    assert _multiset(_rows(u1.union(u2).to_arrow())) == _multiset([{"b": 2}, {"b": 4}])


def test_sql_udf_sibling_engine_func_without_as(spark: SparkSession) -> None:
    """Pass-through engine calls without AS keep Spark-style names (octo C5-L-001)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (-2,)], "a long")
    frame.createOrReplaceTempView("t_udf_abs")
    out = spark.sql("SELECT my_double(a) AS b, abs(a) FROM t_udf_abs").to_arrow()
    assert "b" in out.schema.names
    assert any(name.startswith("abs") for name in out.schema.names)
    rows = _rows(out)
    assert _multiset([{"b": r["b"]} for r in rows]) == _multiset([{"b": 2}, {"b": -4}])
    abs_key = next(name for name in out.schema.names if name != "b")
    assert _multiset([{abs_key: r[abs_key]} for r in rows]) == _multiset(
        [{abs_key: 1}, {abs_key: 2}]
    )


def test_sql_udf_values_source(spark: SparkSession) -> None:
    """SELECT-list UDF over VALUES/subquery FROM (octo C5)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    out = spark.sql("SELECT my_double(a) AS b FROM (VALUES (1), (2)) AS t(a)").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"b": 2}, {"b": 4}])


def test_udf_multi_arg_null_propagation(spark: SparkSession) -> None:
    """Null in any multi-arg input: user sees None (octo C6 S0 fresh-exec pin)."""

    @udf("long")
    def add_long(left: int | None, right: int | None) -> int | None:
        if left is None or right is None:
            return None
        return int(left) + int(right)

    frame = spark.createDataFrame([(1, None), (None, 2), (3, 4)], "a long, b long")
    out = frame.select(add_long("a", "b").alias("s")).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"s": None}, {"s": None}, {"s": 7}])


def test_sql_mask_helpers_unit() -> None:
    """Pure helper pins for registry scan mask / comment strip (octo C6)."""
    from repark.spark.session import (
        _parse_simple_sql_udf_call,
        _sql_mask_strings_and_comments,
        _sql_strip_comments_preserve_strings,
        _sql_udf_in_nested_subquery,
    )

    original = "SELECT 'dbl(a)' AS s FROM t"
    masked = _sql_mask_strings_and_comments(original)
    assert len(masked) == len(original)
    assert "dbl" not in masked
    stripped = _sql_strip_comments_preserve_strings("dbl /*c*/ (a)")
    assert "/*" not in stripped
    registry = {"dbl": {"func": lambda value: value, "return_type_sql": "bigint", "udf": None}}
    parsed = _parse_simple_sql_udf_call("dbl /*c*/ (a) AS b", registry)
    assert parsed is not None
    assert parsed[0] == "dbl"
    assert parsed[2] == "b"
    # U9: CAST/abs parens are not nested subqueries; (SELECT …) is.
    cast_sql = "SELECT CAST(my_double(v) AS BIGINT) AS z FROM t"
    udf_at = cast_sql.index("my_double")
    assert _sql_udf_in_nested_subquery(cast_sql, udf_at) is False
    nest_sql = "SELECT * FROM (SELECT my_double(v) AS z FROM t)"
    nest_at = nest_sql.index("my_double")
    assert _sql_udf_in_nested_subquery(nest_sql, nest_at) is True


# U9 — SQL UDF composition (expression wrap / CTE / DISTINCT / ORDER BY alias)


def test_sql_udf_expression_wrap_plus(spark: SparkSession) -> None:
    """SELECT-list ``my_udf(col) + 1`` materializes UDF then residual engine expr (U9)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    frame.createOrReplaceTempView("t_u9_wrap")
    out = spark.sql("SELECT my_double(a) + 1 AS z FROM t_u9_wrap").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 3}, {"z": 5}])


def test_sql_udf_expression_wrap_cast(spark: SparkSession) -> None:
    """``CAST(my_udf(x) AS …)`` is expression-wrap, not nested-subquery refuse (U9)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(3,)], "v long")
    frame.createOrReplaceTempView("t_u9_cast")
    out = spark.sql("SELECT CAST(my_double(v) AS BIGINT) AS z FROM t_u9_cast").to_arrow()
    assert _rows(out) == [{"z": 6}]


def test_sql_udf_nested_registered_calls(spark: SparkSession) -> None:
    """Nested ``f(g(x))`` of registered UDFs in SELECT list (U9 multi-stage)."""
    spark.udf.register("g_dbl", lambda x: None if x is None else int(x) * 2, "long")
    spark.udf.register("f_inc", lambda x: None if x is None else int(x) + 1, "long")
    frame = spark.createDataFrame([(1,), (2,)], "x long")
    frame.createOrReplaceTempView("t_u9_nest")
    out = spark.sql("SELECT f_inc(g_dbl(x)) AS z FROM t_u9_nest").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 3}, {"z": 5}])


def test_sql_udf_abs_wrap(spark: SparkSession) -> None:
    """Engine function wrapping a registered UDF (``abs(my_udf(x))``) (U9)."""
    spark.udf.register("my_neg", lambda x: None if x is None else -int(x), "long")
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    frame.createOrReplaceTempView("t_u9_abs")
    out = spark.sql("SELECT abs(my_neg(a)) AS z FROM t_u9_abs").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 1}, {"z": 2}])


def test_sql_udf_with_cte_body(spark: SparkSession) -> None:
    """UDF inside WITH/CTE body rewrites per region (U9)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u9_cte_src")
    out = spark.sql(
        "WITH c AS (SELECT my_double(v) AS z FROM t_u9_cte_src) SELECT z FROM c"
    ).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 2}, {"z": 4}])


def test_sql_udf_distinct_projection(spark: SparkSession) -> None:
    """SELECT DISTINCT with UDF projection (post-materialization dedup) (U9)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u9_dist")
    out = spark.sql("SELECT DISTINCT my_double(v) AS z FROM t_u9_dist").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 2}, {"z": 4}])


def test_sql_udf_order_by_alias(spark: SparkSession) -> None:
    """ORDER BY UDF output alias works; never leaks ``__repark_sql_udf_*`` (U9 Q13)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(3,), (1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u9_ord")
    out = spark.sql("SELECT my_double(v) AS z FROM t_u9_ord ORDER BY z").to_arrow()
    assert _rows(out) == [{"z": 2}, {"z": 4}, {"z": 6}]
    out_desc = spark.sql("SELECT my_double(v) AS z FROM t_u9_ord ORDER BY z DESC").to_arrow()
    assert _rows(out_desc) == [{"z": 6}, {"z": 4}, {"z": 2}]


def test_sql_udf_group_by_alias(spark: SparkSession) -> None:
    """GROUP BY on UDF alias materializes then groups (U10); no internal-name leak."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u10_gb")
    out = spark.sql("SELECT my_double(v) AS z FROM t_u10_gb GROUP BY z").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 2}, {"z": 4}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)


def test_sql_udf_where_filter(spark: SparkSession) -> None:
    """WHERE scalar UDF rewrites and filters (U10); never leaks ``__repark_sql_udf_*``."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,), (3,)], "a long")
    frame.createOrReplaceTempView("t_u10_where")
    out = spark.sql("SELECT a FROM t_u10_where WHERE my_double(a) > 3").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 2}, {"a": 3}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)


def test_sql_udf_where_with_select_udf(spark: SparkSession) -> None:
    """SELECT + WHERE both using registered UDF (U10)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,), (3,)], "a long")
    frame.createOrReplaceTempView("t_u10_where_sel")
    out = spark.sql(
        "SELECT my_double(a) AS z FROM t_u10_where_sel WHERE my_double(a) > 3"
    ).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 4}, {"z": 6}])


def test_sql_udf_group_by_matching_expression(spark: SparkSession) -> None:
    """GROUP BY my_udf(v) when SELECT projects the same call (U10)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u10_gb_expr")
    out = spark.sql("SELECT my_double(v) AS z FROM t_u10_gb_expr GROUP BY my_double(v)").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 2}, {"z": 4}])


def test_sql_udf_having_on_alias(spark: SparkSession) -> None:
    """HAVING on UDF SELECT alias after keys-only GROUP BY (U10)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (1,), (2,), (3,)], "v long")
    frame.createOrReplaceTempView("t_u10_having")
    out = spark.sql("SELECT my_double(v) AS z FROM t_u10_having GROUP BY z HAVING z > 2").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 4}, {"z": 6}])


def test_sql_udf_group_by_agg_refuses_loud_no_leak(spark: SparkSession) -> None:
    """GROUP BY + aggregate SELECT still refuses loud without internal leak (U10 bound)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u10_gb_agg")
    with pytest.raises(UnsupportedOperationException, match=r"GROUP BY|aggregate") as caught:
        spark.sql("SELECT my_double(v) AS z, count(*) AS c FROM t_u10_gb_agg GROUP BY z").collect()
    assert "__repark_sql_udf" not in str(caught.value)


def test_sql_udf_join_on_still_refused(spark: SparkSession) -> None:
    """UDF in JOIN ON remains refuse-loud (not U10)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    left = spark.createDataFrame([(1,)], "a long")
    right = spark.createDataFrame([(2,)], "b long")
    left.createOrReplaceTempView("t_u10_jl")
    right.createOrReplaceTempView("t_u10_jr")
    with pytest.raises(UnsupportedOperationException, match=r"JOIN|not supported") as caught:
        spark.sql(
            "SELECT t_u10_jl.a FROM t_u10_jl JOIN t_u10_jr ON my_double(t_u10_jl.a) = t_u10_jr.b"
        ).collect()
    assert "__repark_sql_udf" not in str(caught.value)


def test_sql_udf_where_compound_base_column(spark: SparkSession) -> None:
    """Compound WHERE: UDF residual + base column ref rewrites (U10 C1)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1, "x"), (2, "y"), (3, "z")], "a long, s string")
    frame.createOrReplaceTempView("t_u10_compound")
    out = spark.sql("SELECT a FROM t_u10_compound WHERE my_double(a) > 2 AND s = 'z'").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 3}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)
    # OR with bare base column on the non-UDF side.
    out_or = spark.sql("SELECT a FROM t_u10_compound WHERE my_double(a) > 4 OR a = 1").to_arrow()
    assert _multiset(_rows(out_or)) == _multiset([{"a": 1}, {"a": 3}])


def test_sql_udf_having_aggregate_refuses_loud_no_leak(spark: SparkSession) -> None:
    """Aggregate HAVING after UDF GROUP BY refuses loud without internal leak (U10 C1)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u10_having_agg")
    with pytest.raises(UnsupportedOperationException, match=r"aggregate HAVING|HAVING") as caught:
        spark.sql(
            "SELECT my_double(v) AS z FROM t_u10_having_agg GROUP BY z HAVING count(*) > 1"
        ).collect()
    assert "__repark_sql_udf" not in str(caught.value)
    assert "Physical plan does not support" not in str(caught.value)


def test_sql_udf_group_by_having_whitespace_match(spark: SparkSession) -> None:
    """GROUP BY / HAVING UDF expr match ignores interior whitespace (U10 C2)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u10_ws")
    out = spark.sql("SELECT my_double(v) AS z FROM t_u10_ws GROUP BY my_double( v )").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 2}, {"z": 4}])
    out_h = spark.sql(
        "SELECT my_double(v) AS z FROM t_u10_ws GROUP BY z HAVING my_double( v ) > 2"
    ).to_arrow()
    assert _multiset(_rows(out_h)) == _multiset([{"z": 4}])
    assert all("__repark_sql_udf" not in name for name in out_h.column_names)


def test_sql_udf_where_subquery_refuses_loud_no_leak(spark: SparkSession) -> None:
    """WHERE UDF + nested subquery residual refuses loud (U10 C3)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    frame.createOrReplaceTempView("t_u10_subq")
    with pytest.raises(UnsupportedOperationException, match=r"subquery|EXISTS|WHERE") as caught:
        spark.sql(
            "SELECT a FROM t_u10_subq WHERE my_double(a) > (SELECT max(a) FROM t_u10_subq)"
        ).collect()
    assert "__repark_sql_udf" not in str(caught.value)
    assert "ParserError" not in str(caught.value)


def test_udf_register_reserved_internal_prefix_refused(spark: SparkSession) -> None:
    """Register names must not use ``__repark_sql_udf_*`` materialization prefix (U10 C5)."""
    with pytest.raises(PySparkTypeError, match=r"__repark_sql_udf|reserved|materialization"):
        spark.udf.register("__repark_sql_udf_evil", lambda value: value, "long")
    with pytest.raises(PySparkTypeError, match=r"__repark_sql_udf|reserved|materialization"):
        spark.udf.register("prefix___repark_sql_udf_x", lambda value: value, "long")


def test_sql_udf_where_cast_residual(spark: SparkSession) -> None:
    """CAST in WHERE residual must not identity-project type tokens (U10 C6)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (None,), (3,)], "a long")
    frame.createOrReplaceTempView("t_u10_cast")
    out = spark.sql("SELECT a FROM t_u10_cast WHERE CAST(my_double(a) AS BIGINT) > 2").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 3}])
    # Equality to CAST(NULL AS …) is unknown → empty (three-valued logic).
    out_null = spark.sql(
        "SELECT a FROM t_u10_cast WHERE my_double(a) = CAST(NULL AS BIGINT)"
    ).to_arrow()
    assert _rows(out_null) == []
    assert all("__repark_sql_udf" not in name for name in out.column_names)


def test_sql_udf_where_is_distinct_from_residual(spark: SparkSession) -> None:
    """IS [NOT] DISTINCT FROM with UDF in WHERE must not project bare FROM (F-E1-1)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (None,), (3,)], "a long")
    frame.createOrReplaceTempView("t_u10_distinct")
    out = spark.sql("SELECT a FROM t_u10_distinct WHERE my_double(a) IS DISTINCT FROM 2").to_arrow()
    # my_double(1)=2 → not distinct from 2; null UDF → distinct from 2; my_double(3)=6.
    assert _multiset(_rows(out)) == _multiset([{"a": None}, {"a": 3}])
    out_not = spark.sql(
        "SELECT a FROM t_u10_distinct WHERE my_double(a) IS NOT DISTINCT FROM 2"
    ).to_arrow()
    assert _multiset(_rows(out_not)) == _multiset([{"a": 1}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)


def test_sql_udf_where_trim_both_from_residual(spark: SparkSession) -> None:
    """trim(BOTH … FROM …) residual with UDF must not project BOTH/FROM (F-E1-1)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1, "xxhi"), (2, "hixx"), (3, "zz")], "a long, s string")
    frame.createOrReplaceTempView("t_u10_trim")
    out = spark.sql(
        "SELECT a FROM t_u10_trim WHERE my_double(a) > 0 AND trim(BOTH 'x' FROM s) = 'hi'"
    ).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 1}, {"a": 2}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)


def test_sql_udf_where_substring_from_for_residual(spark: SparkSession) -> None:
    """substring(… FROM … FOR …) residual with UDF must not project FROM/FOR (F-E1-1)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1, "abcdef"), (2, "zz")], "a long, s string")
    frame.createOrReplaceTempView("t_u10_substr")
    out = spark.sql(
        "SELECT a FROM t_u10_substr WHERE my_double(a) > 0 AND substring(s FROM 1 FOR 2) = 'ab'"
    ).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 1}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)


def test_sql_udf_where_extract_year_from_residual(spark: SparkSession) -> None:
    """extract(YEAR FROM …) residual with UDF must not project YEAR/FROM (F-E1-1)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame(
        [(1, "2024-06-15"), (2, "2023-01-01")],
        "a long, d string",
    )
    frame.createOrReplaceTempView("t_u10_extract")
    out = spark.sql(
        "SELECT a FROM t_u10_extract WHERE my_double(a) > 0 "
        "AND extract(YEAR FROM CAST(d AS DATE)) = 2024"
    ).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 1}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)


def test_sql_udf_where_type_token_column_names(spark: SparkSession) -> None:
    """Compound WHERE residual must project real columns named date/double/string/end (F-E1-2)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    # Columns named like CAST type tokens / CASE END — over-reserve must not drop them.
    frame = spark.createDataFrame(
        [
            (1, "x", 1.5, "s1", 10),
            (2, "y", 2.5, "s2", 20),
            (3, "z", 3.5, "s3", 30),
        ],
        "a long, date string, double double, string string, end long",
    )
    frame.createOrReplaceTempView("t_u10_typetok")
    out_date = spark.sql(
        "SELECT a FROM t_u10_typetok WHERE my_double(a) > 0 AND date = 'z'"
    ).to_arrow()
    assert _multiset(_rows(out_date)) == _multiset([{"a": 3}])
    out_double = spark.sql(
        "SELECT a FROM t_u10_typetok WHERE my_double(a) > 2 AND double > 3.0"
    ).to_arrow()
    assert _multiset(_rows(out_double)) == _multiset([{"a": 3}])
    out_string = spark.sql(
        "SELECT a FROM t_u10_typetok WHERE my_double(a) > 0 AND string = 's2'"
    ).to_arrow()
    assert _multiset(_rows(out_string)) == _multiset([{"a": 2}])
    out_end = spark.sql(
        "SELECT a FROM t_u10_typetok WHERE my_double(a) > 0 AND end = 20"
    ).to_arrow()
    assert _multiset(_rows(out_end)) == _multiset([{"a": 2}])
    # CAST residual still must not invent type-token base columns (U10 C6 held).
    out_cast = spark.sql(
        "SELECT a FROM t_u10_typetok WHERE CAST(my_double(a) AS BIGINT) > 4"
    ).to_arrow()
    assert _multiset(_rows(out_cast)) == _multiset([{"a": 3}])
    for table in (out_date, out_double, out_string, out_end, out_cast):
        assert all("__repark_sql_udf" not in name for name in table.column_names)


def test_sql_udf_where_quoted_from_column(spark: SparkSession) -> None:
    """Quoted residual column ``\"from\"`` must project under UDF WHERE (F-E1-2)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    # Engine-accepted quoted identifier as a real column name.
    frame = spark.createDataFrame([(1, "keep"), (2, "drop")], "a long, from string")
    frame.createOrReplaceTempView("t_u10_fromcol")
    out = spark.sql(
        "SELECT a FROM t_u10_fromcol WHERE my_double(a) > 0 AND \"from\" = 'keep'"
    ).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 1}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)


def test_udf_decorator_return_type_kw_and_usearrow() -> None:
    """Complete ``@udf(returnType=…)`` / ``useArrow=`` decorator arg-forms (U9)."""

    @udf(returnType="long", useArrow=True)
    def as_long(value: object) -> object:
        return value

    assert isinstance(as_long, UserDefinedFunction)
    assert as_long._return_type_sql in {"long", "bigint"}

    @udf(returnType=IntegerType())
    def as_int(value: object) -> object:
        return value

    assert as_int._return_type_sql == "int"

    # Duck-typed DataType stand-in (harness / foreign type objects).
    class _ForeignLong:
        def simpleString(self) -> str:  # noqa: N802 — Spark DataType method name
            return "bigint"

    built = udf(lambda value: value, _ForeignLong())
    assert built._return_type_sql in {"long", "bigint"}


def test_spark_udf_register_java_udaf_loud(spark: SparkSession) -> None:
    """``registerJavaUDAF`` stays loud-unsupported (U8 carry / U9 pin)."""
    with pytest.raises(UnsupportedOperationException, match=r"registerJavaUDAF|no JVM"):
        spark.udf.registerJavaUDAF("jagg", "com.example.UDAF")


def test_sql_udf_unaliased_wrap_no_internal_name_leak(spark: SparkSession) -> None:
    """Unaliased wrap/nested defaults never surface ``__repark_sql_udf_*`` (U9-C1-001)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    spark.udf.register("my_inc", lambda x: None if x is None else int(x) + 1, "long")
    frame = spark.createDataFrame([(1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u9_unalias")
    wrap = spark.sql("SELECT my_double(v) + 1 FROM t_u9_unalias")
    assert all("__repark_sql_udf" not in name for name in wrap.columns)
    wrap_vals = sorted(row[0] for row in wrap.collect())
    assert wrap_vals == [3, 5]
    nested = spark.sql("SELECT my_inc(my_double(v)) FROM t_u9_unalias")
    assert nested.columns == ["my_inc"]
    assert all("__repark_sql_udf" not in name for name in nested.columns)
    assert sorted(row[0] for row in nested.collect()) == [3, 5]
    casted = spark.sql("SELECT CAST(my_double(v) AS BIGINT) FROM t_u9_unalias")
    assert all("__repark_sql_udf" not in name for name in casted.columns)
    assert sorted(row[0] for row in casted.collect()) == [2, 4]


def test_sql_udf_cte_does_not_pollute_session_temp_view(spark: SparkSession) -> None:
    """WITH materialization is query-scoped — pre-existing temp views are restored (U9-C1-002)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    spark.createDataFrame([(1,), (2,)], "v long").createOrReplaceTempView("t_u9_cte_src2")
    spark.createDataFrame([(99,)], "z long").createOrReplaceTempView("c_u9_poll")
    assert _rows(spark.sql("SELECT z FROM c_u9_poll").to_arrow()) == [{"z": 99}]
    out = spark.sql(
        "WITH c_u9_poll AS (SELECT my_double(v) AS z FROM t_u9_cte_src2) "
        "SELECT z FROM c_u9_poll ORDER BY z"
    ).to_arrow()
    assert _rows(out) == [{"z": 2}, {"z": 4}]
    # Session view restored to pre-WITH contents (not left as CTE materialization).
    assert _rows(spark.sql("SELECT z FROM c_u9_poll").to_arrow()) == [{"z": 99}]


def test_sql_udf_star_expansion_refuses_loud(spark: SparkSession) -> None:
    """``SELECT *, udf(...)`` / ``udf, *`` refuse before engine ParseException (U9-C1-003)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    spark.createDataFrame([(1,)], "v long").createOrReplaceTempView("t_u9_star")
    with pytest.raises(UnsupportedOperationException, match=r"star \(\*\)|expansion") as caught:
        spark.sql("SELECT *, my_double(v) AS z FROM t_u9_star").collect()
    assert "__repark_sql_udf" not in str(caught.value)
    with pytest.raises(UnsupportedOperationException, match=r"star \(\*\)|expansion") as caught2:
        spark.sql("SELECT my_double(v) AS z, * FROM t_u9_star").collect()
    assert "__repark_sql_udf" not in str(caught2.value)


def test_sql_udf_register_count_does_not_break_count_star(spark: SparkSession) -> None:
    """Registering a UDF named ``count`` must not hijack engine ``count(*)`` (U9-C2-001)."""
    spark.udf.register("count", lambda x: 1 if x is not None else None, "long")
    frame = spark.createDataFrame([(1,), (2,), (3,)], "v long")
    frame.createOrReplaceTempView("t_u9_count")
    # Engine aggregate still works.
    out = spark.sql("SELECT count(*) AS c FROM t_u9_count").to_arrow()
    assert _rows(out) == [{"c": 3}]
    # Column-arg form still routes to the registered Python UDF.
    out_udf = spark.sql("SELECT count(v) AS c FROM t_u9_count").to_arrow()
    assert _multiset(_rows(out_udf)) == _multiset([{"c": 1}, {"c": 1}, {"c": 1}])


def test_sql_udf_set_op_refuses_with_set_message(spark: SparkSession) -> None:
    """UNION / INTERSECT / EXCEPT + UDF refuse with a set-op message (U9-C2-002)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    spark.createDataFrame([(1,)], "v long").createOrReplaceTempView("t_u9_union")
    with pytest.raises(UnsupportedOperationException, match=r"UNION|INTERSECT|EXCEPT"):
        spark.sql(
            "SELECT my_double(v) AS z FROM t_u9_union "
            "UNION ALL SELECT my_double(v) AS z FROM t_u9_union"
        ).collect()


def test_sql_udf_user_exception_not_rewrite_uoe(spark: SparkSession) -> None:
    """User UDF raises surface as PySparkException, not rewrite-shape UOE (U9-C3-001)."""
    from repark.errors import PySparkException

    def boom(value: object) -> object:
        raise ValueError("oracle-sql-udf-boom")

    spark.udf.register("boom_sql", boom, "long")
    spark.createDataFrame([(1,)], "v long").createOrReplaceTempView("t_u9_boom")
    with pytest.raises(PySparkException, match=r"oracle-sql-udf-boom") as caught:
        spark.sql("SELECT boom_sql(v) AS z FROM t_u9_boom").collect()
    # Must not re-frame as "could not be rewritten" shape refuse.
    assert "could not be rewritten" not in str(caught.value)
    assert not isinstance(caught.value, UnsupportedOperationException)


def test_sql_udf_cte_new_name_not_left_in_catalog(spark: SparkSession) -> None:
    """WITH introduces a fresh CTE name only for the query — dropped after plan (U9-C3)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    spark.createDataFrame([(1,), (2,)], "v long").createOrReplaceTempView("t_u9_cte_drop")
    assert spark.catalog.tableExists("c_u9_fresh") is False
    out = spark.sql(
        "WITH c_u9_fresh AS (SELECT v FROM t_u9_cte_drop) SELECT my_double(v) AS z FROM c_u9_fresh"
    ).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 2}, {"z": 4}])
    assert spark.catalog.tableExists("c_u9_fresh") is False


def test_sql_udf_order_by_nulls_clause_refuses_loud(spark: SparkSession) -> None:
    """ORDER BY … NULLS FIRST/LAST after UDF refuses loud (no silent ignore) (U9-C4-001)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(None,), (1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u9_nulls")
    with pytest.raises(UnsupportedOperationException, match=r"ORDER BY") as caught:
        spark.sql("SELECT my_double(v) AS z FROM t_u9_nulls ORDER BY z NULLS LAST").collect()
    assert "__repark_sql_udf" not in str(caught.value)
    with pytest.raises(UnsupportedOperationException, match=r"ORDER BY"):
        spark.sql("SELECT my_double(v) AS z FROM t_u9_nulls ORDER BY z NULLS FIRST").collect()


def test_sql_udf_optional_as_alias(spark: SparkSession) -> None:
    """Spark optional-AS form ``SELECT udf(col) alias`` (no AS keyword) (U9-C5-001)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u9_optas")
    out = spark.sql("SELECT my_double(v) z FROM t_u9_optas").to_arrow()
    assert out.schema.names == ["z"]
    assert _multiset(_rows(out)) == _multiset([{"z": 2}, {"z": 4}])
    # Wrap + optional AS still peels alias (not the trailing literal of an expr).
    out2 = spark.sql("SELECT my_double(v) + 1 AS z FROM t_u9_optas").to_arrow()
    assert _multiset(_rows(out2)) == _multiset([{"z": 3}, {"z": 5}])


def test_sql_udf_multi_arg_sql_and_literal(spark: SparkSession) -> None:
    """Multi-arg registered UDF with column + literal in SQL SELECT list (U9-C5/C8)."""
    spark.udf.register(
        "add2",
        lambda left, right: None if left is None or right is None else int(left) + int(right),
        "long",
    )
    frame = spark.createDataFrame([(1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u9_add")
    out = spark.sql("SELECT add2(v, 3) AS z FROM t_u9_add").to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"z": 4}, {"z": 5}])


def test_sql_udf_cte_column_list_rename(spark: SparkSession) -> None:
    """``WITH c(z) AS (SELECT udf(v) …)`` renames CTE outputs (U9-C6-001)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "v long")
    frame.createOrReplaceTempView("t_u9_cte_cols")
    out = spark.sql(
        "WITH c(z) AS (SELECT my_double(v) FROM t_u9_cte_cols) SELECT z FROM c ORDER BY z"
    ).to_arrow()
    assert _rows(out) == [{"z": 2}, {"z": 4}]
    assert spark.catalog.tableExists("c") is False


def test_sql_udf_sort_by_refuses_loud(spark: SparkSession) -> None:
    """SORT BY / DISTRIBUTE BY / CLUSTER BY + UDF refuse loud (U9-C6/C7)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,)], "v long")
    frame.createOrReplaceTempView("t_u9_sortby")
    with pytest.raises(UnsupportedOperationException, match=r"SORT BY|DISTRIBUTE BY|CLUSTER BY"):
        spark.sql("SELECT my_double(v) AS z FROM t_u9_sortby SORT BY z").collect()


def test_sql_udf_select_without_from(spark: SparkSession) -> None:
    """``SELECT udf(lit)`` with no FROM clause (U9-C7-001)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    out = spark.sql("SELECT my_double(3) AS z").to_arrow()
    assert _rows(out) == [{"z": 6}]
    out_order = spark.sql("SELECT my_double(1) AS z, my_double(2) AS y ORDER BY z").to_arrow()
    assert _rows(out_order) == [{"z": 2, "y": 4}]


# U11 — residual keyword poles (F-E1 class extensions)


def test_sql_udf_where_interval_day_residual(spark: SparkSession) -> None:
    """INTERVAL unit tokens must not be identity-projected in UDF WHERE residual (U11)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    frame.createOrReplaceTempView("t_u11_interval")
    out = spark.sql(
        "SELECT a FROM t_u11_interval WHERE my_double(a) > 0 AND INTERVAL '1' DAY IS NOT NULL"
    ).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 1}, {"a": 2}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)


def test_sql_udf_where_interval_to_unit_not_projected(spark: SparkSession) -> None:
    """INTERVAL multi-unit trailing unit after ``TO`` is syntax, not a column (octo U11 C1).

    Residual rewrite must leave ``DAY TO SECOND`` intact. The engine may still refuse the
    multi-unit form (same without UDF); it must not leak ``__repark_sql_udf_*`` or claim a
    missing column named SECOND/MONTH.
    """
    from repark.spark.session import _sql_where_residual_base_projections

    residual = "__repark_sql_udf_out_0 > 0 AND INTERVAL '1' DAY TO SECOND IS NOT NULL"
    rewritten, parts, _ = _sql_where_residual_base_projections(
        residual,
        base_select_parts=[],
        temp_counter=0,
    )
    assert "DAY TO SECOND" in rewritten
    assert '"SECOND"' not in rewritten
    assert parts == []
    residual_ytm = "__repark_sql_udf_out_0 > 0 AND INTERVAL '1-0' YEAR TO MONTH IS NOT NULL"
    rewritten_ytm, parts_ytm, _ = _sql_where_residual_base_projections(
        residual_ytm,
        base_select_parts=[],
        temp_counter=0,
    )
    assert "YEAR TO MONTH" in rewritten_ytm
    assert '"MONTH"' not in rewritten_ytm
    assert parts_ytm == []

    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    frame.createOrReplaceTempView("t_u11_interval_to")
    refuse_types = (UnsupportedOperationException, AnalysisException, PySparkException)
    with pytest.raises(refuse_types) as caught:
        spark.sql(
            "SELECT a FROM t_u11_interval_to WHERE my_double(a) > 0 "
            "AND INTERVAL '1' DAY TO SECOND IS NOT NULL"
        ).to_arrow()
    message = str(caught.value)
    assert "__repark_sql_udf" not in message
    # Must not reframe as missing column SECOND (the pre-fix residual bug).
    assert "No field named" not in message or "SECOND" not in message


def test_sql_udf_where_date_timestamp_typed_literal_residual(
    spark: SparkSession,
) -> None:
    """Typed ``DATE '…'`` / ``TIMESTAMP '…'`` constructors are syntax in residual (octo U11 C2).

    Residual must not project ``DATE`` as a column or quote-rewrite the constructor. A
    quoted column named ``date`` still works; never leak internals.
    """
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1,), (2,)], "a long")
    frame.createOrReplaceTempView("t_u11_date_lit")
    out = spark.sql(
        "SELECT a FROM t_u11_date_lit WHERE my_double(a) > 0 "
        "AND extract(YEAR FROM DATE '2020-01-01') = 2020"
    ).to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 1}, {"a": 2}])
    out_ts = spark.sql(
        "SELECT a FROM t_u11_date_lit WHERE my_double(a) > 0 "
        "AND TIMESTAMP '2020-01-01 00:00:00' IS NOT NULL"
    ).to_arrow()
    assert _multiset(_rows(out_ts)) == _multiset([{"a": 1}, {"a": 2}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)
    # Column literally named date still projects when not a typed literal.
    frame_date = spark.createDataFrame([(1, 10), (2, 20)], "a long, date long")
    frame_date.createOrReplaceTempView("t_u11_date_col")
    out_col = spark.sql(
        'SELECT a FROM t_u11_date_col WHERE my_double(a) > 0 AND "date" = 10'
    ).to_arrow()
    assert _multiset(_rows(out_col)) == _multiset([{"a": 1}])


def test_sql_udf_where_quoted_and_column(spark: SparkSession) -> None:
    """Quoted residual column ``\"and\"`` must not break the boolean AND keyword (U11).

    The identifier rewriter can case-steal ``AND`` beside a column named ``and``; residual
    projection must temp-alias it so the residual parses. Never leak ``__repark_sql_udf_*``.
    """
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1, 5), (2, 9)], "a long, and long")
    frame.createOrReplaceTempView("t_u11_and")
    out = spark.sql('SELECT a FROM t_u11_and WHERE my_double(a) > 0 AND "and" = 5').to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 1}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)


def test_sql_udf_where_quoted_or_column(spark: SparkSession) -> None:
    """Quoted residual column ``\"or\"`` with boolean OR must not filter-steal (U11)."""
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1, 5), (0, 9)], "a long, or long")
    frame.createOrReplaceTempView("t_u11_or")
    out = spark.sql('SELECT a FROM t_u11_or WHERE my_double(a) > 0 OR "or" = 9').to_arrow()
    # my_double(1)=2>0 → keep; my_double(0)=0 not >0 but or=9 → keep.
    assert _multiset(_rows(out)) == _multiset([{"a": 1}, {"a": 0}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)


def test_sql_udf_where_bare_when_column_refuses_or_requires_quote(
    spark: SparkSession,
) -> None:
    """Bare reserved ``when`` column in residual is not a silent wrong answer (U11 pin).

    repark refuses loud or requires the quoted form; the quoted form must work; no
    internal name leak.
    """
    spark.udf.register("my_double", lambda x: None if x is None else int(x) * 2, "long")
    frame = spark.createDataFrame([(1, 5), (2, 9)], "a long, when long")
    frame.createOrReplaceTempView("t_u11_when")
    # Quoted form is the legal path.
    out = spark.sql('SELECT a FROM t_u11_when WHERE my_double(a) > 0 AND "when" = 5').to_arrow()
    assert _multiset(_rows(out)) == _multiset([{"a": 1}])
    assert all("__repark_sql_udf" not in name for name in out.column_names)
    # Bare form must not succeed with wrong rows (loud refuse or empty/analysis is OK).
    bare_failed = False
    bare_rows: list[dict[str, object]] | None = None
    try:
        bare = spark.sql("SELECT a FROM t_u11_when WHERE my_double(a) > 0 AND when = 5").to_arrow()
        bare_rows = _rows(bare)
        assert all("__repark_sql_udf" not in name for name in bare.column_names)
    except (UnsupportedOperationException, AnalysisException, PySparkException) as error:
        bare_failed = True
        assert "__repark_sql_udf" not in str(error)
    if not bare_failed:
        # If engine accepts bare when as column, values must be correct — not silent empty.
        assert bare_rows is not None
        assert _multiset(bare_rows) == _multiset([{"a": 1}])
