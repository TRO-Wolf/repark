"""FNP-8 candidate matrix for higher-order lambdas on both Spark facade entry points."""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException
from repark.spark import functions as spark_functions
from repark.spark.column import Column
from repark.spark.dataframe.core import DataFrame

SqlColumnBuilder = Callable[[str, str, str, str], Column]


def _transform(column_a: str, _: str, __: str, ___: str) -> Column:
    """Build a unary transform expression."""
    return spark_functions.transform(column_a, lambda element: element + 1)


def _transform_index(column_a: str, _: str, __: str, ___: str) -> Column:
    """Build an indexed transform expression."""
    return spark_functions.transform(column_a, lambda element, index: element + index)


def _filter(column_a: str, _: str, __: str, ___: str) -> Column:
    """Build a unary filter expression."""
    return spark_functions.filter(column_a, lambda element: element > 1)


def _filter_index(column_a: str, _: str, __: str, ___: str) -> Column:
    """Build an indexed filter expression."""
    return spark_functions.filter(column_a, lambda element, index: index % 2 == 0)


def _exists(column_a: str, _: str, __: str, ___: str) -> Column:
    """Build an exists expression."""
    return spark_functions.exists(column_a, lambda element: element > 2)


def _forall(column_a: str, _: str, __: str, ___: str) -> Column:
    """Build a forall expression."""
    return spark_functions.forall(column_a, lambda element: element > 0)


def _aggregate(column_a: str, _: str, __: str, ___: str) -> Column:
    """Build an aggregate expression without a finish function."""
    return spark_functions.aggregate(
        column_a,
        spark_functions.lit(0),
        lambda accumulator, element: accumulator + element,
    )


def _aggregate_finish(column_a: str, _: str, __: str, ___: str) -> Column:
    """Build an aggregate expression with a finish function."""
    return spark_functions.aggregate(
        column_a,
        spark_functions.lit(0),
        lambda accumulator, element: accumulator + element,
        lambda accumulator: accumulator * 10,
    )


def _reduce(column_a: str, _: str, __: str, ___: str) -> Column:
    """Build a reduce expression without a finish function."""
    return spark_functions.reduce(
        column_a,
        spark_functions.lit(0),
        lambda accumulator, element: accumulator + element,
    )


def _reduce_finish(column_a: str, _: str, __: str, ___: str) -> Column:
    """Build a reduce expression with a finish function."""
    return spark_functions.reduce(
        column_a,
        spark_functions.lit(0),
        lambda accumulator, element: accumulator + element,
        lambda accumulator: accumulator * 10,
    )


def _zip_with(column_a: str, column_b: str, _: str, __: str) -> Column:
    """Build a zip-with expression."""
    return spark_functions.zip_with(
        column_a,
        column_b,
        lambda left, right: left + spark_functions.coalesce(right, spark_functions.lit(0)),
    )


def _transform_keys(_: str, __: str, map_one: str, ___: str) -> Column:
    """Build a transform-keys expression."""
    return spark_functions.transform_keys(map_one, lambda key, value: spark_functions.upper(key))


def _transform_values(_: str, __: str, map_one: str, ___: str) -> Column:
    """Build a transform-values expression."""
    return spark_functions.transform_values(map_one, lambda key, value: value + 1)


def _map_filter(_: str, __: str, map_one: str, ___: str) -> Column:
    """Build a map-filter expression."""
    return spark_functions.map_filter(map_one, lambda key, value: value > 1)


def _map_zip_with(_: str, __: str, map_one: str, map_two: str) -> Column:
    """Build a map-zip-with expression."""
    return spark_functions.map_zip_with(
        map_one,
        map_two,
        lambda key, left, right: (
            spark_functions.coalesce(left, spark_functions.lit(0))
            + spark_functions.coalesce(right, spark_functions.lit(0))
        ),
    )


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Yield a session for FNP-8 SQL-door tests."""
    session = ReparkSession.builder.appName("fnp-8-sql-door").getOrCreate()
    yield session
    session.stop()


MATRIX: tuple[tuple[str, str, SqlColumnBuilder], ...] = (
    ("transform", "transform(a, x -> x + 1)", _transform),
    ("transform_index", "transform(a, (x, i) -> x + i)", _transform_index),
    ("filter", "filter(a, x -> x > 1)", _filter),
    ("filter_index", "filter(a, (x, i) -> i % 2 = 0)", _filter_index),
    ("exists", "exists(a, x -> x > 2)", _exists),
    ("forall", "forall(a, x -> x > 0)", _forall),
    ("aggregate", "aggregate(a, 0, (acc, x) -> acc + x)", _aggregate),
    (
        "aggregate_finish",
        "aggregate(a, 0, (acc, x) -> acc + x, acc -> acc * 10)",
        _aggregate_finish,
    ),
    ("reduce", "reduce(a, 0, (acc, x) -> acc + x)", _reduce),
    (
        "reduce_finish",
        "reduce(a, 0, (acc, x) -> acc + x, acc -> acc * 10)",
        _reduce_finish,
    ),
    ("zip_with", "zip_with(a, b, (x, y) -> x + coalesce(y, 0))", _zip_with),
    ("transform_keys", "transform_keys(m1, (k, v) -> upper(k))", _transform_keys),
    ("transform_values", "transform_values(m1, (k, v) -> v + 1)", _transform_values),
    ("map_filter", "map_filter(m1, (k, v) -> v > 1)", _map_filter),
    (
        "map_zip_with",
        "map_zip_with(m1, m2, (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0))",
        _map_zip_with,
    ),
)

FRAME_SQL = (
    "SELECT array(1, 2, 3) AS a, array(10, 20) AS b, "
    "map('a', 1, 'b', 2) AS m1, map('a', 10, 'c', 3) AS m2"
)


def _frame(spark: ReparkSession) -> DataFrame:
    """Build the shared FNP-8 input frame."""
    return spark.sql(FRAME_SQL)


def _column_free_expression(name: str) -> str:
    """Return the column-free SQL expression for one matrix form."""
    expressions = {
        "transform": "transform(array(1,2,3), x -> x + 1)",
        "transform_index": "transform(array(1,2,3), (x,i) -> x + i)",
        "filter": "filter(array(1,2,3), x -> x > 1)",
        "filter_index": "filter(array(1,2,3), (x,i) -> i % 2 = 0)",
        "exists": "exists(array(1,2,3), x -> x > 2)",
        "forall": "forall(array(1,2,3), x -> x > 0)",
        "aggregate": "aggregate(array(1,2,3), 0, (acc,x) -> acc + x)",
        "aggregate_finish": "aggregate(array(1,2,3), 0, (acc,x) -> acc + x, acc -> acc * 10)",
        "reduce": "reduce(array(1,2,3), 0, (acc,x) -> acc + x)",
        "reduce_finish": "reduce(array(1,2,3), 0, (acc,x) -> acc + x, acc -> acc * 10)",
        "zip_with": "zip_with(array(1,2,3), array(10,20), (x,y) -> x + coalesce(y,0))",
        "transform_keys": "transform_keys(map('a',1,'b',2), (k,v) -> upper(k))",
        "transform_values": "transform_values(map('a',1,'b',2), (k,v) -> v + 1)",
        "map_filter": "map_filter(map('a',1,'b',2), (k,v) -> v > 1)",
        "map_zip_with": (
            "map_zip_with(map('a',1,'b',2), map('a',10,'c',3), "
            "(k,v1,v2) -> coalesce(v1,0) + coalesce(v2,0))"
        ),
    }
    return expressions[name]


@pytest.mark.parametrize(
    ("name", "sql", "build"),
    [item for item in MATRIX if item[0] != "transform_index"],
    ids=[item[0] for item in MATRIX if item[0] != "transform_index"],
)
def test_column_and_spark_sql_share_higher_order_answer(
    spark: ReparkSession, name: str, sql: str, build: SqlColumnBuilder
) -> None:
    """Compare both doors for forms outside the separately pinned indexed-width divergence."""
    frame = _frame(spark)
    column_table = frame.select(build("a", "b", "m1", "m2").alias("r")).toArrow()
    sql_table = spark.sql(f"SELECT {sql} AS r FROM ({FRAME_SQL}) AS t").toArrow()
    assert column_table.to_pylist() == sql_table.to_pylist(), name
    assert column_table.schema.field("r").type == sql_table.schema.field("r").type, name
    assert column_table.schema.field("r").nullable == sql_table.schema.field("r").nullable, name


@pytest.mark.parametrize(
    (
        "array_sql",
        "expected_values",
        "field_nullable",
        "element_nullable",
        "column_is_int32",
        "sql_is_int32",
    ),
    (
        ("array(1, 2, 3)", [[1, 3, 5]], False, False, True, True),
        ("array(1, CAST(NULL AS INT), 3)", [[1, None, 5]], False, True, False, True),
        ("CAST(array() AS ARRAY<INT>)", [[]], False, True, True, True),
        ("CAST(NULL AS ARRAY<INT>)", [None], True, True, True, True),
    ),
    ids=("nonnull", "null_element", "empty", "null_array"),
)
def test_indexed_transform_public_arrow_shapes(
    spark: ReparkSession,
    array_sql: str,
    expected_values: list[list[int | None] | None],
    field_nullable: bool,
    element_nullable: bool,
    column_is_int32: bool,
    sql_is_int32: bool,
) -> None:
    """Pin indexed-transform values, widths, and nested nullability on both public doors."""
    frame = spark.sql(f"SELECT {array_sql} AS a")
    column_table = frame.select(_transform_index("a", "", "", "").alias("r")).toArrow()
    sql_table = spark.sql(f"SELECT transform({array_sql}, (x, i) -> x + i) AS r").toArrow()
    for table, is_int32 in ((column_table, column_is_int32), (sql_table, sql_is_int32)):
        assert table.column("r").to_pylist() == expected_values
        field = table.schema.field("r")
        assert field.type.value_type == (pa.int32() if is_int32 else pa.int64())
        assert field.nullable is field_nullable
        assert field.type.value_field.nullable is element_nullable


def test_exists_public_nullability_matches_spark(spark: ReparkSession) -> None:
    """Pin a non-null exists result for a non-null constructor and predicate."""
    frame = spark.sql("SELECT array(1, 2, 3) AS a")
    column_table = frame.select(_exists("a", "", "", "").alias("r")).toArrow()
    sql_table = spark.sql("SELECT exists(array(1, 2, 3), x -> x > 2) AS r").toArrow()
    for table in (column_table, sql_table):
        assert table.column("r").to_pylist() == [True]
        assert table.schema.field("r").type == pa.bool_()
        assert not table.schema.field("r").nullable


@pytest.mark.parametrize(("name", "sql", "_build"), MATRIX, ids=[item[0] for item in MATRIX])
def test_column_free_expr_parses_each_higher_order_form(
    spark: ReparkSession, name: str, sql: str, _build: SqlColumnBuilder
) -> None:
    """Column-free F.expr accepts every FNP-8 lambda spelling."""
    expression = _column_free_expression(name)
    column = spark_functions.expr(expression)
    expression_table = spark.sql("SELECT 1 AS sentinel").select(column.alias("r")).toArrow()
    sql_table = spark.sql(f"SELECT {expression} AS r").toArrow()
    assert expression_table.to_pylist() == sql_table.to_pylist(), sql
    assert expression_table.schema.field("r").type == sql_table.schema.field("r").type, sql
    assert expression_table.schema.field("r").nullable == sql_table.schema.field("r").nullable, sql
    if name == "aggregate":
        for array_sql, initial, expected in (
            ("CAST(NULL AS ARRAY<INT>)", 0, None),
            ("CAST(array() AS ARRAY<INT>)", 42, 42),
        ):
            frame = spark.sql(f"SELECT {array_sql} AS a")
            column_table = frame.select(
                spark_functions.aggregate(
                    "a",
                    spark_functions.lit(initial),
                    lambda accumulator, element: accumulator + element,
                ).alias("r")
            ).toArrow()
            inline = f"aggregate({array_sql}, {initial}, (acc, x) -> acc + x)"
            sql_table = spark.sql(f"SELECT {inline} AS r").toArrow()
            expression_table = (
                spark.sql("SELECT 1").select(spark_functions.expr(inline).alias("r")).toArrow()
            )
            for table in (column_table, sql_table, expression_table):
                assert table.column("r").to_pylist() == [expected]
                assert table.schema.field("r").type == pa.int32()
                assert table.schema.field("r").nullable
        bigint = (
            "aggregate(array(1, 2, 3), CAST(0 AS BIGINT), "
            "(acc, x) -> acc + x, acc -> acc * CAST(10 AS BIGINT))"
        )
        frame = spark.sql("SELECT array(1, 2, 3) AS a")
        bigint_column = frame.select(
            spark_functions.aggregate(
                "a",
                spark_functions.lit(0).cast("bigint"),
                lambda accumulator, element: accumulator + element,
                lambda accumulator: accumulator * spark_functions.lit(10).cast("bigint"),
            ).alias("r")
        ).toArrow()
        bigint_sql = spark.sql(f"SELECT {bigint} AS r").toArrow()
        bigint_expr = (
            spark.sql("SELECT 1").select(spark_functions.expr(bigint).alias("r")).toArrow()
        )
        for table in (bigint_column, bigint_sql, bigint_expr):
            assert table.column("r").to_pylist() == [60]
            assert table.schema.field("r").type == pa.int64()


def test_expr_column_reference_stays_the_ex_fn_4_refusal() -> None:
    """F.expr column references remain the separately declared EX-FN-4 backlog."""
    with pytest.raises(AnalysisException, match="No field named a"):
        spark_functions.expr("transform(a, x -> x + 1)")


def _width_view_frame(spark: ReparkSession) -> DataFrame:
    """Build the shared table-backed width frame behind a temp view."""
    spark.sql(
        "SELECT array(1, 2, 3) AS a, array(10, 20) AS b, map('a', 1, 'b', 2) AS m1"
    ).createOrReplaceTempView("fnp8rev_width")
    return spark.sql("SELECT a, b, m1 FROM fnp8rev_width")


def _zip_nullproof(column_a: str, column_b: str) -> Column:
    """Build a null-proof zip-with expression."""
    return spark_functions.zip_with(
        column_a,
        column_b,
        lambda left, right: (
            spark_functions.coalesce(left, spark_functions.lit(0))
            + spark_functions.coalesce(right, spark_functions.lit(0))
        ),
    )


def _assert_list_int32(table: pa.Table, expected_values: list[list[int | None] | None]) -> None:
    """Pin list values with an Int32 element on the Arrow path."""
    assert table.column("r").to_pylist() == expected_values
    assert table.schema.field("r").type.value_type == pa.int32()


def _assert_map_value_int32(table: pa.Table, expected_values: list[object]) -> None:
    """Pin map values with an Int32 value field on the Arrow path."""
    assert table.column("r").to_pylist() == expected_values
    assert table.schema.field("r").type.item_type == pa.int32()


def test_table_backed_lambda_body_literals_answer_int32(spark: ReparkSession) -> None:
    """Pin Int32 lambda-body widths over a view on both doors."""
    frame = _width_view_frame(spark)
    sql_table = spark.sql("SELECT transform(a, x -> x + 1) AS r FROM fnp8rev_width").toArrow()
    _assert_list_int32(sql_table, [[2, 3, 4]])
    assert not sql_table.schema.field("r").type.value_field.nullable
    column_table = frame.select(_transform("a", "", "", "").alias("r")).toArrow()
    _assert_list_int32(column_table, [[2, 3, 4]])
    assert not column_table.schema.field("r").type.value_field.nullable
    sql_table = spark.sql(
        "SELECT zip_with(a, b, (x, y) -> coalesce(x, 0) + coalesce(y, 0)) AS r FROM fnp8rev_width"
    ).toArrow()
    _assert_list_int32(sql_table, [[11, 22, 3]])
    assert not sql_table.schema.field("r").type.value_field.nullable
    column_table = frame.select(_zip_nullproof("a", "b").alias("r")).toArrow()
    _assert_list_int32(column_table, [[11, 22, 3]])
    assert not column_table.schema.field("r").type.value_field.nullable
    sql_table = spark.sql(
        "SELECT transform_values(m1, (k, v) -> v + 1) AS r FROM fnp8rev_width"
    ).toArrow()
    _assert_map_value_int32(sql_table, [[("a", 2), ("b", 3)]])
    column_table = frame.select(_transform_values("", "", "m1", "").alias("r")).toArrow()
    _assert_map_value_int32(column_table, [[("a", 2), ("b", 3)]])


def test_inline_lambda_body_literals_answer_int32(spark: ReparkSession) -> None:
    """Pin Int32 lambda-body widths over inline literals on both doors."""
    sql_table = spark.sql("SELECT transform(array(1, 2, 3), x -> x + 1) AS r").toArrow()
    _assert_list_int32(sql_table, [[2, 3, 4]])
    inline = spark.sql("SELECT array(1, 2, 3) AS a, array(10, 20) AS b")
    column_table = inline.select(_transform("a", "", "", "").alias("r")).toArrow()
    _assert_list_int32(column_table, [[2, 3, 4]])
    sql_table = spark.sql(
        "SELECT zip_with(array(1, 2, 3), array(10, 20),"
        " (x, y) -> coalesce(x, 0) + coalesce(y, 0)) AS r"
    ).toArrow()
    _assert_list_int32(sql_table, [[11, 22, 3]])
    column_table = inline.select(_zip_nullproof("a", "b").alias("r")).toArrow()
    _assert_list_int32(column_table, [[11, 22, 3]])
    sql_table = spark.sql(
        "SELECT transform_values(map('a', 1, 'b', 2), (k, v) -> v + 1) AS r"
    ).toArrow()
    _assert_map_value_int32(sql_table, [[("a", 2), ("b", 3)]])
    maps = spark.sql("SELECT map('a', 1, 'b', 2) AS m1")
    column_table = maps.select(_transform_values("", "", "m1", "").alias("r")).toArrow()
    _assert_map_value_int32(column_table, [[("a", 2), ("b", 3)]])
    negative = spark.sql("SELECT transform(array(1, 2, 3), x -> x + -5) AS r").toArrow()
    _assert_list_int32(negative, [[-4, -3, -2]])
    assert not negative.schema.field("r").nullable
    assert not negative.schema.field("r").type.value_field.nullable
    wide = spark.sql("SELECT transform(array(1, 2, 3), x -> x + 3000000000) AS r").toArrow()
    assert wide.column("r").to_pylist() == [[3000000001, 3000000002, 3000000003]]
    assert wide.schema.field("r").type.value_type == pa.int64()
    assert not wide.schema.field("r").nullable
    assert not wide.schema.field("r").type.value_field.nullable


def test_zip_with_left_shorter_non_null_elements_answers_all_doors(
    spark: ReparkSession,
) -> None:
    """Pin the left-shorter non-null-element zip on SQL, Column, and F.expr."""
    expression = "zip_with(array(1), array(10, 20), (x, y) -> coalesce(x, 0) + y)"
    sql_table = spark.sql(f"SELECT {expression} AS r").toArrow()
    frame = spark.sql("SELECT array(1) AS a, array(10, 20) AS b")
    column_table = frame.select(
        spark_functions.zip_with(
            "a",
            "b",
            lambda left, right: spark_functions.coalesce(left, spark_functions.lit(0)) + right,
        ).alias("r")
    ).toArrow()
    expression_table = (
        spark.sql("SELECT 1 AS sentinel")
        .select(spark_functions.expr(expression).alias("r"))
        .toArrow()
    )
    for table in (sql_table, column_table, expression_table):
        _assert_list_int32(table, [[11, 20]])
        assert not table.schema.field("r").nullable
    kept = spark.sql(
        "SELECT zip_with(array(1, 2, 3), array(10), (x, y) -> x + coalesce(y, 0)) AS r"
    ).toArrow()
    _assert_list_int32(kept, [[11, 2, 3]])


def test_zip_with_element_nullability_follows_the_lambda(spark: ReparkSession) -> None:
    """Pin null-proof versus nullable zip elements on both doors."""
    nullproof_sql = spark.sql(
        "SELECT zip_with(array(1, 2, 3), array(10), (x, y) -> coalesce(x, 0) + coalesce(y, 0)) AS r"
    ).toArrow()
    assert nullproof_sql.column("r").to_pylist() == [[11, 2, 3]]
    assert nullproof_sql.schema.field("r").type.value_type == pa.int32()
    assert not nullproof_sql.schema.field("r").type.value_field.nullable
    frame = spark.sql("SELECT array(1, 2, 3) AS a, array(10) AS b")
    nullproof_column = frame.select(_zip_nullproof("a", "b").alias("r")).toArrow()
    assert nullproof_column.column("r").to_pylist() == [[11, 2, 3]]
    assert nullproof_column.schema.field("r").type.value_type == pa.int32()
    assert not nullproof_column.schema.field("r").type.value_field.nullable
    nullable_sql = spark.sql(
        "SELECT zip_with(array(1, 2, 3), array(10), (x, y) -> x + y) AS r"
    ).toArrow()
    assert nullable_sql.column("r").to_pylist() == [[11, None, None]]
    assert nullable_sql.schema.field("r").type.value_type == pa.int32()
    assert nullable_sql.schema.field("r").type.value_field.nullable


def test_nested_transform_over_outer_variable_nullability(spark: ReparkSession) -> None:
    """Pin innermost nullability for the outer-variable nesting shape."""
    expression = "transform(array(array(1, 2), array(3)), a -> transform(a, b -> b + 1))"
    sql_table = spark.sql(f"SELECT {expression} AS r").toArrow()
    expression_table = (
        spark.sql("SELECT 1 AS sentinel")
        .select(spark_functions.expr(expression).alias("r"))
        .toArrow()
    )
    for table in (sql_table, expression_table):
        assert table.column("r").to_pylist() == [[[2, 3], [4]]]
        outer = table.schema.field("r")
        assert not outer.nullable
        middle = outer.type.value_field
        assert not middle.nullable
        assert middle.type.value_type == pa.int32()
        assert not middle.type.value_field.nullable
    nullable = spark.sql(
        "SELECT transform(array(array(1, NULL, 3)), a -> transform(a, b -> b + 1)) AS r"
    ).toArrow()
    assert nullable.column("r").to_pylist() == [[[2, None, 4]]]
    assert nullable.schema.field("r").type.value_type.value_type == pa.int32()
    assert nullable.schema.field("r").type.value_type.value_field.nullable


def test_join_fed_lambda_body_literals_answer_int32(spark: ReparkSession) -> None:
    """Pin Int32 widths with not-null elements for join-fed HOFs on both doors."""
    left = spark.sql(
        "SELECT transform(t1.a, x -> x + 1) AS r"
        " FROM (SELECT array(1, 2, 3) AS a, 1 AS k) t1 JOIN (SELECT 1 AS k) t2"
        " ON t1.k = t2.k"
    ).toArrow()
    right = spark.sql(
        "SELECT transform(t2.a, x -> x + 1) AS r FROM (SELECT 1 AS k) t1"
        " JOIN (SELECT array(1, 2, 3) AS a, 1 AS k) t2 ON t1.k = t2.k"
    ).toArrow()
    crossed = spark.sql(
        "SELECT transform(t1.a, x -> x + 1) AS r FROM (SELECT array(1, 2, 3) AS a) t1"
        " CROSS JOIN (SELECT 1 AS k) t2"
    ).toArrow()
    frame = spark.sql(
        "SELECT t1.a AS a FROM (SELECT array(1, 2, 3) AS a, 1 AS k) t1"
        " JOIN (SELECT 1 AS k) t2 ON t1.k = t2.k"
    )
    column_table = frame.select(_transform("a", "", "", "").alias("r")).toArrow()
    kept = spark.sql(
        "SELECT transform(t1.a, x -> x + 1) AS r"
        " FROM (SELECT array(1, 2, 3) AS a, 1 AS k) t1"
        " LEFT JOIN (SELECT 2 AS k) t2 ON t1.k = t2.k"
    ).toArrow()
    for table in (left, right, crossed, column_table, kept):
        _assert_list_int32(table, [[2, 3, 4]])
        assert not table.schema.field("r").nullable
        assert not table.schema.field("r").type.value_field.nullable


def test_scalar_subquery_hof_answers_int32(spark: ReparkSession) -> None:
    """Pin the scalar-subquery HOF answer repark serves past Spark's refusal."""
    table = spark.sql("SELECT transform((SELECT array(1, 2, 3) AS a), x -> x + 1) AS r").toArrow()
    _assert_list_int32(table, [[2, 3, 4]])
    assert not table.schema.field("r").nullable
    assert not table.schema.field("r").type.value_field.nullable


def test_lineage_through_plan_nodes_keeps_int32(spark: ReparkSession) -> None:
    """Pin Int32 HOF widths through aggregate, window, and passthrough nodes."""
    queries = (
        "SELECT transform(a, x -> x + 1) AS r FROM (SELECT array(1, 2, 3) AS a, k"
        " FROM (SELECT 1 AS k) s GROUP BY k, array(1, 2, 3))",
        "SELECT transform(a, x -> x + 1) AS r FROM (SELECT array(1, 2, 3) AS a,"
        " row_number() OVER (ORDER BY k) AS rn FROM (SELECT 1 AS k) s)",
        "SELECT transform(a, x -> x + 1) AS r FROM (SELECT a, rn FROM (SELECT a,"
        " row_number() OVER (ORDER BY k) AS rn"
        " FROM (SELECT array(1, 2, 3) AS a, 1 AS k)))",
        "SELECT transform(a, x -> x + 1) AS r FROM (SELECT array(1, 2, 3) AS a)",
        "WITH c AS (SELECT array(1, 2, 3) AS a) SELECT transform(a, x -> x + 1) AS r FROM c",
        "SELECT transform(a, x -> x + 1) AS r FROM (SELECT array(1, 2, 3) AS a LIMIT 10)",
        "SELECT transform(a, x -> x + 1) AS r FROM (SELECT array(1, 2, 3) AS a, 1 AS k ORDER BY k)",
        "SELECT transform(a, x -> x + 1) AS r FROM (SELECT a, k"
        " FROM (SELECT array(1, 2, 3) AS a, 1 AS k) WHERE k = 1)",
        "SELECT transform(a, x -> x + 1) AS r FROM (SELECT DISTINCT array(1, 2, 3) AS a)",
    )
    for query in queries:
        table = spark.sql(query).toArrow()
        _assert_list_int32(table, [[2, 3, 4]])
        assert not table.schema.field("r").nullable
        assert not table.schema.field("r").type.value_field.nullable


def test_multihop_lineage_keeps_not_null_elements(spark: ReparkSession) -> None:
    """Pin not-null HOF elements over multi-hop views and nullable tables."""
    spark.sql("SELECT array(1, 2, 3) AS a").createOrReplaceTempView("fnp8rev_r2_v")
    spark.sql("CREATE TABLE fnp8rev_r2_t USING PARQUET AS SELECT array(1, 2, 3) AS a")
    lineage_queries = (
        "SELECT transform(a, x -> x + 1) AS r FROM (SELECT a FROM fnp8rev_r2_v)",
        "SELECT transform(a, x -> x + 1) AS r FROM (SELECT a FROM (SELECT a FROM fnp8rev_r2_v))",
        "WITH c AS (SELECT a FROM fnp8rev_r2_v) SELECT transform(a, x -> x + 1) AS r FROM c",
        "WITH c1 AS (SELECT array(1, 2, 3) AS a), c2 AS (SELECT a FROM c1)"
        " SELECT transform(a, x -> x + 1) AS r FROM c2",
    )
    for query in lineage_queries:
        table = spark.sql(query).toArrow()
        _assert_list_int32(table, [[2, 3, 4]])
        assert not table.schema.field("r").nullable
        assert not table.schema.field("r").type.value_field.nullable
    table_queries = (
        "SELECT transform(a, x -> x + 1) AS r FROM (SELECT a FROM fnp8rev_r2_t)",
        "WITH c AS (SELECT a FROM fnp8rev_r2_t) SELECT transform(a, x -> x + 1) AS r FROM c",
        "SELECT transform(a, x -> x + 1) AS r FROM fnp8rev_r2_t",
    )
    for query in table_queries:
        table = spark.sql(query).toArrow()
        _assert_list_int32(table, [[2, 3, 4]])
        assert table.schema.field("r").nullable
        assert table.schema.field("r").type.value_field.nullable


def test_union_of_narrowed_branches_answers_int32(spark: ReparkSession) -> None:
    """Pin Int32 HOF widths over a union of narrowed view branches."""
    spark.sql("SELECT array(1, 2, 3) AS a").createOrReplaceTempView("fnp8rev_r2_v1")
    spark.sql("SELECT array(4) AS a").createOrReplaceTempView("fnp8rev_r2_v2")
    table = spark.sql(
        "SELECT transform(a, x -> x + 1) AS r"
        " FROM (SELECT a FROM fnp8rev_r2_v1 UNION ALL SELECT a FROM fnp8rev_r2_v2)"
    ).toArrow()
    assert sorted(table.column("r").to_pylist()) == [[2, 3, 4], [5]]
    assert table.schema.field("r").type.value_type == pa.int32()
    assert not table.schema.field("r").nullable
    assert not table.schema.field("r").type.value_field.nullable


def test_aggregate_over_lineage_stays_int32(spark: ReparkSession) -> None:
    """Pin immune aggregate answers over join-fed and multi-hop literals."""
    queries = (
        "SELECT aggregate(t1.a, 0, (acc, x) -> acc + x) AS r"
        " FROM (SELECT array(1, 2, 3) AS a, 1 AS k) t1"
        " JOIN (SELECT 1 AS k) t2 ON t1.k = t2.k",
        "SELECT aggregate(a, 0, (acc, x) -> acc + x) AS r"
        " FROM (SELECT a FROM (SELECT array(1, 2, 3) AS a))",
        "SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) AS r",
    )
    for query in queries:
        table = spark.sql(query).toArrow()
        assert table.column("r").to_pylist() == [6]
        assert table.schema.field("r").type == pa.int32()
        assert table.schema.field("r").nullable


def test_values_fed_hof_answers_int32(spark: ReparkSession) -> None:
    """Pin Int32 widths with not-null elements for VALUES-fed HOFs."""
    table = spark.sql(
        "SELECT transform(a, x -> x + 1) AS r"
        " FROM (VALUES (array(1, 2, 3)), (array(4, 5, 6))) AS t(a)"
    ).toArrow()
    assert sorted(table.column("r").to_pylist()) == [[2, 3, 4], [5, 6, 7]]
    assert table.schema.field("r").type.value_type == pa.int32()
    assert not table.schema.field("r").nullable
    assert not table.schema.field("r").type.value_field.nullable


def test_outer_join_padded_side_answers_nullable(spark: ReparkSession) -> None:
    """Pin nullable answers for padded outer-join sides without crashing."""
    padded = spark.sql(
        "SELECT transform(t2.a, x -> x + 1) AS r FROM (SELECT 1 AS k) t1"
        " LEFT JOIN (SELECT array(1, 2, 3) AS a, 2 AS k) t2 ON t1.k = t2.k"
    ).toArrow()
    assert padded.column("r").to_pylist() == [None]
    assert padded.schema.field("r").nullable
    full = spark.sql(
        "SELECT transform(t1.a, x -> x + 1) AS r"
        " FROM (SELECT array(1, 2, 3) AS a, 2 AS k) t1"
        " FULL JOIN (SELECT 1 AS k) t2 ON t1.k = t2.k"
    ).toArrow()
    assert sorted(full.column("r").to_pylist(), key=str) == [None, [2, 3, 4]]
    assert full.schema.field("r").nullable


def test_lateral_view_stays_a_loud_refusal(spark: ReparkSession) -> None:
    """Pin the loud lateral-view refusal that keeps Generate unreachable."""
    with pytest.raises(UnsupportedOperationException, match="LATERAL VIEWS"):
        spark.sql(
            "SELECT transform(a, x -> x + 1) AS r FROM (SELECT array(array(1, 2)) AS arr)"
            " LATERAL VIEW explode(arr) t AS a"
        ).toArrow()
