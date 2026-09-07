"""Live Spark and RePark indexed-transform Arrow-shape detector for FNP-8."""

from __future__ import annotations

import json

import _live_parity as lp
import pyarrow as pa
import pytest


def _assert_indexed_transform_cell(
    table: pa.Table,
    expected_values: list[list[int | None] | None],
    field_nullable: bool,
    element_nullable: bool,
    is_int32: bool,
) -> None:
    """Assert one measured indexed-transform Arrow value and schema cell."""
    assert table.column("r").to_pylist() == expected_values
    field = table.schema.field("r")
    assert field.type.value_type == (pa.int32() if is_int32 else pa.int64())
    assert field.nullable is field_nullable
    assert field.type.value_field.nullable is element_nullable


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("ansi_enabled", (True, False), ids=("ansi_on", "ansi_off"))
def test_live_fnp8_indexed_transform_arrow_shapes(
    spark_engine: lp.Engine, ansi_enabled: bool
) -> None:
    """Pin each RePark door and the Spark oracle for indexed-transform Arrow shapes."""
    value = "true" if ansi_enabled else "false"
    cases = (
        (
            "array(1, 2, 3)",
            [[1, 3, 5]],
            False,
            False,
            True,
            False,
            False,
            True,
            False,
            False,
        ),
        (
            "array(1, NULL, 3)",
            [[1, None, 5]],
            False,
            True,
            False,
            False,
            True,
            True,
            False,
            True,
        ),
        (
            "CAST(array() AS ARRAY<INT>)",
            [[]],
            False,
            True,
            True,
            False,
            True,
            True,
            False,
            True,
        ),
        (
            "CAST(NULL AS ARRAY<INT>)",
            [None],
            True,
            True,
            True,
            True,
            True,
            True,
            True,
            True,
        ),
    )
    repark_engine = lp.build_repark_engine((("spark.sql.ansi.enabled", value),))
    with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", value),)):
        for (
            array_sql,
            expected_values,
            spark_field_nullable,
            spark_element_nullable,
            column_is_int32,
            column_field_nullable,
            column_element_nullable,
            sql_is_int32,
            sql_field_nullable,
            sql_element_nullable,
        ) in cases:
            column_table = repark_engine.arrow_of(
                repark_engine.session.sql(f"SELECT {array_sql} AS a").select(
                    repark_engine.functions.transform(
                        "a", lambda element, index: element + index
                    ).alias("r")
                )
            )
            sql_table = repark_engine.arrow_of(
                repark_engine.session.sql(f"SELECT transform({array_sql}, (x, i) -> x + i) AS r")
            )
            spark_table = spark_engine.arrow_of(
                spark_engine.session.sql(f"SELECT transform({array_sql}, (x, i) -> x + i) AS r")
            )
            _assert_indexed_transform_cell(
                column_table,
                expected_values,
                column_field_nullable,
                column_element_nullable,
                column_is_int32,
            )
            _assert_indexed_transform_cell(
                sql_table,
                expected_values,
                sql_field_nullable,
                sql_element_nullable,
                sql_is_int32,
            )
            _assert_indexed_transform_cell(
                spark_table,
                expected_values,
                spark_field_nullable,
                spark_element_nullable,
                True,
            )


def _assert_map_value_cell(
    table: pa.Table,
    expected_values: list[list[list[object]]],
    value_nullable: bool,
    is_int32: bool,
) -> None:
    """Assert one measured map value, width, and value nullability cell."""
    assert json.loads(json.dumps(table.column("r").to_pylist())) == expected_values
    map_type = table.schema.field("r").type
    assert map_type.item_type == (pa.int32() if is_int32 else pa.int64())
    assert map_type.item_field.nullable is value_nullable


WIDTH_VIEW_SQL = "SELECT array(1, 2, 3) AS a, array(10, 20) AS b, map('a', 1, 'b', 2) AS m1"


def _seed_width_view(repark_engine: lp.Engine, spark_engine: lp.Engine) -> None:
    """Create the shared width view on both engines."""
    spark_engine.session.sql(f"CREATE OR REPLACE TEMP VIEW fnp8rev_live AS {WIDTH_VIEW_SQL}")
    repark_engine.session.sql(WIDTH_VIEW_SQL).createOrReplaceTempView("fnp8rev_live")


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("ansi_enabled", (True, False), ids=("ansi_on", "ansi_off"))
def test_live_fnp8review_table_backed_width_matches_spark(
    spark_engine: lp.Engine, ansi_enabled: bool
) -> None:
    """Pin view-backed lambda-body widths on both RePark doors against live Spark."""
    value = "true" if ansi_enabled else "false"
    repark_engine = lp.build_repark_engine((("spark.sql.ansi.enabled", value),))
    with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", value),)):
        _seed_width_view(repark_engine, spark_engine)
        spark_table = spark_engine.arrow_of(
            spark_engine.session.sql("SELECT transform(a, x -> x + 1) AS r FROM fnp8rev_live")
        )
        sql_table = repark_engine.arrow_of(
            repark_engine.session.sql("SELECT transform(a, x -> x + 1) AS r FROM fnp8rev_live")
        )
        column_table = repark_engine.arrow_of(
            repark_engine.session.sql("SELECT a FROM fnp8rev_live").select(
                repark_engine.functions.transform("a", lambda element: element + 1).alias("r")
            )
        )
        for table, element_nullable in (
            (spark_table, False),
            (sql_table, False),
            (column_table, True),
        ):
            _assert_indexed_transform_cell(table, [[2, 3, 4]], False, element_nullable, True)
        spark_table = spark_engine.arrow_of(
            spark_engine.session.sql(
                "SELECT zip_with(a, b, (x, y) -> coalesce(x, 0) + coalesce(y, 0)) AS r"
                " FROM fnp8rev_live"
            )
        )
        sql_table = repark_engine.arrow_of(
            repark_engine.session.sql(
                "SELECT zip_with(a, b, (x, y) -> coalesce(x, 0) + coalesce(y, 0)) AS r"
                " FROM fnp8rev_live"
            )
        )
        column_table = repark_engine.arrow_of(
            repark_engine.session.sql("SELECT a, b FROM fnp8rev_live").select(
                repark_engine.functions.zip_with(
                    "a",
                    "b",
                    lambda left, right: (
                        repark_engine.functions.coalesce(left, repark_engine.functions.lit(0))
                        + repark_engine.functions.coalesce(right, repark_engine.functions.lit(0))
                    ),
                ).alias("r")
            )
        )
        for table in (spark_table, sql_table, column_table):
            _assert_indexed_transform_cell(table, [[11, 22, 3]], False, False, True)
        expected_map = [[["a", 2], ["b", 3]]]
        values_query = "SELECT transform_values(m1, (k, v) -> v + 1) AS r FROM fnp8rev_live"
        spark_table = spark_engine.arrow_of(spark_engine.session.sql(values_query))
        sql_table = repark_engine.arrow_of(repark_engine.session.sql(values_query))
        column_table = repark_engine.arrow_of(
            repark_engine.session.sql("SELECT m1 FROM fnp8rev_live").select(
                repark_engine.functions.transform_values(
                    "m1", lambda key, map_value: map_value + 1
                ).alias("r")
            )
        )
        _assert_map_value_cell(spark_table, expected_map, False, True)
        _assert_map_value_cell(sql_table, expected_map, True, True)
        _assert_map_value_cell(column_table, expected_map, True, True)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("ansi_enabled", (True, False), ids=("ansi_on", "ansi_off"))
def test_live_fnp8review_zip_left_shorter_matches_spark(
    spark_engine: lp.Engine, ansi_enabled: bool
) -> None:
    """Pin the left-shorter non-null-element zip on three doors against live Spark."""
    value = "true" if ansi_enabled else "false"
    expression = "zip_with(array(1), array(10, 20), (x, y) -> coalesce(x, 0) + y)"
    repark_engine = lp.build_repark_engine((("spark.sql.ansi.enabled", value),))
    with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", value),)):
        spark_table = spark_engine.arrow_of(spark_engine.session.sql(f"SELECT {expression} AS r"))
        sql_table = repark_engine.arrow_of(repark_engine.session.sql(f"SELECT {expression} AS r"))
        column_table = repark_engine.arrow_of(
            repark_engine.session.sql("SELECT array(1) AS a, array(10, 20) AS b").select(
                repark_engine.functions.zip_with(
                    "a",
                    "b",
                    lambda left, right: (
                        repark_engine.functions.coalesce(left, repark_engine.functions.lit(0))
                        + right
                    ),
                ).alias("r")
            )
        )
        expression_table = repark_engine.arrow_of(
            repark_engine.session.sql("SELECT 1 AS sentinel").select(
                repark_engine.functions.expr(expression).alias("r")
            )
        )
        for table in (spark_table, sql_table, column_table, expression_table):
            _assert_indexed_transform_cell(table, [[11, 20]], False, True, True)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("ansi_enabled", (True, False), ids=("ansi_on", "ansi_off"))
def test_live_fnp8review_zip_element_nullability_matches_spark(
    spark_engine: lp.Engine, ansi_enabled: bool
) -> None:
    """Pin null-proof versus nullable zip elements against live Spark."""
    value = "true" if ansi_enabled else "false"
    repark_engine = lp.build_repark_engine((("spark.sql.ansi.enabled", value),))
    nullproof = "zip_with(array(1, 2, 3), array(10), (x, y) -> coalesce(x, 0) + coalesce(y, 0))"
    nullable = "zip_with(array(1, 2, 3), array(10), (x, y) -> x + y)"
    with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", value),)):
        spark_table = spark_engine.arrow_of(spark_engine.session.sql(f"SELECT {nullproof} AS r"))
        sql_table = repark_engine.arrow_of(repark_engine.session.sql(f"SELECT {nullproof} AS r"))
        column_table = repark_engine.arrow_of(
            repark_engine.session.sql("SELECT array(1, 2, 3) AS a, array(10) AS b").select(
                repark_engine.functions.zip_with(
                    "a",
                    "b",
                    lambda left, right: (
                        repark_engine.functions.coalesce(left, repark_engine.functions.lit(0))
                        + repark_engine.functions.coalesce(right, repark_engine.functions.lit(0))
                    ),
                ).alias("r")
            )
        )
        for table in (spark_table, sql_table, column_table):
            _assert_indexed_transform_cell(table, [[11, 2, 3]], False, False, True)
        spark_table = spark_engine.arrow_of(spark_engine.session.sql(f"SELECT {nullable} AS r"))
        sql_table = repark_engine.arrow_of(repark_engine.session.sql(f"SELECT {nullable} AS r"))
        for table in (spark_table, sql_table):
            _assert_indexed_transform_cell(table, [[11, None, None]], False, True, True)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("ansi_enabled", (True, False), ids=("ansi_on", "ansi_off"))
def test_live_fnp8review_nested_outer_variable_matches_spark(
    spark_engine: lp.Engine, ansi_enabled: bool
) -> None:
    """Pin the outer-variable nesting shape against live Spark."""
    value = "true" if ansi_enabled else "false"
    expression = "transform(array(array(1, 2), array(3)), a -> transform(a, b -> b + 1))"
    counterpart = "transform(array(array(1, NULL, 3)), a -> transform(a, b -> b + 1))"
    repark_engine = lp.build_repark_engine((("spark.sql.ansi.enabled", value),))
    with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", value),)):
        spark_table = spark_engine.arrow_of(spark_engine.session.sql(f"SELECT {expression} AS r"))
        sql_table = repark_engine.arrow_of(repark_engine.session.sql(f"SELECT {expression} AS r"))
        for table in (spark_table, sql_table):
            assert table.column("r").to_pylist() == [[[2, 3], [4]]]
            outer = table.schema.field("r")
            assert not outer.nullable
            middle = outer.type.value_field
            assert not middle.nullable
            assert middle.type.value_type == pa.int32()
            assert not middle.type.value_field.nullable
        spark_table = spark_engine.arrow_of(spark_engine.session.sql(f"SELECT {counterpart} AS r"))
        sql_table = repark_engine.arrow_of(repark_engine.session.sql(f"SELECT {counterpart} AS r"))
        for table in (spark_table, sql_table):
            assert table.column("r").to_pylist() == [[[2, None, 4]]]
            middle = table.schema.field("r").type.value_field
            assert middle.type.value_type == pa.int32()
            assert middle.type.value_field.nullable


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("ansi_enabled", (True, False), ids=("ansi_on", "ansi_off"))
def test_live_fnp8_exists_public_nullability_matches_spark(
    spark_engine: lp.Engine, ansi_enabled: bool
) -> None:
    """Pin public exists value, type, and nullability against live Spark."""
    value = "true" if ansi_enabled else "false"
    repark_engine = lp.build_repark_engine((("spark.sql.ansi.enabled", value),))
    query = "SELECT exists(array(1, 2, 3), x -> x > 2) AS r"
    with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", value),)):
        spark_table = spark_engine.arrow_of(spark_engine.session.sql(query))
        sql_table = repark_engine.arrow_of(repark_engine.session.sql(query))
        column_table = repark_engine.arrow_of(
            repark_engine.session.sql("SELECT array(1, 2, 3) AS a").select(
                repark_engine.functions.exists("a", lambda element: element > 2).alias("r")
            )
        )
        for table in (spark_table, sql_table, column_table):
            assert table.column("r").to_pylist() == [True]
            assert table.schema.field("r").type == pa.bool_()
            assert not table.schema.field("r").nullable
