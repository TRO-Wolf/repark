"""Live Spark and RePark indexed-transform Arrow-shape detector for FNP-8."""

from __future__ import annotations

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
