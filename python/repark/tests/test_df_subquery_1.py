"""DF-SUBQUERY-1 — scalar / exists / lateralJoin / asTable pins from the PySpark 4.1.2 oracle.

Fixture ``facade_df_subquery_oracle.json`` was recorded byte-identical from
``probe_dfsubq.py`` (run 16b, Spark classic); every test names its cell.
Row order is engine-dependent, so row sets compare sorted by repr; columns,
schema, and nullability compare exactly.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkException,
    PySparkTypeError,
)
from repark.spark import functions as F  # noqa: N812
from repark.spark.functions import udtf
from repark.spark.row import Row

try:
    from repark.spark.table_arg import TableArg
except ImportError:
    TableArg = None

_ORACLE = json.loads((Path(__file__).parent / "facade_df_subquery_oracle.json").read_text())


def _cell(name: str) -> dict:
    """Return the recorded PySpark 4.1.2 oracle cell."""
    return _ORACLE[name]


@pytest.fixture
def spark() -> ReparkSession:
    """Fresh session per test."""
    session = ReparkSession.builder.appName("pytest-df-subquery-1").getOrCreate()
    yield session
    session.stop()


def _emp(spark: ReparkSession):
    """Build the oracle's emp frame."""
    return spark.createDataFrame(
        [(1, "a", 10), (2, "b", 20), (3, "a", 30), (4, None, None)],
        "id int, dept string, sal int",
    )


def _dept(spark: ReparkSession):
    """Build the oracle's dept frame."""
    return spark.createDataFrame([("a", 100), ("c", 300)], "dept string, budget int")


def _res(frame) -> dict:
    """Shape a frame's answer like the probe's res(): columns/schema/nullable + sorted repr rows."""
    return {
        "columns": frame.columns,
        "schema": frame.schema.simpleString(),
        "rows": sorted(repr(tuple(row)) for row in frame.collect()),
        "nullable": [field.nullable for field in frame.schema.fields],
    }


def _assert_frame(frame, cell: str) -> None:
    """Assert a frame's answer equals the recorded oracle cell (rows as a set)."""
    expected = dict(_cell(cell)["result"])
    expected["rows"] = sorted(expected["rows"])
    assert _res(frame) == expected


def test_scalar_uncorrelated(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-001 — cell scalar_uncorrelated."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.select("id", dept.select(F.max("budget")).scalar().alias("mb")),
        "scalar_uncorrelated",
    )


def test_scalar_correlated_unqualified_binds_inner(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-001 — cell scalar_correlated (inner-scope quirk)."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.select(
            "id",
            dept.where(F.col("dept") == F.col("dept").outer())
            .select(F.max("budget"))
            .scalar()
            .alias("b"),
        ),
        "scalar_correlated",
    )


def test_scalar_correlated_qualified(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-001, C-003 — cell scalar_correlated_qualified."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.alias("e").select(
            "id",
            dept.alias("d")
            .where(F.col("d.dept") == F.col("e.dept").outer())
            .select(F.max("d.budget"))
            .scalar()
            .alias("b"),
        ),
        "scalar_correlated_qualified",
    )


def test_scalar_empty_answers_null(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-001 — cell scalar_empty."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.select("id", dept.where("budget > 1000").select("budget").scalar().alias("b")),
        "scalar_empty",
    )


def test_scalar_in_filter(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-001 — cell scalar_in_filter."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.where(F.col("sal") * 10 < dept.select(F.max("budget")).scalar()),
        "scalar_in_filter",
    )


def test_scalar_multi_col_raises_conditioned(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-001 — cell scalar_multi_col."""
    emp, dept = _emp(spark), _dept(spark)
    error = _cell("scalar_multi_col")["error"]
    with pytest.raises(AnalysisException) as caught:
        emp.select("id", dept.select("dept", "budget").scalar().alias("b")).collect()
    exc = caught.value
    assert exc.getCondition() == error["condition"]
    assert exc.getMessageParameters() == error["params"]
    assert exc.getSqlState() == error["sqlstate"]
    assert "Scalar subquery must return only one column, but got 2" in str(exc)


def test_scalar_multi_row_raises_21000(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-001 — cell scalar_multi_row."""
    emp, dept = _emp(spark), _dept(spark)
    error = _cell("scalar_multi_row")["error"]
    with pytest.raises(PySparkException) as caught:
        emp.select("id", dept.select("budget").scalar().alias("b")).collect()
    text = str(caught.value)
    assert "[SCALAR_SUBQUERY_TOO_MANY_ROWS]" in text
    assert "More than one row returned by a subquery used as an expression" in text
    assert "21000" in text
    assert "SCALAR_SUBQUERY_TOO_MANY_ROWS" in error["message"]


def test_scalar_repr_and_default_name(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-001 — cells scalar_repr, scalar_no_alias_name."""
    emp, dept = _emp(spark), _dept(spark)
    assert repr(dept.select(F.max("budget")).scalar()) == _cell("scalar_repr")["result"]
    _assert_frame(emp.select(dept.select(F.max("budget")).scalar()), "scalar_no_alias_name")


def test_exists_correlated_unqualified_binds_inner(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-002 — cell exists_correlated (inner-scope quirk)."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.where(dept.where(F.col("dept") == F.col("dept").outer()).exists()),
        "exists_correlated",
    )


def test_exists_qualified_correlates(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-002, C-003 — qualified correlation answers exists_sql rows."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.alias("e").where(
            dept.alias("d").where(F.col("d.dept") == F.col("e.dept").outer()).exists()
        ),
        "exists_sql",
    )


def test_exists_not(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-002 — cell exists_not."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.where(~dept.where(F.col("dept") == F.col("dept").outer()).exists()),
        "exists_not",
    )


def test_exists_uncorrelated(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-002 — cells exists_uncorrelated_true/_empty."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(emp.where(dept.exists()), "exists_uncorrelated_true")
    _assert_frame(emp.where(dept.where("budget > 1000").exists()), "exists_uncorrelated_empty")


def test_exists_in_select(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-002 — cell exists_in_select."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.select(
            "id",
            dept.where(F.col("dept") == F.col("dept").outer()).exists().alias("e"),
        ),
        "exists_in_select",
    )


def test_exists_repr_and_default_name(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-002 — cells exists_repr, exists_no_alias_name."""
    emp, dept = _emp(spark), _dept(spark)
    assert repr(dept.exists()) == _cell("exists_repr")["result"]
    _assert_frame(emp.select(dept.exists()), "exists_no_alias_name")


def test_outer_outside_subquery_resolves_plain(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-003 — cell outer_outside_subquery."""
    _assert_frame(_emp(spark).where(F.col("dept").outer() == "a"), "outer_outside_subquery")


def test_lateral_basic(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-004 — cell lateral_basic (inner-scope quirk cross product)."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.lateralJoin(dept.where(F.col("dept") == F.col("dept").outer()).select("budget")),
        "lateral_basic",
    )


def test_lateral_left(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-004 — cell lateral_left."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.lateralJoin(
            dept.where(F.col("dept") == F.col("dept").outer()).select("budget"),
            how="left",
        ),
        "lateral_left",
    )


def test_lateral_cross(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-004 — cell lateral_cross."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(emp.lateralJoin(dept.select("budget"), how="cross"), "lateral_cross")


def test_lateral_on_qualified(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-004 — cell lateral_on keeps both dept columns."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.alias("e").lateralJoin(dept.alias("d"), on=F.col("e.dept") == F.col("d.dept")),
        "lateral_on",
    )


def test_lateral_on_qualified_correlates(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-004 — qualified correlation inside the lateral answers lateral_sql."""
    emp, dept = _emp(spark), _dept(spark)
    _assert_frame(
        emp.alias("e").lateralJoin(
            dept.alias("d").where(F.col("d.dept") == F.col("e.dept").outer()).select("budget")
        ),
        "lateral_sql",
    )


@pytest.mark.parametrize("how", ["right", "full", "left_semi", "nope"])
def test_lateral_unsupported_how_raises(spark: ReparkSession, how: str) -> None:
    """pins: df-subquery-1/C-004 — cells lateral_right/_full/_semi/_bad_how."""
    emp, dept = _emp(spark), _dept(spark)
    cell = "lateral_bad_how" if how == "nope" else f"lateral_{how.replace('left_', '')}"
    error = _cell(cell)["error"]
    with pytest.raises(AnalysisException) as caught:
        emp.lateralJoin(dept.select("budget"), how=how)
    exc = caught.value
    assert exc.getCondition() == error["condition"]
    assert exc.getMessageParameters() == error["params"]
    assert exc.getSqlState() == error["sqlstate"]
    assert f"Unsupported join type '{how}'" in str(exc)


def test_lateral_outer_expr_in_projection(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-004 — cell lateral_outer_expr (hoisted projection)."""
    emp = _emp(spark)
    _assert_frame(
        emp.lateralJoin(spark.range(1).select((F.col("sal").outer() * 2).alias("dbl"))),
        "lateral_outer_expr",
    )


def test_lateral_outer_ref_under_generator_refused(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-004 — cell lateral_tvf_like."""
    emp = _emp(spark)
    error = _cell("lateral_tvf_like")["error"]
    with pytest.raises(AnalysisException) as caught:
        emp.lateralJoin(
            spark.range(1).select(
                F.explode(F.array(F.col("id").outer(), F.col("sal").outer())).alias("v")
            )
        ).collect()
    text = str(caught.value)
    assert f"[{error['condition']}]" in text
    assert "Expressions referencing the outer query are not supported" in text
    assert "0A000" in text


def test_lateral_how_accepts_supported_spellings(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-004 — inner/leftouter/left_outer accepted."""
    emp, dept = _emp(spark), _dept(spark)
    for how in ("inner", "cross", "left", "leftouter", "left_outer"):
        frame = emp.lateralJoin(dept.select("budget"), how=how)
        assert len(frame.columns) == 4


def test_lateral_on_str_names(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-004 — str / list-of-str ``on`` joins on names, right key dropped."""
    emp, dept = _emp(spark), _dept(spark)
    expected_rows = ["(1, 'a', 10, 100)", "(3, 'a', 30, 100)"]
    for on in ("dept", ["dept"]):
        frame = emp.lateralJoin(dept, on=on)
        assert frame.columns == ["id", "dept", "sal", "budget"]
        assert sorted(repr(tuple(row)) for row in frame.collect()) == expected_rows


def test_lateral_rejects_non_dataframe_and_non_str_how(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-004 — argument-shape validation."""
    emp, dept = _emp(spark), _dept(spark)
    with pytest.raises(PySparkTypeError):
        emp.lateralJoin("dept")
    with pytest.raises(PySparkTypeError):
        emp.lateralJoin(dept, how=7)
    with pytest.raises(PySparkTypeError):
        emp.lateralJoin(dept, on=[7])


def test_astable_type_methods_repr(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-005 — cells astable_type, astable_methods, astable_repr."""
    emp = _emp(spark)
    arg = emp.asTable()
    assert type(arg).__name__ == _cell("astable_type")["result"]
    assert isinstance(arg, TableArg)
    assert (
        sorted(n for n in dir(arg) if not n.startswith("_")) == _cell("astable_methods")["result"]
    )
    assert repr(arg).startswith("<repark.spark.table_arg.TableArg object at 0x")


def test_astable_ordering_guards(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-005 — cells astable_order_without_partition/_partition_and_single."""
    emp = _emp(spark)
    error = _cell("astable_order_without_partition")["error"]
    with pytest.raises(IllegalArgumentException, match=re.escape(error["message"])):
        emp.asTable().orderBy("id")
    error = _cell("astable_partition_and_single")["error"]
    with pytest.raises(IllegalArgumentException, match=re.escape(error["message"])):
        emp.asTable().partitionBy("dept").withSinglePartition()
    with pytest.raises(IllegalArgumentException, match=re.escape(error["message"])):
        emp.asTable().withSinglePartition().withSinglePartition()


def test_astable_fluent_returns_table_arg(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-005 — cells astable_partition, astable_withsinglepartition."""
    emp = _emp(spark)
    assert isinstance(emp.asTable().partitionBy("dept").orderBy("id"), TableArg)
    assert isinstance(emp.asTable().withSinglePartition(), TableArg)


def test_astable_select_raises_not_column_or_str(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-005 — cell astable_select_error."""
    emp = _emp(spark)
    error = _cell("astable_select_error")["error"]
    with pytest.raises(PySparkTypeError) as caught:
        emp.select(emp.asTable())
    exc = caught.value
    assert exc.getCondition() == error["condition"]
    assert exc.getMessageParameters() == error["params"]
    assert "got TableArg" in str(exc)


def test_astable_tvf_explode_unchanged(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-005 — cell astable_tvf (pre-existing spark.tvf path).

    Same surface ``_frame_pin`` pins in ``test_session_surface_1.py``: columns, schema,
    rows. Spark's ``nullable=false`` on the exploded column is the pre-existing
    tvf-explode gap recorded under ``DF-SUBQUERY-1``'s registry row.
    """
    expected = _cell("astable_tvf")["result"]
    frame = spark.tvf.explode(F.array(F.lit(1), F.lit(2)))
    assert frame.columns == expected["columns"]
    assert frame.schema.simpleString() == expected["schema"]
    assert sorted(repr(tuple(row)) for row in frame.collect()) == sorted(expected["rows"])


def test_astable_udtf_call(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-005 — cell astable_udtf_call."""

    @udtf(returnType="id int, n int")
    class Count:
        def eval(self, row: Row):
            yield row["id"], 1

    _assert_frame(Count(_emp(spark).asTable()), "astable_udtf_call")


def test_astable_udtf_sql_table_spelling(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-005, C-006 — cell astable_udtf_sql."""
    emp = _emp(spark)
    emp.createOrReplaceTempView("emp")

    @udtf(returnType="id int, n int")
    class Count:
        def eval(self, row: Row):
            yield row["id"], 1

    spark.udtf.register("count_udtf", Count)
    _assert_frame(spark.sql("SELECT * FROM count_udtf(TABLE(emp))"), "astable_udtf_sql")


def test_scalar_sql_door(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-006 — cell scalar_sql."""
    _emp(spark).createOrReplaceTempView("emp")
    _dept(spark).createOrReplaceTempView("dept")
    _assert_frame(
        spark.sql("SELECT id, (SELECT max(budget) FROM dept) mb FROM emp"),
        "scalar_sql",
    )


def test_exists_sql_door(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-006 — cell exists_sql."""
    _emp(spark).createOrReplaceTempView("emp")
    _dept(spark).createOrReplaceTempView("dept")
    _assert_frame(
        spark.sql("SELECT * FROM emp e WHERE EXISTS (SELECT 1 FROM dept d WHERE d.dept = e.dept)"),
        "exists_sql",
    )


def test_lateral_sql_door(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-006 — cell lateral_sql."""
    _emp(spark).createOrReplaceTempView("emp")
    _dept(spark).createOrReplaceTempView("dept")
    _assert_frame(
        spark.sql("SELECT * FROM emp e, LATERAL (SELECT budget FROM dept d WHERE d.dept = e.dept)"),
        "lateral_sql",
    )


def test_exists_sql_door_in_projection(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-006 — the card's EXISTS-in-projection gap answers through the door.

    The qualified correlation keeps ids 1 and 3 True — the same rows `exists_sql` returns
    through the filter — with the field named `e` and non-nullable, matching the
    `exists_in_select` cell's shape.
    """
    _emp(spark).createOrReplaceTempView("emp")
    _dept(spark).createOrReplaceTempView("dept")
    frame = spark.sql(
        "SELECT id, EXISTS(SELECT 1 FROM dept d WHERE d.dept = e.dept) AS e FROM emp e"
    )
    assert frame.columns == ["id", "e"]
    assert frame.schema.simpleString() == "struct<id:int,e:boolean>"
    assert [field.nullable for field in frame.schema.fields] == [True, False]
    assert sorted(repr(tuple(row)) for row in frame.collect()) == [
        "(1, True)",
        "(2, False)",
        "(3, True)",
        "(4, False)",
    ]


def test_lateral_sql_door_outer_ref_in_select(spark: ReparkSession) -> None:
    """pins: df-subquery-1/C-006 — the card's LATERAL outer-ref-in-SELECT gap answers.

    The SQL spelling answers the `lateral_outer_expr` cell's rows exactly.
    """
    _emp(spark).createOrReplaceTempView("emp")
    _assert_frame(
        spark.sql("SELECT * FROM emp e, LATERAL (SELECT e.sal * 2 AS dbl)"),
        "lateral_outer_expr",
    )
