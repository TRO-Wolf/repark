from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark import functions as sf
from repark._native import PyReparkSession
from repark.spark.session import _reset_active_session_for_tests

V = "VALUES (CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE))"
Z = "CAST('1e-20' AS DOUBLE)"
PROBES = {
    "tiny": (Z, True, True),
    "zero": ("CAST(0.0 AS DOUBLE)", False, False),
    "negzero": ("CAST('-0.0' AS DOUBLE)", False, False),
    "dec01": ("CAST(0.1 AS DOUBLE)", True, False),
}
PREDICATES = {
    "in": ("{z} IN (SELECT c FROM " + V + " AS t(c))", False),
    "notin": ("{z} NOT IN (SELECT c FROM " + V + " AS t(c))", True),
    "exists": ("EXISTS (SELECT 1 FROM " + V + " AS t(c) WHERE t.c = {z})", False),
    "notexists": ("NOT EXISTS (SELECT 1 FROM " + V + " AS t(c) WHERE t.c = {z})", True),
    "scalar": ("{z} = (SELECT min(c) FROM " + V + " AS t(c))", False),
}
SELECT_LIST_IN = "Physical plan does not support logical expression InSubquery"
SEATS: dict[str, Callable[[Any, str], Any]] = {
    "fexpr_filter": lambda frame, text: frame.filter(sf.expr(text)),
    "fexpr_select": lambda frame, text: frame.select(sf.expr(text).alias("p")),
    "fexpr_withcolumn": lambda frame, text: frame.withColumn("p", sf.expr(text)),
    "selectexpr": lambda frame, text: frame.selectExpr(text + " AS p"),
    "filter_str": lambda frame, text: frame.filter(text),
}
LATERAL = {
    "comma": (
        f"SELECT t.c, l.x FROM {V} AS t(c), LATERAL (SELECT t.c + 1 AS x) l",
        [["1e-20", "1.0"], ["0.1", "1.1"]],
    ),
    "join_lateral": (
        f"SELECT t.c, l.x FROM {V} AS t(c) JOIN LATERAL (SELECT t.c + 1 AS x) l",
        [["1e-20", "1.0"], ["0.1", "1.1"]],
    ),
    "left_join_lateral": (
        f"SELECT t.c, l.x FROM {V} AS t(c) LEFT JOIN LATERAL (SELECT t.c + 1 AS x) l ON true",
        [["1e-20", "1.0"], ["0.1", "1.1"]],
    ),
    "lateral_where": (
        f"SELECT t.c, l.y FROM {V} AS t(c), LATERAL (SELECT u.y FROM VALUES (0.05), (0.5) AS "
        "u(y) WHERE u.y > t.c) l",
        [["1e-20", "0.05"], ["1e-20", "0.50"], ["0.1", "0.50"]],
    ),
    "lateral_two_cols": (
        f"SELECT t.id, t.c, l.x FROM VALUES (1, CAST(0.1 AS DECIMAL(10,2))), (2, {Z}) AS t(id, c), "
        "LATERAL (SELECT t.c + t.id AS x) l",
        [["1", "0.1", "1.1"], ["2", "1e-20", "2.0"]],
    ),
    "lateral_under_union": (
        f"SELECT 0.5 AS c, 1.5 AS x UNION ALL SELECT t.c, l.x FROM {V} AS t(c), "
        "LATERAL (SELECT t.c + 1 AS x) l",
        [["1e-20", "1.0"], ["0.1", "1.1"], ["0.5", "1.5"]],
    ),
    "scalar_select_count": (
        f"SELECT t.c, (SELECT count(*) FROM VALUES ({Z}), (0.1) AS u(y) WHERE u.y = t.c) AS m "
        f"FROM {V} AS t(c)",
        [["1e-20", "1"], ["0.1", "1"]],
    ),
    "exists_where": (
        f"SELECT t.c FROM {V} AS t(c) WHERE EXISTS (SELECT 1 FROM VALUES ({Z}) AS u(y) "
        "WHERE u.y = t.c)",
        [["1e-20"]],
    ),
    "not_exists_where": (
        f"SELECT t.c FROM {V} AS t(c) WHERE NOT EXISTS (SELECT 1 FROM VALUES ({Z}) AS u(y) "
        "WHERE u.y = t.c)",
        [["0.1"]],
    ),
    "in_where_correlated": (
        f"SELECT t.c FROM {V} AS t(c) WHERE t.c IN (SELECT u.y FROM VALUES ({Z}), "
        "(CAST(0.1 AS DOUBLE)) AS u(y) WHERE u.y >= t.c)",
        [["1e-20"], ["0.1"]],
    ),
    "exists_in_having": (
        f"SELECT t.c, count(*) AS n FROM {V} AS t(c) GROUP BY t.c HAVING EXISTS (SELECT 1 FROM "
        f"VALUES ({Z}) AS u(y) WHERE u.y = t.c)",
        [["1e-20", "1"]],
    ),
}
STRAY = {
    "plain": "SELECT __repark_values_cell(1)",
    "double": f"SELECT __repark_values_cell({Z})",
    "upper": "SELECT __REPARK_VALUES_CELL(1)",
    "mixed_case": "SELECT __Repark_Values_Cell(1)",
    "backquoted": "SELECT `__repark_values_cell`(1)",
    "nested": "SELECT __repark_values_cell(__repark_values_cell(0.1))",
    "nested_fn": "SELECT ABS(__repark_values_cell(-1.5)) AS a",
    "zero_arg": "SELECT __repark_values_cell()",
    "two_arg": "SELECT __repark_values_cell(1, 2)",
    "order_by": "SELECT 1 AS x ORDER BY __repark_values_cell(1)",
    "where": "SELECT 1 AS id WHERE __repark_values_cell(1.5) > 1.0",
    "having": "SELECT count(*) AS n FROM (SELECT 1 AS a) GROUP BY a "
    "HAVING __repark_values_cell(count(*)) > 0",
    "in_cell": f"SELECT c FROM VALUES (__repark_values_cell(0.1)), ({Z}) AS t(c)",
    "in_cell_plain": "SELECT c FROM (VALUES (__repark_values_cell(1.0)), (2.0)) AS t(c)",
    "in_cell_nested": f"SELECT c FROM VALUES (ABS(__repark_values_cell(0.1))), ({Z}) AS t(c)",
    "in_one_row": "SELECT c FROM VALUES (__repark_values_cell(0.1)) AS t(c)",
    "beside_values": f"SELECT __repark_values_cell(1) AS x, c FROM {V} AS t(c)",
    "values_where": f"SELECT c FROM {V} AS t(c) WHERE c > __repark_values_cell(0)",
    "values_having": f"SELECT c FROM {V} AS t(c) GROUP BY c "
    "HAVING __repark_values_cell(COUNT(*)) > 0",
    "subquery": f"SELECT 1 AS id WHERE {Z} IN (SELECT __repark_values_cell(c) FROM {V} AS t(c))",
    "cte": "WITH w AS (SELECT __repark_values_cell(1) AS x) SELECT x FROM w",
}
STRAY_FRAGMENTS: dict[str, Callable[[Any], Any]] = {
    "fexpr_filter": lambda frame: frame.filter(sf.expr("__repark_values_cell(1) = 1")),
    "fexpr_select": lambda frame: frame.select(sf.expr("__repark_values_cell(1)").alias("x")),
    "fexpr_in_cell": lambda frame: frame.filter(
        sf.expr(f"{Z} IN (SELECT c FROM VALUES (__repark_values_cell(0.1)), ({Z}) AS t(c))")
    ),
    "selectexpr": lambda frame: frame.selectExpr("__repark_values_cell(id) AS x"),
    "filter_str": lambda frame: frame.filter("__repark_values_cell(id) = 1"),
    "withcolumn_upper": lambda frame: frame.withColumn("x", sf.expr("__REPARK_VALUES_CELL(id)")),
}


@pytest.fixture(params=["true", "false"])
def spark(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-decimal-double-coercion-2-delta")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(tmp_path / "wh"))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    yield session
    session.stop()


@pytest.fixture(params=["true", "false"])
def native(request: pytest.FixtureRequest) -> Iterator[Any]:
    import repark

    session = PyReparkSession.native(config={"spark.sql.ansi.enabled": request.param})
    previous = getattr(repark, "_ANSI_NATIVE", None)
    repark._ANSI_NATIVE = session
    try:
        yield repark
    finally:
        repark._ANSI_NATIVE = previous


def shown(frame: Any) -> list[list[str]]:
    return [[str(value) for value in row] for row in frame.collect()]


def test_a_literal_probe_through_fexpr_over_a_mixed_values_answers_as_spark(spark: Any) -> None:
    bare = spark.sql("SELECT 1 AS id")
    assert shown(bare.filter(sf.expr(f"{Z} IN (SELECT c FROM {V} AS t(c))"))) == [["1"]]
    exists = f"EXISTS (SELECT 1 FROM {V} AS t(c) WHERE t.c = {Z})"
    assert shown(bare.filter(sf.expr(exists))) == [["1"]]


def test_the_selectexpr_and_scalar_twins_of_the_literal_probe(spark: Any) -> None:
    bare = spark.sql("SELECT 1 AS id")
    exists = f"EXISTS (SELECT 1 FROM {V} AS t(c) WHERE t.c = {Z})"
    assert shown(bare.selectExpr(exists + " AS p")) == [["True"]]
    with pytest.raises(Exception, match=SELECT_LIST_IN):
        bare.selectExpr(f"{Z} IN (SELECT c FROM {V} AS t(c)) AS p").collect()
    scalar = sf.expr(f"(SELECT min(c) FROM {V} AS t(c))")
    assert shown(bare.select(scalar.alias("m"))) == [["1e-20"]]


def family_cells() -> list[str]:
    return [f"{p}/{z}/{seat}" for p in PREDICATES for z in PROBES for seat in SEATS]


@pytest.mark.parametrize("cell", family_cells())
def test_every_fragment_seat_over_a_mixed_values_answers_as_spark(spark: Any, cell: str) -> None:
    predicate, probe, seat = cell.split("/")
    template, negated = PREDICATES[predicate]
    text_z, member, is_min = PROBES[probe]
    truth = (is_min if predicate == "scalar" else member) != negated
    frame = SEATS[seat](spark.sql("SELECT 1 AS id"), template.format(z=text_z))
    if predicate in ("in", "notin") and seat not in ("fexpr_filter", "filter_str"):
        with pytest.raises(Exception, match=SELECT_LIST_IN):
            frame.collect()
        return
    if seat in ("fexpr_filter", "filter_str"):
        assert shown(frame) == ([["1"]] if truth else [])
    elif seat == "fexpr_withcolumn":
        assert shown(frame) == [["1", str(truth)]]
    else:
        assert shown(frame) == [[str(truth)]]


@pytest.mark.parametrize("cell", sorted(LATERAL))
def test_an_outer_reference_to_a_mixed_values_column_answers_as_spark(
    spark: Any, cell: str
) -> None:
    sql, rows = LATERAL[cell]
    frame = spark.sql(sql)
    assert sorted(shown(frame)) == sorted(rows)
    assert "double" in [kind for _, kind in frame.dtypes]


def stray_error(sql_or_name: str) -> str:
    return r"\[UNRESOLVED_ROUTINE\] Cannot resolve routine `" + sql_or_name + "`"


@pytest.mark.parametrize("cell", sorted(STRAY))
def test_a_user_written_values_marker_is_an_unresolved_routine_on_the_spark_door(
    spark: Any, cell: str
) -> None:
    named = "__REPARK_VALUES_CELL" if cell == "upper" else "__repark_values_cell"
    named = "__Repark_Values_Cell" if cell == "mixed_case" else named
    with pytest.raises(Exception, match=stray_error(named)):
        spark.sql(STRAY[cell]).collect()


@pytest.mark.parametrize("cell", sorted(STRAY))
def test_a_user_written_values_marker_is_an_unresolved_routine_on_the_native_door(
    native: Any, cell: str
) -> None:
    named = "__REPARK_VALUES_CELL" if cell == "upper" else "__repark_values_cell"
    named = "__Repark_Values_Cell" if cell == "mixed_case" else named
    with pytest.raises(Exception, match=stray_error(named)):
        native.sql(STRAY[cell]).collect()


@pytest.mark.parametrize("cell", sorted(STRAY_FRAGMENTS))
def test_a_user_written_values_marker_in_a_fragment_is_an_unresolved_routine(
    spark: Any, native: Any, cell: str
) -> None:
    named = "__REPARK_VALUES_CELL" if cell.endswith("_upper") else "__repark_values_cell"
    for frame in (spark.sql("SELECT 1 AS id"), native.sql("SELECT 1 AS id")):
        with pytest.raises(Exception, match=stray_error(named)):
            STRAY_FRAGMENTS[cell](frame).collect()


def test_a_user_written_values_marker_in_an_insert_writes_nothing(spark: Any) -> None:
    spark.sql("CREATE TABLE ice.ns.stray (c DOUBLE) USING iceberg").collect()
    with pytest.raises(Exception, match=stray_error("__repark_values_cell")):
        spark.sql("INSERT INTO ice.ns.stray VALUES (__repark_values_cell(0.1)), (0.2)").collect()
    assert spark.sql("SELECT c FROM ice.ns.stray").collect() == []
