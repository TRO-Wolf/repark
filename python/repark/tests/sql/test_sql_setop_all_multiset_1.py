from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _setop_all_multiset_1_cells import (
    ANSI_MODES,
    CONTROL_OPS,
    DATAFRAME_METHODS,
    EDGE_CELLS,
    ERROR_CELLS,
    FIXTURE,
    GUARD_JOINS,
    GUARD_KINDS,
    OPS,
    SHAPES,
    SPELLINGS,
    TYPE_CELLS,
    WRITE_SHAPES,
    Door,
    cold_read,
    measure,
)

import repark
from repark import ReparkSession
from repark._native import PyReparkSession
from repark.errors import AnalysisException, ParseException, UnsupportedOperationException
from repark.spark import functions as spark_functions
from repark.spark.session import _reset_active_session_for_tests

ORACLE: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
NATIVE_GAPS = {
    "shape|persistent-view": "NATIVE-VIEW-ICEBERG",
    "shape|ctas": "NATIVE-CTAS-ICEBERG-REFUSE-1",
    "shape|merge": "NATIVE-NO-MERGE",
}
NATIVE_MINUS_SPELLINGS = {index for index, text in enumerate(SPELLINGS) if "minus" in text.lower()}
NATIVE_MINUS_TEXT = "MINUS ALL not implemented"
ERROR_STATES = {"column-count": "42826", "column-type": "42825"}
ERROR_CONDITIONS = {
    "column-count": "NUM_COLUMNS_MISMATCH",
    "column-type": "INCOMPATIBLE_COLUMN_TYPE",
}


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp("setop-all-multiset")
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("sql-setop-all-multiset-1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(root / "spark"))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    door = Door("spark-door", root / "spark")
    door.session = session
    found: dict[str, Any] = {}
    for ansi in ANSI_MODES:
        session.conf.set("spark.sql.ansi.enabled", ansi)
        found[ansi] = measure(door, ansi)
    session.conf.set("spark.sql.ansi.enabled", "true")
    with pytest.MonkeyPatch.context() as patch:
        native = PyReparkSession.native()
        native.register_memory_catalog("ice", str(root / "native"))
        native.create_namespace("ice", "ns", str(root / "native" / "ns"))
        patch.setattr(repark, "_ANSI_NATIVE", native)
        native_door = Door("native", root / "native")
        native_door.session = session
        found["native"] = measure(native_door, "native")
    session.stop()
    _reset_active_session_for_tests()
    cold = (
        ReparkSession.builder.appName("sql-setop-all-multiset-1-cold")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    for key, base in (("true", "spark"), ("false", "spark"), ("native", "native")):

        def read(table: str, base: str = base) -> list[Any]:
            frame = cold.read.format("iceberg").load(str(root / base / "ns" / table))
            return [tuple(row.values()) for row in frame.toArrow().to_pylist()]

        cold_read(door, found[key], read)
    yield found
    cold.stop()
    _reset_active_session_for_tests()


def spark_cell(ansi: str, key: str) -> dict[str, Any]:
    return ORACLE["cells"]["true" if ansi == "native" else ansi][key]


def native_minus(mode: str, op: str) -> bool:
    return mode == "native" and op == "MINUS ALL"


def assert_native_minus_refused(cell: dict[str, Any]) -> None:
    assert cell["kind"] == "error", cell
    assert cell["class"] == "UnsupportedOperationException", cell
    assert NATIVE_MINUS_TEXT in cell["text"], cell


def rows_only(cell: dict[str, Any]) -> dict[str, Any]:
    return {"kind": cell["kind"], "rows": cell.get("rows")}


def mode_ids(modes: tuple[str, ...]) -> list[pytest.ParamSet]:
    return [
        pytest.param(mode, id=f"ansi-{mode}" if mode != "native" else "native") for mode in modes
    ]


DOOR_MODES = (*ANSI_MODES, "native")


def test_the_fixture_holds_every_cell() -> None:
    assert ORACLE["spark"] == "4.1.2"
    for ansi in ANSI_MODES:
        cells = ORACLE["cells"][ansi]
        for cell in TYPE_CELLS:
            for op in OPS:
                assert cells[f"type|{cell}|{op}|lr"]["kind"] == "rows"
                assert cells[f"type|{cell}|{op}|rl"]["kind"] == "rows"
        for shape in SHAPES:
            for op in OPS:
                assert f"shape|{shape}|{op}" in cells


@pytest.mark.parametrize("mode", mode_ids(DOOR_MODES))
@pytest.mark.parametrize("direction", ["lr", "rl"])
@pytest.mark.parametrize("op", OPS)
@pytest.mark.parametrize("cell", list(TYPE_CELLS))
def test_every_type_answers_the_spark_multiset(
    measured: dict[str, Any], mode: str, direction: str, op: str, cell: str
) -> None:
    key = f"type|{cell}|{op}|{direction}"
    got = measured[mode][key]
    if native_minus(mode, op):
        assert_native_minus_refused(got)
        return
    expected = spark_cell(mode, key)
    assert rows_only(got) == rows_only(expected), got
    if mode != "native":
        assert got["type"] == expected["type"], got


@pytest.mark.parametrize("mode", mode_ids(DOOR_MODES))
@pytest.mark.parametrize("op", OPS)
@pytest.mark.parametrize("cell", list(EDGE_CELLS))
def test_empty_sides_and_duplicate_only_sides_answer_the_spark_multiset(
    measured: dict[str, Any], mode: str, op: str, cell: str
) -> None:
    key = f"edge|{cell}|{op}|lr"
    if native_minus(mode, op):
        assert_native_minus_refused(measured[mode][key])
        return
    assert rows_only(measured[mode][key]) == rows_only(spark_cell(mode, key))


@pytest.mark.parametrize("mode", mode_ids(DOOR_MODES))
@pytest.mark.parametrize("op", OPS)
@pytest.mark.parametrize("cell", list(ERROR_CELLS))
def test_a_column_count_or_type_mismatch_raises_the_spark_class_with_the_declared_residue(
    measured: dict[str, Any], mode: str, op: str, cell: str
) -> None:
    got = measured[mode][f"error|{cell}|{op}"]
    expected = spark_cell(mode, f"error|{cell}|{op}")
    assert expected["class"] == "AnalysisException"
    assert expected["condition"] == ERROR_CONDITIONS[cell]
    assert expected["sqlstate"] == ERROR_STATES[cell]
    if native_minus(mode, op) and cell == "column-type":
        assert_native_minus_refused(got)
        return
    assert got["kind"] == "error", got
    assert got["class"] == "AnalysisException", got
    assert "SQL-SETOP-ALL" not in got["text"], got
    assert got["bracket"] is None, got


@pytest.mark.parametrize("mode", mode_ids(DOOR_MODES))
@pytest.mark.parametrize("op", OPS)
@pytest.mark.parametrize("shape", SHAPES)
def test_every_shape_answers_the_spark_rows(
    measured: dict[str, Any], mode: str, op: str, shape: str
) -> None:
    key = f"shape|{shape}|{op}"
    got = measured[mode][key]
    expected = spark_cell(mode, key)
    if mode == "native" and f"shape|{shape}" in NATIVE_GAPS:
        assert "SQL-SETOP-ALL" not in json.dumps(got), got
        return
    if native_minus(mode, op):
        if shape in WRITE_SHAPES:
            assert_native_minus_refused(got["statement"])
            assert got["hot"]["rows"] == [], got
            assert got["cold"]["rows"] == [], got
        elif shape == "temp-view":
            assert got["create"] == "error", got
            assert_native_minus_refused(got["create_result"])
        else:
            assert_native_minus_refused(got)
        return
    if shape == "explain":
        assert got == {"kind": "rows"}, got
        return
    if shape in WRITE_SHAPES:
        assert got["kind"] == "ok", got
        assert rows_only(got["hot"]) == rows_only(expected["hot"]), got
        assert got["cold"] == expected["cold"], got
        return
    assert rows_only(got) == rows_only(expected), got


@pytest.mark.parametrize("mode", mode_ids(DOOR_MODES))
@pytest.mark.parametrize("index", range(len(SPELLINGS)))
def test_every_spelling_answers_the_spark_rows(
    measured: dict[str, Any], mode: str, index: int
) -> None:
    key = f"spelling|{index}"
    if mode == "native" and index in NATIVE_MINUS_SPELLINGS:
        assert_native_minus_refused(measured[mode][key])
        return
    assert rows_only(measured[mode][key]) == rows_only(spark_cell(mode, key))


@pytest.mark.parametrize("mode", mode_ids(DOOR_MODES))
@pytest.mark.parametrize("op", CONTROL_OPS)
def test_the_distinct_and_union_controls_keep_the_spark_rows(
    measured: dict[str, Any], mode: str, op: str
) -> None:
    key = f"control|{op}"
    assert rows_only(measured[mode][key]) == rows_only(spark_cell(mode, key))


@pytest.mark.parametrize("mode", mode_ids(ANSI_MODES))
@pytest.mark.parametrize("op", OPS)
@pytest.mark.parametrize(
    "fragment", ["fexpr-scalar", "fexpr-filter-in", "fexpr-filter-col", "selectexpr-scalar"]
)
def test_sql_fragments_answer_the_spark_rows(
    measured: dict[str, Any], mode: str, op: str, fragment: str
) -> None:
    key = f"fragment|{fragment}|{op}"
    assert measured[mode][key] == spark_cell(mode, key)


@pytest.mark.parametrize("mode", mode_ids(ANSI_MODES))
@pytest.mark.parametrize("method", DATAFRAME_METHODS)
@pytest.mark.parametrize("cell", list(TYPE_CELLS))
def test_the_dataframe_methods_answer_the_spark_multiset(
    measured: dict[str, Any], mode: str, method: str, cell: str
) -> None:
    key = f"dataframe|{cell}|{method}"
    got = measured[mode][key]
    expected = spark_cell(mode, key)
    assert rows_only(got) == rows_only(expected), got
    assert got["type"] == expected["type"], got


def test_a_stored_view_body_naming_the_operators_answers_at_read(tmp_path: Path) -> None:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("sql-setop-all-multiset-1-view").getOrCreate()
    session.register_memory_catalog("ice", str(tmp_path))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    door = Door("spark-door", tmp_path)
    door.session = session
    door.seed("v")
    tables = "SELECT a FROM ice.ns.vpl {} SELECT a FROM ice.ns.vpr"
    answers = {}
    for name, stored in (
        ("vi", "INTERSECT ALL"),
        ("vm", "MINUS ALL"),
        ("vmc", "MINUS /* gap */ ALL"),
    ):
        session.sql(f"CREATE VIEW ice.ns.{name} AS {tables.format('INTERSECT')}").collect()
        plain = sorted(row[0] for row in session.sql(f"SELECT a FROM ice.ns.{name}").collect())
        for path in (tmp_path / "ns" / name / "metadata").glob("*.metadata.json"):
            text = path.read_text(encoding="utf-8")
            path.write_text(text.replace(" INTERSECT SELECT", f" {stored} SELECT"), "utf-8")
        read = sorted(row[0] for row in session.sql(f"SELECT a FROM ice.ns.{name}").collect())
        answers[name] = (plain, read)
    ident_bodies = (
        ("v_col", "SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) t", [1]),
        (
            "v_tbl",
            "WITH minus AS (SELECT 1 AS a), except AS (SELECT 2 AS a) SELECT a FROM minus all",
            [1],
        ),
        ("v_lit", "SELECT 'minus all' AS s", ["minus all"]),
        ("v_quo", "SELECT `minus` all FROM (SELECT 1 AS minus, 2 AS except) t", [1]),
    )
    for name, body, expected_rows in ident_bodies:
        session.sql(f"CREATE VIEW ice.ns.{name} AS SELECT 9 AS all").collect()
        for path in (tmp_path / "ns" / name / "metadata").glob("*.metadata.json"):
            text = path.read_text(encoding="utf-8")
            path.write_text(text.replace("SELECT 9 AS all", body), "utf-8")
        table = session.sql(f"SELECT * FROM ice.ns.{name}").toArrow()
        got_rows = [next(iter(row.values())) for row in table.to_pylist()]
        assert got_rows == expected_rows, (name, got_rows)
    session.stop()
    _reset_active_session_for_tests()
    assert answers == {
        "vi": ([1, 2], [1, 2, 2]),
        "vm": ([1, 2], [1, 1, 4]),
        "vmc": ([1, 2], [1, 1, 4]),
    }


@pytest.mark.parametrize("mode", mode_ids(ANSI_MODES))
@pytest.mark.parametrize("kind", GUARD_KINDS)
@pytest.mark.parametrize("seat", ["sql", "df"])
@pytest.mark.parametrize("join", list(GUARD_JOINS))
def test_a_null_safe_semi_or_anti_join_keeps_the_left_multiplicity_and_types(
    measured: dict[str, Any], mode: str, kind: str, seat: str, join: str
) -> None:
    key = f"guard|{seat}|{join}|{kind}"
    got = measured[mode][key]
    expected = spark_cell(mode, key)
    assert rows_only(got) == rows_only(expected), got
    assert got["type"] == expected["type"], got


def arrow_rows(frame: Any, is_native: bool = False) -> list[tuple[Any, ...]]:
    table = frame.to_arrow() if is_native else frame.toArrow()
    return [tuple(row.values()) for row in table.to_pylist()]


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_f1_minus_all_respells_only_set_operators_across_seats(ansi: str) -> None:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName(f"setop-f1-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    try:
        statement_cases = (
            ("SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) t", [(1,)], "struct<all:int>"),
            (
                "SELECT t.minus all FROM (SELECT 1 AS minus, 2 AS except) t",
                [(1,)],
                "struct<all:int>",
            ),
            (
                "SELECT minus /* c */ all FROM (SELECT 1 AS minus, 2 AS except) t",
                [(1,)],
                "struct<all:int>",
            ),
            ("SELECT minus all FROM (SELECT 1 AS minus) t", [(1,)], "struct<all:int>"),
            (
                "WITH minus AS (SELECT 1 AS a), except AS (SELECT 2 AS a) SELECT a FROM minus all",
                [(1,)],
                "struct<a:int>",
            ),
            (
                "WITH minus AS (SELECT 1 AS a), except AS (SELECT 2 AS a) "
                "SELECT `all`.a FROM minus all",
                [(1,)],
                "struct<a:int>",
            ),
            ("SELECT 'minus all' AS s", [("minus all",)], "struct<s:string>"),
            (
                "SELECT `minus` all FROM (SELECT 1 AS minus, 2 AS except) t",
                [(1,)],
                "struct<all:int>",
            ),
            (
                "SELECT minus all FROM (SELECT 1 AS minus, 2 AS except UNION ALL SELECT 1, 2) t "
                "MINUS /* gap */ ALL SELECT 1 AS all",
                [(1,)],
                "struct<all:int>",
            ),
            (
                "SELECT 1 AS a UNION ALL SELECT 1\n  MINUS\n\tALL\nSELECT 1 AS a",
                [(1,)],
                "struct<a:int>",
            ),
            (
                "SELECT 1 AS a UNION ALL SELECT 1 MINUS -- gap\n ALL SELECT 1 AS a",
                [(1,)],
                "struct<a:int>",
            ),
        )
        for sql, expected_rows, expected_schema in statement_cases:
            frame = session.sql(sql)
            assert arrow_rows(frame) == expected_rows, sql
            assert frame.schema.simpleString() == expected_schema, sql
        for broken in ("SELECT 1 FROM WHERE MINUS ALL", "SELECT 1 MINUS ALL FROM"):
            with pytest.raises(ParseException):
                session.sql(broken).collect()
        session.sql(
            "CREATE OR REPLACE TEMPORARY VIEW f1_v_col AS "
            "SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) t"
        ).collect()
        assert arrow_rows(session.sql("SELECT * FROM f1_v_col")) == [(1,)]
        session.sql(
            "CREATE OR REPLACE TEMPORARY VIEW f1_v_tbl AS "
            "WITH minus AS (SELECT 1 AS a), except AS (SELECT 2 AS a) SELECT a FROM minus all"
        ).collect()
        assert arrow_rows(session.sql("SELECT * FROM f1_v_tbl")) == [(1,)]
        session.sql(
            "CREATE OR REPLACE TEMPORARY VIEW f1_v_op AS "
            "SELECT 1 AS a UNION ALL SELECT 1 MINUS /* gap */ ALL SELECT 1 AS a"
        ).collect()
        assert arrow_rows(session.sql("SELECT * FROM f1_v_op")) == [(1,)]
        base = session.sql("SELECT a FROM VALUES (1), (2) AS t(a)")
        filter_cases = (
            ("a IN (SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) t)", [(1,)]),
            (
                "a IN (WITH minus AS (SELECT 1 AS a), except AS (SELECT 2 AS a) "
                "SELECT a FROM minus all)",
                [(1,)],
            ),
            ("a IN (SELECT `minus` all FROM (SELECT 1 AS minus, 2 AS except) t)", [(1,)]),
            ("a = 1 AND 'minus all' = 'minus all'", [(1,)]),
            (
                "a IN (SELECT 1 AS a UNION ALL SELECT 1 MINUS /* gap */ ALL SELECT 1 AS a)",
                [(1,)],
            ),
        )
        for predicate, expected_rows in filter_cases:
            assert arrow_rows(base.filter(predicate)) == expected_rows, predicate
            assert arrow_rows(base.filter(spark_functions.expr(predicate))) == expected_rows, (
                predicate
            )
    finally:
        session.stop()
        _reset_active_session_for_tests()


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_f2_untyped_null_left_operand_answers_with_distinct_twin_schema(ansi: str) -> None:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName(f"setop-f2-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    try:
        rhs_cases = (
            "SELECT CAST(1 AS INT) AS a",
            "SELECT CAST(1 AS BIGINT) AS a",
            "SELECT CAST('x' AS STRING) AS a",
            "SELECT CAST('2024-01-01' AS DATE) AS a",
            "SELECT CAST('2024-01-01 00:00:00' AS TIMESTAMP) AS a",
            "SELECT CAST(1.5 AS DOUBLE) AS a",
            "SELECT CAST(1.5 AS FLOAT) AS a",
            "SELECT CAST(1.50 AS DECIMAL(10,2)) AS a",
            "SELECT CAST(true AS BOOLEAN) AS a",
            "SELECT array(CAST(1 AS INT), CAST(2 AS INT)) AS a",
            "SELECT named_struct('x', CAST(1 AS INT)) AS a",
        )
        for rhs in rhs_cases:
            distinct_i = session.sql(f"SELECT NULL AS a INTERSECT {rhs}")
            all_i = session.sql(f"SELECT NULL AS a INTERSECT ALL {rhs}")
            assert arrow_rows(all_i) == [], rhs
            assert all_i.schema.simpleString() == distinct_i.schema.simpleString(), rhs
            distinct_e = session.sql(f"SELECT NULL AS a EXCEPT {rhs}")
            for op in ("EXCEPT ALL", "MINUS ALL"):
                all_e = session.sql(f"SELECT NULL AS a {op} {rhs}")
                assert arrow_rows(all_e) == [(None,)], (op, rhs)
                assert all_e.schema.simpleString() == distinct_e.schema.simpleString(), (op, rhs)
        multi = session.sql(
            "(SELECT CAST(1 AS INT) AS a, NULL AS b UNION ALL SELECT 1, NULL) INTERSECT ALL "
            "(SELECT CAST(1 AS INT) AS a, CAST(NULL AS STRING) AS b)"
        )
        assert arrow_rows(multi) == [(1, None)]
        assert multi.schema.simpleString() == "struct<a:int,b:void>"
        df_i = session.sql("SELECT NULL AS a").intersectAll(
            session.sql("SELECT CAST(1 AS INT) AS a")
        )
        assert arrow_rows(df_i) == []
        assert df_i.schema.simpleString() == "struct<a:void>"
        df_e = session.sql("SELECT NULL AS a").exceptAll(session.sql("SELECT CAST(1 AS INT) AS a"))
        assert arrow_rows(df_e) == [(None,)]
        assert df_e.schema.simpleString() == "struct<a:void>"
    finally:
        session.stop()
        _reset_active_session_for_tests()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(repark, "_ANSI_NATIVE", PyReparkSession.native())
        for rhs in (
            "SELECT CAST(1 AS INT) AS a",
            "SELECT CAST('x' AS VARCHAR) AS a",
            "SELECT make_array(1, 2) AS a",
        ):
            nat_i = repark.sql(f"SELECT NULL AS a INTERSECT ALL {rhs}")
            nat_di = repark.sql(f"SELECT NULL AS a INTERSECT {rhs}")
            assert arrow_rows(nat_i, is_native=True) == []
            assert nat_i.to_arrow().schema == nat_di.to_arrow().schema
            nat_e = repark.sql(f"SELECT NULL AS a EXCEPT ALL {rhs}")
            nat_de = repark.sql(f"SELECT NULL AS a EXCEPT {rhs}")
            assert arrow_rows(nat_e, is_native=True) == [(None,)]
            assert nat_e.to_arrow().schema == nat_de.to_arrow().schema


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_f3_multiset_set_operations_refuse_map_on_either_side_and_nested(ansi: str) -> None:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName(f"setop-f3-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    try:
        map_queries = (
            ("SELECT NULL AS m {op} SELECT map('a', CAST(1 AS INT)) AS m", "m"),
            ("SELECT map('a', CAST(1 AS INT)) AS m {op} SELECT NULL AS m", "m"),
            (
                "SELECT named_struct('m', map('a', CAST(1 AS INT))) AS s {op} "
                "SELECT named_struct('m', map('a', CAST(1 AS INT))) AS s",
                "s",
            ),
            (
                "SELECT NULL AS s {op} SELECT named_struct('m', map('a', CAST(1 AS INT))) AS s",
                "s",
            ),
            (
                "SELECT array(map('a', CAST(1 AS INT))) AS a {op} "
                "SELECT array(map('a', CAST(1 AS INT))) AS a",
                "a",
            ),
            ("SELECT NULL AS a {op} SELECT array(map('a', CAST(1 AS INT))) AS a", "a"),
            (
                "SELECT named_struct('x', array(map('a', CAST(1 AS INT)))) AS s {op} "
                "SELECT named_struct('x', array(map('a', CAST(1 AS INT)))) AS s",
                "s",
            ),
            (
                "SELECT array(named_struct('m', map('a', CAST(1 AS INT)))) AS a {op} "
                "SELECT array(named_struct('m', map('a', CAST(1 AS INT)))) AS a",
                "a",
            ),
        )
        for op in OPS:
            for template, column in map_queries:
                sql = template.format(op=op)
                with pytest.raises(AnalysisException) as caught:
                    session.sql(sql).collect()
                text = str(caught.value)
                assert "[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE]" in text, (sql, text)
                assert "SQLSTATE: 0A000" in text, (sql, text)
                assert f"column `{column}`" in text, (sql, text)
        with pytest.raises(AnalysisException) as caught:
            session.sql("SELECT NULL AS m").intersectAll(
                session.sql("SELECT map('a', CAST(1 AS INT)) AS m")
            ).collect()
        assert "[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE]" in str(caught.value)
        with pytest.raises(AnalysisException) as caught:
            session.sql("SELECT named_struct('m', map('a', CAST(1 AS INT))) AS s").exceptAll(
                session.sql("SELECT named_struct('m', map('a', CAST(1 AS INT))) AS s")
            ).collect()
        assert "[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE]" in str(caught.value)
        assert arrow_rows(
            session.sql("SELECT NULL AS m EXCEPT SELECT map('a', CAST(1 AS INT)) AS m")
        ) == [(None,)]
        assert (
            arrow_rows(
                session.sql("SELECT NULL AS m INTERSECT SELECT map('a', CAST(1 AS INT)) AS m")
            )
            == []
        )
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_f4_fexpr_is_spark_grammar_on_native_frames_while_filter_str_refuses() -> None:
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(repark, "_ANSI_NATIVE", PyReparkSession.native())
        nat_df = repark.sql("SELECT a FROM (VALUES (1), (2), (3), (4)) AS t(a)")
        filtered = nat_df.filter(
            spark_functions.expr("a IN (SELECT 1 AS a UNION ALL SELECT 1 MINUS ALL SELECT 1 AS a)")
        )
        assert arrow_rows(filtered, is_native=True) == [(1,)]
        projected = nat_df.select(
            spark_functions.expr(
                "(SELECT count(*) FROM (SELECT 1 AS a UNION ALL SELECT 1 MINUS ALL "
                "SELECT 1 AS a) x)"
            ).alias("c")
        )
        assert arrow_rows(projected, is_native=True) == [(1,), (1,), (1,), (1,)]
        with pytest.raises(UnsupportedOperationException) as caught:
            nat_df.filter(
                "a IN (SELECT 1 AS a UNION ALL SELECT 1 MINUS ALL SELECT 1 AS a)"
            ).to_arrow()
        assert NATIVE_MINUS_TEXT in str(caught.value)
        with pytest.raises(UnsupportedOperationException) as caught:
            nat_df.selectExpr(
                "(SELECT count(*) FROM (SELECT 1 AS a UNION ALL SELECT 1 MINUS ALL "
                "SELECT 1 AS a) x) AS c"
            ).to_arrow()
        assert NATIVE_MINUS_TEXT in str(caught.value)


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_f5_lossy_mixed_type_coercion_refuses_before_write_while_exact_widenings_answer(
    ansi: str, tmp_path: Path
) -> None:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName(f"setop-f5-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("ice", str(tmp_path))
        session.sql("CREATE NAMESPACE ice.ns").collect()
        session.sql("CREATE TABLE ice.ns.t (a STRING) USING iceberg").collect()
        refused_pairs = (
            ("SELECT '01' AS a UNION ALL SELECT '01'", "SELECT CAST(1 AS TINYINT) AS a"),
            ("SELECT '01' AS a UNION ALL SELECT '01'", "SELECT CAST(1 AS SMALLINT) AS a"),
            ("SELECT '01' AS a UNION ALL SELECT '01'", "SELECT CAST(1 AS INT) AS a"),
            ("SELECT '01' AS a UNION ALL SELECT '01'", "SELECT CAST(1 AS BIGINT) AS a"),
            ("SELECT '1.00' AS a UNION ALL SELECT '1.00'", "SELECT CAST(1.0 AS FLOAT) AS a"),
            ("SELECT '1.00' AS a UNION ALL SELECT '1.00'", "SELECT CAST(1.0 AS DOUBLE) AS a"),
            (
                "SELECT '1.00' AS a UNION ALL SELECT '1.00'",
                "SELECT CAST(1.00 AS DECIMAL(10,2)) AS a",
            ),
            (
                "SELECT '2024-01-01' AS a UNION ALL SELECT '2024-01-01'",
                "SELECT CAST('2024-01-01' AS DATE) AS a",
            ),
            (
                "SELECT '2024-01-01 00:00:00' AS a UNION ALL SELECT '2024-01-01 00:00:00'",
                "SELECT CAST('2024-01-01 00:00:00' AS TIMESTAMP) AS a",
            ),
            (
                "SELECT CAST(16777217 AS INT) AS a UNION ALL SELECT CAST(16777217 AS INT)",
                "SELECT CAST(16777216.0 AS FLOAT) AS a",
            ),
            (
                "SELECT CAST(9007199254740993 AS BIGINT) AS a UNION ALL "
                "SELECT CAST(9007199254740993 AS BIGINT)",
                "SELECT CAST(9007199254740992.0 AS DOUBLE) AS a",
            ),
            (
                "SELECT CAST('2024-01-01' AS DATE) AS a UNION ALL "
                "SELECT CAST('2024-01-01' AS DATE)",
                "SELECT CAST('2024-01-01 00:00:00' AS TIMESTAMP) AS a",
            ),
            (
                "SELECT named_struct('x', '01') AS a UNION ALL SELECT named_struct('x', '01')",
                "SELECT named_struct('x', CAST(1 AS INT)) AS a",
            ),
            (
                "SELECT array('01') AS a UNION ALL SELECT array('01')",
                "SELECT array(CAST(1 AS INT)) AS a",
            ),
        )
        for lhs, rhs in refused_pairs:
            for op in OPS:
                sql = f"({lhs}) {op} ({rhs})"
                with pytest.raises(AnalysisException) as caught:
                    session.sql(sql).collect()
                assert "SETOP-MIXED-TYPE-COERCION-1" in str(caught.value), (sql, caught.value)
        with pytest.raises(AnalysisException) as caught:
            session.sql("SELECT '01' AS a UNION ALL SELECT '01'").exceptAll(
                session.sql("SELECT CAST(1 AS INT) AS a")
            ).collect()
        assert "SETOP-MIXED-TYPE-COERCION-1" in str(caught.value)
        with pytest.raises(AnalysisException) as caught:
            session.sql(
                "INSERT INTO ice.ns.t "
                "(SELECT '01' AS a UNION ALL SELECT '01') EXCEPT ALL SELECT CAST(1 AS INT) AS a"
            ).collect()
        assert "SETOP-MIXED-TYPE-COERCION-1" in str(caught.value)
        assert arrow_rows(session.sql("SELECT a FROM ice.ns.t")) == []
        allowed_cases = (
            (
                "(SELECT CAST(1 AS INT) AS a UNION ALL SELECT CAST(1 AS INT)) EXCEPT ALL "
                "(SELECT CAST(1 AS BIGINT) AS a UNION ALL SELECT CAST(5000000000 AS BIGINT))",
                [(1,)],
                "struct<a:int>",
            ),
            (
                "(SELECT CAST(1 AS BIGINT) AS a UNION ALL SELECT CAST(1 AS BIGINT)) EXCEPT ALL "
                "(SELECT CAST(1 AS INT) AS a)",
                [(1,)],
                "struct<a:bigint>",
            ),
            (
                "(SELECT CAST(1.5 AS FLOAT) AS a UNION ALL SELECT CAST(1.5 AS FLOAT)) EXCEPT ALL "
                "(SELECT CAST(1.5 AS DOUBLE) AS a)",
                [(1.5,)],
                "struct<a:float>",
            ),
            (
                "(SELECT CAST(1.5 AS DOUBLE) AS a UNION ALL SELECT CAST(1.5 AS DOUBLE)) EXCEPT ALL "
                "(SELECT CAST(1.5 AS FLOAT) AS a)",
                [(1.5,)],
                "struct<a:double>",
            ),
            (
                "(SELECT CAST(1.50 AS DECIMAL(10,2)) AS a UNION ALL "
                "SELECT CAST(1.50 AS DECIMAL(10,2))) EXCEPT ALL (SELECT CAST(1.5 AS DOUBLE) AS a)",
                [(1.5,)],
                "struct<a:double>",
            ),
            (
                "(SELECT CAST(1.5 AS DOUBLE) AS a UNION ALL SELECT CAST(1.5 AS DOUBLE)) EXCEPT ALL "
                "(SELECT CAST(1.50 AS DECIMAL(10,2)) AS a)",
                [(1.5,)],
                "struct<a:double>",
            ),
        )
        for sql, expected_rows, expected_schema in allowed_cases:
            frame = session.sql(sql)
            assert arrow_rows(frame) == expected_rows, sql
            assert frame.schema.simpleString() == expected_schema, sql
        assert (
            arrow_rows(session.sql("(SELECT '01' AS a) EXCEPT (SELECT CAST(1 AS INT) AS a)")) == []
        )
    finally:
        session.stop()
        _reset_active_session_for_tests()


def cold_table_rows(
    warehouse: Path, tables: tuple[str, ...], ansi: str
) -> dict[str, list[tuple[Any, ...]]]:
    _reset_active_session_for_tests()
    cold = (
        ReparkSession.builder.appName(f"sql-setop-cold-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    try:
        return {
            table: sorted(
                arrow_rows(cold.read.format("iceberg").load(str(warehouse / "ns" / table)))
            )
            for table in tables
        }
    finally:
        cold.stop()
        _reset_active_session_for_tests()


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_r1_minus_all_runs_after_spark_normalizers_across_seats(tmp_path: Path, ansi: str) -> None:
    from decimal import Decimal

    _reset_active_session_for_tests()
    warehouse = tmp_path / f"r1-{ansi}"
    session = (
        ReparkSession.builder.appName(f"sql-setop-r1-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("ice", str(warehouse))
        session.sql("CREATE NAMESPACE ice.ns").collect()
        session.sql("CREATE TABLE ice.ns.base_t (a INT) USING iceberg").collect()
        session.sql("INSERT INTO ice.ns.base_t VALUES (1), (1), (2), (3)").collect()
        session.sql("INSERT INTO ice.ns.base_t VALUES (4)").collect()
        snaps = [
            row[0]
            for row in session.sql(
                "SELECT snapshot_id FROM ice.ns.base_t.snapshots "
                "ORDER BY parent_id NULLS FIRST, committed_at"
            ).collect()
        ]
        snap1, snap2 = snaps[0], snaps[-1]
        queries = (
            (
                f"SELECT a FROM ice.ns.base_t FOR VERSION AS OF {snap2} "
                f"MINUS ALL SELECT a FROM ice.ns.base_t FOR VERSION AS OF {snap1}",
                [(4,)],
            ),
            (
                r"SELECT r'a\b' AS a UNION ALL SELECT r'a\b' MINUS ALL SELECT r'a\b' AS a",
                [(r"a\b",)],
            ),
            (
                r"SELECT R'a\b' AS a UNION ALL SELECT R'a\b' MINUS ALL SELECT R'a\b' AS a",
                [(r"a\b",)],
            ),
            (
                "SELECT 1e2BD AS a UNION ALL SELECT 1e2BD MINUS ALL SELECT 100BD AS a",
                [(Decimal("100"),)],
            ),
            (
                r"SELECT 'a\'b' AS a UNION ALL SELECT 'a\'b' MINUS ALL SELECT 'a\'b' AS a",
                [("a'b",)],
            ),
            (
                "SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) t",
                [(1,)],
            ),
        )
        for sql, expected in queries:
            assert arrow_rows(session.sql(sql)) == expected, sql
        session.sql(
            r"CREATE TABLE ice.ns.t_r1_raw USING iceberg AS "
            r"SELECT r'a\b' AS a UNION ALL SELECT r'a\b' MINUS ALL SELECT r'a\b' AS a"
        ).collect()
        assert arrow_rows(session.sql("SELECT a FROM ice.ns.t_r1_raw")) == [(r"a\b",)]
        session.sql("CREATE TABLE ice.ns.m_r1_raw (a STRING) USING iceberg").collect()
        session.sql("INSERT INTO ice.ns.m_r1_raw VALUES ('z')").collect()
        session.sql(
            r"MERGE INTO ice.ns.m_r1_raw t USING ("
            r"SELECT r'a\b' AS a UNION ALL SELECT r'a\b' MINUS ALL SELECT r'a\b' AS a"
            r") s ON t.a = s.a WHEN NOT MATCHED THEN INSERT (a) VALUES (s.a)"
        ).collect()
        assert sorted(arrow_rows(session.sql("SELECT a FROM ice.ns.m_r1_raw"))) == [
            (r"a\b",),
            ("z",),
        ]
        session.sql(
            r"CREATE OR REPLACE TEMPORARY VIEW v_r1_raw AS "
            r"SELECT r'a\b' AS a UNION ALL SELECT r'a\b' MINUS ALL SELECT r'a\b' AS a"
        ).collect()
        assert arrow_rows(session.sql("SELECT a FROM v_r1_raw")) == [(r"a\b",)]
        session.sql(
            r"CREATE VIEW ice.ns.pv_r1_raw AS "
            r"SELECT r'a\b' AS a UNION ALL SELECT r'a\b' MINUS ALL SELECT r'a\b' AS a"
        ).collect()
        assert arrow_rows(session.sql("SELECT a FROM ice.ns.pv_r1_raw")) == [(r"a\b",)]
        session.sql(
            f"CREATE VIEW ice.ns.pv_r1_tt AS "
            f"SELECT a FROM ice.ns.base_t FOR VERSION AS OF {snap2} "
            f"MINUS ALL SELECT a FROM ice.ns.base_t FOR VERSION AS OF {snap1}"
        ).collect()
        assert arrow_rows(session.sql("SELECT a FROM ice.ns.pv_r1_tt")) == [(4,)]
        df_s = session.sql(r"SELECT 'a\b' AS s UNION ALL SELECT 'z'")
        assert (
            arrow_rows(
                df_s.filter(
                    r"s IN (SELECT r'a\b' AS x UNION ALL SELECT r'a\b' MINUS ALL SELECT r'c' AS x)"
                )
            )
            == []
        )
        df_raw = session.sql(r"SELECT r'a\b' AS s UNION ALL SELECT 'z'")
        assert arrow_rows(
            df_raw.filter(
                r"s IN (SELECT r'a\b' AS x UNION ALL SELECT r'a\b' MINUS ALL SELECT r'c' AS x)"
            )
        ) == [(r"a\b",)]
        one = session.sql("SELECT 1 AS k")
        for expr_sql in (
            r"(SELECT count(*) FROM (SELECT r'a\b' AS a UNION ALL "
            r"SELECT r'a\b' MINUS ALL SELECT r'a\b' AS a) x)",
            "(SELECT count(*) FROM (SELECT 1e2BD AS a UNION ALL "
            "SELECT 1e2BD MINUS ALL SELECT 100BD AS a) x)",
        ):
            assert arrow_rows(one.select(spark_functions.expr(expr_sql).alias("c"))) == [(1,)]
            assert arrow_rows(one.selectExpr(f"{expr_sql} AS c")) == [(1,)]
        with pytest.raises(UnsupportedOperationException) as native_caught:
            repark.sql("SELECT 1 AS a UNION ALL SELECT 1 MINUS ALL SELECT 1 AS a").collect()
        assert NATIVE_MINUS_TEXT in str(native_caught.value)
    finally:
        session.stop()
        _reset_active_session_for_tests()
    cold = cold_table_rows(warehouse, ("t_r1_raw", "m_r1_raw"), ansi)
    assert cold["t_r1_raw"] == [(r"a\b",)]
    assert cold["m_r1_raw"] == [(r"a\b",), ("z",)]


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_r2_ddl_dml_values_and_insert_by_name_parens_minus_all(tmp_path: Path, ansi: str) -> None:
    _reset_active_session_for_tests()
    warehouse = tmp_path / f"r2-{ansi}"
    session = (
        ReparkSession.builder.appName(f"sql-setop-r2-{ansi}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    write_cases = (
        (
            "t_r2_cv",
            (),
            "CREATE TABLE ice.ns.t_r2_cv USING iceberg AS "
            "VALUES (1), (1), (4) MINUS ALL VALUES (1)",
        ),
        (
            "t_r2_corv",
            (),
            "CREATE OR REPLACE TABLE ice.ns.t_r2_corv USING iceberg AS "
            "VALUES (1), (1), (4) MINUS ALL VALUES (1)",
        ),
        (
            "t_r2_rv",
            ("CREATE TABLE ice.ns.t_r2_rv (a INT) USING iceberg",),
            "REPLACE TABLE ice.ns.t_r2_rv USING iceberg AS "
            "VALUES (1), (1), (4) MINUS ALL VALUES (1)",
        ),
        (
            "t_r2_tpv",
            (),
            "CREATE TABLE ice.ns.t_r2_tpv USING iceberg "
            "TBLPROPERTIES ('comment' = 'SELECT WITH') AS "
            "VALUES (1), (1), (4) MINUS ALL VALUES (1)",
        ),
        (
            "t_r2_ibn",
            ("CREATE TABLE ice.ns.t_r2_ibn (a INT) USING iceberg",),
            "INSERT INTO ice.ns.t_r2_ibn BY NAME "
            "(SELECT 1 AS a UNION ALL SELECT 1 UNION ALL SELECT 4) MINUS ALL (SELECT 1 AS a)",
        ),
        (
            "t_r2_iobn",
            (
                "CREATE TABLE ice.ns.t_r2_iobn (a INT) USING iceberg",
                "INSERT INTO ice.ns.t_r2_iobn VALUES (9)",
            ),
            "INSERT OVERWRITE ice.ns.t_r2_iobn BY NAME "
            "(SELECT 1 AS a UNION ALL SELECT 1 UNION ALL SELECT 4) MINUS ALL (SELECT 1 AS a)",
        ),
    )
    try:
        session.register_memory_catalog("ice", str(warehouse))
        session.sql("CREATE NAMESPACE ice.ns").collect()
        for table, setup_stmts, stmt in write_cases:
            for setup_stmt in setup_stmts:
                session.sql(setup_stmt).collect()
            session.sql(stmt).collect()
            assert sorted(arrow_rows(session.sql(f"SELECT * FROM ice.ns.{table}"))) == [
                (1,),
                (4,),
            ], stmt
        session.sql(
            "CREATE TABLE ice.ns.t_r2_ident USING iceberg AS "
            "(SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) s)"
        ).collect()
        session.sql("CREATE TABLE ice.ns.t_r2_ibn_ident (`all` INT) USING iceberg").collect()
        session.sql(
            "INSERT INTO ice.ns.t_r2_ibn_ident BY NAME "
            "(SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) s)"
        ).collect()
    finally:
        session.stop()
        _reset_active_session_for_tests()
    names = (*(table for table, _, _ in write_cases), "t_r2_ident", "t_r2_ibn_ident")
    cold = cold_table_rows(warehouse, names, ansi)
    for table, _, stmt in write_cases:
        assert cold[table] == [(1,), (4,)], stmt
    assert cold["t_r2_ident"] == [(1,)]
    assert cold["t_r2_ibn_ident"] == [(1,)]
