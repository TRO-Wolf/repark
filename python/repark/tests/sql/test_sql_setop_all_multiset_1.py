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
from repark.spark.session import _reset_active_session_for_tests

ORACLE: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
NATIVE_GAPS = {
    "shape|persistent-view": "NATIVE-VIEW-ICEBERG",
    "shape|ctas": "NATIVE-CTAS-ICEBERG-REFUSE-1",
}
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
    assert got["kind"] == "error", got
    assert got["class"] == "AnalysisException", got
    assert "SQL-SETOP-ALL" not in got["text"], got
    assert got["bracket"] in (None, "UNRESOLVED_ROUTINE"), got


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
    for name, stored in (("vi", "INTERSECT ALL"), ("vm", "MINUS ALL")):
        session.sql(f"CREATE VIEW ice.ns.{name} AS {tables.format('INTERSECT')}").collect()
        plain = sorted(row[0] for row in session.sql(f"SELECT a FROM ice.ns.{name}").collect())
        for path in (tmp_path / "ns" / name / "metadata").glob("*.metadata.json"):
            text = path.read_text(encoding="utf-8")
            path.write_text(text.replace(" INTERSECT SELECT", f" {stored} SELECT"), "utf-8")
        read = sorted(row[0] for row in session.sql(f"SELECT a FROM ice.ns.{name}").collect())
        answers[name] = (plain, read)
    session.stop()
    _reset_active_session_for_tests()
    assert answers == {"vi": ([1, 2], [1, 2, 2]), "vm": ([1, 2], [1, 1, 4])}
