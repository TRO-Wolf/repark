"""MAP-COLUMN-PANIC-1 — ``map(<key>, <non-literal value>)`` answers Spark and never panics.

Every cell reads its expected answer from ``map_column_panic_1_spark_oracle.json``, recorded from
live Spark 4.1.2 by ``_record_map_column_panic_1.py``: rows, the result type and the top-level
nullable flags for an answer, or the error condition, SQLSTATE and message for a refusal. The
pins run each cell through ``ReparkSession.sql`` over the three-row temp view and read the rows
from the Arrow path. They replay the column-value cell through ``selectExpr``, ``expr``, a
string ``filter`` and ``create_map``, store the map into a ``MAP<STRING, INT>`` Iceberg table
by INSERT, UPDATE and MERGE, and assert that no cell reports a caught Rust panic.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _record_map_column_panic_1 import VIEW_SQL, plain

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException
from repark.spark import functions as spark_functions

_ORACLE: dict[str, Any] = json.loads(
    Path(__file__).with_name("map_column_panic_1_spark_oracle.json").read_text(encoding="utf-8")
)
_CELLS: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in _ORACLE["cells"]}
_ANSWERS: list[str] = [cell_id for cell_id, cell in _CELLS.items() if "error" not in cell]
_PLANNING_REFUSALS: list[str] = [
    cell_id for cell_id, cell in _CELLS.items() if cell.get("class") == "AnalysisException"
]
_RUNTIME_REFUSALS: list[str] = [
    cell_id for cell_id, cell in _CELLS.items() if cell.get("class") == "SparkRuntimeException"
]
_PANIC_MARKER = "repark internal error"
_THROUGH_SQLSTATE = re.compile(r"^.*?SQLSTATE: \w+", re.DOTALL)
_VIEW = "map_column_panic_1_src"


@pytest.fixture
def source(spark_with_warehouse: ReparkSession) -> Iterator[ReparkSession]:
    """A session at zone UTC with the recorded three-row temp view."""
    spark_with_warehouse.conf.set("spark.sql.session.timeZone", "UTC")
    spark_with_warehouse.sql(VIEW_SQL)
    yield spark_with_warehouse


def _arrow_plain(value: Any, arrow_type: pa.DataType) -> Any:
    """Turn an Arrow value into the recorder's plain data, keyed by its Arrow type."""
    if value is None:
        return None
    if pa.types.is_map(arrow_type):
        pairs = [
            [_arrow_plain(key, arrow_type.key_type), _arrow_plain(item, arrow_type.item_type)]
            for key, item in value
        ]
        return {"map": sorted(pairs, key=lambda pair: repr(pair[0]))}
    if pa.types.is_struct(arrow_type):
        return {
            "struct": [
                [field.name, _arrow_plain(value[field.name], field.type)] for field in arrow_type
            ]
        }
    if pa.types.is_list(arrow_type):
        return [_arrow_plain(item, arrow_type.value_type) for item in value]
    return plain(value)


def _table_rows(table: pa.Table) -> list[list[Any]]:
    """Read an Arrow table as the recorder's plain rows."""
    return [
        [_arrow_plain(column[index].as_py(), column.type) for column in table.columns]
        for index in range(table.num_rows)
    ]


def _rows(frame: Any) -> list[list[Any]]:
    """Collect a frame through the Arrow path as the recorder's plain rows."""
    return _table_rows(frame.to_arrow())


def _spark_text(cell: dict[str, Any]) -> str:
    """Return Spark's message up to and including its SQLSTATE, without plan or position."""
    match = _THROUGH_SQLSTATE.match(cell["message"])
    assert match is not None, cell["message"]
    return match.group(0)


@pytest.mark.parametrize("cell_id", _ANSWERS)
def test_an_answered_cell_equals_spark_in_rows_type_and_nullability(
    source: ReparkSession, cell_id: str
) -> None:
    """Rows, the facade type string and the nullable flags of every map column equal Spark's."""
    cell = _CELLS[cell_id]
    frame = source.sql(cell["sql"])
    table = frame.to_arrow()
    assert _table_rows(table) == cell["rows"], cell_id
    assert frame.schema.simpleString() == cell["type"], cell_id
    for field, nullable in zip(table.schema, cell["nullable"], strict=True):
        if pa.types.is_map(field.type):
            assert field.nullable is nullable is False, cell_id


@pytest.mark.parametrize("cell_id", _PLANNING_REFUSALS)
def test_a_planning_refusal_raises_spark_condition_sqlstate_and_text(
    source: ReparkSession, cell_id: str
) -> None:
    """An odd argument count and a map ordering refuse with Spark's condition and text."""
    cell = _CELLS[cell_id]
    with pytest.raises(AnalysisException) as caught:
        source.sql(cell["sql"]).collect()
    text = str(caught.value)
    assert _PANIC_MARKER not in text, text
    assert _spark_text(cell) in text, text
    assert caught.value.getCondition() == cell["error"], text
    assert caught.value.getSqlState() == cell["sqlstate"], text


@pytest.mark.parametrize("cell_id", _RUNTIME_REFUSALS)
def test_a_runtime_refusal_carries_spark_text_and_never_a_panic(
    source: ReparkSession, cell_id: str
) -> None:
    """A repeated key and a NULL key raise Spark's message through every spelling."""
    cell = _CELLS[cell_id]
    with pytest.raises(PySparkException) as caught:
        source.sql(cell["sql"]).collect()
    text = str(caught.value)
    assert _PANIC_MARKER not in text, text
    assert _spark_text(cell) in text, text


@pytest.mark.parametrize("cell_id", _RUNTIME_REFUSALS)
def test_a_runtime_refusal_has_no_condition_divergence(source: ReparkSession, cell_id: str) -> None:
    """Divergence: the engine raises the base class with SQLSTATE but no condition, where
    Spark's ``SparkRuntimeException`` answers ``getCondition`` too."""
    cell = _CELLS[cell_id]
    with pytest.raises(PySparkException) as caught:
        source.sql(cell["sql"]).collect()
    assert type(caught.value) is PySparkException
    assert caught.value.getCondition() is None
    assert caught.value.getSqlState() == cell["sqlstate"]


def test_the_expression_doors_equal_the_sql_door(source: ReparkSession) -> None:
    """``selectExpr``, ``expr``, a string ``filter`` and ``create_map`` answer the SQL cells."""
    expected = _CELLS["column_value"]["rows"]
    view = source.table(_VIEW).orderBy("id")
    by_select_expression = view.selectExpr("id", "map('a', id) AS m")
    assert _rows(by_select_expression) == expected
    by_expression = view.select("id", spark_functions.expr("map('a', id)").alias("m"))
    assert _rows(by_expression) == expected
    by_function = view.select(
        "id",
        spark_functions.create_map(spark_functions.lit("a"), spark_functions.col("id")).alias("m"),
    )
    assert _rows(by_function) == expected
    assert _rows(view.filter("map('a', id)['a'] = 1").select("id")) == [[1]]
    assert _rows(view.where("map('a', id)['a'] = 1").select("id")) == [[1]]
    one_row = view.where("id = 1")
    assert _rows(one_row.select("id", spark_functions.expr("map('a', id)").alias("m"))) == [
        expected[1]
    ]
    assert _rows(one_row.selectExpr("id", "map('a', id) AS m")) == [expected[1]]


def test_a_key_error_raised_through_an_expression_door_is_spark_text(source: ReparkSession) -> None:
    """A repeated key and a NULL key raise Spark's message through ``F.expr`` as well."""
    view = source.table(_VIEW)
    duplicate = view.select(spark_functions.expr("map('a', id, 'a', id + 1)").alias("m"))
    with pytest.raises(PySparkException, match=r"\[DUPLICATED_MAP_KEY\] Duplicate map key a was"):
        duplicate.collect()
    null_key = view.select(spark_functions.expr("map(nk, id)").alias("m"))
    with pytest.raises(PySparkException, match=r"\[NULL_MAP_KEY\] Cannot use null as map key"):
        null_key.collect()


def test_an_odd_count_through_an_expression_door_keeps_the_planner_text_divergence(
    source: ReparkSession,
) -> None:
    """Divergence: ``F.expr`` and a string ``filter`` refuse an odd argument count with
    DataFusion's text and no condition, where Spark raises ``WRONG_NUM_ARGS`` with the count."""
    view = source.table(_VIEW)
    with pytest.raises(AnalysisException, match="make_map requires an even number") as caught:
        view.select(spark_functions.expr("map('a', id, 'b')").alias("m")).collect()
    assert caught.value.getCondition() is None
    with pytest.raises(AnalysisException, match="make_map requires an even number"):
        view.filter("map('a', id, 'b')['a'] = 1").count()


def _create_map_table(source: ReparkSession) -> None:
    """Create the empty ``MAP<STRING, INT>`` target table."""
    source.sql("CREATE TABLE mem.ns.m (id INT, c MAP<STRING, INT>) USING iceberg")


def test_an_insert_select_stores_the_column_map_into_a_map_table(source: ReparkSession) -> None:
    """``INSERT … SELECT id, map('a', id)`` into ``MAP<STRING, INT>`` stores Spark's rows."""
    _create_map_table(source)
    source.sql(f"INSERT INTO mem.ns.m SELECT id, map('a', id) FROM {_VIEW}")
    stored = _rows(source.sql("SELECT id, c FROM mem.ns.m ORDER BY id"))
    assert stored == _CELLS["column_value"]["rows"]


def test_update_and_merge_store_a_column_map(source: ReparkSession) -> None:
    """``UPDATE`` and the MERGE update and insert arms assign a per-row map."""
    _create_map_table(source)
    source.sql(f"INSERT INTO mem.ns.m SELECT id, map('z', 9) FROM {_VIEW}")
    source.sql("UPDATE mem.ns.m SET c = map('u', id + 10) WHERE id = 0")
    source.sql(
        f"MERGE INTO mem.ns.m t USING {_VIEW} s ON t.id = s.id "
        "WHEN MATCHED AND s.id = 1 THEN UPDATE SET c = map('m', s.id + 20)"
    )
    source.sql(
        "MERGE INTO mem.ns.m t USING (SELECT 7 AS id) s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id, c) VALUES (s.id, map('n', s.id))"
    )
    stored = _rows(source.sql("SELECT id, c FROM mem.ns.m ORDER BY id"))
    assert stored == [
        [0, {"map": [["u", 10]]}],
        [1, {"map": [["m", 21]]}],
        [2, {"map": [["z", 9]]}],
        [7, {"map": [["n", 7]]}],
    ]


def test_a_failed_insert_leaves_the_table_unchanged(source: ReparkSession) -> None:
    """A repeated key aborts the INSERT with Spark's text and stores no row."""
    _create_map_table(source)
    with pytest.raises(PySparkException, match=r"\[DUPLICATED_MAP_KEY\]"):
        source.sql(
            f"INSERT INTO mem.ns.m SELECT id, map('a', id, 'a', id + 1) FROM {_VIEW}"
        ).collect()
    assert source.sql("SELECT count(*) AS n FROM mem.ns.m").collect()[0]["n"] == 0
