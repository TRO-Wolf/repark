"""MAP-COLUMN-PANIC-NATIVE-DOOR-1 — the native ``repark.sql()`` door answers a map over a column.

The cells are the ones ``map_column_panic_1_spark_oracle.json`` recorded from live Spark 4.1.2 for
``map(...)`` over a column, a function or a cast. The native door has no temp views and no
``timestamp_micros``, so each statement reads the same three rows from an inline subquery. A cell
is pinned when the native door answers Spark's rows through the Arrow path; the repeated and NULL
key cells pin that the refusal carries Spark's condition text and never a caught Rust panic.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _record_map_column_panic_1 import VIEW_NAME, VIEW_SQL, plain

import repark
from repark.errors import PySparkException

_ORACLE: dict[str, Any] = json.loads(
    Path(__file__).with_name("map_column_panic_1_spark_oracle.json").read_text(encoding="utf-8")
)
_CELLS: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in _ORACLE["cells"]}
_VALUE_CELLS: tuple[str, ...] = (
    "column_value",
    "column_value_one_row",
    "cast_value",
    "ntz_cast_value",
    "struct_value",
    "map_value",
    "two_pairs",
    "column_key",
    "int_key",
    "widen_bigint",
    "null_value",
    "partial_null_value",
    "lookup",
    "map_keys",
    "map_values",
    "predicate",
)
_KEY_REFUSALS: dict[str, str] = {
    "duplicate_key": "DUPLICATED_MAP_KEY",
    "duplicate_literal": "DUPLICATED_MAP_KEY",
    "null_key_column": "NULL_MAP_KEY",
    "null_key_cast": "NULL_MAP_KEY",
    "null_key_literal": "NULL_MAP_KEY",
}
_PANIC_MARKER = "repark internal error"
_VIEW_BODY = VIEW_SQL.split(" AS SELECT ", 1)[1].replace(
    "timestamp_micros(", "to_timestamp_micros("
)
_INLINE_VIEW = f"(SELECT {_VIEW_BODY}) AS {VIEW_NAME}"


def _native_statement(cell_id: str) -> str:
    """Return the recorded statement with the view replaced by its inline subquery."""
    return re.sub(rf"\b{VIEW_NAME}\b", _INLINE_VIEW, _CELLS[cell_id]["sql"])


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


@pytest.mark.parametrize("cell_id", _VALUE_CELLS)
def test_the_native_door_answers_the_recorded_spark_rows(cell_id: str) -> None:
    """A map over a column, a function or a cast answers Spark's rows on the Arrow path."""
    table = repark.sql(_native_statement(cell_id)).to_arrow()
    assert _table_rows(table) == _CELLS[cell_id]["rows"], cell_id


@pytest.mark.parametrize("cell_id", sorted(_KEY_REFUSALS))
def test_a_repeated_or_null_key_refuses_with_spark_text_and_never_a_panic(cell_id: str) -> None:
    """A repeated key and a NULL key raise Spark's condition text, not a caught Rust panic."""
    with pytest.raises(PySparkException) as caught:
        repark.sql(_native_statement(cell_id)).to_arrow()
    text = str(caught.value)
    assert _PANIC_MARKER not in text, text
    assert f"[{_KEY_REFUSALS[cell_id]}]" in text, text


def test_a_constant_map_keeps_the_datafusion_constructor_rows() -> None:
    """A literal map with distinct keys answers the recorded literal rows."""
    table = repark.sql(_native_statement("literal_control")).to_arrow()
    assert _table_rows(table) == _CELLS["literal_control"]["rows"]
