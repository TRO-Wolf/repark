"""createDataFrame column-wise inference and conversion."""

from __future__ import annotations

import datetime
from decimal import Decimal, getcontext
from typing import Any

from repark import _native
from repark.errors import PySparkTypeError, PySparkValueError
from repark.spark._pyarrow import require_pyarrow
from repark.spark.session.create_dataframe_inference import (
    _INFER_NESTED_DICT_AS_STRUCT,
    _LEGACY_FIRST_ELEMENT_COERCE,
    _prepare_nested_cell,
    _sql_type_to_arrow,
    _validate_decimal_envelope,
)
from repark.spark.session.create_dataframe_schema import (
    _column_null_sql_from_raw_tuples,
    _infer_null_sql_from_raw_cells,
)
from repark.spark.session.create_dataframe_tuples import (
    _arrow_table_from_tuples,
    _arrow_type_for_typed_null_sql,
    _pa_array_or_refuse,
    _refuse_duplicate_tuple_column_names,
)
from repark.spark.session.create_dataframe_values import _normalize_create_dataframe_cell
from repark.spark.session.session_time_zone import active_session_time_zone
from repark.spark.session.timestamp_type import (
    default_timestamp_arrow_type,
    is_default_timestamp_ntz,
)


def _rust_cdf_arrow_table(
    names: list[str],
    raw_tuples: list[tuple[Any, ...]],
    engine_types: list[str] | None,
) -> Any:
    """Build the Arrow table through the native inference builder, or None to fall back."""
    export_rows = getattr(_native, "cdf_arrow_export", None)
    if export_rows is None or not raw_tuples:
        return None

    pa = require_pyarrow()
    arrow_schema = None
    if engine_types is not None:
        if len(engine_types) != len(names):
            return None

        try:
            arrow_schema = pa.schema(
                [
                    pa.field(name, _sql_type_to_arrow(sql_type))
                    for name, sql_type in zip(names, engine_types, strict=True)
                ]
            )

        except Exception:
            return None

    try:
        export = export_rows(
            names,
            raw_tuples,
            arrow_schema,
            active_session_time_zone() == "UTC",
            is_default_timestamp_ntz(),
            _INFER_NESTED_DICT_AS_STRUCT.get(),
            _LEGACY_FIRST_ELEMENT_COERCE.get(),
            getcontext().prec,
        )

    except Exception:
        return None

    if export is None:
        return None

    return pa.table(export)


def _arrow_table_from_raw_tuples(
    names: list[str],
    raw_tuples: list[tuple[Any, ...]],
    *,
    engine_types: list[str] | None,
) -> Any:
    """Build the Arrow table through the native path, the column-wise path, or legacy."""
    rust_table = _rust_cdf_arrow_table(names, raw_tuples, engine_types)
    if rust_table is not None:
        return rust_table

    if engine_types is not None:
        from repark.spark.session.create_dataframe_rows import (
            _arrow_table_from_raw_tuples_legacy,
        )

        return _arrow_table_from_raw_tuples_legacy(names, raw_tuples, engine_types)

    return _arrow_table_from_raw_tuples_fast(names, raw_tuples)


def _refuse_infinite_float_column(values: list[Any]) -> Any:
    """Build a float64 array, refusing infinite cells exactly like normalization."""
    pa = require_pyarrow()
    import pyarrow.compute as pc

    column = pa.array(values, type=pa.float64())

    if len(column) > 0 and pc.any(pc.is_inf(column)).as_py():
        raise PySparkTypeError("createDataFrame does not support infinite float values")

    return column


def _first_decimal_violation(values: list[Any]) -> tuple[int, Any] | None:
    """The row index and cell of the first decimal-envelope violation, if any."""
    for row_index, cell in enumerate(values):
        if cell is None:
            continue

        try:
            _validate_decimal_envelope(cell)

        except (PySparkTypeError, PySparkValueError):
            return (row_index, cell)

    return None


def _raise_first_decimal_violation(
    violations: list[tuple[int, int, Any]],
) -> None:
    """Re-raise the row-major-first envelope violation across decimal columns."""
    if violations:
        earliest = min(violations, key=lambda violation: (violation[0], violation[1]))

        _validate_decimal_envelope(earliest[2])


def _slow_column_array(column_name: str, null_sql: str, normalized: list[Any]) -> Any:
    """Build one nested/exotic column through the unchanged tuple converter."""
    pseudo_tuples = [(cell,) for cell in normalized]

    table = _arrow_table_from_tuples(
        [column_name], pseudo_tuples, column_null_sql=[null_sql], engine_types=None
    )

    return table.column(0)


def _normalize_slow_column(column_name: str, raw_values: list[Any]) -> tuple[str, list[Any]]:
    """Null-SQL witness plus normalized cells for one nested/exotic column."""
    raw_pseudo = [(cell,) for cell in raw_values]

    null_sql = _column_null_sql_from_raw_tuples(raw_pseudo, 1, names=[column_name])[0]

    normalized = [
        _normalize_create_dataframe_cell(cell, field_name=column_name) for cell in raw_values
    ]

    return (null_sql, normalized)


def _arrow_table_from_raw_tuples_fast(names: list[str], raw_tuples: list[tuple[Any, ...]]) -> Any:
    """Build the Arrow table with one census pass per column, then one build pass."""
    pa = require_pyarrow()
    width = len(names)

    raw_columns: list[list[Any]] = [
        [row[column_index] for row in raw_tuples] for column_index in range(width)
    ]

    kinds: list[set[type]] = []

    for raw_values in raw_columns:
        present = set(map(type, raw_values))

        present.discard(type(None))

        kinds.append(present)

    slow_null_sql: dict[int, str] = {}

    slow_normalized: dict[int, list[Any]] = {}

    for column_index in range(width):
        if _column_is_slow(kinds[column_index]):
            null_sql, normalized = _normalize_slow_column(
                names[column_index], raw_columns[column_index]
            )

            slow_null_sql[column_index] = null_sql

            slow_normalized[column_index] = normalized

    arrow_types: list[Any] = []

    for column_index in range(width):
        if column_index in slow_normalized:
            arrow_types.append(None)

        else:
            arrow_types.append(
                _fast_column_arrow_type(raw_columns[column_index], kinds[column_index])
            )

    _refuse_duplicate_tuple_column_names(names)

    violations: list[tuple[int, int, Any]] = []

    for column_index in range(width):
        if kinds[column_index] == {Decimal}:
            found = _first_decimal_violation(raw_columns[column_index])

            if found is not None:
                violations.append((found[0], column_index, found[1]))

    _raise_first_decimal_violation(violations)

    built: list[Any] = []

    for column_index in range(width):
        if column_index in slow_normalized:
            built.append(
                _slow_column_array(
                    names[column_index],
                    slow_null_sql[column_index],
                    slow_normalized[column_index],
                )
            )

        else:
            built.append(
                _fast_column_array(
                    names[column_index],
                    raw_columns[column_index],
                    kinds[column_index],
                    arrow_types[column_index],
                )
            )

    return pa.Table.from_arrays(built, names=names)


_FAST_CENSUS: frozenset[frozenset[type]] = frozenset(
    {
        frozenset(),
        frozenset({int}),
        frozenset({float}),
        frozenset({str}),
        frozenset({bool}),
        frozenset({datetime.date}),
        frozenset({datetime.datetime}),
        frozenset({Decimal}),
        frozenset({bytes}),
        frozenset({bytearray}),
        frozenset({memoryview}),
        frozenset({datetime.time}),
    }
)


def _column_is_slow(present: set[type]) -> bool:
    """True when a column census needs the shared normalizer and converter."""
    return frozenset(present) not in _FAST_CENSUS


def _fast_column_arrow_type(raw_values: list[Any], present: set[type]) -> Any:
    """The Arrow type for one single-kind scalar column (Spark inference rules)."""
    pa = require_pyarrow()
    if not present:
        return _arrow_type_for_typed_null_sql(_infer_null_sql_from_raw_cells(raw_values))

    if present == {int}:
        return pa.int64()

    if present == {float}:
        return pa.float64()

    if present == {str}:
        return pa.string()

    if present == {bool}:
        return pa.bool_()

    if present == {datetime.date}:
        return pa.date32()

    if present == {datetime.datetime}:
        return default_timestamp_arrow_type()

    if present == {Decimal}:
        return pa.decimal128(38, 18)

    if present == {bytes} or present == {bytearray} or present == {memoryview}:
        return pa.binary()

    return pa.string()


def _fast_column_array(
    column_name: str, raw_values: list[Any], present: set[type], arrow_type: Any
) -> Any:
    """Convert one single-kind scalar column to its Arrow array."""
    if present == {float}:
        return _refuse_infinite_float_column(raw_values)

    if present == {datetime.datetime}:
        prepared = [_prepare_nested_cell(cell, arrow_type) for cell in raw_values]

        return _pa_array_or_refuse(prepared, arrow_type, column_name)

    if present == {datetime.time}:
        stringed = [None if cell is None else str(cell) for cell in raw_values]

        return _pa_array_or_refuse(stringed, arrow_type, column_name)

    return _pa_array_or_refuse(raw_values, arrow_type, column_name)
