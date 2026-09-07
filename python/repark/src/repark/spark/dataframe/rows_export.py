"""Arrow-to-``Row`` materialization for ``collect`` — native fast path plus the Python fallback."""

from __future__ import annotations

import gc
from typing import Any

from repark.errors import PySparkNotImplementedError
from repark.spark.row import Row


def _arrow_map_pairs(value: Any) -> list[tuple[Any, Any]] | None:
    """Normalize an Arrow ``to_pylist`` map cell to pairs, or ``None``.

    Empty map is ``[]`` / ``{}`` from pylist; non-empty maps are pair-lists or (rarely) dicts.
    Returns an empty list for empty maps; ``None`` when the value is not map-shaped.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return list(value.items())
    if isinstance(value, list):
        if not value:
            return []
        if all(isinstance(item, tuple) and len(item) == 2 for item in value):
            return list(value)
    return None


def _arrow_cell_to_spark_python(value: Any, arrow_type: Any) -> Any:
    """Convert Arrow cells to Spark Python values using the declared schema.

    Maps become dictionaries, including nested maps. Lists and structs recurse.
    """
    import pyarrow as pa

    if value is None:
        return None
    if pa.types.is_map(arrow_type):
        pairs = _arrow_map_pairs(value)
        if pairs is None:
            return value
        key_type = arrow_type.key_type
        item_type = arrow_type.item_type
        return {
            _arrow_cell_to_spark_python(key, key_type): _arrow_cell_to_spark_python(item, item_type)
            for key, item in pairs
        }
    is_list_type = (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    )
    if is_list_type:
        if not isinstance(value, list):
            return value
        element_type = arrow_type.value_type
        return [_arrow_cell_to_spark_python(item, element_type) for item in value]
    if pa.types.is_struct(arrow_type):
        if not isinstance(value, dict):
            return value
        fields = list(arrow_type)
        return {
            field.name: _arrow_cell_to_spark_python(value.get(field.name), field.type)
            for field in fields
        }
    if pa.types.is_timestamp(arrow_type) and getattr(value, "tzinfo", None) is not None:
        from repark.spark.session.session_time_zone import collect_timestamp_as_session_wall

        return collect_timestamp_as_session_wall(value)
    return value


def _refuse_calendar_interval_python_value(value: Any) -> None:
    """Reject calendar intervals because Spark has no Python converter for them."""
    if value is None:
        return
    type_name = type(value).__name__
    if type_name in {"MonthDayNano", "MonthDayNanoInterval"}:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={
                "feature": "Python conversion for calendar interval (make_interval / "
                "CalendarIntervalType)"
            },
        )
    if isinstance(value, dict):
        for key, item in value.items():
            _refuse_calendar_interval_python_value(key)
            _refuse_calendar_interval_python_value(item)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _refuse_calendar_interval_python_value(item)


def arrow_type_needs_spark_python_convert(arrow_type: Any) -> bool:
    """True when cells need ``_arrow_cell_to_spark_python`` (map / tz-aware timestamp)."""
    import pyarrow as pa

    if pa.types.is_map(arrow_type) or (
        pa.types.is_timestamp(arrow_type) and arrow_type.tz is not None
    ):
        return True
    if _is_list_type(arrow_type):
        return arrow_type_needs_spark_python_convert(arrow_type.value_type)
    if pa.types.is_struct(arrow_type):
        return any(arrow_type_needs_spark_python_convert(field.type) for field in arrow_type)
    return False


def arrow_type_may_hold_calendar_interval(arrow_type: Any) -> bool:
    """Return whether an Arrow type can contain a calendar interval."""
    import pyarrow as pa

    if pa.types.is_interval(arrow_type):
        return True
    if _is_list_type(arrow_type):
        return arrow_type_may_hold_calendar_interval(arrow_type.value_type)
    if pa.types.is_struct(arrow_type):
        return any(arrow_type_may_hold_calendar_interval(field.type) for field in arrow_type)
    if pa.types.is_map(arrow_type):
        return arrow_type_may_hold_calendar_interval(
            arrow_type.key_type
        ) or arrow_type_may_hold_calendar_interval(arrow_type.item_type)
    return False


def _is_list_type(arrow_type: Any) -> bool:
    import pyarrow as pa

    return (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    )


def _native_cell_type(arrow_type: Any) -> bool:
    import pyarrow as pa

    return (
        pa.types.is_null(arrow_type)
        or pa.types.is_boolean(arrow_type)
        or pa.types.is_integer(arrow_type)
        or pa.types.is_float32(arrow_type)
        or pa.types.is_float64(arrow_type)
        or pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_string_view(arrow_type)
        or pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_binary_view(arrow_type)
    )


def rows_from_arrow_table_python(table: Any) -> list[Row]:
    """Convert an Arrow table or batch to ``Row`` objects entirely in Python."""
    names = list(table.column_names)
    column_count = table.num_columns
    row_count = table.num_rows
    if row_count == 0:
        return []
    if column_count == 0:
        empty_names: list[str] = []
        empty_values: list[Any] = []
        return [Row.from_ordered_fields(empty_names, empty_values) for _ in range(row_count)]

    field_types = [table.schema.field(index).type for index in range(column_count)]
    needs_convert = [
        arrow_type_needs_spark_python_convert(field_type) for field_type in field_types
    ]
    may_calendar = any(
        arrow_type_may_hold_calendar_interval(field_type) for field_type in field_types
    )

    columns_python: list[list[Any]] = []
    for index in range(column_count):
        raw_values = table.column(index).to_pylist()
        if needs_convert[index]:
            column_type = field_types[index]
            columns_python.append(
                [_arrow_cell_to_spark_python(cell, column_type) for cell in raw_values]
            )
        else:
            columns_python.append(raw_values)

    if may_calendar:
        for column_values in columns_python:
            for value in column_values:
                _refuse_calendar_interval_python_value(value)

    return [Row.from_ordered_fields(names, values) for values in zip(*columns_python, strict=True)]


def _supplied_columns(table: Any, field_types: list[Any]) -> dict[int, list[Any]]:
    supplied: dict[int, list[Any]] = {}
    for index, field_type in enumerate(field_types):
        if arrow_type_needs_spark_python_convert(field_type):
            raw_values = table.column(index).to_pylist()
            supplied[index] = [_arrow_cell_to_spark_python(cell, field_type) for cell in raw_values]
        elif not _native_cell_type(field_type):
            supplied[index] = table.column(index).to_pylist()
    return supplied


def _rows_from_value_tuples(names: list[str], values: list[Any]) -> list[Row]:
    shared_names = tuple(names)
    build = Row.from_ordered_fields
    return [build(shared_names, cells) for cells in values]


def rows_from_arrow_table(table: Any) -> list[Row]:
    """Convert an Arrow table or batch to ``Row`` objects by column position."""
    from repark import _native

    row_count = table.num_rows
    if row_count == 0:
        return []
    column_count = table.num_columns
    if column_count == 0:
        return rows_from_arrow_table_python(table)
    if not hasattr(table, "__arrow_c_array__"):
        return rows_from_arrow_table_python(table)

    field_types = [table.schema.field(index).type for index in range(column_count)]
    if any(arrow_type_may_hold_calendar_interval(field_type) for field_type in field_types):
        return rows_from_arrow_table_python(table)

    collecting = gc.isenabled()
    if collecting:
        gc.disable()
    try:
        supplied = _supplied_columns(table, field_types)
        values = _native.rows_from_record_batch(table, supplied)
        if values is None:
            return rows_from_arrow_table_python(table)
        return _rows_from_value_tuples(list(table.column_names), values)
    finally:
        if collecting:
            gc.enable()
