"""mapInArrow schema coercion and batch validation for the DataFrame facade."""

from __future__ import annotations

from typing import Any

from repark.errors import PySparkException, PySparkTypeError
from repark.spark.types import StructType


def _coerce_map_in_arrow_schema(schema: Any) -> tuple[StructType, Any]:
    """Parse mapInArrow ``schema`` into ``(StructType, pyarrow.Schema)``.

    Arrow widths match the session createDataFrame path (:func:`_sql_type_to_arrow`) so
    ``SMALLINT``/``TINYINT``/``FLOAT`` stay int16/int8/float32 — not fail-open string or
    float64.
    """
    import pyarrow as pa

    from repark.spark.session import _parse_create_dataframe_schema, _sql_type_to_arrow
    from repark.spark.types import struct_type_from_arrow

    if schema is None:
        raise PySparkTypeError("mapInArrow schema is required (StructType or DDL string)")
    names, engine_types = _parse_create_dataframe_schema(schema)
    if names is None or engine_types is None:
        raise PySparkTypeError(
            "mapInArrow schema must be a StructType or DDL string with types "
            f"(got {type(schema).__name__})"
        )
    arrow_fields: list[pa.Field] = [
        pa.field(name, _sql_type_to_arrow(sql_type), nullable=True)
        for name, sql_type in zip(names, engine_types, strict=True)
    ]
    arrow_schema = pa.schema(arrow_fields)
    return struct_type_from_arrow(arrow_schema), arrow_schema


def _nanosecond_twin_of(got: Any, want: Any) -> bool:
    """True when ``got`` is ``want`` with nanosecond timestamps, at any list depth."""
    import pyarrow as pa

    while (pa.types.is_list(got) and pa.types.is_list(want)) or (
        pa.types.is_large_list(got) and pa.types.is_large_list(want)
    ):
        got, want = got.value_type, want.value_type
    return (
        pa.types.is_timestamp(got)
        and pa.types.is_timestamp(want)
        and got.unit == "ns"
        and want.unit == "us"
        and got.tz == want.tz
    )


def _floor_to_declared_microseconds(column: Any, want: Any) -> Any:
    """Floor a nanosecond-twin column to the declared microsecond type.

    Notes:
        The floor runs on the innermost values. Each list layer is rebuilt from its own
        offsets, with a null offset where the list is null.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    layers: list[tuple[Any, Any]] = []
    values, target = column, want
    while not pa.types.is_timestamp(target):
        layers.append((values, target))
        values, target = values.values, target.value_type
    floored = pc.floor_temporal(values, 1, "microsecond").cast(target)
    for layer, layer_type in reversed(layers):
        offsets = layer.offsets
        if layer.null_count:
            absent = pa.concat_arrays([layer.is_null(), pa.array([False])])
            offsets = pc.if_else(absent, pa.scalar(None, offsets.type), offsets)
        floored = type(layer).from_arrays(offsets, floored, type=layer_type)
    return floored


def _validate_map_in_arrow_batch(
    batch: Any,
    expected: Any,
    declared: StructType,
) -> Any:
    """Return a yielded RecordBatch reconciled with the declared mapInArrow schema.

    Notes:
        A nanosecond timestamp column whose declared type is the same timestamp kind in
        microseconds is floored to microseconds. Any other type difference fails loud.
    """
    import pyarrow as pa

    if not isinstance(batch, pa.RecordBatch):
        raise PySparkTypeError(
            f"mapInArrow expected pyarrow.RecordBatch, got {type(batch).__name__}"
        )
    got = batch.schema
    if got.names != list(expected.names):
        raise PySparkException(
            "mapInArrow schema mismatch: field names "
            f"expected {list(expected.names)}, got {got.names}"
        )
    floored: dict[int, Any] = {}
    for index, (want_field, got_field) in enumerate(zip(expected, got, strict=True)):
        if want_field.type == got_field.type:
            continue
        if _nanosecond_twin_of(got_field.type, want_field.type):
            floored[index] = _floor_to_declared_microseconds(batch.column(index), want_field.type)
            continue
        declared_field = declared.fields[index]
        raise PySparkException(
            "mapInArrow schema mismatch on field "
            f"{want_field.name!r}: expected type {want_field.type} "
            f"({declared_field.dataType.simpleString()}), got {got_field.type}"
        )
    if not floored:
        return batch
    columns = [floored.get(index, column) for index, column in enumerate(batch.columns)]
    return pa.RecordBatch.from_arrays(columns, names=got.names)
