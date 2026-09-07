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


def _validate_map_in_arrow_batch(
    batch: Any,
    expected: Any,
    declared: StructType,
) -> None:
    """Loud schema mismatch for a yielded RecordBatch vs declared mapInArrow schema."""
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
    for index, (want_field, got_field) in enumerate(zip(expected, got, strict=True)):
        if want_field.type != got_field.type:
            declared_field = declared.fields[index]
            raise PySparkException(
                "mapInArrow schema mismatch on field "
                f"{want_field.name!r}: expected type {want_field.type} "
                f"({declared_field.dataType.simpleString()}), got {got_field.type}"
            )
