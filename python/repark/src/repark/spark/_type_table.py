"""Python-side bridge to the shared Rust type table.

The Rust ``type_table`` owns Arrow ↔ Spark-descriptor ↔ DDL semantics; this module holds the
Python descriptor encode/decode and the token fallbacks for descriptor trees containing foreign
``DataType`` subtypes the table cannot see. Class references resolve lazily so ``types.py`` keeps
a one-directional import of this module.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from typing import Any

_AtomicRow = tuple[
    str | Callable[[Any], dict[str, Any]],
    str | Callable[[Any], str] | None,
    str | Callable[[Any], str] | None,
]
_ATOMIC_ROWS: dict[type, _AtomicRow] | None = None
_DESCRIPTOR_TYPES: dict[str, type] | None = None
_DESCRIPTOR_HEADS: dict[type, str | Callable[[Any], dict[str, Any]]] | None = None
_TYPES_MODULE: Any = None


def _types() -> Any:
    """The ``repark.spark.types`` module, resolved once after it finishes loading."""
    global _TYPES_MODULE
    if _TYPES_MODULE is None:
        from repark.spark import types as t

        _TYPES_MODULE = t
    return _TYPES_MODULE


def _atomic_rows() -> dict[type, _AtomicRow]:
    """Class→row table: ``(descriptor head or builder, simpleString, _engine_type)``."""
    global _ATOMIC_ROWS
    if _ATOMIC_ROWS is None:
        from repark.spark import types as t

        _ATOMIC_ROWS = {
            t.NullType: ("null", "void", "void"),
            t.StringType: (
                lambda data_type: {"kind": "string", "collation": data_type.collation},
                lambda data_type: (
                    "string"
                    if data_type.collation == "UTF8_BINARY"
                    else f"string collate {data_type.collation}"
                ),
                "string",
            ),
            t.CharType: (
                lambda data_type: {"kind": "char", "length": data_type.length},
                lambda data_type: f"char({data_type.length})",
                "string",
            ),
            t.VarcharType: (
                lambda data_type: {"kind": "varchar", "length": data_type.length},
                lambda data_type: f"varchar({data_type.length})",
                "string",
            ),
            t.BinaryType: ("binary", "binary", "binary"),
            t.BooleanType: ("boolean", "boolean", "boolean"),
            t.DateType: ("date", "date", "date"),
            t.TimestampType: ("timestamp", "timestamp", "timestamp"),
            t.TimestampNTZType: ("timestamp_ntz", "timestamp_ntz", "timestamp_ntz"),
            t.TimeType: (
                lambda data_type: {"kind": "time", "precision": data_type.precision},
                lambda data_type: f"time({data_type.precision})",
                lambda data_type: f"time({data_type.precision})",
            ),
            t.DecimalType: (
                lambda data_type: {
                    "kind": "decimal",
                    "precision": data_type.precision,
                    "scale": data_type.scale,
                },
                lambda data_type: f"decimal({data_type.precision},{data_type.scale})",
                lambda data_type: f"decimal({data_type.precision},{data_type.scale})",
            ),
            t.DoubleType: ("double", "double", "double"),
            t.FloatType: ("float", "float", "float"),
            t.ByteType: ("byte", "tinyint", "byte"),
            t.IntegerType: ("integer", "int", "int"),
            t.LongType: ("long", "bigint", "long"),
            t.ShortType: ("short", "smallint", "short"),
            t.CalendarIntervalType: ("calendar_interval", "interval", "interval"),
            t.DayTimeIntervalType: (
                lambda data_type: {
                    "kind": "day_time_interval",
                    "start": t.DayTimeIntervalType._fields[data_type.startField],
                    "end": t.DayTimeIntervalType._fields[data_type.endField],
                },
                lambda data_type: data_type._str_repr(),
                lambda data_type: data_type._str_repr(),
            ),
            t.YearMonthIntervalType: (
                lambda data_type: {
                    "kind": "year_month_interval",
                    "start": t.YearMonthIntervalType._fields[data_type.startField],
                    "end": t.YearMonthIntervalType._fields[data_type.endField],
                },
                lambda data_type: data_type._str_repr(),
                lambda data_type: data_type._str_repr(),
            ),
            t.VariantType: ("variant", "variant", "variant"),
        }
    return _ATOMIC_ROWS


def _descriptor_types() -> dict[str, type]:
    """Kind→class map for the unparametrised atomic descriptor kinds."""
    global _DESCRIPTOR_TYPES
    if _DESCRIPTOR_TYPES is None:
        _DESCRIPTOR_TYPES = {
            row[0]: cls for cls, row in _atomic_rows().items() if isinstance(row[0], str)
        }
    return _DESCRIPTOR_TYPES


def _descriptor_heads() -> dict[type, str | Callable[[Any], dict[str, Any]]]:
    """Exact-type map: class → descriptor kind string or descriptor builder."""
    global _DESCRIPTOR_HEADS
    if _DESCRIPTOR_HEADS is None:
        _DESCRIPTOR_HEADS = {cls: row[0] for cls, row in _atomic_rows().items()}
    return _DESCRIPTOR_HEADS


def _atomic_token(data_type: Any, index: int) -> str | None:
    """Table ``simpleString``/``_engine_type`` answer; ``None`` for nested types."""
    rows = _atomic_rows()
    cls = type(data_type)
    row = rows.get(cls)
    if row is None:
        for parent in cls.__mro__[1:]:
            row = rows.get(parent)
            if row is not None:
                break
    if row is None:
        return None
    answer = row[index + 1]
    return answer if isinstance(answer, str) or answer is None else answer(data_type)


def _datatype_to_descriptor(data_type: Any) -> dict[str, Any] | None:
    """Build the Rust type-table descriptor for a :class:`DataType`; ``None`` when unknown."""
    head = _descriptor_heads().get(type(data_type))
    if head is not None:
        return {"kind": head} if isinstance(head, str) else head(data_type)
    t = _types()

    if isinstance(data_type, t.ArrayType):
        return {
            "kind": "array",
            "element": _datatype_to_descriptor(data_type.elementType),
            "contains_null": data_type.containsNull,
        }
    if isinstance(data_type, t.MapType):
        return {
            "kind": "map",
            "key": _datatype_to_descriptor(data_type.keyType),
            "value": _datatype_to_descriptor(data_type.valueType),
            "value_contains_null": data_type.valueContainsNull,
        }
    if isinstance(data_type, t.StructField):
        return {
            "kind": "field",
            "name": data_type.name,
            "type": _datatype_to_descriptor(data_type.dataType),
            "nullable": data_type.nullable,
            "metadata": None,
        }
    if isinstance(data_type, t.StructType):
        return {
            "kind": "struct",
            "fields": [_datatype_to_descriptor(field) for field in data_type.fields],
        }
    return None


def _descriptor_to_datatype(descriptor: dict[str, Any]) -> Any:
    """Construct the public Spark type for a Rust type-table descriptor."""
    t = _types()

    kind = descriptor["kind"]
    atomic_class = _descriptor_types().get(kind)
    if atomic_class is not None:
        return atomic_class()
    if kind == "string":
        return t.StringType(descriptor["collation"])
    if kind == "char":
        return t.CharType(descriptor["length"])
    if kind == "varchar":
        return t.VarcharType(descriptor["length"])
    if kind == "time":
        return t.TimeType(descriptor["precision"])
    if kind == "decimal":
        return t.DecimalType(descriptor["precision"], descriptor["scale"])
    if kind in ("day_time_interval", "year_month_interval"):
        if kind == "day_time_interval":
            interval_class = t.DayTimeIntervalType
        else:
            interval_class = t.YearMonthIntervalType
        inverted_fields = interval_class._inverted_fields
        return interval_class(
            inverted_fields[descriptor["start"]],
            inverted_fields[descriptor["end"]],
        )
    if kind == "array":
        return t.ArrayType(
            _descriptor_to_datatype(descriptor["element"]),
            descriptor["contains_null"],
        )
    if kind == "map":
        return t.MapType(
            _descriptor_to_datatype(descriptor["key"]),
            _descriptor_to_datatype(descriptor["value"]),
            descriptor["value_contains_null"],
        )
    if kind == "struct":
        return t.StructType([_descriptor_to_datatype(field) for field in descriptor["fields"]])
    if kind == "field":
        metadata_text = descriptor.get("metadata")
        return t.StructField(
            descriptor["name"],
            _descriptor_to_datatype(descriptor["type"]),
            descriptor["nullable"],
            json.loads(metadata_text) if metadata_text else {},
        )
    raise TypeError(f"unsupported type-table descriptor kind {kind!r}")


def _descriptor_children(descriptor: dict[str, Any]) -> Iterator[Any]:
    """Yield the child descriptor slots (``element``/``key``/``value``/``type``/``fields``)."""
    for key in ("element", "key", "value", "type"):
        if key in descriptor:
            yield descriptor[key]
    yield from descriptor.get("fields", ())


def _descriptor_tree_has_none(descriptor: dict[str, Any] | None) -> bool:
    """True when a descriptor tree contains an unknown (``None``) subtype slot."""
    if descriptor is None:
        return True
    return any(_descriptor_tree_has_none(child) for child in _descriptor_children(descriptor))


def _descriptor_has_wide_decimal(descriptor: dict[str, Any]) -> bool:
    """True when a decimal node falls outside the Arrow FFI storage envelope."""
    if descriptor.get("kind") == "decimal":
        return not (1 <= descriptor["precision"] <= 38) or not (-128 <= descriptor["scale"] <= 127)
    return any(_descriptor_has_wide_decimal(child) for child in _descriptor_children(descriptor))


def _descriptor_token(data_type: Any, native_name: str, fallback: Callable[[Any], str]) -> str:
    """Token answer from the shared type table (Python fallback on unknown)."""
    descriptor = _datatype_to_descriptor(data_type)
    if descriptor is None or _descriptor_tree_has_none(descriptor):
        return fallback(data_type)
    from repark import _native

    return getattr(_native, native_name)(descriptor)


def _nested_token_python(
    data_type: Any,
    child: Callable[[Any], str],
    leaf: Callable[[Any], str],
    *,
    upper: bool = False,
) -> str:
    """Container-token walker for descriptor trees the table cannot see."""
    t = _types()

    if upper:
        array_name, map_name, struct_name = "ARRAY", "MAP", "STRUCT"
    else:
        array_name, map_name, struct_name = "array", "map", "struct"
    if isinstance(data_type, t.ArrayType):
        return f"{array_name}<{child(data_type.elementType)}>"
    if isinstance(data_type, t.MapType):
        return f"{map_name}<{child(data_type.keyType)},{child(data_type.valueType)}>"
    if isinstance(data_type, t.StructType):
        inner = ",".join(f"{field.name}:{child(field.dataType)}" for field in data_type.fields)
        return f"{struct_name}<{inner}>"
    if not upper and isinstance(data_type, t.StructField):
        return f"{data_type.name}:{child(data_type.dataType)}"
    return leaf(data_type)


def _simple_string_python(data_type: Any) -> str:
    """``simpleString`` for descriptors the table cannot see (unknown subtype inside)."""
    return _nested_token_python(
        data_type,
        lambda item: item.simpleString(),
        lambda item: type(item).typeName(),
    )


def _engine_token_python(data_type: Any) -> str:
    """``_engine_type`` for descriptors the table cannot see (unknown subtype inside)."""
    return _nested_token_python(
        data_type,
        lambda item: item._engine_type(),
        lambda item: item.simpleString(),
    )


def _datatype_to_ddl_token(data_type: Any) -> str:
    """Uppercase DDL type token for ``StructType.toDDL``."""
    return _descriptor_token(data_type, "ddl_token_from_descriptor", _ddl_token_python)


def _ddl_token_python(data_type: Any) -> str:
    """DDL token for descriptors the table cannot see (unknown subtype inside)."""
    return _nested_token_python(
        data_type,
        _datatype_to_ddl_token,
        lambda item: item.simpleString().upper(),
        upper=True,
    )
