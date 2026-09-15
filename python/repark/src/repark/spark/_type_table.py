"""Python-side bridge to the shared Rust type table.

The Rust ``type_table`` owns Arrow ↔ Spark-descriptor ↔ DDL semantics; this module holds the
Python descriptor encode/decode and the token fallbacks for descriptor trees containing foreign
``DataType`` subtypes the table cannot see. Class references resolve lazily so ``types.py`` keeps
a one-directional import of this module.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any

_AtomicRow = tuple[
    str | Callable[[Any], dict[str, Any]],
    str | Callable[[Any], str] | None,
    str | Callable[[Any], str] | None,
    str | Callable[[Any], str] | None,
    str | Callable[[Any], str] | None,
]
_ATOMIC_ROWS: dict[type, _AtomicRow] | None = None
_DESCRIPTOR_TYPES: dict[str, type] | None = None
_DESCRIPTOR_HEADS: dict[type, str | Callable[[Any], dict[str, Any]]] | None = None
_TYPES_MODULE: Any = None
_NATIVE_MODULE: Any = None
_NATIVE_FNS: dict[str, Any] = {}
_FIELD_NAME_COLON = re.compile(r"^([A-Za-z_][\w]*)\s*:\s*(.+)$")


def _types() -> Any:
    """The ``repark.spark.types`` module, resolved once after it finishes loading."""
    global _TYPES_MODULE
    if _TYPES_MODULE is None:
        from repark.spark import types as t

        _TYPES_MODULE = t
    return _TYPES_MODULE


def _native() -> Any:
    """The ``repark._native`` extension module, bound once after package load."""
    global _NATIVE_MODULE
    if _NATIVE_MODULE is None:
        from repark import _native

        _NATIVE_MODULE = _native
    return _NATIVE_MODULE


def _native_function(name: str) -> Callable[..., Any]:
    """One ``repark._native`` function, bound once."""
    function = _NATIVE_FNS.get(name)
    if function is None:
        function = getattr(_native(), name)
        _NATIVE_FNS[name] = function
    return function


def _inherited_type_name(data_type: Any) -> str:
    """Base ``simpleString`` for classes without an override: ``type(self).typeName()``."""
    return type(data_type).typeName()


def _atomic_rows() -> dict[type, _AtomicRow]:
    """Class→row table: ``(descriptor head or builder, simpleString, _engine_type, SQL, DDL)``.

    ``None`` in the SQL/DDL marker columns means base's ``isinstance`` chain has no arm for the
    class — the leaf falls to base's own ``_engine_type()`` / ``simpleString().upper()``.
    """
    global _ATOMIC_ROWS
    if _ATOMIC_ROWS is None:
        from repark.spark import types as t

        _ATOMIC_ROWS = {
            t.NullType: ("null", _inherited_type_name, "void", "VOID", "VOID"),
            t.StringType: (
                lambda data_type: {"kind": "string", "collation": data_type.collation},
                lambda data_type: (
                    "string"
                    if data_type.collation == "UTF8_BINARY"
                    else f"string collate {data_type.collation}"
                ),
                "string",
                "STRING",
                "STRING",
            ),
            t.CharType: (
                lambda data_type: {"kind": "char", "length": data_type.length},
                lambda data_type: f"char({data_type.length})",
                "string",
                "STRING",
                lambda data_type: f"CHAR({data_type.length})",
            ),
            t.VarcharType: (
                lambda data_type: {"kind": "varchar", "length": data_type.length},
                lambda data_type: f"varchar({data_type.length})",
                "string",
                "STRING",
                lambda data_type: f"VARCHAR({data_type.length})",
            ),
            t.BinaryType: ("binary", _inherited_type_name, "binary", "BINARY", "BINARY"),
            t.BooleanType: ("boolean", _inherited_type_name, "boolean", "BOOLEAN", "BOOLEAN"),
            t.DateType: ("date", _inherited_type_name, "date", "DATE", "DATE"),
            t.TimestampType: (
                "timestamp",
                _inherited_type_name,
                "timestamp",
                "TIMESTAMP",
                "TIMESTAMP",
            ),
            t.TimestampNTZType: (
                "timestamp_ntz",
                _inherited_type_name,
                "timestamp_ntz",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ",
            ),
            t.TimeType: (
                lambda data_type: {"kind": "time", "precision": data_type.precision},
                lambda data_type: f"time({data_type.precision})",
                lambda data_type: f"time({data_type.precision})",
                None,
                lambda data_type: f"TIME({data_type.precision})",
            ),
            t.DecimalType: (
                lambda data_type: {
                    "kind": "decimal",
                    "precision": data_type.precision,
                    "scale": data_type.scale,
                },
                lambda data_type: f"decimal({data_type.precision},{data_type.scale})",
                lambda data_type: f"decimal({data_type.precision},{data_type.scale})",
                lambda data_type: f"DECIMAL({data_type.precision},{data_type.scale})",
                lambda data_type: f"DECIMAL({data_type.precision},{data_type.scale})",
            ),
            t.DoubleType: ("double", _inherited_type_name, "double", "DOUBLE", "DOUBLE"),
            t.FloatType: ("float", "float", "float", "FLOAT", "FLOAT"),
            t.ByteType: ("byte", "tinyint", "byte", "TINYINT", "TINYINT"),
            t.IntegerType: ("integer", "int", "int", "INT", "INT"),
            t.LongType: ("long", "bigint", "long", "BIGINT", "BIGINT"),
            t.ShortType: ("short", "smallint", "short", "SMALLINT", "SMALLINT"),
            t.CalendarIntervalType: (
                "calendar_interval",
                "interval",
                "interval",
                None,
                "INTERVAL",
            ),
            t.DayTimeIntervalType: (
                lambda data_type: {
                    "kind": "day_time_interval",
                    "start": t.DayTimeIntervalType._fields[data_type.startField],
                    "end": t.DayTimeIntervalType._fields[data_type.endField],
                },
                lambda data_type: data_type._str_repr(),
                lambda data_type: data_type._str_repr(),
                None,
                None,
            ),
            t.YearMonthIntervalType: (
                lambda data_type: {
                    "kind": "year_month_interval",
                    "start": t.YearMonthIntervalType._fields[data_type.startField],
                    "end": t.YearMonthIntervalType._fields[data_type.endField],
                },
                lambda data_type: data_type._str_repr(),
                lambda data_type: data_type._str_repr(),
                None,
                None,
            ),
            t.VariantType: ("variant", "variant", "variant", None, "VARIANT"),
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


_ARROW_ORDER: tuple[type, ...] | None = None
_SQL_ORDER: tuple[type, ...] | None = None
_DDL_ORDER: tuple[type, ...] | None = None


def _arrow_order() -> tuple[type, ...]:
    """Base's ``repark_type_to_arrow`` isinstance order (integer family before string)."""
    global _ARROW_ORDER
    if _ARROW_ORDER is None:
        t = _types()
        _ARROW_ORDER = (
            t.NullType,
            t.BooleanType,
            t.ByteType,
            t.ShortType,
            t.IntegerType,
            t.LongType,
            t.FloatType,
            t.DoubleType,
            t.StringType,
            t.CharType,
            t.VarcharType,
            t.BinaryType,
            t.DateType,
            t.TimestampType,
            t.TimestampNTZType,
            t.DecimalType,
            t.ArrayType,
            t.MapType,
            t.StructType,
        )
    return _ARROW_ORDER


def _sql_order() -> tuple[type, ...]:
    """Base's ``_data_type_to_sql_type`` isinstance order (integer family first)."""
    global _SQL_ORDER
    if _SQL_ORDER is None:
        t = _types()
        _SQL_ORDER = (
            t.IntegerType,
            t.LongType,
            t.ShortType,
            t.ByteType,
            t.DoubleType,
            t.FloatType,
            t.BooleanType,
            t.StringType,
            t.CharType,
            t.VarcharType,
            t.BinaryType,
            t.DateType,
            t.TimestampType,
            t.TimestampNTZType,
            t.DecimalType,
            t.NullType,
            t.ArrayType,
            t.MapType,
            t.StructType,
        )
    return _SQL_ORDER


def _ddl_order() -> tuple[type, ...]:
    """Base's ``_datatype_to_ddl_token`` isinstance order (string/binary before int)."""
    global _DDL_ORDER
    if _DDL_ORDER is None:
        t = _types()
        _DDL_ORDER = (
            t.NullType,
            t.StringType,
            t.BinaryType,
            t.BooleanType,
            t.ByteType,
            t.ShortType,
            t.IntegerType,
            t.LongType,
            t.FloatType,
            t.DoubleType,
            t.DateType,
            t.TimestampType,
            t.TimestampNTZType,
            t.TimeType,
            t.DecimalType,
            t.ArrayType,
            t.MapType,
            t.StructType,
            t.VariantType,
            t.CalendarIntervalType,
            t.CharType,
            t.VarcharType,
        )
    return _DDL_ORDER


def _primary_class(data_type: Any, order: tuple[type, ...] | None = None) -> type | None:
    """First ``order`` class ``data_type`` is an instance of (base's isinstance chains)."""
    cls = type(data_type)
    if cls in _descriptor_heads():
        return cls
    t = _types()
    if cls is t.ArrayType or cls is t.MapType or cls is t.StructType:
        return cls
    if order is None or not isinstance(data_type, t.DataType):
        return None
    for klass in order:
        if isinstance(data_type, klass):
            return klass
    return None


def _atomic_token(data_type: Any, index: int, order: tuple[type, ...] | None = None) -> str | None:
    """Table token answer by column (0=simpleString, 1=_engine_type, 2=SQL, 3=DDL).

    For subclasses the first ``order`` class the instance is an ``isinstance`` of supplies
    the row — base's per-surface isinstance-chain order. ``order=None`` scans the class MRO
    instead (base's method-resolution order). ``None`` means no matching tabled class or no
    marker arm for it.
    """
    rows = _atomic_rows()
    cls = type(data_type)
    row = rows.get(cls)
    if row is None:
        candidates = cls.__mro__[1:] if order is None else order
        for parent in candidates:
            if isinstance(data_type, parent):
                row = rows.get(parent)
                if row is not None:
                    break
    if row is None:
        return None
    answer = row[index + 1]
    return answer if isinstance(answer, str) or answer is None else answer(data_type)


def _datatype_to_descriptor(
    data_type: Any, order: tuple[type, ...] | None = None
) -> dict[str, Any] | None:
    """Build the Rust type-table descriptor for a :class:`DataType`; ``None`` when unknown."""
    t = _types()
    heads = _descriptor_heads()
    head = heads.get(type(data_type))
    if head is not None:
        if isinstance(head, str):
            return {"kind": head}
        try:
            return head(data_type)
        except AttributeError:
            return None
    primary = _primary_class(data_type, order)

    if primary is t.ArrayType:
        element = _datatype_to_descriptor(data_type.elementType, order)
        if element is None:
            return None
        return {"kind": "array", "element": element, "contains_null": data_type.containsNull}
    if primary is t.MapType:
        key = _datatype_to_descriptor(data_type.keyType, order)
        value = _datatype_to_descriptor(data_type.valueType, order)
        if key is None or value is None:
            return None
        return {
            "kind": "map",
            "key": key,
            "value": value,
            "value_contains_null": data_type.valueContainsNull,
        }
    if primary is t.StructType:
        fields = [_field_descriptor(field, order) for field in data_type.fields]
        if any(field is None for field in fields):
            return None
        return {"kind": "struct", "fields": fields}
    if primary is not None:
        head = heads[primary]
        if isinstance(head, str):
            return {"kind": head}
        try:
            return head(data_type)
        except AttributeError:
            return None
    if order is None:
        if isinstance(data_type, t.ArrayType):
            element = _datatype_to_descriptor(data_type.elementType)
            if element is None:
                return None
            return {
                "kind": "array",
                "element": element,
                "contains_null": data_type.containsNull,
            }
        if isinstance(data_type, t.MapType):
            key = _datatype_to_descriptor(data_type.keyType)
            value = _datatype_to_descriptor(data_type.valueType)
            if key is None or value is None:
                return None
            return {
                "kind": "map",
                "key": key,
                "value": value,
                "value_contains_null": data_type.valueContainsNull,
            }
        if isinstance(data_type, t.StructType):
            fields = [_field_descriptor(field, None) for field in data_type.fields]
            if any(field is None for field in fields):
                return None
            return {"kind": "struct", "fields": fields}
    return None


def _field_descriptor(field: Any, order: tuple[type, ...] | None) -> dict[str, Any] | None:
    """Descriptor for a struct member; ``None`` when its data type is unknown."""
    child = _datatype_to_descriptor(field.dataType, order)
    if child is None:
        return None
    return {
        "kind": "field",
        "name": field.name,
        "type": child,
        "nullable": field.nullable,
        "metadata": None,
    }


def _descriptor_to_datatype(descriptor: Any) -> Any:
    """Construct the public Spark type for a Rust type-table descriptor tuple."""
    t = _types()

    kind = descriptor[0]
    atomic_class = _descriptor_types().get(kind)
    if atomic_class is not None:
        return atomic_class()
    if kind == "string":
        return t.StringType(descriptor[1])
    if kind == "char":
        return t.CharType(descriptor[1])
    if kind == "varchar":
        return t.VarcharType(descriptor[1])
    if kind == "time":
        return t.TimeType(descriptor[1])
    if kind == "decimal":
        return t.DecimalType(descriptor[1], descriptor[2])
    if kind in ("day_time_interval", "year_month_interval"):
        if kind == "day_time_interval":
            interval_class = t.DayTimeIntervalType
        else:
            interval_class = t.YearMonthIntervalType
        inverted_fields = interval_class._inverted_fields
        return interval_class(
            inverted_fields[descriptor[1]],
            inverted_fields[descriptor[2]],
        )
    if kind == "geometry":
        return t.GeometryType("ANY" if descriptor[1] == -1 else descriptor[1])
    if kind == "geography":
        return t.GeographyType("ANY" if descriptor[1] == -1 else descriptor[1])
    if kind == "array":
        return t.ArrayType(
            _descriptor_to_datatype(descriptor[1]),
            descriptor[2],
        )
    if kind == "map":
        return t.MapType(
            _descriptor_to_datatype(descriptor[1]),
            _descriptor_to_datatype(descriptor[2]),
            descriptor[3],
        )
    if kind == "struct":
        return t.StructType([_descriptor_to_datatype(field) for field in descriptor[1]])
    if kind == "field":
        metadata_text = descriptor[4]
        return t.StructField(
            descriptor[1],
            _descriptor_to_datatype(descriptor[2]),
            descriptor[3],
            json.loads(metadata_text) if metadata_text else {},
        )
    raise TypeError(f"unsupported type-table descriptor kind {kind!r}")


def _descriptor_token(
    data_type: Any,
    native_name: str,
    fallback: Callable[[Any], str],
    order: tuple[type, ...] | None = None,
) -> str:
    """Token answer from the shared type table (Python fallback on unknown)."""
    descriptor = _datatype_to_descriptor(data_type, order)
    if descriptor is None:
        return fallback(data_type)
    return _native_function(native_name)(descriptor)


def _leaf_ddl(data_type: Any) -> str:
    """``_datatype_to_ddl_token`` leaf: ordered marker arm, else ``simpleString().upper()``."""
    marker = _atomic_token(data_type, 3, _ddl_order())
    if marker is not None:
        return marker
    return data_type.simpleString().upper()


def _datatype_to_ddl_token(data_type: Any) -> str:
    """Uppercase DDL type token for ``StructType.toDDL``."""
    t = _types()
    primary = _primary_class(data_type, _ddl_order())
    if primary is t.ArrayType or primary is t.MapType or primary is t.StructType:
        return _descriptor_token(
            data_type, "ddl_token_from_descriptor", _ddl_token_python, _ddl_order()
        )
    return _leaf_ddl(data_type)


def _ddl_token_python(data_type: Any) -> str:
    """DDL token for descriptors the table cannot see (unknown subtype inside)."""
    t = _types()
    primary = _primary_class(data_type, _ddl_order())
    if primary is t.ArrayType:
        return f"ARRAY<{_datatype_to_ddl_token(data_type.elementType)}>"
    if primary is t.MapType:
        return (
            f"MAP<{_datatype_to_ddl_token(data_type.keyType)},"
            f"{_datatype_to_ddl_token(data_type.valueType)}>"
        )
    if primary is t.StructType:
        inner = ",".join(
            f"{field.name}:{_datatype_to_ddl_token(field.dataType)}" for field in data_type.fields
        )
        return f"STRUCT<{inner}>"
    return _leaf_ddl(data_type)


def _parse_field_list(text: str) -> Any:
    """Parse ``a int, b string`` or ``a: int, b: string`` into a :class:`StructType`."""
    t = _types()
    fields: list[Any] = []
    for part in t._split_top_level(text, ","):
        part = part.strip()
        if not part:
            continue
        colon_match = _FIELD_NAME_COLON.match(part)
        if colon_match is not None:
            name = colon_match.group(1).strip().strip('`"')
            type_text = colon_match.group(2)
            fields.append(t.StructField(name, t._parse_complex_or_atomic(type_text), True))
            continue
        tokens = part.split(None, 1)
        if len(tokens) != 2:
            raise ValueError(f"cannot parse field: {part!r}")
        name, type_text = tokens[0].strip().strip('`"'), tokens[1]
        fields.append(t.StructField(name, t._parse_complex_or_atomic(type_text), True))
    return t.StructType(fields)


def _parse_datatype_string_python(text: str) -> Any:
    """Base's pure-Python ``_parse_datatype_string`` (residue for unbounded integers)."""
    t = _types()
    stripped = text.strip()
    if not stripped:
        return t.StructType([])
    lower = stripped.lower()
    if (
        t._FIXED_DECIMAL.fullmatch(stripped)
        or t._LENGTH_CHAR.fullmatch(stripped)
        or t._LENGTH_VARCHAR.fullmatch(stripped)
        or t._TIME.fullmatch(stripped)
        or lower.startswith(("array<", "map<", "struct<"))
        or lower in t._ATOMIC_TYPE_NAMES
        or t._STRING_COLLATE.fullmatch(stripped)
    ):
        return t._parse_complex_or_atomic(stripped)
    if "," in stripped or ":" in stripped or " " in stripped:
        try:
            return _parse_field_list(stripped)
        except ValueError:
            pass
    return t._parse_complex_or_atomic(stripped)


def _parse_datatype_string(text: str) -> Any:
    """Parse a DDL / simpleString type or field list via the Rust type table.

    Parameters beyond ``i64`` and non-printable text leave the Rust path for base's Python
    parse, so value, ``simpleString`` and refusal bytes equal base.
    """
    if not text.isprintable():
        return _parse_datatype_string_python(text)
    try:
        return _descriptor_to_datatype(_native_function("spark_descriptor_from_ddl")(text))
    except OverflowError:
        return _parse_datatype_string_python(text)
