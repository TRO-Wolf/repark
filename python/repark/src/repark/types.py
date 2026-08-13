"""Spark SQL type objects for :meth:`repark.column.Column.cast` and schema surface.

Near-drop-in for ``pyspark.sql.types`` constructors used by scripts and the Apache
``test_types`` census (simpleString / typeName / json / fromDDL / StructType.add /
ArrayType / MapType / Row-adjacent). Each type maps to a *canonical engine type string*
(``_engine_type``) that native ``PyColumn.cast`` parses into an Arrow ``DataType``.

These are deliberately plain classes, not Pydantic models: PySpark scripts construct them
with the exact positional signatures ``StringType()`` / ``DecimalType(10, 4)`` /
``StringType("UTF8_LCASE")``.
"""

from __future__ import annotations

import calendar
import datetime
import json
import re
from collections.abc import Iterator
from typing import Any, ClassVar
from zoneinfo import ZoneInfo

# ==================================================================================================
# DataType base
# ==================================================================================================


class DataType:
    """Base class for the repark cast / schema type objects (like ``pyspark.sql.types.DataType``).

    Subclasses implement :meth:`_engine_type`, the canonical string the native cast understands.
    """

    def _engine_type(self) -> str:
        """Return the canonical engine type string (e.g. ``"string"``, ``"decimal(10,4)"``)."""
        return self.simpleString()

    @classmethod
    def typeName(cls) -> str:  # noqa: N802 — PySpark camelCase
        """PySpark ``DataType.typeName()`` — class name without the ``Type`` suffix, lowercased.

        Oracle (Spark 4.x): ``IntegerType().typeName() == "integer"``,
        ``StringType().typeName() == "string"``. Classmethod so ``IntegerType.typeName()`` works.
        """
        name = cls.__name__
        if name.endswith("Type"):
            name = name[: -len("Type")]
        return name.lower()

    def simpleString(self) -> str:  # noqa: N802 — PySpark camelCase
        """PySpark ``DataType.simpleString()`` — compact display form.

        Default is :meth:`typeName`; atomic types that Spark shortens (``int`` not ``integer``)
        override this.
        """
        return type(self).typeName()

    def jsonValue(self) -> str | dict[str, Any]:  # noqa: N802 — PySpark camelCase
        """PySpark ``DataType.jsonValue()`` — JSON-serializable type descriptor.

        Atomic types return the type name string (Spark 4.x). Complex types return a dict.
        """
        return type(self).typeName()

    def json(self) -> str:
        """JSON string of :meth:`jsonValue` (Spark separators / sort_keys)."""
        return json.dumps(self.jsonValue(), separators=(",", ":"), sort_keys=True)

    def needConversion(self) -> bool:  # noqa: N802 — PySpark camelCase
        """Whether Python ↔ internal conversion is required (Spark default False)."""
        return False

    def toInternal(self, obj: Any) -> Any:  # noqa: N802 — PySpark camelCase
        """Convert a Python object to the internal SQL representation."""
        return obj

    def fromInternal(self, obj: Any) -> Any:  # noqa: N802 — PySpark camelCase
        """Convert an internal SQL object to a native Python object."""
        return obj

    @classmethod
    def fromDDL(cls, ddl: str) -> DataType:  # noqa: N802 — PySpark camelCase
        """Parse a DDL / simpleString type or field-list into a :class:`DataType`.

        Pure-Python port of Spark 4 ``DataType.fromDDL`` (no JVM). Supports atomic names,
        ``decimal(p,s)``, ``char(n)`` / ``varchar(n)`` / ``time(n)``, ``array<…>``,
        ``map<k,v>``, ``struct<…>``, and field lists ``a int, b string`` / ``a: int, b: string``.
        """
        return _parse_datatype_string(ddl)

    def __eq__(self, other: object) -> bool:
        """Value equality: same class+state, or same type name+simpleString (overlay residue).

        After the pyspark→repark types overlay, private parsers may still construct *pyspark*
        class instances (captured in ``_all_mappable_types``) while user constructors are
        repark classes. Compare by type name + ``simpleString`` so ``test_parse_datatype_json``
        and schema equality hold across both.
        """
        if other is None or not hasattr(other, "simpleString"):
            return NotImplemented
        if type(self) is type(other):
            return getattr(self, "__dict__", {}) == getattr(other, "__dict__", {})
        if type(other).__name__ == type(self).__name__:
            try:
                return self.simpleString() == other.simpleString()  # type: ignore[operator]
            except Exception:
                return False
        return False

    def __hash__(self) -> int:
        """Hash on type name + simpleString (consistent with cross-impl :meth:`__eq__`)."""
        return hash((type(self).__name__, self.simpleString()))

    def __repr__(self) -> str:
        """Render as the class name (PySpark renders ``StringType()`` / ``DecimalType(10,4)``)."""
        return f"{type(self).__name__}()"


# ==================================================================================================
# Atomic types
# ==================================================================================================


class NullType(DataType):
    """Null / VOID type (Spark ``NullType``; ``typeName`` is ``void``)."""

    def __init__(self) -> None:
        """No-arg constructor (explicit so ``NullType(1)`` TypeError message is Spark-shaped)."""

    @classmethod
    def typeName(cls) -> str:  # noqa: N802
        """Spark uses ``void`` not ``null``."""
        return "void"

    def _engine_type(self) -> str:
        """Engine tag for null/void."""
        return "void"


class StringType(DataType):
    """UTF-8 string (Arrow ``Utf8``). Optional collation (Spark 4+)."""

    def __init__(self, collation: str = "UTF8_BINARY") -> None:
        """Store optional ``collation`` (default ``UTF8_BINARY``)."""
        self.collation = collation

    def isUTF8BinaryCollation(self) -> bool:  # noqa: N802 — PySpark camelCase
        """True when collation is the default binary."""
        return self.collation == "UTF8_BINARY"

    def _engine_type(self) -> str:
        """The engine string ``"string"`` (collation is schema metadata only)."""
        return "string"

    def simpleString(self) -> str:  # noqa: N802
        """``string`` or ``string collate NAME``."""
        if self.isUTF8BinaryCollation():
            return "string"
        return f"string collate {self.collation}"

    def jsonValue(self) -> str:  # noqa: N802
        """JSON form matches simpleString for collated strings."""
        return self.simpleString()

    def __repr__(self) -> str:
        """``StringType()`` or ``StringType('COLLATION')``."""
        if self.isUTF8BinaryCollation():
            return "StringType()"
        return f"StringType({self.collation!r})"


class CharType(DataType):
    """Fixed-length character type ``char(n)``."""

    def __init__(self, length: int) -> None:
        """Store ``length`` limitation."""
        self.length = length

    def _engine_type(self) -> str:
        """Engine string (stored as string)."""
        return "string"

    def simpleString(self) -> str:  # noqa: N802
        """``char(n)``."""
        return f"char({self.length})"

    def jsonValue(self) -> str:  # noqa: N802
        """JSON form ``char(n)``."""
        return self.simpleString()

    def __repr__(self) -> str:
        """``CharType(n)``."""
        return f"CharType({self.length})"


class VarcharType(DataType):
    """Variable-length character type ``varchar(n)``."""

    def __init__(self, length: int) -> None:
        """Store ``length`` limitation."""
        self.length = length

    def _engine_type(self) -> str:
        """Engine string (stored as string)."""
        return "string"

    def simpleString(self) -> str:  # noqa: N802
        """``varchar(n)``."""
        return f"varchar({self.length})"

    def jsonValue(self) -> str:  # noqa: N802
        """JSON form ``varchar(n)``."""
        return self.simpleString()

    def __repr__(self) -> str:
        """``VarcharType(n)``."""
        return f"VarcharType({self.length})"


class BinaryType(DataType):
    """Binary / byte array (Arrow ``Binary``)."""

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """The engine string ``"binary"``."""
        return "binary"


class BooleanType(DataType):
    """Boolean (Arrow ``Boolean``)."""

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """The engine string ``"boolean"``."""
        return "boolean"


class DateType(DataType):
    """Calendar date, days since the epoch (Arrow ``Date32``)."""

    EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """The engine string ``"date"``."""
        return "date"

    def needConversion(self) -> bool:  # noqa: N802
        """Dates convert between ``datetime.date`` and day ordinals."""
        return True

    def toInternal(self, value: datetime.date | None) -> int | None:  # noqa: N802
        """Days since 1970-01-01."""
        if value is not None:
            return value.toordinal() - self.EPOCH_ORDINAL
        return None

    def fromInternal(self, value: int | None) -> datetime.date | None:  # noqa: N802
        """``datetime.date`` from day ordinal."""
        if value is not None:
            return datetime.date.fromordinal(value + self.EPOCH_ORDINAL)
        return None


class TimestampType(DataType):
    """Microsecond LTZ timestamp (Arrow ``timestamp[us, tz=UTC]``)."""

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """The engine string ``"timestamp"`` (native parse maps to µs+UTC)."""
        return "timestamp"

    def needConversion(self) -> bool:  # noqa: N802
        """Timestamps convert to microsecond epoch ints."""
        return True

    def toInternal(self, value: datetime.datetime | None) -> int | None:  # noqa: N802
        """Microseconds since epoch (aware → UTC; naive localized in the session zone)."""
        if value is not None:
            if value.tzinfo is not None:
                seconds = calendar.timegm(value.utctimetuple())
            else:
                from repark.session.session_time_zone import localize_naive_datetime_to_utc

                utc = localize_naive_datetime_to_utc(value)
                seconds = calendar.timegm(utc.utctimetuple())
            return int(seconds) * 1_000_000 + value.microsecond
        return None

    def fromInternal(self, value: int | None) -> datetime.datetime | None:  # noqa: N802
        """Naive session-zone wall from microsecond epoch (not host ``mktime``)."""
        if value is not None:
            from repark.session.session_time_zone import active_session_time_zone

            zone = ZoneInfo(active_session_time_zone())
            return datetime.datetime.fromtimestamp(value // 1_000_000, tz=zone).replace(
                tzinfo=None,
                microsecond=value % 1_000_000,
            )
        return None


class TimestampNTZType(DataType):
    """Timestamp without time zone (Spark ``timestamp_ntz``)."""

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """Engine tag."""
        return "timestamp_ntz"

    @classmethod
    def typeName(cls) -> str:  # noqa: N802
        """``timestamp_ntz``."""
        return "timestamp_ntz"

    def needConversion(self) -> bool:  # noqa: N802
        """NTZ timestamps convert to microsecond epoch ints."""
        return True

    def toInternal(self, value: datetime.datetime | None) -> int | None:  # noqa: N802
        """Microseconds since epoch treating the wall time as UTC."""
        if value is not None:
            seconds = calendar.timegm(value.timetuple())
            return int(seconds) * 1_000_000 + value.microsecond
        return None

    def fromInternal(self, value: int | None) -> datetime.datetime | None:  # noqa: N802
        """Naive datetime from microsecond epoch (UTC)."""
        if value is not None:
            return datetime.datetime.utcfromtimestamp(value // 1_000_000).replace(
                microsecond=value % 1_000_000
            )
        return None


class TimeType(DataType):
    """Time-of-day type ``time(precision)`` (Spark 4.1+)."""

    def __init__(self, precision: int = 6) -> None:
        """Store fractional-second ``precision`` (default 6)."""
        self.precision = precision

    def _engine_type(self) -> str:
        """Engine tag."""
        return f"time({self.precision})"

    def simpleString(self) -> str:  # noqa: N802
        """``time(n)``."""
        return f"time({self.precision})"

    def jsonValue(self) -> str:  # noqa: N802
        """JSON form ``time(n)``."""
        return self.simpleString()

    def __repr__(self) -> str:
        """``TimeType(n)``."""
        return f"TimeType({self.precision})"


class DecimalType(DataType):
    """Fixed-precision decimal (Arrow ``Decimal128(precision, scale)``).

    Constructed with PySpark's positional signature ``DecimalType(precision, scale)``; the defaults
    ``(10, 0)`` match PySpark's own defaults.
    """

    def __init__(self, precision: int = 10, scale: int = 0) -> None:
        """Store the decimal ``precision`` (total digits) and ``scale`` (fractional digits)."""
        self.precision = precision
        self.scale = scale
        self.hasPrecisionInfo = True  # public Spark attribute

    def _engine_type(self) -> str:
        """The engine string ``"decimal(precision,scale)"``."""
        return f"decimal({self.precision},{self.scale})"

    def simpleString(self) -> str:  # noqa: N802
        """Spark form ``decimal(p,s)``."""
        return f"decimal({self.precision},{self.scale})"

    def jsonValue(self) -> str:  # noqa: N802
        """Spark returns the simpleString form for decimals."""
        return self.simpleString()

    def __repr__(self) -> str:
        """Render as ``DecimalType(precision,scale)`` (PySpark's repr shape)."""
        return f"DecimalType({self.precision},{self.scale})"


class DoubleType(DataType):
    """64-bit IEEE-754 float (Arrow ``Float64``)."""

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """The engine string ``"double"``."""
        return "double"


class FloatType(DataType):
    """32-bit IEEE-754 float (Arrow ``Float32``)."""

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """The engine string ``"float"``."""
        return "float"

    def simpleString(self) -> str:  # noqa: N802
        """Spark short form ``float``."""
        return "float"


class ByteType(DataType):
    """8-bit signed integer (Arrow ``Int8``)."""

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """The engine string ``"byte"`` / tinyint."""
        return "byte"

    def simpleString(self) -> str:  # noqa: N802
        """Spark short form ``tinyint``."""
        return "tinyint"


class IntegerType(DataType):
    """32-bit signed integer (Arrow ``Int32``)."""

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """The engine string ``"int"``."""
        return "int"

    def simpleString(self) -> str:  # noqa: N802
        """Spark short form ``"int"`` (``typeName`` remains ``"integer"``)."""
        return "int"


class LongType(DataType):
    """64-bit signed integer (Arrow ``Int64``; PySpark ``LongType``).

    Engine cast string is ``"long"`` / bigint path. Distinct from :class:`IntegerType` so
    ``Column.cast(LongType())`` and schema literals resolve (X1 census / ColumnTests).
    """

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """The engine string ``"long"``."""
        return "long"

    def simpleString(self) -> str:  # noqa: N802
        """Spark short form ``"bigint"`` (``typeName`` remains ``"long"``)."""
        return "bigint"


class ShortType(DataType):
    """16-bit signed integer (Arrow ``Int16``)."""

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """The engine string ``"short"``."""
        return "short"

    def simpleString(self) -> str:  # noqa: N802
        """Spark short form ``smallint``."""
        return "smallint"


class CalendarIntervalType(DataType):
    """Calendar interval type (Spark ``interval`` display)."""

    def __init__(self) -> None:
        """No-arg only — ``CalendarIntervalType(3)`` must TypeError with Spark's message."""

    @classmethod
    def typeName(cls) -> str:  # noqa: N802
        """Spark typeName is ``interval`` (not ``calendarinterval``)."""
        return "interval"

    def _engine_type(self) -> str:
        """Engine tag."""
        return "interval"

    def simpleString(self) -> str:  # noqa: N802
        """Spark display ``interval``."""
        return "interval"


class DayTimeIntervalType(DataType):
    """Day-time ANSI interval type (Spark ``DayTimeIntervalType`` / ``datetime.timedelta``).

    E1: constructor validation raises :class:`repark.errors.PySparkRuntimeError` with
    ``INVALID_INTERVAL_CASTING`` so Apache ``check_error`` isinstance + parameter-key
    equality PASSes (G16 / true-EC family A).
    """

    DAY = 0
    HOUR = 1
    MINUTE = 2
    SECOND = 3

    _fields: ClassVar[dict[int, str]] = {
        DAY: "day",
        HOUR: "hour",
        MINUTE: "minute",
        SECOND: "second",
    }
    _inverted_fields: ClassVar[dict[str, int]] = {
        "day": DAY,
        "hour": HOUR,
        "minute": MINUTE,
        "second": SECOND,
    }

    def __init__(
        self,
        startField: int | None = None,  # noqa: N803 — PySpark arg name
        endField: int | None = None,  # noqa: N803
    ) -> None:
        """Build a day-time interval field range (defaults day→second)."""
        start_field = startField
        end_field = endField
        if start_field is None and end_field is None:
            start_field = DayTimeIntervalType.DAY
            end_field = DayTimeIntervalType.SECOND
        elif start_field is not None and end_field is None:
            end_field = start_field

        fields = DayTimeIntervalType._fields
        if start_field not in fields or end_field not in fields:
            from repark.errors import PySparkRuntimeError

            raise PySparkRuntimeError(
                errorClass="INVALID_INTERVAL_CASTING",
                messageParameters={
                    "start_field": str(start_field),
                    "end_field": str(end_field),
                },
            )
        self.startField = start_field
        self.endField = end_field

    def _str_repr(self) -> str:
        fields = DayTimeIntervalType._fields
        start_name = fields[self.startField]
        end_name = fields[self.endField]
        if start_name == end_name:
            return f"interval {start_name}"
        return f"interval {start_name} to {end_name}"

    def simpleString(self) -> str:  # noqa: N802
        """Spark ``interval day to second`` / ``interval hour`` form."""
        return self._str_repr()

    def jsonValue(self) -> str:  # noqa: N802
        """JSON value is the simpleString (Spark 4)."""
        return self._str_repr()

    def _engine_type(self) -> str:
        """Engine tag for cast surface."""
        return self._str_repr()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.startField}, {self.endField})"


class YearMonthIntervalType(DataType):
    """Year-month ANSI interval type (Spark ``YearMonthIntervalType``).

    E1: constructor validation raises :class:`repark.errors.PySparkRuntimeError` with
    ``INVALID_INTERVAL_CASTING`` (true-EC family A / G16).
    """

    YEAR = 0
    MONTH = 1

    _fields: ClassVar[dict[int, str]] = {
        YEAR: "year",
        MONTH: "month",
    }
    _inverted_fields: ClassVar[dict[str, int]] = {
        "year": YEAR,
        "month": MONTH,
    }

    def __init__(
        self,
        startField: int | None = None,  # noqa: N803 — PySpark arg name
        endField: int | None = None,  # noqa: N803
    ) -> None:
        """Build a year-month interval field range (defaults year→month)."""
        start_field = startField
        end_field = endField
        if start_field is None and end_field is None:
            start_field = YearMonthIntervalType.YEAR
            end_field = YearMonthIntervalType.MONTH
        elif start_field is not None and end_field is None:
            end_field = start_field

        fields = YearMonthIntervalType._fields
        if start_field not in fields or end_field not in fields:
            from repark.errors import PySparkRuntimeError

            raise PySparkRuntimeError(
                errorClass="INVALID_INTERVAL_CASTING",
                messageParameters={
                    "start_field": str(start_field),
                    "end_field": str(end_field),
                },
            )
        self.startField = start_field
        self.endField = end_field

    def _str_repr(self) -> str:
        fields = YearMonthIntervalType._fields
        start_name = fields[self.startField]
        end_name = fields[self.endField]
        if start_name == end_name:
            return f"interval {start_name}"
        return f"interval {start_name} to {end_name}"

    def simpleString(self) -> str:  # noqa: N802
        """Spark ``interval year to month`` / ``interval year`` form."""
        return self._str_repr()

    def jsonValue(self) -> str:  # noqa: N802
        """JSON value is the simpleString (Spark 4)."""
        return self._str_repr()

    def _engine_type(self) -> str:
        """Engine tag for cast surface."""
        return self._str_repr()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.startField}, {self.endField})"


class VariantType(DataType):
    """Spark Variant type marker (schema / DDL surface)."""

    def __init__(self) -> None:
        """No-arg constructor."""

    def _engine_type(self) -> str:
        """Engine tag."""
        return "variant"

    def simpleString(self) -> str:  # noqa: N802
        """``variant``."""
        return "variant"


# ==================================================================================================
# Complex types — Array / Map / Struct
# ==================================================================================================


class ArrayType(DataType):
    """Array data type (``array<element>``)."""

    def __init__(self, elementType: DataType, containsNull: bool = True) -> None:  # noqa: N803
        """Store ``elementType`` and whether the array may contain nulls."""
        if not isinstance(elementType, DataType):
            raise TypeError(f"elementType {elementType!r} should be an instance of {DataType}")
        self.elementType = elementType
        self.containsNull = containsNull

    def _engine_type(self) -> str:
        """Engine array string."""
        return f"array<{self.elementType._engine_type()}>"

    def simpleString(self) -> str:  # noqa: N802
        """``array<elementSimple>``."""
        return f"array<{self.elementType.simpleString()}>"

    def jsonValue(self) -> dict[str, Any]:  # noqa: N802
        """Spark array JSON descriptor."""
        return {
            "type": type(self).typeName(),
            "elementType": self.elementType.jsonValue(),
            "containsNull": self.containsNull,
        }

    @classmethod
    def fromJson(  # noqa: N802
        cls,
        json: dict[str, Any],
        fieldPath: str = "",  # noqa: N803 — Spark API name
        collationsMap: dict[str, str] | None = None,  # noqa: N803 — Spark API name
    ) -> ArrayType:
        """Build from Spark datatype JSON (optional collation map for element)."""
        element_path = "element" if fieldPath == "" else f"{fieldPath}.element"
        element_type = _parse_datatype_json_value(json["elementType"], element_path, collationsMap)
        return cls(element_type, json.get("containsNull", True))

    def __repr__(self) -> str:
        """``ArrayType(IntegerType(), True)``."""
        return f"ArrayType({self.elementType!r}, {self.containsNull})"


class MapType(DataType):
    """Map data type (``map<key,value>``)."""

    def __init__(
        self,
        keyType: DataType,  # noqa: N803
        valueType: DataType,  # noqa: N803
        valueContainsNull: bool = True,  # noqa: N803
    ) -> None:
        """Store key/value types and value nullability."""
        if not isinstance(keyType, DataType):
            raise TypeError(f"keyType {keyType!r} should be an instance of {DataType}")
        if not isinstance(valueType, DataType):
            raise TypeError(f"valueType {valueType!r} should be an instance of {DataType}")
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def _engine_type(self) -> str:
        """Engine map string."""
        return f"map<{self.keyType._engine_type()},{self.valueType._engine_type()}>"

    def simpleString(self) -> str:  # noqa: N802
        """``map<key,value>``."""
        return f"map<{self.keyType.simpleString()},{self.valueType.simpleString()}>"

    def jsonValue(self) -> dict[str, Any]:  # noqa: N802
        """Spark map JSON descriptor."""
        return {
            "type": type(self).typeName(),
            "keyType": self.keyType.jsonValue(),
            "valueType": self.valueType.jsonValue(),
            "valueContainsNull": self.valueContainsNull,
        }

    @classmethod
    def fromJson(  # noqa: N802
        cls,
        json: dict[str, Any],
        fieldPath: str = "",  # noqa: N803 — Spark API name
        collationsMap: dict[str, str] | None = None,  # noqa: N803 — Spark API name
    ) -> MapType:
        """Build from Spark datatype JSON."""
        key_path = "key" if fieldPath == "" else f"{fieldPath}.key"
        value_path = "value" if fieldPath == "" else f"{fieldPath}.value"
        return cls(
            _parse_datatype_json_value(json["keyType"], key_path, collationsMap),
            _parse_datatype_json_value(json["valueType"], value_path, collationsMap),
            json.get("valueContainsNull", True),
        )

    def __repr__(self) -> str:
        """``MapType(StringType(), IntegerType(), True)``."""
        return f"MapType({self.keyType!r}, {self.valueType!r}, {self.valueContainsNull})"


class StructField(DataType):
    """A named field inside a :class:`StructType` (PySpark ``StructField``)."""

    def __init__(
        self,
        name: str,
        dataType: DataType,  # noqa: N803
        nullable: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Store field ``name``, ``dataType``, nullability, and optional ``metadata``."""
        if not isinstance(dataType, DataType):
            raise TypeError(f"dataType {dataType!r} should be an instance of {DataType}")
        if not isinstance(name, str):
            raise TypeError(f"field name {name!r} should be a string")
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata: dict[str, Any] = metadata if metadata is not None else {}

    def _engine_type(self) -> str:
        """Field is not a cast target; used only in struct engine strings."""
        return f"{self.name}:{self.dataType._engine_type()}"

    def simpleString(self) -> str:  # noqa: N802
        """``name:type``."""
        return f"{self.name}:{self.dataType.simpleString()}"

    def typeName(self) -> str:  # type: ignore[override]  # noqa: N802
        """Spark raises when ``typeName`` is called on a field (not a type)."""
        raise TypeError("StructField does not have typeName().")

    def jsonValue(self) -> dict[str, Any]:  # noqa: N802
        """Spark field JSON (name / type / nullable / metadata)."""
        return {
            "name": self.name,
            "type": self.dataType.jsonValue(),
            "nullable": self.nullable,
            "metadata": self.metadata,
        }

    @classmethod
    def fromJson(cls, json: dict[str, Any]) -> StructField:  # noqa: N802
        """Build from Spark field JSON, applying ``metadata.__COLLATIONS``.

        Spark 4 stores collation on the field metadata map (type token stays ``"string"``).
        Construction of a collated :class:`StringType` stays legal (A5); first evaluation
        refuses. Popping the key without applying it was the G15 silently-wrong-count path.
        """
        metadata = dict(json.get("metadata") or {})
        field_name = str(json.get("name", ""))
        collations_map = _collations_map_from_field_metadata(metadata, field_name)
        metadata.pop(_COLLATIONS_METADATA_KEY, None)
        metadata.pop("collations", None)
        return cls(
            json["name"],
            _parse_datatype_json_value(json["type"], field_name, collations_map or None),
            json.get("nullable", True),
            metadata,
        )

    def __eq__(self, other: object) -> bool:
        """Value equality on name, type, nullability (metadata ignored for schema eq)."""
        if not isinstance(other, StructField):
            return NotImplemented
        return (
            self.name == other.name
            and self.dataType == other.dataType
            and self.nullable == other.nullable
        )

    def __hash__(self) -> int:
        """Hash name/type/nullability."""
        return hash((self.name, self.dataType, self.nullable))

    def __repr__(self) -> str:
        """``StructField('name', Type(), nullable)`` — Spark omits metadata in repr."""
        return f"StructField({self.name!r}, {self.dataType!r}, {self.nullable})"


class StructType(DataType):
    """A schema of named fields (PySpark ``StructType``)."""

    def __init__(self, fields: list[StructField] | None = None) -> None:
        """Store an ordered list of :class:`StructField` values."""
        if not fields:
            self.fields: list[StructField] = []
            self.names: list[str] = []
        else:
            self.fields = list(fields)
            self.names = [field.name for field in self.fields]
            if not all(isinstance(field, StructField) for field in self.fields):
                raise TypeError("fields should be a list of StructField")

    def _engine_type(self) -> str:
        """Struct engine string ``struct<field:type,...>``."""
        return (
            "struct<"
            + ",".join(f"{field.name}:{field.dataType._engine_type()}" for field in self.fields)
            + ">"
        )

    def simpleString(self) -> str:  # noqa: N802
        """Spark ``StructType.simpleString()`` — ``struct<field:type,...>``."""
        return "struct<" + ",".join(field.simpleString() for field in self.fields) + ">"

    def jsonValue(self) -> dict[str, Any]:  # noqa: N802
        """Spark ``StructType.jsonValue()`` — type plus fields list."""
        return {
            "type": type(self).typeName(),
            "fields": [field.jsonValue() for field in self.fields],
        }

    @classmethod
    def fromJson(cls, json: dict[str, Any]) -> StructType:  # noqa: N802
        """Build from Spark struct JSON."""
        return cls([StructField.fromJson(field) for field in json["fields"]])

    def add(
        self,
        field: str | StructField,
        data_type: str | DataType | None = None,
        nullable: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> StructType:
        """Append a field (name+type or :class:`StructField`); return ``self`` for chaining."""
        if isinstance(field, StructField):
            self.fields.append(field)
            self.names.append(field.name)
            return self
        if isinstance(field, str) and data_type is None:
            raise ValueError(
                "data_type is required when passing the name of a struct_field to create"
            )
        resolved: DataType
        if isinstance(data_type, str):
            resolved = _parse_datatype_json_value(data_type)
        elif isinstance(data_type, DataType):
            resolved = data_type
        elif hasattr(data_type, "simpleString") and hasattr(type(data_type), "typeName"):
            # pyspark / UDT leftovers after overlay — accept duck-typed DataType-like objects
            # by wrapping through their simpleString when possible.
            try:
                resolved = _parse_complex_or_atomic(data_type.simpleString())  # type: ignore[union-attr]
            except Exception as error:
                raise TypeError(
                    f"data_type must be str or DataType, got {type(data_type).__name__}"
                ) from error
        else:
            raise TypeError(f"data_type must be str or DataType, got {type(data_type).__name__}")
        self.fields.append(StructField(field, resolved, nullable, metadata))
        self.names.append(field)
        return self

    def fieldNames(self) -> list[str]:  # noqa: N802 — PySpark camelCase
        """Field names in order (Spark ``fieldNames``)."""
        return list(self.names)

    def toDDL(self) -> str:  # noqa: N802 — PySpark camelCase
        """DDL field list (``a INT,b STRING NOT NULL``) — pure Python Spark 4 shape."""
        parts: list[str] = []
        for field in self.fields:
            type_sql = _datatype_to_ddl_token(field.dataType)
            null_suffix = "" if field.nullable else " NOT NULL"
            parts.append(f"{field.name} {type_sql}{null_suffix}")
        return ",".join(parts)

    def treeString(  # noqa: N802 — PySpark camelCase
        self,
        maxDepth: int = -1,  # noqa: N803 — Spark API name
    ) -> str:
        """Spark ``printSchema`` tree (``root\\n |-- name: type (nullable = …)``).

        ``maxDepth`` ≤ 0 prints the full tree (Spark). ``maxDepth`` ≥ 1 truncates nested
        children so only ``maxDepth`` levels of fields appear (Apache ``test_tree_string``).
        """
        depth = maxDepth if maxDepth and maxDepth > 0 else 10_000
        lines = ["root"]
        _append_datatype_tree(self, lines, prefix=" |", remaining_depth=depth, as_root=True)
        lines.append("")
        return "\n".join(lines)

    def __iter__(self) -> Iterator[StructField]:
        """Iterate fields."""
        return iter(self.fields)

    def __len__(self) -> int:
        """Number of fields."""
        return len(self.fields)

    def __getitem__(self, key: str | int | slice) -> Any:
        """Access field by name, index, or slice (slice → :class:`StructType`)."""
        if isinstance(key, str):
            for field in self.fields:
                if field.name == key:
                    return field
            raise KeyError(key)
        if isinstance(key, int):
            return self.fields[key]
        if isinstance(key, slice):
            return StructType(self.fields[key])
        raise TypeError(f"StructType indices must be str, int, or slice, not {type(key).__name__}")

    def __repr__(self) -> str:
        """Render as ``StructType([StructField(...), ...])``."""
        return f"StructType([{', '.join(repr(field) for field in self.fields)}])"


# ==================================================================================================
# DDL / JSON parse helpers (pure Python — no JVM)
# ==================================================================================================


_ATOMIC_TYPE_NAMES: dict[str, type[DataType]] = {
    "string": StringType,
    "str": StringType,
    # Bare VARCHAR (no length) is treated as string — nested engine markers from
    # session._data_type_to_sql_type historically emitted VARCHAR (octo X2 C1).
    "varchar": StringType,
    "binary": BinaryType,
    "boolean": BooleanType,
    "bool": BooleanType,
    "byte": ByteType,
    "tinyint": ByteType,
    "short": ShortType,
    "smallint": ShortType,
    "int": IntegerType,
    "integer": IntegerType,
    "long": LongType,
    "bigint": LongType,
    "float": FloatType,
    "real": FloatType,
    "double": DoubleType,
    "date": DateType,
    "timestamp": TimestampType,
    "timestamp_ntz": TimestampNTZType,
    "void": NullType,
    "null": NullType,
    "variant": VariantType,
    "interval": CalendarIntervalType,
    "calendarinterval": CalendarIntervalType,
}

_FIXED_DECIMAL = re.compile(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.I)
_LENGTH_CHAR = re.compile(r"char\s*\(\s*(\d+)\s*\)", re.I)
_LENGTH_VARCHAR = re.compile(r"varchar\s*\(\s*(\d+)\s*\)", re.I)
_TIME = re.compile(r"time\s*\(\s*(\d+)\s*\)", re.I)
_STRING_COLLATE = re.compile(r"string\s+collate\s+(\w+)", re.I)


def _parse_atomic_token(token: str) -> DataType | None:
    """Parse a single atomic / parameterized type token; None if not atomic-shaped."""
    stripped = token.strip()
    if not stripped:
        return None
    lower = stripped.lower()
    if lower in _ATOMIC_TYPE_NAMES:
        return _ATOMIC_TYPE_NAMES[lower]()
    match = _FIXED_DECIMAL.fullmatch(stripped)
    if match:
        return DecimalType(int(match.group(1)), int(match.group(2)))
    match = _LENGTH_CHAR.fullmatch(stripped)
    if match:
        return CharType(int(match.group(1)))
    match = _LENGTH_VARCHAR.fullmatch(stripped)
    if match:
        return VarcharType(int(match.group(1)))
    match = _TIME.fullmatch(stripped)
    if match:
        return TimeType(int(match.group(1)))
    match = _STRING_COLLATE.fullmatch(stripped)
    if match:
        return StringType(match.group(1))
    if lower == "decimal":
        return DecimalType()
    if lower.startswith("time"):
        return TimeType()
    return None


def _split_top_level(text: str, separator: str = ",") -> list[str]:
    """Split on ``separator`` not inside ``<>`` / ``()``."""
    parts: list[str] = []
    depth_angle = 0
    depth_paren = 0
    start = 0
    for index, char in enumerate(text):
        if char == "<":
            depth_angle += 1
        elif char == ">":
            depth_angle = max(0, depth_angle - 1)
        elif char == "(":
            depth_paren += 1
        elif char == ")":
            depth_paren = max(0, depth_paren - 1)
        elif char == separator and depth_angle == 0 and depth_paren == 0:
            parts.append(text[start:index])
            start = index + 1
    parts.append(text[start:])
    return parts


def _parse_complex_or_atomic(text: str) -> DataType:
    """Parse ``array<…>`` / ``map<…>`` / ``struct<…>`` or atomic token."""
    stripped = text.strip()
    lower = stripped.lower()
    if lower.startswith("array<") and stripped.endswith(">"):
        inner = stripped[stripped.index("<") + 1 : -1]
        return ArrayType(_parse_complex_or_atomic(inner), True)
    if lower.startswith("map<") and stripped.endswith(">"):
        inner = stripped[stripped.index("<") + 1 : -1]
        parts = _split_top_level(inner, ",")
        if len(parts) != 2:
            raise ValueError(f"cannot parse map type: {text!r}")
        return MapType(
            _parse_complex_or_atomic(parts[0]),
            _parse_complex_or_atomic(parts[1]),
            True,
        )
    if lower.startswith("struct<") and stripped.endswith(">"):
        inner = stripped[stripped.index("<") + 1 : -1].strip()
        if not inner:
            return StructType([])
        fields: list[StructField] = []
        for part in _split_top_level(inner, ","):
            part = part.strip()
            if not part:
                continue
            if ":" in part:
                name, type_text = part.split(":", 1)
            else:
                # ``name type`` form inside struct<> is uncommon; require colon.
                tokens = part.split(None, 1)
                if len(tokens) != 2:
                    raise ValueError(f"cannot parse struct field: {part!r}")
                name, type_text = tokens
            fields.append(
                StructField(name.strip().strip('`"'), _parse_complex_or_atomic(type_text), True)
            )
        return StructType(fields)
    atomic = _parse_atomic_token(stripped)
    if atomic is not None:
        return atomic
    raise ValueError(f"cannot parse datatype: {text!r}")


def _parse_field_list(text: str) -> StructType:
    """Parse ``a int, b string`` or ``a: int, b: string`` into a StructType.

    Colon form only when the *name* is immediately followed by ``:`` (``a: int``). Do not
    split on colons inside nested ``STRUCT<field: type>`` when the outer form is
    space-separated (``c2 STRUCT<c3: INT>`` — Apache ``test_tree_string``).
    """
    fields: list[StructField] = []
    name_colon = re.compile(r"^([A-Za-z_][\w]*)\s*:\s*(.+)$")
    for part in _split_top_level(text, ","):
        part = part.strip()
        if not part:
            continue
        colon_match = name_colon.match(part)
        if colon_match is not None:
            name = colon_match.group(1).strip().strip('`"')
            type_text = colon_match.group(2)
            fields.append(StructField(name, _parse_complex_or_atomic(type_text), True))
            continue
        tokens = part.split(None, 1)
        if len(tokens) != 2:
            raise ValueError(f"cannot parse field: {part!r}")
        name, type_text = tokens[0].strip().strip('`"'), tokens[1]
        fields.append(StructField(name, _parse_complex_or_atomic(type_text), True))
    return StructType(fields)


def _parse_datatype_string(text: str) -> DataType:
    """Parse a DDL / simpleString type or field list (Spark ``_parse_datatype_string`` shape)."""
    stripped = text.strip()
    if not stripped:
        return StructType([])
    lower = stripped.lower()
    # Parameterized atomics / complex types without a field name — parse as types first.
    if (
        _FIXED_DECIMAL.fullmatch(stripped)
        or _LENGTH_CHAR.fullmatch(stripped)
        or _LENGTH_VARCHAR.fullmatch(stripped)
        or _TIME.fullmatch(stripped)
        or lower.startswith(("array<", "map<", "struct<"))
        or lower in _ATOMIC_TYPE_NAMES
        or _STRING_COLLATE.fullmatch(stripped)
    ):
        return _parse_complex_or_atomic(stripped)
    # Field list: ``a: int``, ``a int, b string``, ``a time(6)`` (single field).
    if "," in stripped or ":" in stripped or " " in stripped:
        try:
            return _parse_field_list(stripped)
        except ValueError:
            pass
    return _parse_complex_or_atomic(stripped)


_COLLATIONS_METADATA_KEY = "__COLLATIONS"


def _collation_name_from_json_value(value: Any) -> str:
    """Spark stores ``provider.NAME`` under ``__COLLATIONS``; a bare name is kept as-is.

    Construction does not validate the provider (A5). ``icu.UNICODE_CI`` → ``UNICODE_CI``.
    """
    text = str(value)
    parts = text.split(".")
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[1]
    return text


def _collations_map_from_field_metadata(
    metadata: dict[str, Any], field_name: str
) -> dict[str, str]:
    """Read Spark field ``__COLLATIONS`` (or a ``collations`` alias) into a path→name map."""
    raw = metadata.get(_COLLATIONS_METADATA_KEY)
    if raw is None:
        raw = metadata.get("collations")
    if raw is None:
        return {}
    if isinstance(raw, str):
        return {field_name: _collation_name_from_json_value(raw)}
    if isinstance(raw, dict):
        return {str(key): _collation_name_from_json_value(item) for key, item in raw.items()}
    return {}


def _parse_datatype_json_value(
    json_value: dict[str, Any] | str,
    field_path: str = "",
    collations_map: dict[str, str] | None = None,
) -> DataType:
    """Parse Spark datatype JSON value (string or dict) into a :class:`DataType`."""
    if not isinstance(json_value, dict):
        if (
            collations_map is not None
            and field_path in collations_map
            and str(json_value).lower() == "string"
        ):
            return StringType(collations_map[field_path])
        atomic = _parse_atomic_token(str(json_value))
        if atomic is not None:
            return atomic
        # Fall through to complex simpleString forms.
        return _parse_complex_or_atomic(str(json_value))
    type_name = json_value["type"]
    if type_name == "array":
        return ArrayType.fromJson(json_value, field_path, collations_map)
    if type_name == "map":
        return MapType.fromJson(json_value, field_path, collations_map)
    if type_name == "struct":
        return StructType.fromJson(json_value)
    if type_name in _ATOMIC_TYPE_NAMES:
        if collations_map is not None and field_path in collations_map and type_name == "string":
            return StringType(collations_map[field_path])
        return _ATOMIC_TYPE_NAMES[type_name]()
    raise ValueError(f"unsupported JSON data type: {type_name!r}")


def _tree_type_label(data_type: DataType) -> str:
    """Type name used in :meth:`StructType.treeString` (Spark tree uses typeName, not simple)."""
    if isinstance(data_type, DecimalType):
        return data_type.simpleString()
    if isinstance(data_type, (CharType, VarcharType, TimeType)):
        return data_type.simpleString()
    # ANSI intervals print the field range (``interval day to second``), not typeName.
    if isinstance(data_type, (DayTimeIntervalType, YearMonthIntervalType)):
        return data_type.simpleString()
    return type(data_type).typeName()


def _append_datatype_tree(
    data_type: DataType,
    lines: list[str],
    *,
    prefix: str,
    remaining_depth: int,
    as_root: bool = False,
) -> None:
    """Recursively append Spark treeString lines for nested struct/array/map types."""
    if remaining_depth <= 0:
        return
    if isinstance(data_type, StructType):
        for field in data_type.fields:
            type_label = _tree_type_label(field.dataType)
            lines.append(
                f"{prefix}-- {field.name}: {type_label} (nullable = {str(field.nullable).lower()})"
            )
            child_depth = remaining_depth - 1
            if child_depth > 0 and isinstance(field.dataType, (StructType, ArrayType, MapType)):
                _append_datatype_tree(
                    field.dataType,
                    lines,
                    prefix=prefix + "    |",
                    remaining_depth=child_depth,
                )
        return
    if as_root:
        return
    if isinstance(data_type, ArrayType):
        lines.append(
            f"{prefix}-- element: {_tree_type_label(data_type.elementType)} "
            f"(containsNull = {str(data_type.containsNull).lower()})"
        )
        child_depth = remaining_depth - 1
        if child_depth > 0 and isinstance(data_type.elementType, (StructType, ArrayType, MapType)):
            _append_datatype_tree(
                data_type.elementType,
                lines,
                prefix=prefix + "    |",
                remaining_depth=child_depth,
            )
        return
    if isinstance(data_type, MapType):
        lines.append(f"{prefix}-- key: {_tree_type_label(data_type.keyType)}")
        # key children if complex
        if remaining_depth > 1 and isinstance(data_type.keyType, (StructType, ArrayType, MapType)):
            _append_datatype_tree(
                data_type.keyType,
                lines,
                prefix=prefix + "    |",
                remaining_depth=remaining_depth - 1,
            )
        lines.append(
            f"{prefix}-- value: {_tree_type_label(data_type.valueType)} "
            f"(valueContainsNull = {str(data_type.valueContainsNull).lower()})"
        )
        if remaining_depth > 1 and isinstance(
            data_type.valueType, (StructType, ArrayType, MapType)
        ):
            _append_datatype_tree(
                data_type.valueType,
                lines,
                prefix=prefix + "    |",
                remaining_depth=remaining_depth - 1,
            )


def _datatype_to_ddl_token(data_type: DataType) -> str:
    """Uppercase DDL type token for :meth:`StructType.toDDL`."""
    if isinstance(data_type, NullType):
        return "VOID"
    if isinstance(data_type, StringType):
        return "STRING"
    if isinstance(data_type, BinaryType):
        return "BINARY"
    if isinstance(data_type, BooleanType):
        return "BOOLEAN"
    if isinstance(data_type, ByteType):
        return "TINYINT"
    if isinstance(data_type, ShortType):
        return "SMALLINT"
    if isinstance(data_type, IntegerType):
        return "INT"
    if isinstance(data_type, LongType):
        return "BIGINT"
    if isinstance(data_type, FloatType):
        return "FLOAT"
    if isinstance(data_type, DoubleType):
        return "DOUBLE"
    if isinstance(data_type, DateType):
        return "DATE"
    if isinstance(data_type, TimestampType):
        return "TIMESTAMP"
    if isinstance(data_type, TimestampNTZType):
        return "TIMESTAMP_NTZ"
    if isinstance(data_type, TimeType):
        return f"TIME({data_type.precision})"
    if isinstance(data_type, DecimalType):
        return f"DECIMAL({data_type.precision},{data_type.scale})"
    if isinstance(data_type, ArrayType):
        return f"ARRAY<{_datatype_to_ddl_token(data_type.elementType)}>"
    if isinstance(data_type, MapType):
        return (
            f"MAP<{_datatype_to_ddl_token(data_type.keyType)},"
            f"{_datatype_to_ddl_token(data_type.valueType)}>"
        )
    if isinstance(data_type, StructType):
        inner = ",".join(
            f"{field.name}:{_datatype_to_ddl_token(field.dataType)}" for field in data_type.fields
        )
        return f"STRUCT<{inner}>"
    if isinstance(data_type, VariantType):
        return "VARIANT"
    if isinstance(data_type, CalendarIntervalType):
        return "INTERVAL"
    if isinstance(data_type, CharType):
        return f"CHAR({data_type.length})"
    if isinstance(data_type, VarcharType):
        return f"VARCHAR({data_type.length})"
    return data_type.simpleString().upper()


# ==================================================================================================
# Arrow ↔ repark schema helpers
# ==================================================================================================


def _arrow_type_to_repark(arrow_type: object) -> DataType:
    """Map a ``pyarrow.DataType`` onto the closest repark :class:`DataType` (schema surface)."""
    import pyarrow as pa

    if pa.types.is_int8(arrow_type):
        return ByteType()
    if pa.types.is_int16(arrow_type):
        return ShortType()
    if pa.types.is_int32(arrow_type):
        return IntegerType()
    if pa.types.is_int64(arrow_type):
        return LongType()
    if pa.types.is_float32(arrow_type):
        return FloatType()
    if pa.types.is_floating(arrow_type):
        return DoubleType()
    if pa.types.is_boolean(arrow_type):
        return BooleanType()
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return StringType()
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return BinaryType()
    if pa.types.is_date(arrow_type):
        return DateType()
    if pa.types.is_timestamp(arrow_type):
        if getattr(arrow_type, "tz", None) is None:
            return TimestampNTZType()
        return TimestampType()
    if pa.types.is_decimal(arrow_type):
        return DecimalType(arrow_type.precision, arrow_type.scale)  # type: ignore[attr-defined]
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        value_type = arrow_type.value_type  # type: ignore[attr-defined]
        return ArrayType(_arrow_type_to_repark(value_type), True)
    if pa.types.is_map(arrow_type):
        return MapType(
            _arrow_type_to_repark(arrow_type.key_type),  # type: ignore[attr-defined]
            _arrow_type_to_repark(arrow_type.item_type),  # type: ignore[attr-defined]
            True,
        )
    if pa.types.is_struct(arrow_type):
        fields = [
            StructField(field.name, _arrow_type_to_repark(field.type), field.nullable)
            for field in arrow_type  # type: ignore[attr-defined]
        ]
        return StructType(fields)
    if pa.types.is_null(arrow_type):
        return NullType()
    return StringType()


def struct_type_from_arrow(schema: object) -> StructType:
    """Build a :class:`StructType` from a ``pyarrow.Schema``."""
    import pyarrow as pa

    assert isinstance(schema, pa.Schema)
    fields = [
        StructField(field.name, _arrow_type_to_repark(field.type), field.nullable)
        for field in schema
    ]
    return StructType(fields)


def repark_type_to_arrow(data_type: DataType) -> Any:
    """Map a repark :class:`DataType` to a ``pyarrow.DataType`` (createDataFrame nested)."""
    import pyarrow as pa

    if isinstance(data_type, NullType):
        return pa.null()
    if isinstance(data_type, BooleanType):
        return pa.bool_()
    if isinstance(data_type, ByteType):
        return pa.int8()
    if isinstance(data_type, ShortType):
        return pa.int16()
    if isinstance(data_type, IntegerType):
        return pa.int32()
    if isinstance(data_type, LongType):
        return pa.int64()
    if isinstance(data_type, FloatType):
        return pa.float32()
    if isinstance(data_type, DoubleType):
        return pa.float64()
    if isinstance(data_type, (StringType, CharType, VarcharType)):
        return pa.string()
    if isinstance(data_type, BinaryType):
        return pa.binary()
    if isinstance(data_type, DateType):
        return pa.date32()
    if isinstance(data_type, TimestampType):
        return pa.timestamp("us", tz="UTC")
    if isinstance(data_type, TimestampNTZType):
        return pa.timestamp("us")
    if isinstance(data_type, DecimalType):
        return pa.decimal128(data_type.precision, data_type.scale)
    if isinstance(data_type, ArrayType):
        return pa.list_(repark_type_to_arrow(data_type.elementType))
    if isinstance(data_type, MapType):
        return pa.map_(
            repark_type_to_arrow(data_type.keyType),
            repark_type_to_arrow(data_type.valueType),
        )
    if isinstance(data_type, StructType):
        return pa.struct(
            [(field.name, repark_type_to_arrow(field.dataType)) for field in data_type.fields]
        )
    return pa.string()


# ==================================================================================================
# Private Spark type helpers (F1 true-EC) — Apache test_types imports these from pyspark.sql.types
# ==================================================================================================


def _merge_type(a: DataType, b: DataType, name: str | None = None) -> DataType:
    """Merge two Spark SQL types for schema inference (pyspark ``_merge_type`` parity).

    Raises :class:`~repark.errors.PySparkTypeError` with ``CANNOT_MERGE_TYPE`` and
    ``data_type1`` / ``data_type2`` parameter keys when leaf types conflict (Apache
    ``test_merge_type`` / F1 true-EC). NullType is the identity; nested Array/Map/Struct
    merge recursively.
    """
    from repark.errors import PySparkTypeError

    if isinstance(a, NullType):
        return b
    if isinstance(b, NullType):
        return a
    if isinstance(a, TimestampType) and isinstance(b, TimestampNTZType):
        return a
    if isinstance(a, TimestampNTZType) and isinstance(b, TimestampType):
        return b
    # Spark AtomicType + StringType → StringType (map-key soft merge in test_merge_type).
    atomic = (
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        BooleanType,
        BinaryType,
        DateType,
        TimestampType,
        TimestampNTZType,
        DecimalType,
    )
    if isinstance(a, atomic) and isinstance(b, StringType):
        return b
    if isinstance(a, StringType) and isinstance(b, atomic):
        return a
    if type(a) is not type(b):
        raise PySparkTypeError(
            errorClass="CANNOT_MERGE_TYPE",
            messageParameters={
                "data_type1": type(a).__name__,
                "data_type2": type(b).__name__,
            },
        )
    if isinstance(a, StructType) and isinstance(b, StructType):
        other_fields = {field.name: field.dataType for field in b.fields}
        fields: list[StructField] = [
            StructField(
                field.name,
                _merge_type(
                    field.dataType,
                    other_fields.get(field.name, NullType()),
                    name=f"field {field.name}" if name is None else f"field {field.name} in {name}",
                ),
            )
            for field in a.fields
        ]
        seen = {field.name for field in fields}
        for field_name, field_type in other_fields.items():
            if field_name not in seen:
                fields.append(StructField(field_name, field_type))
        return StructType(fields)
    if isinstance(a, ArrayType) and isinstance(b, ArrayType):
        return ArrayType(
            _merge_type(
                a.elementType,
                b.elementType,
                name=f"element in array {name}" if name is not None else "element in array",
            ),
            True,
        )
    if isinstance(a, MapType) and isinstance(b, MapType):
        return MapType(
            _merge_type(
                a.keyType,
                b.keyType,
                name=f"key of map {name}" if name is not None else "key of map",
            ),
            _merge_type(
                a.valueType,
                b.valueType,
                name=f"value of map {name}" if name is not None else "value of map",
            ),
            True,
        )
    return a


def _make_type_verifier(
    data_type: DataType,
    nullable: bool = True,
    name: str | None = None,
) -> Any:
    """Build a callable that checks a Python value against ``data_type`` (Spark parity).

    Apache ``test_verify_type_exception_msg`` pins:

    * non-nullable ``None`` → :class:`~repark.errors.PySparkValueError`
      ``FIELD_NOT_NULLABLE_WITH_NAME`` with ``field_name``
    * wrong nested type → :class:`~repark.errors.PySparkTypeError`
      ``FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME`` with ``field_name`` / ``data_type`` /
      ``obj`` / ``obj_type``

    Full Spark acceptable-types matrix is not re-implemented; leaf atomic / nested Struct
    / Array / Map checks cover the Apache error-class rows (F1 true-EC).
    """
    from repark.errors import PySparkTypeError, PySparkValueError

    def verifier(obj: Any) -> None:
        if obj is None:
            if nullable:
                return
            if name is not None:
                raise PySparkValueError(
                    errorClass="FIELD_NOT_NULLABLE_WITH_NAME",
                    messageParameters={"field_name": str(name)},
                )
            raise PySparkValueError(errorClass="FIELD_NOT_NULLABLE", messageParameters={})

        if isinstance(data_type, StructType):
            if not isinstance(obj, (list, tuple)):
                if name is not None:
                    raise PySparkTypeError(
                        errorClass="FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME",
                        messageParameters={
                            "field_name": str(name),
                            "data_type": str(data_type),
                            "obj": repr(obj),
                            "obj_type": str(type(obj)),
                        },
                    )
                raise PySparkTypeError(
                    errorClass="FIELD_DATA_TYPE_UNACCEPTABLE",
                    messageParameters={
                        "data_type": str(data_type),
                        "obj": repr(obj),
                        "obj_type": str(type(obj)),
                    },
                )
            if len(obj) != len(data_type.fields):
                raise PySparkValueError(
                    errorClass="FIELD_LENGTH_MISMATCH",
                    messageParameters={
                        "expected": str(len(data_type.fields)),
                        "actual": str(len(obj)),
                    },
                )
            for field, value in zip(data_type.fields, obj, strict=True):
                nested_name = (
                    f"field {field.name}" if name is None else f"field {field.name} in {name}"
                )
                _make_type_verifier(field.dataType, field.nullable, name=nested_name)(value)
            return

        if isinstance(data_type, ArrayType):
            if not isinstance(obj, list):
                if name is not None:
                    raise PySparkTypeError(
                        errorClass="FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME",
                        messageParameters={
                            "field_name": str(name),
                            "data_type": str(data_type),
                            "obj": repr(obj),
                            "obj_type": str(type(obj)),
                        },
                    )
                raise PySparkTypeError(
                    errorClass="FIELD_DATA_TYPE_UNACCEPTABLE",
                    messageParameters={
                        "data_type": str(data_type),
                        "obj": repr(obj),
                        "obj_type": str(type(obj)),
                    },
                )
            for item in obj:
                _make_type_verifier(
                    data_type.elementType,
                    data_type.containsNull,
                    name=f"element in array {name}" if name is not None else "element in array",
                )(item)
            return

        if isinstance(data_type, MapType):
            if not isinstance(obj, dict):
                if name is not None:
                    raise PySparkTypeError(
                        errorClass="FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME",
                        messageParameters={
                            "field_name": str(name),
                            "data_type": str(data_type),
                            "obj": repr(obj),
                            "obj_type": str(type(obj)),
                        },
                    )
                raise PySparkTypeError(
                    errorClass="FIELD_DATA_TYPE_UNACCEPTABLE",
                    messageParameters={
                        "data_type": str(data_type),
                        "obj": repr(obj),
                        "obj_type": str(type(obj)),
                    },
                )
            for key, value in obj.items():
                _make_type_verifier(
                    data_type.keyType,
                    False,
                    name=f"key of map {name}" if name is not None else "key of map",
                )(key)
                _make_type_verifier(
                    data_type.valueType,
                    data_type.valueContainsNull,
                    name=f"value of map {name}" if name is not None else "value of map",
                )(value)
            return

        if (
            isinstance(data_type, IntegerType)
            and isinstance(obj, int)
            and not isinstance(obj, bool)
        ):
            return
        if isinstance(data_type, (StringType, CharType, VarcharType)):
            # Spark accepts many types for string fields; type check is soft here.
            return
        if isinstance(data_type, IntegerType):
            # Reject bool/float/str/other (octo C3-Q-002) — was soft fall-through.
            if name is not None:
                raise PySparkTypeError(
                    errorClass="FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME",
                    messageParameters={
                        "field_name": str(name),
                        "data_type": str(data_type),
                        "obj": repr(obj),
                        "obj_type": str(type(obj)),
                    },
                )
            raise PySparkTypeError(
                errorClass="FIELD_DATA_TYPE_UNACCEPTABLE",
                messageParameters={
                    "data_type": str(data_type),
                    "obj": repr(obj),
                    "obj_type": str(type(obj)),
                },
            )

    return verifier


# === G15: collation refuse (first evaluation, not construction) ===============================

COLLATION_REFUSAL_NEEDLE = "does not implement collation"
_DEFAULT_BINARY_COLLATION = "UTF8_BINARY"


def collation_refusal_message(requested: str) -> str:
    """Actionable G15 refusal — same needles as both SQL doors."""
    return (
        f"repark {COLLATION_REFUSAL_NEEDLE}: requested `{requested}`. Spark 4 would apply "
        "that collation to comparisons and ORDER BY; repark refuses rather than silently "
        "ignore it. Use binary/default ordering — omit COLLATE, keep StringType() / "
        "UTF8_BINARY, and do not set a session collation."
    )


def is_collation_session_key(key: str) -> bool:
    """True when a Spark SQLConf / session key would change compare/order collation."""
    return "collation" in key.lower()


def refuse_collation_session_key(key: str) -> None:
    """Refuse a session/builder conf key that requests collation semantics."""
    if not is_collation_session_key(key):
        return
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(collation_refusal_message(key))


def refuse_evaluated_collation(data_type: Any) -> None:
    """Refuse first evaluation of a non-binary ``StringType`` (nested-aware).

    Construction and ``simpleString`` stay legal (A5). Evaluation is createDataFrame,
    cast/try_cast, and any schema→engine mapping.
    """
    from repark.errors import UnsupportedOperationException

    if isinstance(data_type, StringType) and not data_type.isUTF8BinaryCollation():
        raise UnsupportedOperationException(collation_refusal_message(data_type.collation))
    if isinstance(data_type, ArrayType):
        refuse_evaluated_collation(data_type.elementType)
        return
    if isinstance(data_type, MapType):
        refuse_evaluated_collation(data_type.keyType)
        refuse_evaluated_collation(data_type.valueType)
        return
    if isinstance(data_type, StructType):
        for field in data_type.fields:
            refuse_evaluated_collation(field.dataType)
        return
    if isinstance(data_type, StructField):
        refuse_evaluated_collation(data_type.dataType)


def refuse_collated_type_string(type_text: str) -> None:
    """Refuse a ``string collate NAME`` cast/DDL token other than UTF8_BINARY."""
    match = _STRING_COLLATE.fullmatch(type_text.strip())
    if match is None:
        return
    name = match.group(1)
    if name.upper() == _DEFAULT_BINARY_COLLATION:
        return
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(collation_refusal_message(name))


__all__ = [
    "ArrayType",
    "BinaryType",
    "BooleanType",
    "ByteType",
    "CalendarIntervalType",
    "CharType",
    "DataType",
    "DateType",
    "DayTimeIntervalType",
    "DecimalType",
    "DoubleType",
    "FloatType",
    "IntegerType",
    "LongType",
    "MapType",
    "NullType",
    "ShortType",
    "StringType",
    "StructField",
    "StructType",
    "TimeType",
    "TimestampNTZType",
    "TimestampType",
    "VarcharType",
    "VariantType",
    "YearMonthIntervalType",
    "repark_type_to_arrow",
    "struct_type_from_arrow",
]
