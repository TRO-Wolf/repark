"""Spark SQL abstract type bases, the spatial types, and ``UserDefinedType``.

``DataType`` lives here so the abstract bases can subclass it without an import
cycle: :mod:`repark.spark.types` imports and re-exports every public name.
"""

from __future__ import annotations

import base64
import json
import pickle
import re
from types import MethodType
from typing import Any, ClassVar, NoReturn

from repark.errors import (
    IllegalArgumentException,
    PySparkNotImplementedError,
    PySparkValueError,
    UnsupportedOperationException,
)

_SIMPLE_STRING_FAST: dict[type, str] = {}


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
        answer = _SIMPLE_STRING_FAST.get(type(self))
        if answer is not None:
            return answer
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
        from repark.spark._type_table import _parse_datatype_string

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


class AtomicType(DataType):
    """Internal type for everything that is not null, UDTs, arrays, structs, maps."""


class NumericType(AtomicType):
    """Numeric data types (Spark ``NumericType``)."""


class IntegralType(NumericType):
    """Integral data types (Spark ``IntegralType``)."""


class FractionalType(NumericType):
    """Fractional data types (Spark ``FractionalType``)."""


class DatetimeType(AtomicType):
    """Super class of all datetime data types (Spark ``DatetimeType``)."""


class AnyTimeType(DatetimeType):
    """A TIME type of any valid precision (Spark ``AnyTimeType``)."""


class AnsiIntervalType(AtomicType):
    """The interval type which conforms to the ANSI SQL standard."""


_SUPPORTED_SRS: tuple[tuple[int, str, bool], ...] = (
    (0, "SRID:0", False),
    (3857, "EPSG:3857", False),
    (4326, "OGC:CRS84", True),
)


def _attached_error_class(error: object) -> str | None:
    """Return the Spark error class attached on a native exception."""
    value = getattr(error, "_spark_error_class", None)
    return value if isinstance(value, str) else None


def _attached_message_parameters(error: object) -> dict[str, str] | None:
    """Return the Spark message parameters attached on a native exception."""
    value = getattr(error, "_spark_message_parameters", None)
    if not isinstance(value, dict):
        return None
    return {str(key): str(item) for key, item in value.items()}


def _raise_spatial_illegal(error_class: str, message: str, parameters: dict[str, str]) -> NoReturn:
    """Raise a native IllegalArgumentException carrying Spark's structured error API."""
    error = IllegalArgumentException(f"[{error_class}] {message}")
    error._spark_error_class = error_class
    error._spark_message_parameters = parameters
    error.getErrorClass = MethodType(_attached_error_class, error)
    error.getCondition = MethodType(_attached_error_class, error)
    error.getMessageParameters = MethodType(_attached_message_parameters, error)
    raise error


def _raise_invalid_srid(srid: object) -> NoReturn:
    """Raise Spark's ``ST_INVALID_SRID_VALUE`` refusal for ``srid``."""
    _raise_spatial_illegal(
        "ST_INVALID_SRID_VALUE",
        f"Invalid or unsupported SRID (spatial reference identifier) value: {srid}.",
        {"srid": str(srid)},
    )


def _raise_invalid_crs(crs: object) -> NoReturn:
    """Raise Spark's ``ST_INVALID_CRS_VALUE`` refusal for ``crs``."""
    _raise_spatial_illegal(
        "ST_INVALID_CRS_VALUE",
        f"Invalid or unsupported CRS (coordinate reference system) value: '{crs}'.",
        {"crs": str(crs)},
    )


def _raise_invalid_alg(alg: object) -> NoReturn:
    """Raise Spark's ``ST_INVALID_ALGORITHM_VALUE`` refusal for ``alg``."""
    _raise_spatial_illegal(
        "ST_INVALID_ALGORITHM_VALUE",
        f"Invalid or unsupported edge interpolation algorithm value: '{alg}'.",
        {"alg": str(alg)},
    )


def _geographic_string_id(srid: int) -> str | None:
    """CRS string id for ``srid`` when the SRS is geographic, else None."""
    for srs_srid, string_id, is_geographic in _SUPPORTED_SRS:
        if srs_srid == srid and is_geographic:
            return string_id
    return None


def _geographic_srid(string_id: str) -> int | None:
    """SRID for a geographic CRS ``string_id``, else None."""
    for srs_srid, candidate, is_geographic in _SUPPORTED_SRS:
        if candidate == string_id and is_geographic:
            return srs_srid
    return None


def _cartesian_string_id(srid: int) -> str | None:
    """CRS string id for ``srid`` over every supported SRS, else None."""
    for srs_srid, string_id, _is_geographic in _SUPPORTED_SRS:
        if srs_srid == srid:
            return string_id
    return None


def _cartesian_srid(string_id: str) -> int | None:
    """SRID for a Cartesian CRS ``string_id``, else None."""
    for srs_srid, candidate, _is_geographic in _SUPPORTED_SRS:
        if candidate == string_id:
            return srs_srid
    return None


class SpatialType(AtomicType):
    """Super class of all spatial data types: GeographyType and GeometryType."""

    MIXED_SRID: ClassVar[int] = -1
    MIXED_CRS: ClassVar[str] = "SRID:ANY"

    def _engine_type(self) -> str:
        """Refuse: no engine surface reaches a spatial value (V3-GEO-1)."""
        raise UnsupportedOperationException(
            f"repark does not support the {self.simpleString()} column type: Spark "
            "GEOGRAPHY/GEOMETRY have no Arrow representation and no vendored WKB codec, "
            "so no engine surface reaches a spatial value (V3-GEO-1; FNP-16-geospatial)."
        )


class GeographyType(SpatialType):
    """OGC geographic spatial type ``geography(srid)`` (Spark 4.1+)."""

    DEFAULT_CRS: ClassVar[str] = "OGC:CRS84"
    DEFAULT_ALG: ClassVar[str] = "SPHERICAL"
    DEFAULT_SRID: ClassVar[int] = 4326

    def __init__(self, srid: int | str) -> None:
        """Validate and store ``srid``; ``"ANY"`` marks the mixed-SRID form."""
        self.srid: int
        self._crs: str
        if srid == "ANY":
            self.srid = SpatialType.MIXED_SRID
            self._crs = SpatialType.MIXED_CRS
        elif not isinstance(srid, int) or (crs := _geographic_string_id(srid)) is None:
            _raise_invalid_srid(srid)
        else:
            self.srid = srid
            self._crs = crs
        self._alg: str = GeographyType.DEFAULT_ALG

    @classmethod
    def _from_crs(cls, crs: str, alg: str) -> GeographyType:
        """Build from a CRS string + edge algorithm (Spark JSON parse path)."""
        if alg != cls.DEFAULT_ALG:
            _raise_invalid_alg(alg)
        if crs.lower() == cls.MIXED_CRS.lower():
            return GeographyType("ANY")
        srid = _geographic_srid(crs)
        if srid is None:
            _raise_invalid_crs(crs)
        geography = GeographyType(srid)
        geography._crs = crs
        geography._alg = alg
        return geography

    def simpleString(self) -> str:  # noqa: N802
        """``geography(srid)`` or ``geography(any)``."""
        if self.srid == SpatialType.MIXED_SRID:
            return "geography(any)"
        return f"geography({self.srid})"

    def __repr__(self) -> str:
        """``GeographyType(srid)`` or ``GeographyType(ANY)``."""
        if self.srid == SpatialType.MIXED_SRID:
            return "GeographyType(ANY)"
        return f"GeographyType({self.srid})"

    def jsonValue(self) -> str:  # noqa: N802
        """``geography(CRS, ALG)`` — the CRS form, not the SRID form."""
        return f"geography({self._crs}, {self._alg})"

    def needConversion(self) -> bool:  # noqa: N802
        """Spatial values convert through their WKB payload (Spark)."""
        return True


class GeometryType(SpatialType):
    """OGC Cartesian spatial type ``geometry(srid)`` (Spark 4.1+)."""

    DEFAULT_CRS: ClassVar[str] = "OGC:CRS84"
    DEFAULT_SRID: ClassVar[int] = 4326

    def __init__(self, srid: int | str) -> None:
        """Validate and store ``srid``; ``"ANY"`` marks the mixed-SRID form."""
        self.srid: int
        self._crs: str
        if srid == "ANY":
            self.srid = SpatialType.MIXED_SRID
            self._crs = SpatialType.MIXED_CRS
        elif not isinstance(srid, int) or (crs := _cartesian_string_id(srid)) is None:
            _raise_invalid_srid(srid)
        else:
            self.srid = srid
            self._crs = crs

    @classmethod
    def _from_crs(cls, crs: str) -> GeometryType:
        """Build from a CRS string (Spark JSON parse path)."""
        if crs.lower() == cls.MIXED_CRS.lower():
            return GeometryType("ANY")
        srid = _cartesian_srid(crs)
        if srid is None:
            _raise_invalid_crs(crs)
        geometry = GeometryType(srid)
        geometry._crs = crs
        return geometry

    def simpleString(self) -> str:  # noqa: N802
        """``geometry(srid)`` or ``geometry(any)``."""
        if self.srid == SpatialType.MIXED_SRID:
            return "geometry(any)"
        return f"geometry({self.srid})"

    def __repr__(self) -> str:
        """``GeometryType(srid)`` or ``GeometryType(ANY)``."""
        if self.srid == SpatialType.MIXED_SRID:
            return "GeometryType(ANY)"
        return f"GeometryType({self.srid})"

    def jsonValue(self) -> str:  # noqa: N802
        """``geometry(CRS)`` — the CRS form, not the SRID form."""
        return f"geometry({self._crs})"

    def needConversion(self) -> bool:  # noqa: N802
        """Spatial values convert through their WKB payload (Spark)."""
        return True


class UserDefinedType(DataType):
    """User-defined type (Spark ``UserDefinedType``); column use refused (TYPES-UDT-1)."""

    @classmethod
    def typeName(cls) -> str:  # noqa: N802
        """Spark UDT typeName is the class name lowercased (``Type`` kept)."""
        return cls.__name__.lower()

    @classmethod
    def sqlType(cls) -> DataType:  # noqa: N802
        """Underlying SQL storage type — refused exactly like Spark's base UDT."""
        raise PySparkNotImplementedError(
            "[NOT_IMPLEMENTED] sqlType() is not implemented.",
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "sqlType()"},
        )

    @classmethod
    def module(cls) -> str:
        """Python module of the UDT — refused like Spark's base UDT."""
        raise PySparkNotImplementedError(
            "[NOT_IMPLEMENTED] module() is not implemented.",
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "module()"},
        )

    @classmethod
    def scalaUDT(cls) -> str:  # noqa: N802
        """Paired Scala UDT class name — Spark's default empty string."""
        return ""

    @classmethod
    def _cachedSqlType(cls) -> DataType:  # noqa: N802
        """Cache ``sqlType()`` on the class (Spark ``_cachedSqlType``)."""
        if not hasattr(cls, "_cached_sql_type"):
            cls._cached_sql_type = cls.sqlType()
        return cls._cached_sql_type  # type: ignore[attr-defined]

    def needConversion(self) -> bool:  # noqa: N802
        """UDTs serialize through ``sqlType()`` (Spark)."""
        return True

    def toInternal(self, obj: Any) -> Any:  # noqa: N802
        """``serialize`` then the cached ``sqlType()`` conversion (Spark template)."""
        if obj is not None:
            return self._cachedSqlType().toInternal(self.serialize(obj))
        return None

    def fromInternal(self, obj: Any) -> Any:  # noqa: N802
        """Cached ``sqlType()`` conversion then ``deserialize`` (Spark template)."""
        value = self._cachedSqlType().fromInternal(obj)
        if value is not None:
            return self.deserialize(value)
        return None

    def serialize(self, obj: Any) -> Any:
        """Convert a user-type object into a SQL datum — base refuses like Spark."""
        raise PySparkNotImplementedError(
            "[NOT_IMPLEMENTED] toInternal() is not implemented.",
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "toInternal()"},
        )

    def deserialize(self, datum: Any) -> Any:
        """Convert a SQL datum into a user-type object — base refuses like Spark."""
        raise PySparkNotImplementedError(
            "[NOT_IMPLEMENTED] fromInternal() is not implemented.",
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "fromInternal()"},
        )

    def simpleString(self) -> str:  # noqa: N802
        """Spark renders ``udt``."""
        return "udt"

    def jsonValue(self) -> dict[str, Any]:  # noqa: N802
        """Spark's UDT JSON dict — ``pyClass`` hits ``module()`` first."""
        if self.scalaUDT():
            return {
                "type": "udt",
                "class": self.scalaUDT(),
                "pyClass": f"{self.module()}.{type(self).__name__}",
                "sqlType": self.sqlType().jsonValue(),
            }
        payload = pickle.dumps(type(self))
        return {
            "type": "udt",
            "pyClass": f"{self.module()}.{type(self).__name__}",
            "serializedClass": base64.b64encode(payload).decode("utf8"),
            "sqlType": self.sqlType().jsonValue(),
        }

    def __eq__(self, other: object) -> bool:
        """UDTs compare by class (Spark ``UserDefinedType.__eq__``)."""
        return type(self) is type(other)

    __hash__ = None

    def _engine_type(self) -> str:
        """Refuse column use through the TYPES-UDT-1 declared refusal."""
        raise PySparkNotImplementedError(
            "[NOT_IMPLEMENTED] UserDefinedType is not implemented.",
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "UserDefinedType"},
        )


_GEOMETRY_NAME = re.compile(r"geometry$")
_GEOMETRY_CRS = re.compile(r"geometry\s*\(\s*([\w]+:-?[\w]+)\s*\)")
_GEOGRAPHY_NAME = re.compile(r"geography$")
_GEOGRAPHY_CRS = re.compile(r"geography\s*\(\s*([\w]+:-?[\w]+)\s*\)")
_GEOGRAPHY_CRS_ALG = re.compile(r"geography\s*\(\s*([\w]+:-?[\w]+)\s*,\s*(\w+)\s*\)")
_GEOGRAPHY_ALG = re.compile(r"geography\s*\(\s*(\w+)\s*\)")
_SPATIAL_TOKEN_LEAD = re.compile(r"(geometry|geography)", re.I)


def _parse_spatial_json_token(text: str) -> DataType | None:
    """Spark JSON spatial forms: bare names and CRS spellings; None when not matched."""
    stripped = text.strip()
    if _GEOMETRY_NAME.fullmatch(stripped):
        return GeometryType._from_crs(GeometryType.DEFAULT_CRS)
    match = _GEOMETRY_CRS.fullmatch(stripped)
    if match:
        return GeometryType._from_crs(match.group(1))
    if _GEOGRAPHY_NAME.fullmatch(stripped):
        return GeographyType._from_crs(GeographyType.DEFAULT_CRS, GeographyType.DEFAULT_ALG)
    match = _GEOGRAPHY_CRS.fullmatch(stripped)
    if match:
        return GeographyType._from_crs(match.group(1), GeographyType.DEFAULT_ALG)
    match = _GEOGRAPHY_CRS_ALG.fullmatch(stripped)
    if match:
        return GeographyType._from_crs(match.group(1), match.group(2))
    match = _GEOGRAPHY_ALG.fullmatch(stripped)
    if match:
        return GeographyType._from_crs(GeographyType.DEFAULT_CRS, match.group(1))
    return None


def _refuse_spatial_json_token(text: str) -> None:
    """Refuse a spatial token the JSON CRS forms did not accept (Spark CANNOT_PARSE_DATATYPE)."""
    if _SPATIAL_TOKEN_LEAD.match(text.strip()) is None:
        return
    raise PySparkValueError(
        f"[CANNOT_PARSE_DATATYPE] Unable to parse datatype. {text}.",
        errorClass="CANNOT_PARSE_DATATYPE",
        messageParameters={"msg": text},
    )


def _struct_to_internal(names: list[str], fields: list[Any], obj: Any) -> tuple[Any, ...] | None:
    """Spark ``StructType.toInternal`` — dict / tuple / list / object → field tuple."""
    if obj is None:
        return None
    flags = tuple(field.needConversion() for field in fields)
    if isinstance(obj, dict):
        values: Any = (obj.get(name) for name in names)
    elif isinstance(obj, (tuple, list)):
        values = iter(obj)
    elif hasattr(obj, "__dict__"):
        data = obj.__dict__
        values = (data.get(name) for name in names)
    else:
        raise PySparkValueError(
            f"[UNEXPECTED_TUPLE_WITH_STRUCT] Unexpected tuple {obj} with StructType.",
            errorClass="UNEXPECTED_TUPLE_WITH_STRUCT",
            messageParameters={"tuple": str(obj)},
        )
    if any(flags):
        return tuple(
            field.toInternal(value) if convert else value
            for field, value, convert in zip(fields, values, flags, strict=False)
        )
    return tuple(values)


def _struct_from_internal(names: list[str], fields: list[Any], obj: Any) -> Any:
    """Spark ``StructType.fromInternal`` — field values → a named ``Row``."""
    from repark.spark.row import Row

    if obj is None or isinstance(obj, Row):
        return obj
    flags = tuple(field.needConversion() for field in fields)
    if any(flags):
        values = [
            field.fromInternal(value) if convert else value
            for field, value, convert in zip(fields, obj, flags, strict=False)
        ]
    else:
        values = list(obj)
    return Row.from_ordered_fields(names, values)
