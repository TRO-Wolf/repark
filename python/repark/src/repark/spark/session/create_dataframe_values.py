"""createDataFrame value and schema literals."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.errors import PySparkTypeError
from repark.spark._idents import sql_string_literal


if TYPE_CHECKING:
    from repark.spark.session.create_dataframe_inference import _validate_decimal_envelope
    from repark.spark.session.create_dataframe_schema import _parse_schema_ddl


_TYPED_NULL_SQL = "CAST(NULL AS VARCHAR)"


_DECIMAL_PRECISION = 38


_DECIMAL_SCALE = 18


_DECIMAL_MAX_ABS = 10 ** (_DECIMAL_PRECISION - _DECIMAL_SCALE)


def _sql_literal(value: Any) -> str:
    """Render a Python scalar as a SQL literal for VALUES-based createDataFrame."""

    import datetime as dt

    from decimal import Decimal

    value = _normalize_create_dataframe_cell(value)

    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)

    if isinstance(value, float):
        # NaN already normalized to None; reject inf so we never emit a bare `inf` token.

        if value == float("inf") or value == float("-inf"):
            raise PySparkTypeError("createDataFrame does not support infinite float values")

        return repr(value)

    if isinstance(value, str):
        return sql_string_literal(value)

    if isinstance(value, dt.datetime):
        # TIMESTAMP literal. Session is UTC; tz-aware values convert to UTC first so absolute

        # time is preserved (wall-clock strip via replace(tzinfo=None) alone is wrong for non-UTC).

        wall = value.astimezone(dt.UTC).replace(tzinfo=None) if value.tzinfo is not None else value

        if wall.microsecond:
            rendered = wall.strftime("%Y-%m-%d %H:%M:%S.%f")

        else:
            rendered = wall.strftime("%Y-%m-%d %H:%M:%S")

        return f"TIMESTAMP '{rendered}'"

    if isinstance(value, dt.date):
        return f"DATE '{value.isoformat()}'"

    if isinstance(value, Decimal):
        # Spark's inferred DecimalType for Python Decimal is DECIMAL(38, 18); pin that width so

        # createDataFrame → to_arrow type matches the live oracle (INT-002). Emit fixed-point

        # text via format(..., 'f') — str(Decimal) can be scientific (1E-10) which SQL parsers

        # mis-handle inside CAST. Refuse values outside the envelope rather than silent zero

        # (1E-19 → 0) or round (C2-L-002).

        _validate_decimal_envelope(value)

        return f"CAST({format(value, 'f')} AS DECIMAL({_DECIMAL_PRECISION}, {_DECIMAL_SCALE}))"

    raise PySparkTypeError(
        f"createDataFrame does not support values of type {type(value).__name__} yet"
    )


def _numpy_datetime64_unit(value: Any) -> str | None:
    """Return the numpy ``datetime64`` unit (``'D'``, ``'ns'``, …) or ``None`` if unknown.



    Used so all-null ``NaT`` witnesses pick DATE vs TIMESTAMP the same way non-null cells do

    after ``.item()`` (calendar units ``D``/``W``/``M``/``Y`` → ``datetime.date`` — C3-Q-001).

    """

    dtype = getattr(value, "dtype", None)

    if dtype is None:
        return None

    text = str(dtype)

    if "[" in text and text.endswith("]"):
        return text[text.rindex("[") + 1 : -1]

    return None


_NUMPY_DATETIME64_DATE_UNITS = frozenset({"D", "W", "M", "Y"})


def _supported_array_typecodes() -> frozenset[str]:
    """Spark-supported ``array.array`` typecodes on this platform (F1 / test_array_types)."""

    import ctypes

    import sys

    supported: set[str] = {"f", "d"}

    for typecode, ctype in (
        ("b", ctypes.c_byte),
        ("h", ctypes.c_short),
        ("i", ctypes.c_int),
        ("l", ctypes.c_long),
    ):
        if ctypes.sizeof(ctype) * 8 <= 64:
            supported.add(typecode)

    for typecode, ctype in (
        ("B", ctypes.c_ubyte),
        ("H", ctypes.c_ushort),
        ("I", ctypes.c_uint),
        ("L", ctypes.c_ulong),
    ):
        # JVM has no unsigned — need signed slot at least 1 bit larger.

        if ctypes.sizeof(ctype) * 8 + 1 <= 64:
            supported.add(typecode)

    if sys.version_info[0] < 4:
        supported.add("u")

    return frozenset(supported)


_ARRAY_TYPECODES_SUPPORTED: frozenset[str] | None = None


def _array_typecodes_supported() -> frozenset[str]:
    """Cached :func:`_supported_array_typecodes`."""

    global _ARRAY_TYPECODES_SUPPORTED

    if _ARRAY_TYPECODES_SUPPORTED is None:
        _ARRAY_TYPECODES_SUPPORTED = _supported_array_typecodes()

    return _ARRAY_TYPECODES_SUPPORTED


def _normalize_create_dataframe_cell(value: Any, *, field_name: str | None = None) -> Any:
    """Coerce pandas / numpy / Row-adjacent scalars to plain Python for SQL literals.



    Missing markers (``None``, ``NaN``, pandas ``NA`` / ``NaT``) become ``None``. Numpy scalar

    wrappers unwrap via ``.item()`` — except ``numpy.datetime64[ns]`` (and finer), where

    ``.item()`` returns an epoch int; those cast to ``datetime64[us]`` first so VALUES emits

    TIMESTAMP, not a silent integer (prior C3-L-002). ``numpy.timedelta64`` refuses (``.item()``

    can return a bare int for unit ``ns`` — silent duration→count — C3-L-001). pandas

    ``Timestamp`` becomes ``datetime.datetime`` (tz-aware kept; UTC conversion happens in

    :func:`_sql_literal`).



    **ML vectors (R-ML-SKELETON):** :class:`~repark.ml.linalg.DenseVector` → dense float list;

    :class:`~repark.ml.linalg.SparseVector` → sparse struct dict. Mixed dense widths are

    rejected later in :func:`_arrow_table_from_tuples` (v1 fixed-width only).



    **array.array (F1 / test_array_types):** supported typecodes → ``list``; unsupported

    raise :class:`~repark.errors.PySparkTypeError` with ``CANNOT_INFER_TYPE_FOR_FIELD`` when

    ``field_name`` is known (Apache check_error keys).

    """

    if value is None:
        return None

    type_name = type(value).__name__

    module_name = type(value).__module__

    # Nested Row kept as Row for struct vs map inference (dict → map, Row → struct).

    # Children still need recursive normalize when building Arrow (see _prepare_nested_cell).

    # array.array → list for Spark-supported typecodes (Apache test_array_types).

    if module_name == "array" and type_name == "array":
        typecode = getattr(value, "typecode", None)

        if typecode is None or typecode not in _array_typecodes_supported():
            if field_name is not None:
                raise PySparkTypeError(
                    errorClass="CANNOT_INFER_TYPE_FOR_FIELD",
                    messageParameters={"field_name": str(field_name)},
                )

            raise PySparkTypeError(
                errorClass="UNSUPPORTED_DATA_TYPE",
                messageParameters={"data_type": f"array({typecode})"},
            )

        return list(value)

    # repark.ml vectors (lazy import — avoid hard cycle on session import).

    if (
        module_name.startswith("repark.ml")
        or module_name.startswith("repark.spark.ml")
        or module_name.startswith("pyspark.ml")
    ):
        if type_name == "DenseVector" or (hasattr(value, "toArray") and type_name == "DenseVector"):
            return list(value.toArray())

        if type_name == "SparseVector":
            if hasattr(value, "as_struct_dict"):
                return value.as_struct_dict()

            # pyspark SparseVector: size + indices + values attributes

            size = int(value.size) if not callable(value.size) else int(value.size())

            indices = list(getattr(value, "indices", []))

            values = [float(item) for item in getattr(value, "values", [])]

            return {"size": size, "indices": indices, "values": values}

    # Infinite floats refuse on every path (Arrow CDF + VALUES) — C2-Q-002.

    if isinstance(value, float) and (value == float("inf") or value == float("-inf")):
        raise PySparkTypeError("createDataFrame does not support infinite float values")

    # Nested list — normalize children (array.array / DenseVector inside lists).

    if isinstance(value, list):
        return [_normalize_create_dataframe_cell(item) for item in value]

    # pandas missing markers (NA / NaT) and float NaN — avoid importing pandas unless present.

    module_name = type(value).__module__

    type_name = type(value).__name__

    if module_name.startswith("pandas"):
        if type_name in {"NAType", "NaTType"}:
            return None

        if type_name == "Timestamp":
            return value.to_pydatetime()

        if type_name == "Timedelta":
            raise PySparkTypeError("createDataFrame does not support values of type Timedelta yet")

        if type_name == "Interval":
            raise PySparkTypeError("createDataFrame does not support values of type Interval yet")

        if type_name == "Period":
            # Period is not a SQL scalar; refuse so all-null PeriodDtype cannot soft-succeed

            # as VARCHAR while non-null Period fails (C4-Q-002 / C4-L-002).

            raise PySparkTypeError("createDataFrame does not support values of type Period yet")

    if module_name.startswith("numpy"):
        # numpy.nan is a float; other scalars unwrap.

        if type_name == "float64" or type_name == "float32":
            as_float = float(value)

            if as_float != as_float:
                return None

            return as_float

        if type_name in {"complex64", "complex128", "complexfloating"}:
            # Refuse before .item() → Python complex soft-path (C5-Q-002).

            raise PySparkTypeError(
                f"createDataFrame does not support values of type {type_name} yet"
            )

        if type_name == "datetime64":
            # NaT → None. Unit 'ns' (and finer) .item() returns int epoch ns — recover wall-clock

            # via us cast so we never emit a bare integer SQL literal for a timestamp cell.

            # Calendar units D/W/M/Y .item() → datetime.date (DATE SQL); finer → datetime.

            if str(value) == "NaT":
                return None

            item = value.item()

            if isinstance(item, (int, float)):
                casted = value.astype("datetime64[us]")

                if str(casted) == "NaT":
                    return None

                return casted.item()

            return item  # datetime.datetime or datetime.date

        if type_name == "timedelta64":
            # Unit ns .item() returns int — silent duration→count if unwrapped (C3-L-001).

            raise PySparkTypeError(
                "createDataFrame does not support values of type numpy.timedelta64 yet"
            )

        if hasattr(value, "item"):
            return _normalize_create_dataframe_cell(value.item())

    if isinstance(value, complex):
        # Python complex (incl. unwrapped numpy complex) — not a SQL scalar (C5-Q-002).

        raise PySparkTypeError("createDataFrame does not support values of type complex yet")

    if isinstance(value, float) and value != value:
        return None

    return value


def _is_pandas_dataframe(data: Any) -> bool:
    """Duck-type a pandas DataFrame without importing pandas at module load."""

    return type(data).__module__.startswith("pandas") and type(data).__name__ == "DataFrame"


def _is_polars_dataframe(data: Any) -> bool:
    """Duck-type a polars DataFrame without importing polars at module load."""

    return type(data).__module__.startswith("polars") and type(data).__name__ == "DataFrame"


def _coerce_schema_names(schema: Any) -> list[str] | None:
    """Validate name-only ``schema=`` (list/tuple of str).



    See :func:`_parse_create_dataframe_schema`.

    """

    names, _engine_types = _parse_create_dataframe_schema(schema)

    return names


def _parse_create_dataframe_schema(
    schema: Any,
) -> tuple[list[str] | None, list[str] | None]:
    """Parse ``createDataFrame(..., schema=)`` into ``(names, engine_type_strings|None)``.



    Forms (R-PARITY3, live PySpark 4.1.2):



    * ``None`` → ``(None, None)``

    * ``list``/``tuple`` of ``str`` → names only (types inferred)

    * :class:`~repark.types.StructType` → names + engine type strings per field

    * DDL string ``"a INT, b STRING"`` → same as StructType



    A bare ``str`` that is **not** a DDL schema would character-iterate into per-character

    column names — we only accept DDL when it parses as ``name TYPE`` pairs.

    """

    if schema is None:
        return None, None

    # Late import avoids a hard cycle (types imports nothing from session).

    from repark.spark.types import (
        DataType,
        StructType,
    )

    if isinstance(schema, StructType):
        names = [field.name for field in schema.fields]

        engine_types = [_data_type_to_sql_type(field.dataType) for field in schema.fields]

        return names, engine_types

    if isinstance(schema, str):
        parsed = _parse_schema_ddl(schema)

        if parsed is not None:
            return parsed

        raise PySparkTypeError(
            "createDataFrame schema string must be a DDL field list like "
            f"'a INT, b STRING' (got {schema!r}; a bare non-DDL string would be "
            "character-iterated into column names)"
        )

    if isinstance(schema, (list, tuple)):
        names = list(schema)

        for index, name in enumerate(names):
            if not isinstance(name, str):
                raise PySparkTypeError(
                    "createDataFrame schema names must be str; "
                    f"got {type(name).__name__} at index {index}"
                )

        return names, None

    if isinstance(schema, DataType):
        # Spark wraps a bare atomic/complex type as a single-column StructType named

        # ``value`` (Apache ``test_reciprocal_trig_functions`` / ``createDataFrame(lst,

        # DoubleType())`` — F2).

        from repark.spark.types import StructField

        wrapped = StructType([StructField("value", schema, True)])

        names = [field.name for field in wrapped.fields]

        engine_types = [_data_type_to_sql_type(field.dataType) for field in wrapped.fields]

        return names, engine_types

    raise PySparkTypeError(
        "createDataFrame schema must be a list/tuple of column name strings, "
        f"a StructType, a DDL string, or a scalar DataType; got {type(schema).__name__}"
    )


def _data_type_to_sql_type(data_type: Any) -> str:
    """Map a repark :class:`~repark.types.DataType` to a SQL cast target for VALUES cells."""

    from repark.spark.types import (
        ArrayType,
        BinaryType,
        BooleanType,
        ByteType,
        CharType,
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        MapType,
        NullType,
        ShortType,
        StringType,
        StructType,
        TimestampNTZType,
        TimestampType,
        VarcharType,
    )

    if isinstance(data_type, IntegerType):
        return "INT"

    if isinstance(data_type, LongType):
        return "BIGINT"

    if isinstance(data_type, ShortType):
        return "SMALLINT"

    if isinstance(data_type, ByteType):
        return "TINYINT"

    if isinstance(data_type, DoubleType):
        return "DOUBLE"

    if isinstance(data_type, FloatType):
        return "FLOAT"

    if isinstance(data_type, BooleanType):
        return "BOOLEAN"

    if isinstance(data_type, (StringType, CharType, VarcharType)):
        # STRING (not VARCHAR): nested ARRAY/MAP/STRUCT engine markers are re-parsed by

        # DataType.fromDDL in _sql_type_to_arrow; fromDDL does not treat bare VARCHAR as

        # string (only string / str / varchar(n)). Using VARCHAR made every nested type

        # that contained a string field silently fall back to pa.string() (octo X2 C1).

        # G15: a non-binary StringType collation would be silently stripped here
        # (engine token is always STRING) — that is the silently-wrong-count path.
        if isinstance(data_type, StringType):
            from repark.spark.types import refuse_evaluated_collation

            refuse_evaluated_collation(data_type)

        return "STRING"

    if isinstance(data_type, BinaryType):
        return "BINARY"

    if isinstance(data_type, DateType):
        return "DATE"

    if isinstance(data_type, TimestampType):
        return "TIMESTAMP"

    if isinstance(data_type, TimestampNTZType):
        return "TIMESTAMP_NTZ"

    if isinstance(data_type, DecimalType):
        return f"DECIMAL({data_type.precision},{data_type.scale})"

    if isinstance(data_type, NullType):
        # G3b D-5: HONOR the requested void instead of silently substituting VARCHAR.
        # An explicit ``NullType()`` / ``ArrayType(NullType())`` used to come back as
        # ``string`` / ``array<string>`` with no warning — the schema the caller asked for
        # was not the schema they got, and nothing said so. VOID round-trips end to end:
        # ``_sql_type_to_arrow`` maps it to ``pa.null()``, the engine accepts
        # ``CAST(NULL AS VOID)`` on the empty-frame seed, and the DF-2 void machinery
        # (drop_null_lists / make_array(NULL)) already handles ``array<void>``.

        return "VOID"

    if isinstance(data_type, ArrayType):
        # Nested complex types are applied via Arrow schema (engine_types marker below).

        return f"ARRAY<{_data_type_to_sql_type(data_type.elementType)}>"

    if isinstance(data_type, MapType):
        return (
            f"MAP<{_data_type_to_sql_type(data_type.keyType)},"
            f"{_data_type_to_sql_type(data_type.valueType)}>"
        )

    if isinstance(data_type, StructType):
        inner = ",".join(
            f"{field.name}:{_data_type_to_sql_type(field.dataType)}" for field in data_type.fields
        )

        return f"STRUCT<{inner}>"

    # Fallback: engine string if present.

    engine = getattr(data_type, "_engine_type", None)

    if callable(engine):
        return str(engine())

    raise PySparkTypeError(
        f"createDataFrame schema field type {type(data_type).__name__} is not supported"
    )
