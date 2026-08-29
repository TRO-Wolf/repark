"""createDataFrame nested Arrow type inference."""

from __future__ import annotations

import contextvars

from typing import TYPE_CHECKING, Any

from repark.errors import PySparkTypeError, PySparkValueError


if TYPE_CHECKING:
    from repark.spark.session.create_dataframe_tuples import (
        _SPARK_SCALAR_MERGE_LABELS,
        _python_scalar_merge_kind,
    )
    from repark.spark.session.create_dataframe_values import (
        _DECIMAL_MAX_ABS,
        _DECIMAL_PRECISION,
        _DECIMAL_SCALE,
    )


def _validate_decimal_envelope(value: Any) -> None:
    """Refuse Decimal values outside Spark's inferred DECIMAL(38, 18) envelope (C2-L-002)."""

    from decimal import ROUND_DOWN, Decimal

    if not value.is_finite():
        raise PySparkTypeError(
            f"createDataFrame does not support non-finite Decimal values (got {value!s})"
        )

    if abs(value) >= _DECIMAL_MAX_ABS:
        raise PySparkValueError(
            f"createDataFrame Decimal value {value!s} exceeds DECIMAL("
            f"{_DECIMAL_PRECISION}, {_DECIMAL_SCALE}) magnitude "
            f"(|value| must be < 10**{_DECIMAL_PRECISION - _DECIMAL_SCALE})"
        )

    quantum = Decimal(1).scaleb(-_DECIMAL_SCALE)

    quantized = value.quantize(quantum, rounding=ROUND_DOWN)

    if quantized != value:
        raise PySparkValueError(
            f"createDataFrame Decimal value {value!s} is outside DECIMAL("
            f"{_DECIMAL_PRECISION}, {_DECIMAL_SCALE}) scale "
            "(fractional digits beyond 18 are not representable without rounding; "
            "refuse rather than silent zero/round)"
        )


def _infer_arrow_type_from_python_sample(sample: Any) -> Any:
    """Best-effort pyarrow type from a single Python sample cell (nested createDataFrame)."""

    import datetime as _dt

    from decimal import Decimal as _Decimal

    import pyarrow as pa

    if sample is None:
        return pa.string()

    if isinstance(sample, bool):
        return pa.bool_()

    if isinstance(sample, int) and not isinstance(sample, bool):
        return pa.int64()

    if isinstance(sample, float):
        return pa.float64()

    if isinstance(sample, str):
        return pa.string()

    if isinstance(sample, (bytes, bytearray, memoryview)):
        return pa.binary()

    if isinstance(sample, _dt.datetime):
        from repark.spark.session.timestamp_type import default_timestamp_arrow_type

        return default_timestamp_arrow_type()

    if isinstance(sample, _dt.date):
        return pa.date32()

    if isinstance(sample, _Decimal):
        return pa.decimal128(38, 18)

    if isinstance(sample, list):
        non_null = [item for item in sample if item is not None]

        # Live Spark merges ALL element types (ArrayType _merge_type) unless legacy
        # first-element conf is on. List-of-dict → struct field union; nested
        # list<list<dict>> must also merge sibling element schemas.

        # Empty list under conf true → list<null> so multi-row / multi-sample merge
        # can still adopt a concrete element type (empty→string was
        # swallowing later struct elements via string-wins-all).

        if _INFER_NESTED_DICT_AS_STRUCT.get() and not non_null:
            return pa.list_(pa.null())

        if _INFER_NESTED_DICT_AS_STRUCT.get() and non_null:
            if all(isinstance(item, dict) for item in non_null):
                if _LEGACY_FIRST_ELEMENT_COERCE.get():
                    return pa.list_(_infer_struct_arrow_from_dict_samples([non_null[0]]))

                return pa.list_(_infer_struct_arrow_from_dict_samples(non_null))

            if _LEGACY_FIRST_ELEMENT_COERCE.get():
                return pa.list_(_infer_arrow_type_from_python_sample(non_null[0]))

            merged_element = _infer_arrow_type_from_python_sample(non_null[0])

            for item in non_null[1:]:
                merged_element = _merge_inferred_arrow_types(
                    merged_element, _infer_arrow_type_from_python_sample(item)
                )

            return pa.list_(merged_element)

        element = next((item for item in sample if item is not None), None)

        return pa.list_(_infer_arrow_type_from_python_sample(element))

    if type(sample).__name__ == "Row" and type(sample).__module__.startswith("repark"):
        return pa.struct(
            [
                (name, _infer_arrow_type_from_python_sample(value))
                for name, value in zip(sample.__fields__, list(sample), strict=True)
            ]
        )

    # Spark createDataFrame: bare tuple → struct with positional ``_1``, ``_2``, … fields

    # (1-based; Apache ``test_print_schema`` / nested ``(2, 2)`` — F2). Namedtuples are handled

    # as row shapes earlier; here the sample is a nested cell.

    if isinstance(sample, tuple):
        if not sample:
            return pa.struct([])

        return pa.struct(
            [
                (f"_{index + 1}", _infer_arrow_type_from_python_sample(value))
                for index, value in enumerate(sample)
            ]
        )

    if isinstance(sample, dict):
        # Conf true → StructType for dict-valued cells (SPARK-35929); row-dicts never
        # reach this helper (they go through key-union / mapping bind).

        if _INFER_NESTED_DICT_AS_STRUCT.get():
            return _infer_struct_arrow_from_dict_samples([sample])

        # Spark schema inference: Python dict → map (key type from samples). Non-str keys
        # must not force map<string,…> then fail at array build.

        # Mixed value types (e.g. Legs [{"LegId":1,"Side":"Buy"}]) → map value string
        # (Spark 4.1.2 stringifies map values under key-union / mixed inference).

        if not sample:
            return pa.map_(pa.string(), pa.string())

        key_sample = next(iter(sample.keys()))

        value_types: list[Any] = []

        for value in sample.values():
            if value is None:
                continue

            value_types.append(_infer_arrow_type_from_python_sample(value))

        if not value_types:
            value_arrow = pa.string()

        else:
            first_value_type = value_types[0]

            if all(value_type.equals(first_value_type) for value_type in value_types[1:]):
                value_arrow = first_value_type

            else:
                value_arrow = pa.string()

        return pa.map_(
            _infer_arrow_type_from_python_sample(key_sample),
            value_arrow,
        )

    return pa.string()


_LEGACY_FIRST_ELEMENT_COERCE: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "repark_legacy_first_element_coerce", default=False
)


_INFER_NESTED_DICT_AS_STRUCT: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "repark_infer_nested_dict_as_struct", default=False
)


def _arrow_type_merge_label(arrow_type: Any) -> str:
    """Spark-ish type name for CANNOT_MERGE_TYPE messages from Arrow types."""

    import pyarrow as pa

    if pa.types.is_boolean(arrow_type):
        return "BooleanType"

    if pa.types.is_integer(arrow_type):
        return "LongType"

    if pa.types.is_floating(arrow_type):
        return "DoubleType"

    if pa.types.is_decimal(arrow_type):
        return "DecimalType"

    if pa.types.is_timestamp(arrow_type):
        return "TimestampType"

    if pa.types.is_date(arrow_type):
        return "DateType"

    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "StringType"

    if pa.types.is_struct(arrow_type):
        return "StructType"

    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return "ArrayType"

    if pa.types.is_map(arrow_type):
        return "MapType"

    return str(arrow_type)


def _arrow_type_is_nested(arrow_type: Any) -> bool:
    """True for list/struct/map (string must not silently win over these — octo C3)."""

    import pyarrow as pa

    return bool(
        pa.types.is_struct(arrow_type)
        or pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_map(arrow_type)
    )


def _merge_inferred_arrow_types(left: Any, right: Any) -> Any:
    """Merge two inferred Arrow types (Spark ``_merge_type`` subset for dict-as-struct).



    NullType is soft (merges as the other side). String wins over **atomic** only

    (live Spark long+string → string) — never over nested list/struct/map (that

    stringified dict cells — octo C3-L-001). Long+Double / other incompatible scalar

    pairs refuse ``CANNOT_MERGE_TYPE``. Nested list/struct/map recurse.

    """

    import pyarrow as pa

    if left.equals(right):
        return left

    # NullType (empty-list element under conf true) absorbs into the concrete side.

    if pa.types.is_null(left):
        return right

    if pa.types.is_null(right):
        return left

    if (pa.types.is_list(left) or pa.types.is_large_list(left)) and (
        pa.types.is_list(right) or pa.types.is_large_list(right)
    ):
        return pa.list_(_merge_inferred_arrow_types(left.value_type, right.value_type))

    if pa.types.is_struct(left) and pa.types.is_struct(right):
        return _merge_struct_arrow_types(left, right)

    if pa.types.is_map(left) and pa.types.is_map(right):
        return pa.map_(
            _merge_inferred_arrow_types(left.key_type, right.key_type),
            _merge_inferred_arrow_types(left.item_type, right.item_type),
        )

    # Atomic + String → String (Spark promotes; Apache long+str field pin).

    # Nested + String refuses (do not stringify struct/list cells).

    if pa.types.is_string(left) or pa.types.is_large_string(left):
        if _arrow_type_is_nested(right):
            left_label = _arrow_type_merge_label(left)

            right_label = _arrow_type_merge_label(right)

            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type {left_label} and {right_label}"
            )

        return left

    if pa.types.is_string(right) or pa.types.is_large_string(right):
        if _arrow_type_is_nested(left):
            left_label = _arrow_type_merge_label(left)

            right_label = _arrow_type_merge_label(right)

            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type {left_label} and {right_label}"
            )

        return right

    left_label = _arrow_type_merge_label(left)

    right_label = _arrow_type_merge_label(right)

    raise PySparkTypeError(f"[CANNOT_MERGE_TYPE] Can not merge type {left_label} and {right_label}")


def _merge_struct_arrow_types(left: Any, right: Any) -> Any:
    """Union two struct types: keep left field order, append new fields from right.



    Live Spark ``_merge_type`` for StructType (field-union order pin).

    """

    import pyarrow as pa

    right_by_name = {field.name: field.type for field in right}

    fields: list[tuple[str, Any]] = []

    seen: set[str] = set()

    for field in left:
        name = field.name

        seen.add(name)

        if name in right_by_name:
            fields.append((name, _merge_inferred_arrow_types(field.type, right_by_name[name])))

        else:
            fields.append((name, field.type))

    for field in right:
        if field.name not in seen:
            fields.append((field.name, field.type))

    return pa.struct(fields)


def _infer_struct_arrow_from_dict_samples(samples: list[dict[str, Any]]) -> Any:
    """Build a struct Arrow type by unioning keys across dict *cell* samples.



    Field order: insertion order of the first sample that contributes each key

    (Spark dict-as-struct uses ``dict.items()`` order, not sorted row-key-union order).

    Null values do not contribute a field type (live: ``{"a": None, "b": 1}`` → only ``b``).

    Non-string keys refuse (Spark ``field name … should be a string``).

    """

    import pyarrow as pa

    field_order: list[str] = []

    field_types: dict[str, Any] = {}

    for sample in samples:
        if not isinstance(sample, dict):
            continue

        for key, value in sample.items():
            # Null *values* do not contribute a field type (live Spark). Null *keys*

            # are not valid struct field names — refuse; do not

            # silently skip and drop the cell's association.

            if key is None:
                raise PySparkTypeError("field name None should be a string")

            if value is None:
                continue

            if not isinstance(key, str):
                raise PySparkTypeError(f"field name {key!r} should be a string")

            inferred = _infer_arrow_type_from_python_sample(value)

            if key not in field_types:
                field_order.append(key)

                field_types[key] = inferred

            else:
                field_types[key] = _merge_inferred_arrow_types(field_types[key], inferred)

    return pa.struct([(name, field_types[name]) for name in field_order])


def _prepare_nested_cell(cell: Any, arrow_type: Any) -> Any:
    """Convert Row / dict / list cells into shapes ``pa.array`` accepts for ``arrow_type``.



    Also coerces Python values toward the declared Arrow type (Spark createDataFrame

    stringifies non-strings into StringType columns — Apache ``test_convert_list_to_str``).

    """

    import pyarrow as pa

    if cell is None:
        return None

    # Declared string column: Spark ``to_str`` for non-string Python values.

    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        if isinstance(cell, str):
            return cell

        return str(cell)

    if pa.types.is_integer(arrow_type):
        # Never ``int(float)`` / Arrow Decimal→int truncate on list/scalar/map cells.
        # Spark refuses Long+Double/Decimal/Boolean.

        if isinstance(cell, bool):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type LongType and BooleanType "
                f"(got bool {cell!r} for integer field)"
            )

        if isinstance(cell, float):
            if _LEGACY_FIRST_ELEMENT_COERCE.get():
                # Legacy first-element mode: Spark truncates toward the inferred Long.

                return int(cell)

            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type LongType and DoubleType "
                f"(got float {cell!r} for integer field)"
            )

        from decimal import Decimal as _Decimal

        if isinstance(cell, _Decimal):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type LongType and DecimalType "
                f"(got Decimal {cell!r} for integer field)"
            )

        if isinstance(cell, int):
            return int(cell)

    if pa.types.is_floating(arrow_type):
        # Double + Boolean → 1.0 via pa.array was silent wrong.

        if isinstance(cell, bool):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type DoubleType and BooleanType "
                f"(got bool {cell!r} for floating field)"
            )

        from decimal import Decimal as _Decimal

        if isinstance(cell, _Decimal):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type DoubleType and DecimalType "
                f"(got Decimal {cell!r} for floating field)"
            )

        if isinstance(cell, (int, float)):
            return float(cell)

    if pa.types.is_decimal(arrow_type):
        # Inferred Decimal + Double/Boolean refuse; int is allowed under explicit Decimal schema.

        if isinstance(cell, bool):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type DecimalType and BooleanType "
                f"(got bool {cell!r} for decimal field)"
            )

        if isinstance(cell, float):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type DecimalType and DoubleType "
                f"(got float {cell!r} for decimal field)"
            )

    if pa.types.is_timestamp(arrow_type):
        # Never accept int/float as epoch under timestamp (silent 1970-01-01).

        import datetime as _dt

        from decimal import Decimal as _Decimal

        if isinstance(cell, _dt.datetime):
            if arrow_type.tz:
                from repark.spark.session.session_time_zone import localize_naive_datetime_to_utc

                return localize_naive_datetime_to_utc(cell)
            return cell.replace(tzinfo=None) if cell.tzinfo is not None else cell

        # ``date`` is listed after the datetime return (datetime is a date subclass).

        if isinstance(cell, (bool, int, float, _Decimal, _dt.date)):
            kind = _python_scalar_merge_kind(cell) or type(cell).__name__

            label = _SPARK_SCALAR_MERGE_LABELS.get(kind, kind)

            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type TimestampType and {label} "
                f"(got {cell!r} for timestamp field)"
            )

    if pa.types.is_date(arrow_type):
        # Never accept int as day-epoch under date32 (silent 1970-01-02).

        import datetime as _dt

        from decimal import Decimal as _Decimal

        if isinstance(cell, _dt.datetime):
            raise PySparkTypeError(
                "[CANNOT_MERGE_TYPE] Can not merge type DateType and TimestampType "
                f"(got {cell!r} for date field)"
            )

        if isinstance(cell, _dt.date):
            return cell

        if isinstance(cell, (bool, int, float, _Decimal)):
            kind = _python_scalar_merge_kind(cell) or type(cell).__name__

            label = _SPARK_SCALAR_MERGE_LABELS.get(kind, kind)

            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type DateType and {label} "
                f"(got {cell!r} for date field)"
            )

    if type(cell).__name__ == "Row" and type(cell).__module__.startswith("repark"):
        # Struct from Row: dict of field → prepared value (schema field order, name match).

        if pa.types.is_struct(arrow_type):
            prepared: dict[str, Any] = {}

            for field in arrow_type:
                name = field.name

                try:
                    value = cell[name]

                except Exception:
                    value = None

                prepared[name] = _prepare_nested_cell(value, field.type)

            return prepared

        return dict(zip(cell.__fields__, list(cell), strict=True))

    if isinstance(cell, dict) and pa.types.is_map(arrow_type):
        # Arrow map cells: list of (key, value) pairs; prepare keys for non-string map keys.

        key_type = arrow_type.key_type

        item_type = arrow_type.item_type

        return [
            (
                _prepare_nested_cell(key, key_type),
                _prepare_nested_cell(value, item_type),
            )
            for key, value in cell.items()
        ]

    if isinstance(cell, dict) and pa.types.is_struct(arrow_type):
        return {
            field.name: _prepare_nested_cell(cell.get(field.name), field.type)
            for field in arrow_type
        }

    if isinstance(cell, (list, tuple)) and pa.types.is_struct(arrow_type):
        # Positional tuple/list → struct by field order (Spark createDataFrame).

        fields = list(arrow_type)

        if len(cell) != len(fields):
            raise PySparkTypeError(
                f"createDataFrame struct expects {len(fields)} field(s), got {len(cell)}"
            )

        return {
            field.name: _prepare_nested_cell(cell[index], field.type)
            for index, field in enumerate(fields)
        }

    if isinstance(cell, list) and (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        value_type = arrow_type.value_type

        return [_prepare_nested_cell(item, value_type) for item in cell]

    return cell


def _normalize_nested_sql_type_aliases(sql_type: str) -> str:
    """Rewrite SQL aliases inside nested type markers so :meth:`DataType.fromDDL` accepts them.



    ``_data_type_to_sql_type`` historically emitted ``VARCHAR`` for strings; bare VARCHAR is

    not an atomic fromDDL token (only ``string`` / ``str`` / ``varchar(n)``). Nested markers

    must use STRING (or be rewritten here) or the whole column silently became string.

    """

    import re as _re

    return _re.sub(r"\bVARCHAR\b", "STRING", sql_type, flags=_re.IGNORECASE)


def _sql_type_to_arrow(sql_type: str) -> Any:
    """Map engine CAST type strings to pyarrow types (createDataFrame schema path)."""

    import pyarrow as pa

    from repark.spark.types import DataType, repark_type_to_arrow

    stripped = sql_type.strip()

    upper = stripped.upper()

    # Nested ARRAY<> / MAP<> / STRUCT<> from _data_type_to_sql_type — parse via types.

    # Fail loud on parse errors (never silent pa.string() — that stringified nested cells

    # and looked like a successful createDataFrame with wrong schema).

    if upper.startswith(("ARRAY<", "MAP<", "STRUCT<")):
        normalized = _normalize_nested_sql_type_aliases(stripped)

        try:
            return repark_type_to_arrow(DataType.fromDDL(normalized))

        except Exception as error:
            raise PySparkTypeError(
                f"createDataFrame cannot map nested schema type {sql_type!r} to Arrow: {error}"
            ) from error

    # Strip DECIMAL(p,s) precision for matching.

    base = upper.split("(", 1)[0].strip()

    mapping = {
        "BOOLEAN": pa.bool_(),
        "BOOL": pa.bool_(),
        "TINYINT": pa.int8(),
        "SMALLINT": pa.int16(),
        "INT": pa.int32(),
        "INTEGER": pa.int32(),
        "BIGINT": pa.int64(),
        "LONG": pa.int64(),
        "FLOAT": pa.float32(),
        "REAL": pa.float32(),
        "DOUBLE": pa.float64(),
        "FLOAT8": pa.float64(),
        "VARCHAR": pa.string(),
        "STRING": pa.string(),
        "TEXT": pa.string(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC"),
        "TIMESTAMP_NTZ": pa.timestamp("us"),
        "BINARY": pa.binary(),
        "BYTEA": pa.binary(),
        # G3b D-5: an explicitly requested void column stays void (pa.null()), never
        # a silent pa.string() substitution.
        "VOID": pa.null(),
        "NULL": pa.null(),
    }

    if base in mapping:
        return mapping[base]

    if base == "DECIMAL" or base == "NUMERIC":
        # DECIMAL(p,s) — default 38,18 when unparsed; try extract.

        precision, scale = 38, 18

        if "(" in upper:
            inside = upper[upper.index("(") + 1 : upper.rindex(")")]

            parts = [part.strip() for part in inside.split(",")]

            if len(parts) == 2:
                precision, scale = int(parts[0]), int(parts[1])

        return pa.decimal128(precision, scale)

    # Fallback matches prior VALUES-path default for unknown (string).

    return pa.string()
