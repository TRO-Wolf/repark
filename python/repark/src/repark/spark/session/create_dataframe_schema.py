"""createDataFrame schema and null inference."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.errors import PySparkTypeError, PySparkValueError


if TYPE_CHECKING:
    from repark.spark.session.create_dataframe_values import (
        _DECIMAL_PRECISION,
        _DECIMAL_SCALE,
        _NUMPY_DATETIME64_DATE_UNITS,
        _TYPED_NULL_SQL,
        _data_type_to_sql_type,
        _normalize_create_dataframe_cell,
        _numpy_datetime64_unit,
    )


def _parse_schema_ddl(ddl: str) -> tuple[list[str], list[str]] | None:
    """Parse ``'a INT, b STRING'`` / nested ``'a ARRAY<INT>'`` → names + SQL types.



    Returns None if not a field-list DDL (bare type tokens / character-iteration trap).

    Nested array/map/struct field types use :meth:`DataType.fromDDL` so Apache-style

    nested DDL schemas work the same as StructType (octo X2 C1).

    """

    stripped = ddl.strip()

    # Single token without a type is not DDL (character-iteration trap).

    if not stripped or (" " not in stripped and ":" not in stripped and "<" not in stripped):
        return None

    from repark.spark.types import DataType, StructType

    try:
        parsed = DataType.fromDDL(stripped)

    except (ValueError, TypeError):
        return None

    if not isinstance(parsed, StructType) or not parsed.fields:
        return None

    names = [field.name for field in parsed.fields]

    engine_types = [_data_type_to_sql_type(field.dataType) for field in parsed.fields]

    return names, engine_types


def _datetime64_unit_from_dtype(dtype: Any) -> str | None:
    """Extract a numpy/pandas ``datetime64[unit]`` unit with case preserved (C5-Q-001 / C5-L-001).



    Numpy units are case-sensitive: ``M`` = month (calendar → DATE), ``m`` = minute (TIMESTAMP).

    Callers must not lowercase the dtype text before extracting the unit. Returns ``None`` when

    the spelling is not ``datetime64[…]`` (ArrowDtype ``timestamp[…]`` / bare timestamp).

    """

    raw = str(dtype)

    marker = "datetime64["

    start = raw.find(marker)

    if start < 0:
        return None

    unit_start = start + len(marker)

    unit_end = raw.find("]", unit_start)

    if unit_end < 0:
        return None

    unit = raw[unit_start:unit_end]

    # DatetimeTZDtype: ``datetime64[us, UTC]`` — unit is the token before the comma.

    if "," in unit:
        unit = unit.split(",", 1)[0].strip()

    return unit or None


def _null_sql_for_pandas_dtype(dtype: Any) -> str:
    """Map a pandas dtype to ``CAST(NULL AS …)`` for all-null columns (C3-Q-001 / C4-* / C5-*).



    Integer dtypes always map to ``BIGINT`` so all-null vs non-null occupancy cannot change

    Arrow width (VALUES emits bare Python ``int`` → int64 for every non-null integer cell —

    C4-Q-001). ``ArrowDtype`` spellings (``timestamp[ns][pyarrow]``, ``date32[day][pyarrow]``,

    ``double[pyarrow]``, …) are recognized (C4-L-002). Timedelta / duration refuse loud so

    all-null cannot soft-succeed as VARCHAR while non-null Timedelta raises (C4-Q-002).

    ``IntervalDtype`` refuses before the ``startswith("int")`` arm (``"interval…".startswith(

    "int")`` is true — silent BIGINT fail-open — C3-L-002). ``PeriodDtype`` refuses before the

    date arm (``period[D]`` ends with ``[d]`` → silent DATE; ``period[M]`` was VARCHAR while

    non-null Period raised — C4-Q-002 / C4-L-002). Categorical maps via ``categories.dtype`` so

    int categories cannot flip VARCHAR↔int64 by null occupancy (C4-Q-003). Unsupported

    ArrowDtype time/binary/nested refuse rather than VARCHAR (parity with polars — C4-Q-004 /

    C4-L-003). Calendar ``datetime64[D|W|M|Y]`` → DATE so all-null occupancy matches non-null

    unit-D cells (C3-Q-001); unit match is **case-sensitive** so minute ``m`` is never month

    ``M`` (C5-Q-001 / C5-L-001). ``complex*`` refuses (C5-Q-002). ``SparseDtype`` unwraps to

    ``subtype`` so Sparse[int64]/Sparse[bool] cannot flip VARCHAR↔payload (C5-Q-003 /

    C5-SAF-002). Sparse[object] all-null is re-typed from cell witnesses in

    ``_rows_from_pandas`` (NaN→DOUBLE, NaT→TIMESTAMP — C6-Q-001), not via this map alone.

    """

    raw_text = str(dtype)

    text = raw_text.lower()

    type_name = type(dtype).__name__

    # Period before date/[d]: "period[d]" ends with "[d]" and would soft-map to DATE.

    if "period" in text or type_name == "PeriodDtype":
        raise PySparkTypeError(
            f"createDataFrame does not support pandas Period dtypes yet (got dtype {dtype!s})"
        )

    # Interval before int/float: "interval…".startswith("int") and
    # "float" in "interval[float64,…]".

    if "interval" in text:
        raise PySparkTypeError(
            f"createDataFrame does not support pandas Interval dtypes yet (got dtype {dtype!s})"
        )

    # complex before float/VARCHAR: all-null must not soft-succeed as VARCHAR.

    if "complex" in text or type_name.startswith("Complex"):
        raise PySparkTypeError(
            f"createDataFrame does not support pandas complex dtypes yet (got dtype {dtype!s})"
        )

    # SparseDtype before int/bool/float arms: ``Sparse[int64, nan]`` does not startswith("int")
    # and would fall through to VARCHAR while non-null cells type as int64. Sparse[object]:
    # subtype unwrap alone → VARCHAR; cell witnesses run in ``_rows_from_pandas``.
    # Keep unwrap for typed subtypes (int/bool/float/…).

    if "sparse" in text or type_name == "SparseDtype":
        subtype = getattr(dtype, "subtype", None)

        if subtype is not None:
            return _null_sql_for_pandas_dtype(subtype)

        return _TYPED_NULL_SQL

    # Timestamp / datetime before bare "date" / "time" substrings (incl. ArrowDtype).

    if "datetime64" in text or text.startswith("datetime") or "timestamp" in text:
        # Calendar units → DATE (null-occupancy stable with non-null unit-D → date mapping).
        # Unit is extracted case-sensitively from the raw dtype string; lowercasing the text
        # would map minute ``m`` to month ``M`` and flip all-null DATE vs non-null TIMESTAMP.

        unit = _datetime64_unit_from_dtype(dtype)

        if unit is not None and unit in _NUMPY_DATETIME64_DATE_UNITS:
            return "CAST(NULL AS DATE)"

        return "CAST(NULL AS TIMESTAMP)"

    if "timedelta" in text or "duration" in text:
        raise PySparkTypeError(
            "createDataFrame does not support pandas timedelta/duration dtypes yet "
            f"(got dtype {dtype!s})"
        )

    # Categorical: non-null cells are the underlying category values (int → int64, …). Map
    # all-null via categories.dtype so occupancy cannot flip VARCHAR↔payload type.

    if "category" in text or type_name == "CategoricalDtype":
        categories = getattr(dtype, "categories", None)

        categories_dtype = getattr(categories, "dtype", None) if categories is not None else None

        if categories_dtype is not None:
            return _null_sql_for_pandas_dtype(categories_dtype)

        return _TYPED_NULL_SQL

    # Unsupported ArrowDtype shapes — refuse before VARCHAR. Nested list/struct/map land
    # via pa.Table.from_pandas. Dictionary stays refuse (category unwrap is separate).

    if (text.endswith("[pyarrow]") or "[pyarrow]" in text or type_name == "ArrowDtype") and (
        text.startswith("time")
        or "time32" in text
        or "time64" in text
        or "binary" in text
        or "large_binary" in text
        or "dictionary" in text
    ):
        raise PySparkTypeError(
            "createDataFrame does not support pandas Arrow time/binary/dictionary dtypes yet "
            f"(got dtype {dtype!s})"
        )

    if text in {"bool", "boolean"} or text.startswith("bool"):
        return "CAST(NULL AS BOOLEAN)"

    # float* + ArrowDtype double[pyarrow] / float[pyarrow].

    if (
        "float" in text
        or text.startswith("float")
        or text.startswith("double")
        or "double[" in text
        or text in {"float32", "float64", "float16", "double"}
    ):
        return "CAST(NULL AS DOUBLE)"

    if (
        text.startswith("int")
        or text.startswith("uint")
        or "int[" in text
        or "uint[" in text
        or text
        in {
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
        }
    ):
        # VALUES path always widens non-null Python int → int64; keep all-null stable.

        return "CAST(NULL AS BIGINT)"

    # date32 / date64 / pure date — after datetime/timestamp so "datetime" is not misread.

    # Period already refused above so period[D] cannot land here via endswith("[d]").

    if (
        text == "date"
        or text.startswith("date32")
        or text.startswith("date64")
        or "date32" in text
        or "date64" in text
        or (text.endswith("[d]") and "datetime" not in text and "timestamp" not in text)
    ):
        return "CAST(NULL AS DATE)"

    if "decimal" in text:
        return f"CAST(NULL AS DECIMAL({_DECIMAL_PRECISION}, {_DECIMAL_SCALE}))"

    # string / object / unknown — stable VARCHAR fallback. Object-dtype all-null columns
    # are re-typed from cell witnesses in ``_rows_from_pandas``; pure None stays VARCHAR here.

    return _TYPED_NULL_SQL


def _null_sql_for_polars_dtype(dtype: Any) -> str:
    """Map a polars dtype to ``CAST(NULL AS …)`` for all-null columns (C3-Q-001 / C4-*).



    All integer widths → ``BIGINT`` (null-occupancy-stable with VALUES int literals — C4-Q-001).

    ``Duration`` refuses (parity with non-null Timedelta / duration refuse — C4-Q-002).

    Binary / Time / Object refuse so all-null cannot soft-succeed as VARCHAR while non-null

    cells raise at the SQL literal boundary (C3-L-003). Nested ``List`` / ``Struct`` /

    ``Array`` are **accepted** via the polars ``.to_arrow()`` path (r21 T1) — they never

    hit VALUES literals.

    """

    text = str(dtype)

    text_lower = text.lower()

    if text_lower.startswith("datetime") or "datetime(" in text_lower:
        return "CAST(NULL AS TIMESTAMP)"

    if text_lower.startswith("duration") or "duration(" in text_lower:
        raise PySparkTypeError(
            f"createDataFrame does not support polars Duration dtypes yet (got dtype {dtype!s})"
        )

    # Binary / Time / Object still refuse (engine cannot represent / no VALUES path).
    # Nested List/Struct/Array pass through (Arrow C-stream path).

    if text in {"Binary", "Time", "Object"} or text_lower in {"binary", "time", "object"}:
        raise PySparkTypeError(
            "createDataFrame does not support polars binary/time/object dtypes yet "
            f"(got dtype {dtype!s})"
        )

    if text == "Date" or text_lower == "date":
        return "CAST(NULL AS DATE)"

    if text in {"Boolean", "Bool"} or text_lower in {"boolean", "bool"}:
        return "CAST(NULL AS BOOLEAN)"

    if text in {"Float64", "Float32"} or text_lower.startswith("float"):
        return "CAST(NULL AS DOUBLE)"

    if (
        text in {"Int64", "Int32", "Int16", "Int8", "UInt64", "UInt32", "UInt16", "UInt8"}
        or text_lower.startswith("int")
        or text_lower.startswith("uint")
    ):
        # Match VALUES bare-int → int64; no data-dependent int32/int64 flip.

        return "CAST(NULL AS BIGINT)"

    if text_lower.startswith("decimal"):
        return f"CAST(NULL AS DECIMAL({_DECIMAL_PRECISION}, {_DECIMAL_SCALE}))"

    return _TYPED_NULL_SQL


def _pandas_dtype_needs_object_null_witness(dtype: Any) -> bool:
    """True when all-null typing must scan cells (object / Sparse[object] — C5-SAF-001 / C6-Q-001).



    Top-level object is untyped. Sparse[object] unwraps to object in the dtype map and would

    soft-map VARCHAR while non-null Sparse[object] cells type as DOUBLE/TIMESTAMP from values —

    a null-occupancy flip unless the object NaN/NaT witness runs (C6-Q-001).

    """

    text = str(dtype).lower()

    type_name = type(dtype).__name__

    if text == "object" or type_name in {"ObjectDType", "object"}:
        return True

    if "sparse" in text or type_name == "SparseDtype":
        subtype = getattr(dtype, "subtype", None)

        if subtype is None:
            return False

        subtype_text = str(subtype).lower()

        subtype_type_name = type(subtype).__name__

        return subtype_text == "object" or subtype_type_name in {"ObjectDType", "object"}

    return False


def _infer_null_sql_from_raw_cells(cells: list[Any]) -> str:
    """Infer ``CAST(NULL AS …)`` for an all-null column from pre-normalize witnesses (C4-L-001).



    Normalize keeps ``float('nan')`` and erases ``NaT`` to ``None``. On

    list/dict/Row/tuple paths there is no frame dtype, so without this witness scan the VALUES

    emitter would emit VARCHAR for all-NaN (Spark double) and all-NaT (Spark timestamp).

    Pure ``None`` columns stay VARCHAR (C2-L-003).

    """

    import datetime as dt

    from decimal import Decimal

    saw_timestamp = False

    saw_date = False

    saw_decimal = False

    saw_float = False

    saw_bool = False

    saw_int = False

    for value in cells:
        if value is None:
            continue

        module_name = type(value).__module__

        type_name = type(value).__name__

        if module_name.startswith("pandas"):
            if type_name == "NAType":
                # Untyped pandas missing — no dtype witness.

                continue

            if type_name == "NaTType":
                saw_timestamp = True

                continue

            if type_name == "Timestamp":
                saw_timestamp = True

                continue

            if type_name == "Timedelta":
                raise PySparkTypeError(
                    "createDataFrame does not support values of type Timedelta yet"
                )

        if module_name.startswith("numpy"):
            if type_name == "datetime64":
                # Calendar units D/W/M/Y → DATE; finer (and ns) → TIMESTAMP. Must not force
                # TIMESTAMP for every datetime64: non-null unit-D becomes DATE, so all-null
                # NaT[D] would otherwise flip Arrow type by null occupancy.

                unit = _numpy_datetime64_unit(value)

                if unit is not None and unit in _NUMPY_DATETIME64_DATE_UNITS:
                    saw_date = True

                else:
                    saw_timestamp = True

                continue

            if type_name == "timedelta64":
                raise PySparkTypeError(
                    "createDataFrame does not support values of type numpy.timedelta64 yet"
                )

            if type_name in {"float64", "float32", "float16"}:
                saw_float = True

                continue

            if type_name in {
                "int64",
                "int32",
                "int16",
                "int8",
                "uint64",
                "uint32",
                "uint16",
                "uint8",
            }:
                saw_int = True

                continue

            if type_name in {"bool_", "bool"}:
                saw_bool = True

                continue

            if hasattr(value, "item"):
                # Unwrap other numpy scalars and re-inspect.

                try:
                    unwrapped = value.item()

                except (ValueError, AttributeError):
                    continue

                if unwrapped is value:
                    continue

                # Classify the unwrapped Python scalar (one level).

                if isinstance(unwrapped, float):
                    saw_float = True

                elif isinstance(unwrapped, bool):
                    saw_bool = True

                elif isinstance(unwrapped, int):
                    saw_int = True

                elif isinstance(unwrapped, dt.datetime):
                    saw_timestamp = True

                elif isinstance(unwrapped, dt.date):
                    saw_date = True

                elif isinstance(unwrapped, Decimal):
                    saw_decimal = True

                continue

        if isinstance(value, float):
            saw_float = True

            continue

        if isinstance(value, bool):
            saw_bool = True

            continue

        if isinstance(value, int):
            saw_int = True

            continue

        if isinstance(value, dt.datetime):
            saw_timestamp = True

            continue

        if isinstance(value, dt.date):
            saw_date = True

            continue

        if isinstance(value, Decimal):
            saw_decimal = True

            continue

        # str / unknown: do not force a type from a non-null witness of an unsupported shape;

        # all-null after normalize with only opaque witnesses falls through to VARCHAR.

    if saw_timestamp:
        return "CAST(NULL AS TIMESTAMP)"

    if saw_date:
        return "CAST(NULL AS DATE)"

    if saw_decimal:
        return f"CAST(NULL AS DECIMAL({_DECIMAL_PRECISION}, {_DECIMAL_SCALE}))"

    if saw_float:
        return "CAST(NULL AS DOUBLE)"

    if saw_bool:
        return "CAST(NULL AS BOOLEAN)"

    if saw_int:
        return "CAST(NULL AS BIGINT)"

    return _TYPED_NULL_SQL


def _column_null_sql_from_raw_tuples(
    tuples: list[tuple[Any, ...]],
    width: int,
    names: list[str] | None = None,
) -> list[str]:
    """Per-column all-null CAST for non-frame paths from raw (pre-normalize) cells (C4-L-001).



    When ``names`` is provided, unsupported ``array.array`` typecodes raise

    ``CANNOT_INFER_TYPE_FOR_FIELD`` with the column name (F1 / test_array_types).

    """

    column_null_sql: list[str] = []

    for column_index in range(width):
        cells = [row[column_index] for row in tuples]

        field_name: str | None = None

        if names is not None and column_index < len(names):
            field_name = names[column_index]

        if all(
            _normalize_create_dataframe_cell(cell, field_name=field_name) is None for cell in cells
        ):
            column_null_sql.append(_infer_null_sql_from_raw_cells(cells))

        else:
            # Non-all-null: entry unused by ``_values_sql_with_typed_nulls``; stable default.

            column_null_sql.append(_TYPED_NULL_SQL)

    return column_null_sql


def _schema_names_and_permutation(
    source_names: list[str],
    schema: list[str] | None,
    *,
    kind: str,
) -> tuple[list[str], list[int]]:
    """Resolve ``schema=[names]`` against ordered source column names (C2-L-001).



    Returns ``(output_names, permutation)`` where ``permutation[i]`` is the source index that

    feeds output column ``i``.



    * ``schema is None`` → identity (keep source names and order).

    * same name multiset as ``source_names`` → **by-name reorder** (values follow names).

    * same length, no shared names → **positional rename**.

    * length mismatch or partial name overlap → fail loud (no silent swap / project / drop).

    """

    if schema is None:
        return list(source_names), list(range(len(source_names)))

    if len(schema) != len(source_names):
        raise PySparkValueError(
            f"schema length {len(schema)} does not match {kind} column count {len(source_names)}"
        )

    if len(set(source_names)) != len(source_names):
        raise PySparkValueError(
            f"createDataFrame {kind} has duplicate column names; "
            "ambiguous schema bind is not supported"
        )

    if len(set(schema)) != len(schema):
        raise PySparkValueError(
            "createDataFrame schema has duplicate names; ambiguous schema bind is not supported"
        )

    source_set = set(source_names)

    schema_set = set(schema)

    if source_set == schema_set:
        index_by_name = {name: index for index, name in enumerate(source_names)}

        permutation = [index_by_name[name] for name in schema]

        return list(schema), permutation

    overlap = source_set & schema_set

    if overlap:
        raise PySparkValueError(
            f"createDataFrame schema partially overlaps {kind} column names "
            f"{sorted(overlap)!r}; pass a pure rename (disjoint names) or a pure "
            "reorder (same names in a different order) — mixed bind is not supported"
        )

    # Pure rename: positional cells under the new names.

    return list(schema), list(range(len(source_names)))


def _apply_permutation(row: tuple[Any, ...], permutation: list[int]) -> tuple[Any, ...]:
    """Reorder a source-order row tuple by ``permutation`` (output index → source index)."""

    return tuple(row[source_index] for source_index in permutation)
