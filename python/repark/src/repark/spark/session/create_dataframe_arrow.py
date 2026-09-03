"""createDataFrame Arrow column conversion."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.errors import PySparkTypeError, PySparkValueError


if TYPE_CHECKING:
    from repark.spark.session.create_dataframe_inference import (
        _sql_type_to_arrow,
        _validate_decimal_envelope,
    )
    from repark.spark.session.create_dataframe_rows import (
        _refuse_duplicate_pandas_columns,
        _rows_from_pandas,
    )
    from repark.spark.session.create_dataframe_schema import (
        _infer_null_sql_from_raw_cells,
        _null_sql_for_pandas_dtype,
        _null_sql_for_polars_dtype,
        _pandas_dtype_needs_object_null_witness,
        _schema_names_and_permutation,
    )
    from repark.spark.session.create_dataframe_tuples import _arrow_table_from_tuples
    from repark.spark.session.create_dataframe_values import (
        _DECIMAL_PRECISION,
        _DECIMAL_SCALE,
        _normalize_create_dataframe_cell,
    )


def _arrow_null_sql_to_type(null_sql: str) -> Any:
    """Map ``CAST(NULL AS TYPE)`` (or bare TYPE) to a pyarrow type for object witnesses."""

    import pyarrow as pa

    upper = null_sql.upper().strip()

    if " AS " in upper:
        type_sql = upper.rsplit(" AS ", 1)[-1].strip()

        if type_sql.endswith(")") and type_sql.count("(") < type_sql.count(")"):
            type_sql = type_sql[:-1].strip()

    else:
        type_sql = upper

    if type_sql in {"VARCHAR", "STRING"}:
        return pa.string()

    return _sql_type_to_arrow(type_sql)


def _normalize_frame_arrow_column(column: Any, *, engine_type: str | None) -> Any:
    """Spark-parity Arrow column normalize after native pandas/polars export (P2a).



    * dictionary (category) → decoded values (ChunkedArray-safe)

    * refuse non-finite floats (inf) **before** typed cast — critic-octo C1: an early

      return on ``engine_type`` previously skipped the is_inf gate so StructType/DDL

      DoubleType/FloatType frames silently accepted ±inf while the untyped path refused

    * decimal envelope validate **before** rescale cast — native path must raise the same

      ``PySparkValueError`` as the list path (C2-L-002), not bare ArrowInvalid on rescale

    * integer widths → int64 when no declared engine type (VALUES parity — C4-Q-001)

    * float32 → float64 when no declared engine type

    * decimal* → decimal128(38, 18) when no declared engine type

    * large_string / string_view → utf8 string (tuple-path / interchange pins)

    """

    import pyarrow as pa

    import pyarrow.compute as pc

    # ChunkedArray has no dictionary_decode; cast to value type instead.

    if pa.types.is_dictionary(column.type):
        value_type = column.type.value_type

        column = column.cast(value_type)

    # Inf refuse must run for BOTH untyped and engine_type paths.

    if pa.types.is_floating(column.type) and len(column) > 0:
        # is_inf is null-safe; any true → refuse (tuple path refuses bare inf).

        inf_mask = pc.is_inf(column)

        if pc.any(inf_mask).as_py():
            raise PySparkTypeError("createDataFrame does not support infinite float values")

    # Decimal envelope before any rescale cast (list path validates Python Decimal first).

    if pa.types.is_decimal(column.type):
        _validate_decimal_column_envelope(column)

    if engine_type is not None:
        target = _sql_type_to_arrow(engine_type)

        if pa.types.is_timestamp(target) and target.tz is not None:
            return _localize_naive_timestamp_column(column)

        if not column.type.equals(target):
            column = column.cast(target)

        return column

    if pa.types.is_integer(column.type) and not pa.types.is_int64(column.type):
        column = column.cast(pa.int64())

    elif pa.types.is_float32(column.type):
        column = column.cast(pa.float64())

    elif pa.types.is_decimal(column.type):
        if column.type.precision != _DECIMAL_PRECISION or column.type.scale != _DECIMAL_SCALE:
            column = column.cast(pa.decimal128(_DECIMAL_PRECISION, _DECIMAL_SCALE))

    elif pa.types.is_large_string(column.type) or pa.types.is_string_view(column.type):
        column = column.cast(pa.string())

    elif pa.types.is_large_binary(column.type):
        column = column.cast(pa.binary())

    # Inferred pandas/polars naive timestamps stay naive.
    # Python-tuple naive datetime is LTZ via `_arrow_table_from_tuples`.

    return column


def _localize_naive_timestamp_column(column: Any) -> Any:
    """Naive timestamp cells → session-zone instants stored as ``timestamp[us, tz=UTC]``.

    When ``spark.sql.timestampType=TIMESTAMP_NTZ`` the inferred default is a naive
    wall clock — do not localize.
    """

    import datetime as _dt

    import pyarrow as pa

    from repark.spark.session.session_time_zone import localize_naive_datetime_to_utc
    from repark.spark.session.timestamp_type import is_default_timestamp_ntz

    if not pa.types.is_timestamp(column.type):
        return column

    if is_default_timestamp_ntz():
        values: list[Any] = []
        for cell in column.to_pylist():
            if cell is None:
                values.append(None)
            elif getattr(cell, "tzinfo", None) is not None:
                values.append(cell.astimezone(_dt.UTC).replace(tzinfo=None))
            else:
                values.append(cell)
        return pa.array(values, type=pa.timestamp("us"))

    if column.type.tz is not None and column.type.unit == "us":
        return column

    values: list[Any] = []
    for cell in column.to_pylist():
        if cell is None:
            values.append(None)
        elif getattr(cell, "tzinfo", None) is not None:
            values.append(cell.astimezone(_dt.UTC))
        else:
            values.append(localize_naive_datetime_to_utc(cell))
    return pa.array(values, type=pa.timestamp("us", tz="UTC"))


def _validate_decimal_column_envelope(column: Any) -> None:
    """Refuse Decimal values outside DECIMAL(38,18) on a native Arrow column (C2-L-002)."""

    import pyarrow as pa

    if not pa.types.is_decimal(column.type):
        return

    for value in column.to_pylist():
        if value is not None:
            _validate_decimal_envelope(value)


def _arrow_table_from_pandas(
    data: Any,
    schema: list[str] | None,
    *,
    engine_types: list[str] | None,
) -> Any:
    """Native pandas → Arrow (no full-frame row loop). Schema bind + refuse + cast rules.



    # === r20 P2a: cdf-extractor ===

    Uses ``pa.Table.from_pandas`` for the bulk conversion. Refuse classes fire via the same

    dtype map as the legacy extractor (Period/Interval/timedelta/complex/nested). Object /

    Sparse[object] all-null columns still run the NaN→DOUBLE / NaT→TIMESTAMP witness

    (C5-SAF-001 / C6-Q-001). Integer widths widen to int64 (C4-Q-001).

    """

    import pyarrow as pa

    _refuse_duplicate_pandas_columns(data)

    source_columns = [str(column) for column in data.columns]

    names, permutation = _schema_names_and_permutation(source_columns, schema, kind="pandas")

    if len(data) == 0:
        # Typed StructType/DDL empty frames keep declared types (list-path parity);
        # name-only schema cannot infer payload types → CANNOT_INFER_EMPTY_SCHEMA.

        if engine_types is not None:
            column_null_sql = [f"CAST(NULL AS {sql_type})" for sql_type in engine_types]

            return _arrow_table_from_tuples(
                names,
                [],
                column_null_sql=column_null_sql,
                engine_types=engine_types,
            )

        raise PySparkValueError(
            "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema for empty pandas DataFrame; "
            "pass a non-empty frame or a typed StructType schema "
            "(repark createDataFrame is VALUES-only and has no StructType path yet)"
        )

    # SparseDtype: ``pa.Table.from_pandas`` refuses sparse blocks, and densify-with-fill
    # corrupts nulls (int fill becomes a sentinel). Use the cell extractor for any frame
    # with Sparse columns (rare; not the ingest hot path).

    for source_index in range(data.shape[1]):
        dtype = data.iloc[:, source_index].dtype

        dtype_text = str(dtype).lower()

        if "sparse" in dtype_text or type(dtype).__name__ == "SparseDtype":
            names_s, tuples_s, column_null_sql_s = _rows_from_pandas(data, schema)

            return _arrow_table_from_tuples(
                names_s,
                tuples_s,
                column_null_sql=column_null_sql_s,
                engine_types=engine_types,
            )

    # Per-source-column: refuse dtypes + collect object-column all-null type overrides.

    object_null_types: dict[int, Any] = {}
    object_null_values: dict[int, list[Any]] = {}

    for source_index, column_name in enumerate(data.columns):
        series = data.iloc[:, source_index]

        dtype = series.dtype

        if _pandas_dtype_needs_object_null_witness(dtype):
            raw_cells = [series.iloc[row_index] for row_index in range(len(series))]

            # Refuse bad cells (inf / Period cell / Decimal envelope) even when mixed.

            normalized = [
                _normalize_create_dataframe_cell(cell, field_name=str(column_name))
                for cell in raw_cells
            ]

            if all(
                cell is None or (isinstance(cell, float) and cell != cell) for cell in normalized
            ):
                null_sql = _infer_null_sql_from_raw_cells(raw_cells)

                object_null_types[source_index] = _arrow_null_sql_to_type(null_sql)
                object_null_values[source_index] = normalized

        else:
            # Typed columns: refuse at dtype map (side effect); discard null-SQL.

            _null_sql_for_pandas_dtype(dtype)

    table = pa.Table.from_pandas(data, preserve_index=False)

    # Drop pandas metadata so engine consumers see a plain schema.

    table = table.replace_schema_metadata(None)

    out_arrays: list[Any] = []

    for out_index, source_index in enumerate(permutation):
        engine_type = None if engine_types is None else engine_types[out_index]

        if source_index in object_null_types:
            column = pa.array(
                object_null_values[source_index],
                type=object_null_types[source_index],
            )

        else:
            column = _normalize_frame_arrow_column(
                table.column(source_index), engine_type=engine_type
            )

            _validate_decimal_column_envelope(column)

        if engine_type is not None and source_index in object_null_types:
            target = _sql_type_to_arrow(engine_type)

            if not column.type.equals(target):
                try:
                    column = column.cast(target)

                except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as error:
                    # Witness type (NaT→timestamp, NaN→double) vs StructType mismatch:
                    # fail as PySparkTypeError, not a raw Arrow stack.

                    raise PySparkTypeError(
                        f"createDataFrame cannot cast inferred null type {column.type} to "
                        f"schema type {engine_type}: {error}"
                    ) from error

        out_arrays.append(column)

    return pa.Table.from_arrays(out_arrays, names=names)


def _arrow_table_from_polars(
    data: Any,
    schema: list[str] | None,
    *,
    engine_types: list[str] | None,
) -> Any:
    """Native polars → Arrow (no per-row ``.row()`` loop). Schema bind + refuse + cast.



    # === r20 P2a: cdf-extractor ===

    # === r21 T1: cdf-ingest ===

    Uses polars ``.to_arrow()``. Duration / binary / time refuse via the dtype map; nested

    ``List``/``Struct``/``Array`` pass through Arrow (r21 T1). Integer widths widen to

    int64 (C4-Q-001).

    """

    import pyarrow as pa

    source_columns = list(data.columns)

    names, permutation = _schema_names_and_permutation(source_columns, schema, kind="polars")

    if data.height == 0:
        # Typed StructType/DDL empty frames keep declared types (list-path parity).

        if engine_types is not None:
            column_null_sql = [f"CAST(NULL AS {sql_type})" for sql_type in engine_types]

            return _arrow_table_from_tuples(
                names,
                [],
                column_null_sql=column_null_sql,
                engine_types=engine_types,
            )

        raise PySparkValueError(
            "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema for empty polars DataFrame; "
            "pass a non-empty frame or a typed StructType schema "
            "(repark createDataFrame is VALUES-only and has no StructType path yet)"
        )

    for dtype in data.dtypes:
        _null_sql_for_polars_dtype(dtype)

    table = data.to_arrow()

    out_arrays: list[Any] = []

    for out_index, source_index in enumerate(permutation):
        engine_type = None if engine_types is None else engine_types[out_index]

        column = _normalize_frame_arrow_column(table.column(source_index), engine_type=engine_type)

        _validate_decimal_column_envelope(column)

        out_arrays.append(column)

    return pa.Table.from_arrays(out_arrays, names=names)
