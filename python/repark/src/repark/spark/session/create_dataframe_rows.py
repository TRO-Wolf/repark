"""createDataFrame row binding and materialization."""

from __future__ import annotations

import contextlib

from typing import TYPE_CHECKING, Any

from repark.spark._idents import quote_ident as _quote_ident

from repark.spark.dataframe import DataFrame

from repark.errors import PySparkTypeError, PySparkValueError

from repark.spark._temp_views import scratch_view_name


if TYPE_CHECKING:
    from repark.spark.session.create_dataframe_inference import (
        _INFER_NESTED_DICT_AS_STRUCT,
        _LEGACY_FIRST_ELEMENT_COERCE,
    )
    from repark.spark.session.create_dataframe_schema import (
        _apply_permutation,
        _column_null_sql_from_raw_tuples,
        _infer_null_sql_from_raw_cells,
        _null_sql_for_pandas_dtype,
        _null_sql_for_polars_dtype,
        _pandas_dtype_needs_object_null_witness,
        _schema_names_and_permutation,
    )
    from repark.spark.session.create_dataframe_tuples import _arrow_table_from_tuples
    from repark.spark.session.create_dataframe_values import (
        _TYPED_NULL_SQL,
        _is_pandas_dataframe,
        _is_polars_dataframe,
        _normalize_create_dataframe_cell,
        _parse_create_dataframe_schema,
        _sql_literal,
    )


def _spark_dict_key_union_order(mappings: list[dict[str, Any]]) -> list[str]:
    """Spark createDataFrame dict key-union column order (live 4.1.2 oracle).

    PySpark ``_infer_schema`` sorts each dict's items alphabetically; ``_merge_type`` keeps
    the first schema's field order and **appends** newly seen keys from later rows (still in
    that later row's sorted-key order). Result for
    ``[{"c":1,"a":2},{"b":3,"a":4},{"d":5,"c":6}]`` → ``["a","c","b","d"]``.
    """

    if not mappings:
        return []

    names = sorted(mappings[0].keys())

    seen = set(names)

    for mapping in mappings[1:]:
        for key in sorted(mapping.keys()):
            if key not in seen:
                names.append(key)

                seen.add(key)

    return names


def _bind_named_row(
    mapping: dict[str, Any],
    names: list[str],
    *,
    kind: str,
    allow_extra: bool = False,
    allow_missing: bool = False,
) -> tuple[Any, ...]:
    """Bind a name→value mapping to ``names``.

    * Default (Row path / strict name lists): missing keys and extra keys fail loud
      (BUG-007 / C1-L-001 / C2-L-004 — a typo must not become an all-null column).
    * Dict key-union / StructType null-fill: ``allow_missing=True`` yields ``None`` for
      absent keys (Spark null fill); ``allow_extra=True`` ignores keys not in ``names``
      (Spark drops extras under an explicit StructType schema).

    Explicit ``None`` values are always kept (SQL NULL).
    """

    if not allow_missing:
        missing = [name for name in names if name not in mapping]

        if missing:
            raise PySparkValueError(
                f"createDataFrame {kind} row is missing field(s) {missing!r} "
                f"(expected keys {list(names)!r}; silent NULL-fill is not supported)"
            )

    if not allow_extra:
        extra = [key for key in mapping if key not in names]

        if extra:
            raise PySparkValueError(
                f"createDataFrame {kind} row has unexpected field(s) {extra!r} "
                f"(expected keys {list(names)!r}; silent drop is not supported)"
            )

    # Raw cells — normalize later so all-null NaN/NaT can still witness DOUBLE/TIMESTAMP (C4-L-001).

    if allow_missing:
        return tuple(mapping.get(name) for name in names)

    return tuple(mapping[name] for name in names)


def _rows_from_mapping_list(
    data: list[Any],
    schema: list[str] | None,
    *,
    kind: str,
    as_mapping: Any,
    null_fill: bool = False,
    key_union: bool = False,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Convert homogeneous dict/Row lists to (names, row tuples) with unified schema bind.

    Cells are left un-normalized so the caller can infer all-null CAST types from NaN/NaT
    witnesses before erasure (C4-L-001).

    * ``kind="dict"`` + ``key_union=True`` (schema=None): Spark key-union across rows with
      null fill for missing fields (live 4.1.2 oracle).
    * ``kind="dict"`` + ``null_fill=True`` (StructType/DDL schema): bind to schema field
      names; missing → None; extras dropped.
    * ``kind="Row"`` / strict name lists: exact key-set match — refuse missing and extra.
    """

    mappings: list[dict[str, Any]] = []

    for row_index, row in enumerate(data):
        if kind == "dict" and not isinstance(row, dict):
            raise PySparkTypeError(
                "createDataFrame dict lists must be homogeneous; "
                f"got element type {type(row).__name__} at index {row_index}"
            )

        if kind == "Row":
            from repark.spark.row import Row

            if not isinstance(row, Row):
                raise PySparkTypeError(
                    "createDataFrame Row lists must be homogeneous; "
                    f"got element type {type(row).__name__} at index {row_index}"
                )

        mappings.append(as_mapping(row))

    if key_union and kind == "dict" and schema is None:
        # Spark-parity: sorted first-row keys, then append newly seen keys (sorted per row).

        source_names = _spark_dict_key_union_order(mappings)

        names, permutation = list(source_names), list(range(len(source_names)))

        allow_missing = True

        allow_extra = True

    elif null_fill and kind == "dict" and schema is not None:
        # Explicit StructType / DDL: schema field order is authoritative; null-fill + drop extras.

        source_names = list(schema)

        names, permutation = list(schema), list(range(len(schema)))

        allow_missing = True

        allow_extra = True

    else:
        source_names = list(mappings[0].keys())

        names, permutation = _schema_names_and_permutation(source_names, schema, kind=kind)

        allow_missing = False

        allow_extra = False

    tuples: list[tuple[Any, ...]] = []

    for mapping in mappings:
        source_row = _bind_named_row(
            mapping,
            source_names,
            kind=kind,
            allow_extra=allow_extra,
            allow_missing=allow_missing,
        )

        tuples.append(_apply_permutation(source_row, permutation))

    return names, tuples


def _refuse_duplicate_pandas_columns(data: Any) -> None:
    """Fail loud on duplicate pandas column names (critic-octo C2).

    ``data[name].dtype`` returns a DataFrame when names collide (AttributeError on ``.dtype``),
    and ``pa.Table.from_pandas`` raises a bare ValueError. Surface a stable PySparkValueError
    before either path.
    """

    source_columns = [str(column) for column in data.columns]

    if len(source_columns) != len(set(source_columns)):
        raise PySparkValueError(
            f"createDataFrame pandas DataFrame has duplicate column names: {source_columns}"
        )


def _rows_from_pandas(
    data: Any, schema: list[str] | None
) -> tuple[list[str], list[tuple[Any, ...]], list[str]]:
    """Convert a pandas DataFrame to (column names, row tuples, per-col null SQL) for VALUES.

    Empty frames raise :class:`PySparkValueError` (Spark ``CANNOT_INFER_EMPTY_SCHEMA``) — the
    VALUES path has no StructType schema, so types cannot be inferred from zero rows.

    Schema bind: pure reorder by name, pure rename positionally (C2-L-001); length and partial
    overlap fail loud. Per-column null SQL preserves source dtypes for all-null columns
    (C3-Q-001) so Arrow types are not silently forced to string.
    """

    _refuse_duplicate_pandas_columns(data)

    source_columns = [str(column) for column in data.columns]

    names, permutation = _schema_names_and_permutation(source_columns, schema, kind="pandas")

    if len(data) == 0:
        raise PySparkValueError(
            "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema for empty pandas DataFrame; "
            "pass a non-empty frame or a typed StructType schema "
            "(repark createDataFrame is VALUES-only and has no StructType path yet)"
        )

    # Positional series (iloc) — name lookup is wrong under duplicate labels (octo C2).

    column_series = [data.iloc[:, source_index] for source_index in range(data.shape[1])]

    # Per-column null SQL: dtype map for typed columns; object / Sparse[object] is untyped so

    # all-null columns witness raw cells (NaN→DOUBLE, NaT→TIMESTAMP) like the list path

    # (C5-SAF-001 / C6-Q-001). Sparse[int64]/Sparse[bool] stay on the dtype-map unwrap path.

    source_null_sql: list[str] = []

    for series in column_series:
        dtype = series.dtype

        if _pandas_dtype_needs_object_null_witness(dtype):
            raw_cells = [series.iloc[row_index] for row_index in range(len(series))]

            if all(_normalize_create_dataframe_cell(cell) is None for cell in raw_cells):
                source_null_sql.append(_infer_null_sql_from_raw_cells(raw_cells))

            else:
                # Non-all-null: entry unused by ``_values_sql_with_typed_nulls``.

                source_null_sql.append(_TYPED_NULL_SQL)

        else:
            source_null_sql.append(_null_sql_for_pandas_dtype(dtype))

    column_null_sql = [source_null_sql[source_index] for source_index in permutation]

    tuples: list[tuple[Any, ...]] = []

    for row_index in range(len(data)):
        source_row = tuple(
            _normalize_create_dataframe_cell(series.iloc[row_index]) for series in column_series
        )

        tuples.append(_apply_permutation(source_row, permutation))

    return names, tuples, column_null_sql


def _rows_from_polars(
    data: Any, schema: list[str] | None
) -> tuple[list[str], list[tuple[Any, ...]], list[str]]:
    """Convert a polars DataFrame to (column names, row tuples, per-col null SQL) for VALUES.

    Empty frames raise (same CANNOT_INFER_EMPTY_SCHEMA class as pandas). Schema bind matches
    pandas (name reorder / positional rename / fail-loud partial). All-null typed columns keep
    dtype-matched CAST nulls (C3-Q-001).
    """

    source_columns = list(data.columns)

    names, permutation = _schema_names_and_permutation(source_columns, schema, kind="polars")

    if data.height == 0:
        raise PySparkValueError(
            "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema for empty polars DataFrame; "
            "pass a non-empty frame or a typed StructType schema "
            "(repark createDataFrame is VALUES-only and has no StructType path yet)"
        )

    source_null_sql = [_null_sql_for_polars_dtype(dtype) for dtype in data.dtypes]

    column_null_sql = [source_null_sql[source_index] for source_index in permutation]

    tuples = []

    for row_index in range(data.height):
        source_row = tuple(
            _normalize_create_dataframe_cell(cell) for cell in data.row(row_index, named=False)
        )

        tuples.append(_apply_permutation(source_row, permutation))

    return names, tuples, column_null_sql


def _empty_frame_sql(names: list[str]) -> str:
    """Build a zero-row SELECT with typed null columns (stable Arrow string types)."""

    nulls = ", ".join(f"{_TYPED_NULL_SQL} AS {_quote_ident(name)}" for name in names)

    return f"SELECT {nulls} WHERE 1 = 0"


def _empty_typed_arrow_frame(
    session: ReparkSession,
    names: list[str],
    engine_types: list[str],
) -> DataFrame:
    """Zero-row createDataFrame keeping StructType/DDL/scalar DataType types (octo C2-Q-001)."""

    if len(engine_types) != len(names):
        raise PySparkValueError(
            f"schema type count {len(engine_types)} does not match name count {len(names)}"
        )

    column_null_sql = [f"CAST(NULL AS {sql_type})" for sql_type in engine_types]

    arrow_table = _arrow_table_from_tuples(
        names, [], column_null_sql=column_null_sql, engine_types=engine_types
    )

    return _materialize_arrow_as_memtable_frame(session, arrow_table)


def _values_sql_with_typed_nulls(
    names: list[str],
    tuples: list[tuple[Any, ...]],
    *,
    column_null_sql: list[str] | None = None,
) -> str:
    """Emit VALUES SQL; all-null columns use a typed CAST (default VARCHAR — C2-L-003 / C3-Q-001).

    When ``column_null_sql`` is provided (pandas/polars source dtypes), all-null columns use
    that CAST so Arrow types match the source dtype rather than silent string.
    """

    width = len(names)

    if column_null_sql is not None and len(column_null_sql) != width:
        raise PySparkValueError(
            f"column_null_sql length {len(column_null_sql)} does not match schema width {width}"
        )

    all_null_columns = {
        column_index
        for column_index in range(width)
        if all(row[column_index] is None for row in tuples)
    }

    value_rows: list[str] = []

    for row in tuples:
        if len(row) != width:
            raise PySparkValueError("ragged rows are not supported by createDataFrame")

        cells: list[str] = []

        for column_index, cell in enumerate(row):
            if cell is None and column_index in all_null_columns:
                if column_null_sql is not None:
                    cells.append(column_null_sql[column_index])

                else:
                    cells.append(_TYPED_NULL_SQL)

            else:
                cells.append(_sql_literal(cell))

        value_rows.append("(" + ", ".join(cells) + ")")

    values_sql = ", ".join(value_rows)

    alias_cols = ", ".join(_quote_ident(name) for name in names)

    return f"SELECT * FROM (VALUES {values_sql}) AS t({alias_cols})"


def _create_dataframe_from_rows(
    session: ReparkSession,
    data: Any,
    schema: Any,
) -> DataFrame:
    """Materialize row data as a DataFrame via Arrow MemTable (C-stream; IPC skew fallback).

    Non-empty inputs and typed empty frames build a ``pyarrow.Table`` then register via
    :func:`_materialize_arrow_as_memtable_frame`. Untyped empty frames still use a
    ``WHERE 1 = 0`` VALUES seed via :func:`_materialize_values_as_memtable_frame`.
    """

    # Legacy first-element coerce follows the session conf.
    legacy_first = str(
        session.conf.get("spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled", "false")
    ).lower() in {"true", "1"}

    # Nested dict-cell → StructType (Spark SPARK-35929); strip() matches other bool conf
    # parsers in this module (octo C2-Q-001).
    infer_dict_as_struct = str(
        session.conf.get("spark.sql.pyspark.inferNestedDictAsStruct.enabled", "true")
    ).strip().lower() in {"true", "1"}

    token_legacy = _LEGACY_FIRST_ELEMENT_COERCE.set(legacy_first)

    token_struct = _INFER_NESTED_DICT_AS_STRUCT.set(infer_dict_as_struct)

    try:
        return _create_dataframe_from_rows_inner(session, data, schema)

    finally:
        _LEGACY_FIRST_ELEMENT_COERCE.reset(token_legacy)

        _INFER_NESTED_DICT_AS_STRUCT.reset(token_struct)


def _create_dataframe_from_rows_inner(
    session: ReparkSession,
    data: Any,
    schema: Any,
) -> DataFrame:
    """Body of :func:`_create_dataframe_from_rows` under the legacy-coerce contextvar."""

    from repark.spark.row import Row

    schema_names, engine_types = _parse_create_dataframe_schema(schema)

    schema = schema_names

    column_null_sql: list[str] | None = None

    # Frame-shaped inputs first — never `if not data` on a DataFrame (pandas raises
    # "truth value is ambiguous"; G-INT).

    # pandas/polars → native Arrow (from_pandas / .to_arrow) + cast/null/refuse rules,
    # then P1a C-stream materialize. No per-row Python tuple explode on the hot path.

    if _is_pandas_dataframe(data):
        import repark.spark.session as _session_pkg

        arrow_table = _session_pkg._arrow_table_from_pandas(data, schema, engine_types=engine_types)

        return _materialize_arrow_as_memtable_frame(session, arrow_table)

    if _is_polars_dataframe(data):
        import repark.spark.session as _session_pkg

        arrow_table = _session_pkg._arrow_table_from_polars(data, schema, engine_types=engine_types)

        return _materialize_arrow_as_memtable_frame(session, arrow_table)

    if isinstance(data, list):
        if not data:
            if not schema:
                raise PySparkValueError(
                    "createDataFrame on empty data requires a schema (column name list)"
                )

            # Typed schema (StructType / DDL / bare DataType wrap) must keep declared

            # types on a 0-row frame — string default was silent wrong (octo C2-Q-001).

            if engine_types is not None:
                return _empty_typed_arrow_frame(session, list(schema), engine_types)

            return _materialize_values_as_memtable_frame(session, _empty_frame_sql(list(schema)))

        first = data[0]

        if isinstance(first, dict):
            # schema=None → Spark key-union; StructType/DDL → null-fill field names.
            names, tuples = _rows_from_mapping_list(
                data,
                schema,
                kind="dict",
                as_mapping=lambda row: row,
                null_fill=engine_types is not None,
                key_union=schema is None,
            )

        elif isinstance(first, Row):
            # Row stays fail-loud on key mismatch (Spark STRUCT_ARRAY_LENGTH_MISMATCH class).

            names, tuples = _rows_from_mapping_list(
                data, schema, kind="Row", as_mapping=lambda row: row.asDict()
            )

        elif isinstance(first, (list, tuple)):
            width = len(first)

            fields = getattr(first, "_fields", None)

            if fields is not None:
                # collections.namedtuple / typing.NamedTuple — source names are _fields

                # (C3-Q-002). schema= uses the same by-name reorder / positional rename /

                # fail-loud partial as dict/Row (C6-L-001); never positional-only when names

                # are known (that swapped values vs dict/Row/pandas/polars under reorder).

                names, permutation = _schema_names_and_permutation(
                    list(fields), schema, kind="namedtuple"
                )

            elif schema is not None:
                names = list(schema)

                permutation = list(range(width))

            else:
                names = [f"_{index + 1}" for index in range(width)]

                permutation = list(range(width))

            # Spark pads a short name list with ``_2``, ``_3``, … (1-based position of the

            # missing columns — Apache ``test_infer_schema_not_enough_names``). Too many names

            # still fails loud (width mismatch).

            if len(names) < width:
                names = list(names) + [f"_{index + 1}" for index in range(len(names), width)]

            if len(names) != width:
                raise PySparkValueError(
                    f"schema length {len(names)} does not match row width {width}"
                )

            tuples = []

            for row_index, row in enumerate(data):
                # Refuse str / other iterables — character-iterating a string yields wrong rows

                # (C1-Q-002). Only list/tuple are row shapes on this path.

                if not isinstance(row, (list, tuple)):
                    raise PySparkTypeError(
                        "createDataFrame tuple/list rows must be homogeneous list/tuple rows; "
                        f"got element type {type(row).__name__} at index {row_index}"
                    )

                if len(row) != width:
                    raise PySparkValueError(
                        "ragged rows are not supported by createDataFrame "
                        f"(row 0 width {width}, row {row_index} width {len(row)})"
                    )

                # Raw cells — normalize after all-null type inference (C4-L-001).

                tuples.append(_apply_permutation(tuple(row), permutation))

        elif schema is not None and len(schema) == 1:
            # Scalar cells + single-column schema (typically from a bare DataType wrap →

            # ``StructField("value", …)``). Spark accepts ``createDataFrame([0.0, 1.0],

            # DoubleType())`` (F2 / Apache test_reciprocal_trig_functions).

            names = list(schema)

            tuples = []

            for row_index, cell in enumerate(data):
                if isinstance(cell, (list, tuple, dict)) or (
                    type(cell).__name__ == "Row"
                    and type(cell).__module__.startswith(("repark", "pyspark"))
                ):
                    raise PySparkTypeError(
                        "createDataFrame scalar-schema path expects scalar cells; "
                        f"got element type {type(cell).__name__} at index {row_index}"
                    )

                tuples.append((cell,))

        else:
            raise PySparkTypeError(
                "createDataFrame expects a list of tuples/lists, dicts, or Row, "
                f"got element type {type(first).__name__}"
            )

    else:
        raise PySparkTypeError(
            "createDataFrame expects a list of rows, a pandas DataFrame, or a polars "
            f"DataFrame, got {type(data).__name__}"
        )

    if not tuples:
        if not names:
            raise PySparkValueError(
                "createDataFrame on empty data requires a schema (column name list)"
            )

        if engine_types is not None:
            return _empty_typed_arrow_frame(session, names, engine_types)

        return _materialize_values_as_memtable_frame(session, _empty_frame_sql(names))

    # Non-frame paths: infer CAST from pre-normalize NaN/NaT/… witnesses, then erase missing

    # markers. Frame paths already provide dtype-matched column_null_sql and normalized cells.

    if column_null_sql is None:
        width = len(names)

        column_null_sql = _column_null_sql_from_raw_tuples(tuples, width, names=names)

        tuples = [
            tuple(
                _normalize_create_dataframe_cell(cell, field_name=names[column_index])
                for column_index, cell in enumerate(row)
            )
            for row in tuples
        ]

    # Explicit StructType / DDL types override null-witness casts so IntegerType stays INT

    # (int32) rather than the VALUES-path Python-int → BIGINT widening (R-PARITY3 / G-INT).

    if engine_types is not None:
        if len(engine_types) != len(names):
            raise PySparkValueError(
                f"schema type count {len(engine_types)} does not match name count {len(names)}"
            )

        column_null_sql = [f"CAST(NULL AS {sql_type})" for sql_type in engine_types]

    # R-PERF-ARROW-CDF + P1a C-stream: build a pyarrow.Table with the inferred/declared types

    # and register a MemTable via Arrow C Stream (no IPC encode/to_vec; no VALUES SQL plan).

    arrow_table = _arrow_table_from_tuples(
        names, tuples, column_null_sql=column_null_sql, engine_types=engine_types
    )

    return _materialize_arrow_as_memtable_frame(session, arrow_table)


def _drop_cdf_temp_view(session_ref: ReparkSession, name: str) -> None:
    """Best-effort drop of a createDataFrame scratch view on DataFrame GC."""
    with contextlib.suppress(Exception):
        # Session may already be stopped; best-effort cleanup only.
        session_ref._ensure_alive().drop_temp_view(name)


def _register_cdf_view_cleanup(session: ReparkSession, frame: DataFrame, view_name: str) -> None:
    """Drop ``__repark_cdf_*`` when the owning DataFrame is GC'd (R-FACADE-HYGIENE W7).

    Uses :func:`weakref.finalize` — no new public close API. Pin is bounded-growth after
    ``gc.collect()`` x2, not exact-zero.
    """

    import weakref

    weakref.finalize(frame, _drop_cdf_temp_view, session, view_name)


def _materialize_values_as_memtable_frame(session: ReparkSession, values_sql: str) -> DataFrame:
    """Plan VALUES once, collect into a MemTable temp view, return a scan of that view.

    Retained for untyped empty-frame SQL paths (`WHERE 1 = 0`). Non-empty createDataFrame
    and typed empty frames use :func:`_materialize_arrow_as_memtable_frame`.
    """

    ephemeral = session.sql(values_sql)

    view_name = scratch_view_name(session._ensure_alive(), "__repark_cdf_")

    native = session._ensure_alive()

    registered = False

    try:
        native.materialize_as_temp_view(view_name, ephemeral._inner)

        registered = True

        frame = session.sql(f"SELECT * FROM {view_name}")

    except BaseException:
        # Drop orphan MemTable if sql() fails after register (parity mapInArrow C3-SAF-001).

        # BaseException so KeyboardInterrupt/SystemExit also release the view (octo C3).

        if registered:
            with contextlib.suppress(Exception):
                native.drop_temp_view(view_name)

        raise

    _register_cdf_view_cleanup(session, frame, view_name)

    # SE-1 declared-sorted door: tag the handed-back frame as the *source* frame over this
    # MemTable view, so ``DataFrame.declareSorted`` knows which registered view to verify
    # and declare. Transformed frames get a fresh handle from ``_spawn`` and never inherit
    # the tag.

    frame._source_view_name = view_name

    return frame


def _materialize_arrow_as_memtable_frame(session: ReparkSession, table: Any) -> DataFrame:
    """Register a ``pyarrow.Table`` as a MemTable temp view; return a scan of that view.

    Prefers the Arrow **C Stream** seam (``register_arrow_stream_as_temp_view``) so the
    table rides ``__arrow_c_stream__`` into the engine with **no** IPC encode /
    ``ipc_bytes.to_vec()`` intermediate (P1a / scout #4). When the native C-stream symbol
    is absent (version-skew), falls back to the R-PERF-ARROW-CDF IPC path.

    If registration succeeds and the follow-up ``SELECT * FROM`` view scan fails (or a
    ``BaseException`` such as ``KeyboardInterrupt`` is raised after register), the MemTable
    is dropped immediately so the session does not retain an untracked ``__repark_cdf_*``
    view (octo P1a C1 SAF-001 / C3; same discipline as mapInArrow C3-SAF-001).
    """

    import pyarrow as pa

    if not isinstance(table, pa.Table):
        raise TypeError(f"expected pyarrow.Table, got {type(table).__name__}")

    view_name = scratch_view_name(session._ensure_alive(), "__repark_cdf_")

    native = session._ensure_alive()

    register_stream = getattr(native, "register_arrow_stream_as_temp_view", None)

    registered = False

    try:
        if callable(register_stream):
            # pa.Table is an Arrow C Stream exporter — same path mapInArrow uses.
            register_stream(view_name, table)

        else:
            # Version-skew fallback: IPC encode + register_ipc_stream_as_temp_view.

            import io

            import pyarrow.ipc as pa_ipc

            sink = io.BytesIO()

            with pa_ipc.new_stream(sink, table.schema) as writer:
                for batch in table.to_batches():
                    writer.write_batch(batch)

            native.register_ipc_stream_as_temp_view(view_name, sink.getvalue())

        registered = True

        frame = session.sql(f"SELECT * FROM {view_name}")

    except BaseException:
        # BaseException: also drop on KeyboardInterrupt/SystemExit after register (octo C3).

        if registered:
            with contextlib.suppress(Exception):
                native.drop_temp_view(view_name)

        raise

    _register_cdf_view_cleanup(session, frame, view_name)

    # SE-1 declared-sorted door: tag the handed-back frame as the *source* frame over this
    # MemTable view, so ``DataFrame.declareSorted`` knows which registered view to verify
    # and declare. Transformed frames get a fresh handle from ``_spawn`` and never inherit
    # the tag.

    frame._source_view_name = view_name

    return frame
