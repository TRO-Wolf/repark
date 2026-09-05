"""Typed Spark DataFrame facade over the native ``PyDataFrame``.

The engine computes plans in Rust and exports rows through the Arrow C stream. The facade owns
Spark argument validation, display names, and temporary-view lifecycle.
"""

from __future__ import annotations

import contextlib
import functools
import logging
import re
import uuid
import warnings
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, overload

from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkAttributeError,
    PySparkException,
    PySparkNotImplementedError,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)

# SQL identifier helpers.
from repark.spark._idents import quote_ident as _quote_ident_sql
from repark.spark._temp_views import home_view_ref, scratch_view_name
from repark.spark.column import Column, _bound_generator_array, sort_nulls_first_for
from repark.spark.dataframe.udf_bridge import (
    _apply_ordered_window_pandas_udf,
    _map_in_pandas_arrow_batches,
    _run_pandas_udf_arrow_batches,
    _run_python_udf_arrow_batches,
)
from repark.spark.row import Row
from repark.spark.types import DataType, StructField, StructType


def _arrow_map_pairs(value: Any) -> list[tuple[Any, Any]] | None:
    """Normalize an Arrow ``to_pylist`` map cell to pairs, or ``None``.

    Empty map is ``[]`` / ``{}`` from pylist; non-empty maps are pair-lists or (rarely) dicts.
    Returns an empty list for empty maps; ``None`` when the value is not map-shaped.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return list(value.items())
    if isinstance(value, list):
        if not value:
            return []
        if all(isinstance(item, tuple) and len(item) == 2 for item in value):
            return list(value)
    return None


def _arrow_cell_to_spark_python(value: Any, arrow_type: Any) -> Any:
    """Convert Arrow cells to Spark Python values using the declared schema.

    Maps become dictionaries, including nested maps. Lists and structs recurse.
    """
    import pyarrow as pa

    if value is None:
        return None
    if pa.types.is_map(arrow_type):
        # Empty map is ``[]`` from to_pylist — must become ``{}`` (not empty array).
        pairs = _arrow_map_pairs(value)
        if pairs is None:
            return value
        key_type = arrow_type.key_type
        item_type = arrow_type.item_type
        return {
            _arrow_cell_to_spark_python(key, key_type): _arrow_cell_to_spark_python(item, item_type)
            for key, item in pairs
        }
    is_list_type = (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    )
    if is_list_type:
        if not isinstance(value, list):
            return value
        element_type = arrow_type.value_type
        return [_arrow_cell_to_spark_python(item, element_type) for item in value]
    if pa.types.is_struct(arrow_type):
        if not isinstance(value, dict):
            return value
        fields = list(arrow_type)
        return {
            field.name: _arrow_cell_to_spark_python(value.get(field.name), field.type)
            for field in fields
        }
    if pa.types.is_timestamp(arrow_type) and getattr(value, "tzinfo", None) is not None:
        from repark.spark.session.session_time_zone import collect_timestamp_as_session_wall

        return collect_timestamp_as_session_wall(value)
    return value


def _refuse_calendar_interval_python_value(value: Any) -> None:
    """Reject calendar intervals because Spark has no Python converter for them."""
    if value is None:
        return
    type_name = type(value).__name__
    if type_name in {"MonthDayNano", "MonthDayNanoInterval"}:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={
                "feature": "Python conversion for calendar interval (make_interval / "
                "CalendarIntervalType)"
            },
        )
    if isinstance(value, dict):
        for key, item in value.items():
            _refuse_calendar_interval_python_value(key)
            _refuse_calendar_interval_python_value(item)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _refuse_calendar_interval_python_value(item)


if TYPE_CHECKING:
    import numpy as np
    import pandas as pd
    import polars as pl
    import pyarrow as pa

    from repark.spark.merge import MergeIntoWriter

logger = logging.getLogger(__name__)

# ``show(vertical=True)`` is supported under the Spark display style. Styled displays remain
# horizontal and warn once when vertical output is requested.
_vertical_show_warned = False

# WriterV2.option/options are accepted for signature parity but ignored beyond tableProperty.
# Warn ONCE per process so migrated scripts learn the options are not applied, without spamming.
# Reset by `_reset_writer_v2_option_warnings_for_tests` / `_reset_dropin_warnings_for_tests`.
_writer_v2_option_warned = False

# Shared with ReparkSession.stop — must match session._STOPPED_MESSAGE wording.
_STOPPED_MESSAGE = "Cannot call methods on a stopped ReparkSession"

# SQL keywords the filter-predicate rewriter never treats as a column reference, even when a
# column casefolds to one of them: Spark's grammar reads the keyword, so ``filter("true")`` is
# the boolean literal and ``b IS NOT NULL`` is the null test — never a bind to a column named
# ``true`` / ``null``. Every member has a nameable input: ``createDataFrame([(1, 2)], [kw, "b"])``
# builds a frame whose column is literally named ``true`` / ``false`` / ``null``, and each is
# pinned with its discriminator in test_filter_predicate_rewrite.py (live PySpark 4.1.2 agrees:
# on a ["false", "b"] frame, filter("false") is zero rows and filter("true") is every row).
_SQL_LITERAL_KEYWORDS = frozenset({"true", "false", "null"})

# Semi/anti joins filter the left side and emit no right-side columns.
# Engine `how` tokens whose output schema is the LEFT side alone. Semi/anti joins are filters
# spelled as joins: the right side decides which left rows survive and contributes no columns.
_SEMI_JOIN_HOWS = frozenset({"leftsemi", "leftanti"})

# Mid-stream sort failures can surface through either DataFusion or PyArrow.
# Substrings that mark a mid-stream engine memory / ExternalSorter failure (DataFusion
# Resources exhausted + ExternalSorter / FairSpillPool messages). Case-insensitive match.
_EXPORT_MEMORY_ERROR_MARKERS: tuple[str, ...] = (
    "resources exhausted",
    "externalsorter",
    "externalsortermerge",
    # Sort-preserving merge uses the same memory pool as ExternalSorter.
    "sortpreservingmergeexec",
    "sortpreservingmerge",
    "not enough memory to continue external sort",
    "memory pool",
    "failed to allocate additional",
    "datafusion.runtime.memory_limit",
)
# pyarrow sometimes wraps capsule failures with this noise instead of the engine message.
_PYARROW_DYNAMIC_SOURCE_NOISE = "dynamically evaluated source"


def _export_error_message_is_noise(message: str) -> bool:
    """True when ``message`` is pyarrow capsule noise that hides the engine payload."""
    lower = message.lower()
    return _PYARROW_DYNAMIC_SOURCE_NOISE in lower and not any(
        marker in lower for marker in _EXPORT_MEMORY_ERROR_MARKERS
    )


def _export_error_message(error: BaseException) -> str:
    """Extract the best human message from a mid-stream Arrow/engine export failure.

    Prefers the DataFusion payload over pyarrow's "Could not get source, probably due
    dynamically evaluated source code" wrapper. Walks ``__cause__`` / ``__context__`` and
    ``args`` so the operator sees the ExternalSorter / pool text, not the capsule noise.
    """
    candidates: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = error
    depth = 0
    while current is not None and depth < 12:
        identity = id(current)
        if identity in seen:
            break
        seen.add(identity)
        text = str(current).strip()
        if text:
            candidates.append(text)
        for argument in getattr(current, "args", ()) or ():
            if isinstance(argument, str) and argument.strip():
                candidates.append(argument.strip())
            elif isinstance(argument, BaseException):
                nested = str(argument).strip()
                if nested:
                    candidates.append(nested)
        nxt: BaseException | None = current.__cause__
        if nxt is None and current.__context__ is not None:
            nxt = current.__context__
        current = nxt
        depth += 1

    if not candidates:
        return repr(error)

    useful = [message for message in candidates if not _export_error_message_is_noise(message)]
    if not useful:
        useful = candidates
    # Prefer the longest non-noise candidate that still carries engine detail.
    chosen = max(useful, key=len)
    # Drop a leading "External error: " shell DataFusion adds on the Arrow boundary.
    if chosen.startswith("External error: "):
        chosen = chosen[len("External error: ") :]
    return chosen


def _export_engine_error(error: BaseException) -> PySparkException:
    """Map a mid-stream Arrow export failure to ``PySparkException`` with useful context."""
    message = _export_error_message(error)
    lower = message.lower()
    is_memory = any(marker in lower for marker in _EXPORT_MEMORY_ERROR_MARKERS)
    if is_memory and "repark.memory.limit.gb" not in lower:
        message = (
            f"{message.rstrip()}\n"
            "REPARK: raise the FairSpillPool via "
            "SparkSession.builder.config('repark.memory.limit.gb', N).getOrCreate() "
            "(build-time; RAM-relative, cap 8 GiB; 0 = unbounded) or "
            "spark.conf.set('datafusion.runtime.memory_limit', 'NG') "
            "(runtime; same pool — one truth, not two knobs)."
        )
    return PySparkException(message)


def _drop_mia_temp_views(session: Any, names: list[str]) -> None:
    """Drop mapInArrow scratch views when a DataFrame is finalized."""
    for view_name in list(names):
        with contextlib.suppress(Exception):
            session.drop_temp_view(view_name)
    names.clear()


def _coerce_sample_seed(value: object, *, label: str) -> int:
    """Coerce a ``sample()`` seed argument to int; bools and non-numerics refuse."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"sample() {label} must be int, got {type(value).__name__}")
    return int(value)


def _quote_filter_ident_token(
    match: re.Match[str],
    *,
    columns_by_fold: dict[str, list[str]],
) -> str:
    """Quote one matched filter token, or return it unchanged when it names no column."""
    token = match.group(1)
    if token.casefold() in _SQL_LITERAL_KEYWORDS:
        return token
    matches = columns_by_fold.get(token.casefold())
    if matches is None:
        return token
    if len(matches) > 1:
        candidates = ", ".join(f"`{name}`" for name in matches)
        raise AnalysisException(
            f"[AMBIGUOUS_REFERENCE] Reference `{token}` is ambiguous, could be: [{candidates}]."
        )
    return _quote_ident_sql(matches[0])


def _quote_filter_idents_in_fragment(
    fragment: str,
    *,
    ident_pattern: re.Pattern[str],
    columns_by_fold: dict[str, list[str]],
) -> str:
    """Quote every bare identifier in ``fragment`` that names a column of this frame."""
    return ident_pattern.sub(
        functools.partial(_quote_filter_ident_token, columns_by_fold=columns_by_fold),
        fragment,
    )


def _emit_join_side_columns(
    side_frame: DataFrame,
    side_alias: str,
    side_tag: str,
    *,
    display_counts: dict[str, int],
    proj_parts: list[str],
    display_names: list[str],
    engine_names: list[str],
    origin_map: dict[tuple[str, str], str],
) -> None:
    """Project one join side into ``proj_parts`` / origin map (walk by position)."""
    # Walk by position so frames that already carry duplicate display names
    # (chained joins) do not hit AMBIGUOUS_REFERENCE on name lookup.
    if side_frame._display_names is not None and side_frame._engine_names is not None:
        pairs = list(zip(side_frame._display_names, side_frame._engine_names, strict=True))
    else:
        pairs = [(name, name) for name in side_frame.columns]
    for display, source_engine in pairs:
        if display_counts.get(display, 0) > 1:
            # Ordinal = len(engine_names) so chained joins that already carry
            # Duplicate display names on one side use distinct engine fields.
            engine_out = f"__repark_{side_tag}_{side_frame._plan_id}_{len(engine_names)}_{display}"
        else:
            engine_out = display
        proj_parts.append(
            f"{side_alias}.{_quote_ident_sql(source_engine)} AS {_quote_ident_sql(engine_out)}"
        )
        display_names.append(display)
        engine_names.append(engine_out)
        # Direct binds from this side's plan_id (last-write if display dups —
        # bare joined["b"] stays AMBIGUOUS; parent origins use nested map).
        origin_map[(side_frame._plan_id, display)] = engine_out
        # Propagate nested origin map (chained joins / prior selects).
        if side_frame._origin_map is not None:
            for (plan_id, field), nested_engine in side_frame._origin_map.items():
                if nested_engine == source_engine:
                    origin_map[(plan_id, field)] = engine_out


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


# Sentinel for applyInPandas single-pass group boundary scan (not a real group key).
_APPLY_IN_PANDAS_KEY_MISSING: object = object()


def _apply_in_pandas_scalar_key_equal(left: Any, right: Any) -> bool:
    """Null- and NaN-safe equality for one group-key cell (Spark groups NaN with NaN)."""
    import math

    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    if (
        isinstance(left, float)
        and isinstance(right, float)
        and math.isnan(left)
        and math.isnan(right)
    ):
        return True
    return bool(left == right)


def _apply_in_pandas_keys_equal(left: tuple[Any, ...], right: tuple[Any, ...]) -> bool:
    """Null- and NaN-safe equality for a multi-column group key tuple."""
    if len(left) != len(right):
        return False
    return all(
        _apply_in_pandas_scalar_key_equal(left_cell, right_cell)
        for left_cell, right_cell in zip(left, right, strict=True)
    )


def _apply_in_pandas_row_key(batch: Any, key_names: list[str], row_index: int) -> tuple[Any, ...]:
    """Build the group-key tuple for one row of a RecordBatch (``as_py`` cells)."""
    return tuple(batch.column(name)[row_index].as_py() for name in key_names)


def _apply_in_pandas_table_from_segments(segments: list[Any]) -> Any:
    """Build one ``pyarrow.Table`` from group segments, promoting schemas across batch edges.

    Engine streams share one schema, but hand-built / boundary-stitched segments can differ
    when a string/binary column is all-null in one batch (Arrow ``null`` type) and concrete
    in the next. ``Table.from_batches`` rejects that; ``concat_tables(..., promote)`` unifies
    null→concrete so boundary-stitch stays O(group) without a facade re-group.
    """
    import pyarrow as pa

    if not segments:
        raise PySparkException("applyInPandas internal error: empty group segment list")
    try:
        return pa.Table.from_batches(segments)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as error:
        tables = [pa.Table.from_batches([segment]) for segment in segments]
        try:
            return pa.concat_tables(tables, promote_options="default")
        except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError, TypeError) as promote_error:
            raise PySparkException(
                "applyInPandas failed stitching group segments across batch boundaries "
                f"(incompatible schemas): {promote_error}"
            ) from error


def _validate_apply_in_pandas_result_columns(
    out_pdf: Any,
    expected_names: list[str],
) -> None:
    """Validate returned column names against the declared schema."""
    got_names = [str(name) for name in out_pdf.columns]
    # Spark: empty DataFrame() with no columns → empty group result (no mismatch).
    if len(out_pdf) == 0 and len(got_names) == 0:
        return
    expected_set = set(expected_names)
    got_set = set(got_names)
    missing = [name for name in expected_names if name not in got_set]
    unexpected = [name for name in got_names if name not in expected_set]
    if not missing and not unexpected:
        return
    parts: list[str] = []
    if missing:
        parts.append(f"Missing: {', '.join(missing)}")
    if unexpected:
        parts.append(f"Unexpected: {', '.join(unexpected)}")
    raise PySparkException(
        "applyInPandas schema mismatch: column names of the returned data do not match "
        f"specified schema. {'. '.join(parts)}."
    )


def _iter_apply_in_pandas_group_tables(
    input_batches: Iterator[Any],
    key_names: list[str],
) -> Iterator[Any]:
    """Yield one table per contiguous key group from a sorted batch stream.

    The current group and one input batch remain buffered. Empty keys form one global group.
    """
    pending_segments: list[Any] = []
    current_key: Any = _APPLY_IN_PANDAS_KEY_MISSING

    if not key_names:
        segments = [batch for batch in input_batches if batch.num_rows > 0]
        if segments:
            yield _apply_in_pandas_table_from_segments(segments)
        return

    for batch in input_batches:
        if batch.num_rows == 0:
            continue
        missing = [name for name in key_names if name not in batch.schema.names]
        if missing:
            raise PySparkException(
                "applyInPandas group key column(s) missing from streamed batch: "
                f"{missing}; batch fields={list(batch.schema.names)}"
            )
        run_start = 0
        run_key = _apply_in_pandas_row_key(batch, key_names, 0)
        row_count = batch.num_rows
        for row_index in range(1, row_count + 1):
            if row_index < row_count:
                next_key = _apply_in_pandas_row_key(batch, key_names, row_index)
                if _apply_in_pandas_keys_equal(next_key, run_key):
                    continue
            segment = batch.slice(run_start, row_index - run_start)
            if current_key is _APPLY_IN_PANDAS_KEY_MISSING:
                current_key = run_key
                pending_segments = [segment]
            elif _apply_in_pandas_keys_equal(current_key, run_key):
                # Boundary stitch: same group continues across the previous batch edge.
                pending_segments.append(segment)
            else:
                yield _apply_in_pandas_table_from_segments(pending_segments)
                current_key = run_key
                pending_segments = [segment]
            if row_index < row_count:
                run_start = row_index
                run_key = next_key

    if pending_segments:
        yield _apply_in_pandas_table_from_segments(pending_segments)


def _by_name_casefold_map(columns: list[str], *, surface: str) -> dict[str, str]:
    """Map case-folded names to originals for case-insensitive by-name writes.

    Two columns that collide only by case raise :class:`~repark.errors.AnalysisException`
    (ambiguous), matching Spark's loud refusal rather than last-write-wins.

    Write surfaces only — the whole column list is conformed against a target schema there, so
    every name in it *is* a reference. The filter-predicate rewriter must NOT use this helper: a
    predicate references a subset of the frame, so it resolves per token
    (:meth:`DataFrame._quote_filter_sql_identifiers`).
    """
    mapping: dict[str, str] = {}
    for column in columns:
        key = column.casefold()
        prior = mapping.get(key)
        if prior is not None:
            if prior != column:
                raise AnalysisException(
                    f"ambiguous {surface} column name {column!r} collides with {prior!r} "
                    f"under case-insensitive matching (spark.sql.caseSensitive=false)"
                )
            # Exact duplicate names must not silently overwrite the prior entry.
            raise AnalysisException(
                f"duplicate {surface} column name {column!r} "
                f"(case-insensitive matching, spark.sql.caseSensitive=false)"
            )
        mapping[key] = column
    return mapping


def _reset_dropin_warnings_for_tests() -> None:
    """Test helper: re-arm process-wide display warnings."""
    global _vertical_show_warned
    _vertical_show_warned = False
    _reset_writer_v2_option_warnings_for_tests()


def _reset_writer_v2_option_warnings_for_tests() -> None:
    """Test helper: re-arm the process-once WriterV2.option/options ignored warning."""
    global _writer_v2_option_warned
    _writer_v2_option_warned = False


def _warn_writer_v2_option_once(*, stacklevel: int = 2) -> None:
    """Emit the WriterV2 option-ignored disclosure at most once per process."""
    global _writer_v2_option_warned
    if _writer_v2_option_warned:
        return
    warnings.warn(
        "DataFrameWriterV2.option/options are accepted for PySpark signature parity but "
        "ignored by repark (storage options beyond tableProperty are out of scope for Group I). "
        "Use tableProperty(...) for Iceberg table properties.",
        UserWarning,
        stacklevel=stacklevel,
    )
    _writer_v2_option_warned = True


# Cache materialization size guard. Read from builder or spark.conf.
# Zero or unset disables the guard; the execution pool still bounds work.
_CACHE_MAX_BYTES_KEY = "repark.cache.max_bytes"
# Object-identity MemTable names created by cache/persist (not checkpoints, not CDF/MIA).
_CACHE_VIEW_PREFIX = "__repark_cache_"


def _cache_conf_lookup(alive_token: dict[str, Any], key: str) -> str | None:
    """Runtime conf then builder snapshot for a cache-related conf key.

    Honors ``runtime_conf_unset`` tombstones the same way :class:`~repark.session.RuntimeConfig`
    does — an explicit ``spark.conf.unset(key)`` must not resurrect a builder snapshot value.
    """
    tomb = alive_token.get("runtime_conf_unset")
    if isinstance(tomb, set) and key in tomb:
        return None
    store = alive_token.get("runtime_conf")
    if isinstance(store, dict):
        raw = store.get(key)
        if raw is not None and str(raw) != "":
            return str(raw)
    builder = alive_token.get("builder_config") or {}
    if isinstance(builder, dict):
        raw = builder.get(key)
        if raw is not None and str(raw) != "":
            return str(raw)
    return None


def _resolve_cache_max_bytes(alive_token: dict[str, Any]) -> int | None:
    """Parse ``repark.cache.max_bytes``; ``None`` when unset or zero (no size guard).

    Raises :class:`~repark.errors.IllegalArgumentException` for a non-integer / negative value
    or a value that does not fit ``u64`` (PyO3 ``Option<u64>`` boundary) so a bad conf fails
    at materialize time with a named key (not a silent ignore / raw OverflowError).
    """
    from repark.errors import IllegalArgumentException

    raw = _cache_conf_lookup(alive_token, _CACHE_MAX_BYTES_KEY)
    if raw is None:
        return None
    try:
        value = int(str(raw).strip())
    except ValueError as error:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {raw!r} in the config "
            f"{_CACHE_MAX_BYTES_KEY!r} is invalid. Expected a non-negative integer byte budget "
            f"(0 = no size guard)."
        ) from error
    if value < 0:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {raw!r} in the config "
            f"{_CACHE_MAX_BYTES_KEY!r} is invalid. Expected a non-negative integer byte budget "
            f"(0 = no size guard)."
        )
    if value == 0:
        return None
    # PyO3 Option<u64> rejects > 2**64-1 with OverflowError — map to named IAE.
    if value > 0xFFFF_FFFF_FFFF_FFFF:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {raw!r} in the config "
            f"{_CACHE_MAX_BYTES_KEY!r} is invalid. Expected a non-negative integer byte budget "
            f"fitting u64 (0 = no size guard)."
        )
    return value


def _register_cache_frame(alive_token: dict[str, Any], frame: DataFrame) -> None:
    """Track a DataFrame marked for cache/persist so :meth:`Catalog.clearCache` can drop it."""
    import weakref

    registry = alive_token.get("cache_frames")
    if not isinstance(registry, weakref.WeakSet):
        registry = weakref.WeakSet()
        alive_token["cache_frames"] = registry
    registry.add(frame)


def _warn_storage_level_cosmetic_once(
    alive_token: dict[str, Any],
    level: Any,
    *,
    stacklevel: int = 3,
) -> None:
    """Warn once per session when StorageLevel flags claim disk/off-heap/replication.

    repark always pins to an in-process MemTable; those flags are signature parity only
    ``MEMORY_ONLY`` with replication 1 is honest and does not warn.
    """
    if alive_token.get("storage_level_cosmetic_warned"):
        return
    use_disk = bool(getattr(level, "useDisk", False))
    use_off_heap = bool(getattr(level, "useOffHeap", False))
    try:
        replication = int(getattr(level, "replication", 1))
    except (TypeError, ValueError):
        replication = 1
    if not (use_disk or use_off_heap or replication != 1):
        return
    warnings.warn(
        "repark StorageLevel disk / off-heap / replication flags are accepted for PySpark "
        "signature parity but ignored — cache/persist always materializes to a single-node "
        "in-process MemTable (no disk spill, no off-heap, no replication). "
        "Set repark.cache.max_bytes to refuse oversized materialize (OTH-005/014).",
        UserWarning,
        stacklevel=stacklevel,
    )
    alive_token["storage_level_cosmetic_warned"] = True


def _is_numeric_type_key(type_key: str) -> bool:
    """Whether a native logical type key is a Spark ``NumericType`` (int / long / double / decimal).

    Drives the zero-arg ``GroupedData`` shortcuts (``groupBy(g).sum()`` aggregates every numeric
    column) and the na-fill width-preserving path.
    """
    return type_key in {"int", "long", "double"} or type_key.startswith("decimal(")


def _normalize_subset(
    subset: str | list[str] | tuple[str, ...] | None,
    *,
    accept_str: bool,
    allowed_phrase: str,
    error_class: str = "NOT_LIST_OR_TUPLE",
) -> list[str] | None:
    """Normalize a PySpark ``subset`` argument to a list of column names (or ``None``).

    A bare ``str`` is wrapped to ``[subset]`` when ``accept_str`` (``fillna`` / ``dropna``) — so a
    column name is never iterated character-by-character — while ``dropDuplicates`` passes
    ``accept_str=False`` (PySpark rejects a bare ``str`` there). A ``list`` / ``tuple`` is copied.
    Anything else raises :class:`~repark.errors.PySparkTypeError` naming ``allowed_phrase``
    (mirroring PySpark's ``NOT_LIST_OR_TUPLE`` / ``NOT_LIST_OR_STR_OR_TUPLE`` errors — and, since
    PySpark's matching exception class.
    """
    if subset is None:
        return None
    if accept_str and isinstance(subset, str):
        return [subset]
    if isinstance(subset, (list, tuple)):
        names: list[str] = []
        for index, item in enumerate(subset):
            if not isinstance(item, str):
                raise PySparkTypeError(
                    errorClass="NOT_STR",
                    messageParameters={
                        "arg_name": "subset",
                        "arg_type": type(item).__name__,
                    },
                )
            names.append(item)
            _ = index  # keep enumerate for future position-aware diagnostics
        return names
    # PySpark's class is per-surface, NOT derivable from accept_str (oracle 4.1.2:
    # dropDuplicates + fillna → NOT_LIST_OR_TUPLE, dropna → NOT_LIST_OR_STR_OR_TUPLE).
    raise PySparkTypeError(
        errorClass=error_class,
        messageParameters={
            "arg_name": "subset",
            "arg_type": type(subset).__name__,
        },
    )


class DataFrame:
    """Lazy result handle over the native ``PyDataFrame``.

    Actions execute the plan. Interchange methods export Arrow. Each action re-executes unless
    cache or persist pins the result.
    """

    __slots__ = (
        "__weakref__",
        "_alive_token",
        "_cache_view",
        "_checkpoint_lazy",
        # Sticky metadata for adjacent same-spec window merging.
        "_collapse_base",
        "_display_names",
        # Display and origin metadata for join identity.
        "_engine_names",
        # Smart CSV diagnostics.
        # Diagnostics from smartCsv (describe_ingest); None for ordinary frames.
        "_ingest_report",
        "_inner",
        "_layer_defined",
        "_layer_map",
        "_layer_window_key",
        "_lineage_inner",
        "_map_bridge",
        "_mia_action_views",
        "_mia_cleanup_registered",
        "_mia_plan_ready",
        "_mia_temp_views",
        "_origin_map",
        "_origin_not_emitted",  # right-side plan ids a semi/anti join did not emit
        "_persist_requested",
        "_plan_id",
        "_session",
        # Source view eligible for declared-sort registration.
        "_source_view_name",
        "_storage_level",
        # True after tightenNulls=True on this source or an ancestor.
        "_tighten_derived",
    )

    def __init__(
        self,
        inner: Any,
        session: Any,
        alive_token: dict[str, bool] | None = None,
    ) -> None:
        """Wrap a native ``PyDataFrame`` and its owning session.

        The shared ``alive_token`` makes held frames fail after ``ReparkSession.stop``.
        """
        self._inner = inner
        self._session = session
        self._alive_token: dict[str, bool] = (
            alive_token if alive_token is not None else {"alive": True}
        )
        # Cache is object-identity based and lazy until first action.
        self._persist_requested = False
        self._cache_view: str | None = None
        self._lineage_inner: Any | None = None
        self._storage_level: Any | None = None
        self._checkpoint_lazy = False
        self._ingest_report: dict[str, Any] | None = None
        # Deferred facade bridge, or None for ordinary frames.
        self._map_bridge: dict[str, Any] | None = None
        # MemTable names for deferred bridge results; dropped during finalization.
        self._mia_temp_views: list[str] = []
        # Action views are replaced on the next action; plan views remain valid for children.
        self._mia_action_views: list[str] = []
        # One plan-stable bridge snapshot serves all plan children.
        self._mia_plan_ready = False
        self._mia_cleanup_registered = False
        # Schema-bound Columns use this facade plan token to resolve join sides.
        self._plan_id: str = uuid.uuid4().hex[:12]
        # Join outputs may map duplicate display names to unique engine fields.
        self._display_names: list[str] | None = None
        self._engine_names: list[str] | None = None
        self._origin_map: dict[tuple[str, str], str] | None = None
        self._origin_not_emitted: frozenset[str] = frozenset()
        self._collapse_base: DataFrame | None = None
        self._layer_window_key: tuple[Any, ...] | None = None
        self._layer_map: dict[str, Any] | None = None
        self._layer_defined: frozenset[str] | None = None
        # Set only on source frames. Transformed frames cannot declare the source view sorted.
        self._source_view_name: str | None = None
        # Propagate the tighten-null property when a derived frame combines parents.
        self._tighten_derived: bool = False

    def _ensure_alive(self) -> None:
        """Raise if the owning :class:`ReparkSession` has been stopped."""
        if not self._alive_token.get("alive", True):
            raise RuntimeError(_STOPPED_MESSAGE)

    def _spawn(self, inner: Any, *others: DataFrame) -> DataFrame:
        """Return a child sharing this frame's session and liveness token.

        Cache marks stay on the current object. Semi/anti origin exclusions and tighten-null
        metadata propagate to descendants; identity maps use ``_spawn_preserving_identity``.
        """
        self._ensure_alive()
        child = DataFrame(inner, self._session, self._alive_token)
        child._origin_not_emitted = self._origin_not_emitted
        child._tighten_derived = self._tighten_derived or any(
            other._tighten_derived for other in others
        )
        return child

    def _spawn_preserving_identity(self, inner: Any) -> DataFrame:
        """Spawn a child that keeps display, engine, and origin maps (filter / limit / cache).

        Column sets and engine field names are unchanged; only the plan is refined. A fresh
        ``_plan_id`` is still assigned (this is a new plan node) while origin keys from
        parents remain resolvable via the copied map.
        """
        child = self._spawn(inner)
        if self._display_names is not None:
            child._display_names = list(self._display_names)
            child._engine_names = (
                list(self._engine_names) if self._engine_names is not None else None
            )
            child._origin_map = dict(self._origin_map) if self._origin_map is not None else None
        return child

    def _identity_child(self) -> DataFrame:
        """Spawn a same-plan child and preserve deferred bridge state."""
        child = self._spawn(self._inner)
        if self._map_bridge is not None and self._cache_view is None:
            child._map_bridge = dict(self._map_bridge)
            child._mia_plan_ready = self._mia_plan_ready
        # Identity-preserving operations keep display, engine, and origin maps.
        if self._display_names is not None:
            child._display_names = list(self._display_names)
            child._engine_names = (
                list(self._engine_names) if self._engine_names is not None else None
            )
            child._origin_map = dict(self._origin_map) if self._origin_map is not None else None
        return child

    def _materialize_cache_if_needed(self) -> None:
        """Materialize a pending cache, persist, or lazy checkpoint request.

        Cache size limits apply after collection. This single-node path does not spill to disk.
        """
        # Cache materialization.
        needs = self._persist_requested or self._checkpoint_lazy
        if not needs:
            return
        # Already cache-pinned and not converting to checkpoint → done.
        # Checkpoint after cache must still run: early-return on `_cache_view`
        # alone left `_checkpoint_lazy` sticky with lineage untruncated.
        if self._cache_view is not None and not self._checkpoint_lazy:
            return
        self._ensure_alive()
        is_checkpoint = self._checkpoint_lazy
        if self._map_bridge is not None:
            # Run bridge into ``_inner``; keep ``_map_bridge`` for cache so ``unpersist``
            # restores re-run. Checkpoint truncates lineage.
            self._inner = self._execute_map_in_arrow_bridge(replace_ephemeral_views=True)
            if is_checkpoint:
                self._map_bridge = None
        prefix = "__repark_ckpt_" if is_checkpoint else _CACHE_VIEW_PREFIX
        view_name = scratch_view_name(self._session, prefix)
        if not is_checkpoint:
            max_bytes = _resolve_cache_max_bytes(self._alive_token)
            # Cache path only — never route VALUES/createDataFrame through this entry point.
            lineage = self._inner
            self._session.materialize_as_cache_view(view_name, lineage, max_bytes)
            # Commit handle state only after successful materialize.
            self._inner = self._session.sql(f"SELECT * FROM {view_name}")
            self._lineage_inner = lineage
            self._cache_view = view_name
            _register_cache_frame(self._alive_token, self)
            return
        # Checkpoint: lineage truncate; keep VALUES seam (not a session cache registry entry).
        # If converting an already-cached pin, drop the old __repark_cache_* view after the
        # ckpt view is registered so clearCache no longer owns this handle's MemTable.
        old_cache_view = self._cache_view
        self._session.materialize_as_temp_view(view_name, self._inner)
        self._inner = self._session.sql(f"SELECT * FROM {view_name}")
        if old_cache_view is not None and old_cache_view != view_name:
            self._session.drop_temp_view(old_cache_view)
        # Truncate lineage; do not advertise as cached (oracle: is_cached False).
        self._checkpoint_lazy = False
        self._persist_requested = False
        self._storage_level = None
        self._cache_view = None
        self._lineage_inner = None

    def _prepare_for_plan(self) -> None:
        """Materialize one plan-stable ``mapInArrow`` snapshot before child plans.

        Keep the bridge so later actions re-run it unless cache or persist pins the result.
        """
        if self._map_bridge is not None:
            if self._cache_view is not None:
                # Already pinned — child plans use the MemTable; do not clear bridge.
                return
            if self._persist_requested or self._checkpoint_lazy:
                self._materialize_cache_if_needed()
                return
            self._materialize_map_bridge_once()

    def _plan(self) -> Any:
        """Native ``PyDataFrame`` for plan building (mapInArrow plan snapshot; bridge kept)."""
        self._ensure_alive()
        self._prepare_for_plan()
        return self._inner

    def _native_for_registration(self) -> Any:
        """Return real rows for temp-view or writer registration."""
        return self._action_inner()

    def _action_inner(self) -> Any:
        """Return the native frame for an action, re-running uncached bridges."""
        self._ensure_alive()
        if self._map_bridge is not None:
            if self._cache_view is not None:
                # Already pinned — do not re-run the UDF.
                return self._inner
            if self._persist_requested or self._checkpoint_lazy:
                self._materialize_cache_if_needed()
                return self._inner
            # Fresh bridge execution each action; leave ``_map_bridge`` in place for re-run.
            result = self._execute_map_in_arrow_bridge(replace_ephemeral_views=True)
            # When no plan-stable snapshot is live, ``_inner`` may still point at a prior
            # action-ephemeral (e.g. post-unpersist lineage restore). ``replace_ephemeral``
            # just dropped that view — rebind so direct ``_inner`` readers and a later
            # ``_prepare_for_plan`` cannot use a dangling MemTable.
            # Leave ``_inner`` alone when ``_mia_plan_ready``: it is a plan-stable view that
            # action tracking deliberately preserves.
            if not self._mia_plan_ready:
                self._inner = result
            return result
        self._materialize_cache_if_needed()
        return self._inner

    def _materialize_map_bridge_once(self) -> None:
        """Snapshot the mapInArrow bridge once for child plan construction."""
        if self._map_bridge is None:
            return
        if self._mia_plan_ready:
            return
        self._inner = self._execute_map_in_arrow_bridge(replace_ephemeral_views=False)
        self._mia_plan_ready = True

    def _ensure_mia_view_cleanup(self) -> None:
        """Attach one finalizer to drop this facade's MIA views."""
        if self._mia_cleanup_registered:
            return
        import weakref

        self._mia_cleanup_registered = True
        weakref.finalize(self, _drop_mia_temp_views, self._session, self._mia_temp_views)

    def _track_mia_view(self, view_name: str, *, replace_ephemeral: bool) -> None:
        """Track an MIA view and optionally replace prior action views."""
        import contextlib

        if replace_ephemeral:
            for old_name in list(self._mia_action_views):
                with contextlib.suppress(Exception):
                    self._session.drop_temp_view(old_name)
                with contextlib.suppress(ValueError):
                    self._mia_temp_views.remove(old_name)
            self._mia_action_views.clear()
            self._mia_action_views.append(view_name)
        self._mia_temp_views.append(view_name)
        self._ensure_mia_view_cleanup()

    def _iter_map_in_arrow_output(
        self,
        *,
        max_output_rows: int | None = None,
    ) -> Iterator[Any]:
        """Yield validated output batches from the mapInArrow user func.

        Upstream is O(batch) via ``RecordBatchReader`` (never collect-all-then-UDF). The
        upstream reader is closed best-effort on every exit path including early user-func
        failure / ``None`` return. When ``max_output_rows`` is
        set, stops after that many output rows (peek path).
        """
        import contextlib
        import traceback

        import pyarrow as pa

        bridge = self._map_bridge
        if bridge is None:
            raise RuntimeError("mapInArrow bridge missing")
        parent: DataFrame = bridge["parent"]
        func = bridge["func"]
        declared_schema: StructType = bridge["schema"]
        expected_arrow: pa.Schema = bridge["arrow_schema"]

        parent._ensure_alive()
        parent_for_stream: DataFrame = parent
        if parent._map_bridge is not None:
            nested_inner = parent._action_inner()
            parent_for_stream = DataFrame(nested_inner, parent._session, parent._alive_token)

        try:
            input_reader = pa.RecordBatchReader.from_stream(parent_for_stream)
        except Exception as error:
            raise PySparkException(
                f"mapInArrow failed opening upstream Arrow stream: {error}"
            ) from error

        rows_kept = 0
        try:
            try:
                output = func(iter(input_reader))
            except PySparkException:
                raise
            except Exception as error:
                detail = traceback.format_exc()
                raise PySparkException(
                    f"mapInArrow user function raised {type(error).__name__}: {error}\n{detail}"
                ) from error

            if output is None:
                raise PySparkException(
                    "mapInArrow user function must return an iterator of "
                    "pyarrow.RecordBatch (got None)"
                )

            try:
                iterator = iter(output)
            except TypeError as error:
                raise PySparkException(
                    "mapInArrow user function must return an iterator of "
                    f"pyarrow.RecordBatch (got {type(output).__name__})"
                ) from error

            for item in iterator:
                if not isinstance(item, pa.RecordBatch):
                    raise PySparkException(
                        "mapInArrow user function must yield pyarrow.RecordBatch; "
                        f"got {type(item).__name__}"
                    )
                _validate_map_in_arrow_batch(item, expected_arrow, declared_schema)
                aligned = item.cast(expected_arrow)
                if max_output_rows is not None:
                    remaining = max_output_rows - rows_kept
                    if remaining <= 0:
                        break
                    if aligned.num_rows > remaining:
                        aligned = aligned.slice(0, remaining)
                    yield aligned
                    rows_kept += aligned.num_rows
                    if rows_kept >= max_output_rows:
                        break
                else:
                    yield aligned
        except PySparkException:
            raise
        except Exception as error:
            detail = traceback.format_exc()
            raise PySparkException(
                f"mapInArrow user function raised {type(error).__name__}: {error}\n{detail}"
            ) from error
        finally:
            close = getattr(input_reader, "close", None)
            if callable(close):
                with contextlib.suppress(Exception):
                    close()

    def _consume_map_in_arrow_batches(
        self,
        *,
        max_output_rows: int | None = None,
    ) -> Any:
        """Run the mapInArrow bridge and return a ``pyarrow.Table`` (optional row cap)."""
        import pyarrow as pa

        bridge = self._map_bridge
        if bridge is None:
            raise RuntimeError("mapInArrow bridge missing")
        expected_arrow: pa.Schema = bridge["arrow_schema"]
        batches = list(self._iter_map_in_arrow_output(max_output_rows=max_output_rows))
        if not batches:
            return pa.Table.from_batches([], schema=expected_arrow)
        return pa.Table.from_batches(batches, schema=expected_arrow)

    def _execute_map_in_arrow_bridge(self, *, replace_ephemeral_views: bool = True) -> Any:
        """Stream parent batches through ``func`` and re-ingest as a MemTable scan.

        Memory contract: upstream is pulled one Arrow batch at a time via the existing
        lazy ``__arrow_c_stream__`` export (O(batch) on the input side). Output batches are
        drained in pure Python (safe GIL ownership for nested parent-stream pulls), then
        re-ingested via the I4 Arrow **C Stream** seam
        (``register_arrow_stream_as_temp_view``) — no intermediate IPC encode/decode buffer.
        When the native symbol is absent (version-skew guard), fall back to the IPC path.

        ``self._session`` is the **native** ``PyReparkSession`` handle (same as every other
        DataFrame method), not the Python facade.
        """
        import pyarrow as pa

        bridge = self._map_bridge
        if bridge is None:
            raise RuntimeError("mapInArrow bridge missing")
        expected_arrow = bridge["arrow_schema"]

        register_stream = getattr(self._session, "register_arrow_stream_as_temp_view", None)
        if callable(register_stream):
            # Drain the user iterator in pure Python first (same GIL ownership as the IPC path).
            # A *live* RecordBatchReader over this generator fed into Rust's C-stream drain is
            # unsafe: Rust holds the GIL → pyarrow get_next re-enters Python → the generator
            # pulls the parent `__arrow_c_stream__` → StreamingBatchReader::next does
            # Python::attach + detach and aborts with "GIL is released / thread state is NULL".
            # Materializing output batches here keeps parent-stream pulls on the normal Python
            # thread state; the C-stream import then only yields already-buffered RecordBatches
            # (no nested engine stream re-entry). Win vs IPC: no encode/decode byte buffer.
            # User-func exceptions surface as PySparkException here, before the native call.
            # Use Table (not RecordBatchReader.from_batches) so tests that patch
            # ``pa.RecordBatchReader`` for ``from_stream`` tracking stay compatible.
            batches = list(self._iter_map_in_arrow_output(max_output_rows=None))
            table = pa.Table.from_batches(batches, schema=expected_arrow)
            return self._register_arrow_stream_as_inner(
                table, replace_ephemeral=replace_ephemeral_views
            )

        # Fallback: IPC path when native C-stream register is absent (version-skew).
        return self._execute_map_in_arrow_bridge_ipc(
            replace_ephemeral_views=replace_ephemeral_views
        )

    def _execute_map_in_arrow_bridge_ipc(self, *, replace_ephemeral_views: bool = True) -> Any:
        """IPC-encode output batches then ``register_ipc_stream_as_temp_view`` (fallback path)."""
        import io

        import pyarrow.ipc as pa_ipc

        bridge = self._map_bridge
        if bridge is None:
            raise RuntimeError("mapInArrow bridge missing")
        expected_arrow = bridge["arrow_schema"]

        sink = io.BytesIO()
        writer: Any | None = None
        try:
            for aligned in self._iter_map_in_arrow_output(max_output_rows=None):
                if writer is None:
                    writer = pa_ipc.new_stream(sink, expected_arrow)
                writer.write_batch(aligned)
        finally:
            if writer is not None:
                writer.close()

        if writer is None:
            # Empty iterator: schema-only IPC stream (zero batches).
            with pa_ipc.new_stream(sink, expected_arrow):
                pass

        return self._register_ipc_bytes_as_inner(
            sink.getvalue(), replace_ephemeral=replace_ephemeral_views
        )

    def _register_arrow_stream_as_inner(
        self, stream_obj: Any, *, replace_ephemeral: bool = False
    ) -> Any:
        """Register an Arrow C Stream exporter as a native MemTable scan."""
        import contextlib

        view_name = scratch_view_name(self._session, "__repark_mia_")
        tracked = False
        try:
            self._session.register_arrow_stream_as_temp_view(view_name, stream_obj)
            self._track_mia_view(view_name, replace_ephemeral=replace_ephemeral)
            tracked = True
            return self._session.sql(f"SELECT * FROM {view_name}")
        except Exception:
            if not tracked:
                with contextlib.suppress(Exception):
                    self._session.drop_temp_view(view_name)
            raise

    def _register_ipc_bytes_as_inner(
        self, ipc_bytes: bytes, *, replace_ephemeral: bool = False
    ) -> Any:
        """Register IPC bytes as a native MemTable scan and track its temporary view."""
        import contextlib

        view_name = scratch_view_name(self._session, "__repark_mia_")
        tracked = False
        try:
            self._session.register_ipc_stream_as_temp_view(view_name, ipc_bytes)
            # Own the view before sql() so finalize drops it even if SELECT fails.
            self._track_mia_view(view_name, replace_ephemeral=replace_ephemeral)
            tracked = True
            return self._session.sql(f"SELECT * FROM {view_name}")
        except Exception:
            if not tracked:
                with contextlib.suppress(Exception):
                    self._session.drop_temp_view(view_name)
            raise

    def mapInArrow(  # noqa: N802 — PySpark method name
        self,
        func: Callable[[Iterator[Any]], Iterator[Any]],
        schema: Any,
    ) -> DataFrame:
        """Apply ``func`` to Arrow record-batch iterators lazily.

        Actions run the bridge; repeated actions rerun it unless cache or persist pins the result.
        ``schema`` is validated against every yielded batch. User errors become
        ``PySparkException`` with the original traceback. Batch boundaries are not contractual.
        """
        self._ensure_alive()
        if not callable(func):
            raise PySparkTypeError(f"mapInArrow func must be callable, got {type(func).__name__}")
        declared, arrow_schema = _coerce_map_in_arrow_schema(schema)
        # Schema-only placeholder (empty IPC) so schema()/columns work without running func.
        # Register against the *result* DataFrame so weakref.finalize can drop the view
        # (native PyDataFrame is not weakref-able).
        import contextlib
        import io

        import pyarrow.ipc as pa_ipc

        sink = io.BytesIO()
        with pa_ipc.new_stream(sink, arrow_schema):
            pass
        view_name = scratch_view_name(self._session, "__repark_mia_")
        # If sql fails after register, drop eagerly so the MemTable is not orphaned without a
        # finalize owner. Track immediately after a successful sql.
        self._session.register_ipc_stream_as_temp_view(view_name, sink.getvalue())
        try:
            placeholder_inner = self._session.sql(f"SELECT * FROM {view_name}")
        except Exception:
            with contextlib.suppress(Exception):
                self._session.drop_temp_view(view_name)
            raise
        out = self._spawn(placeholder_inner)
        out._track_mia_view(view_name, replace_ephemeral=False)
        out._map_bridge = {
            "parent": self,
            "func": func,
            "schema": declared,
            "arrow_schema": arrow_schema,
        }
        return out

    map_in_arrow = mapInArrow

    def mapInPandas(  # noqa: N802 — PySpark method name
        self,
        func: Callable[[Any], Any],
        schema: Any,
    ) -> DataFrame:
        """Apply a pandas-DataFrame iterator UDF through ``mapInArrow``.

        Requires the optional ``pandas`` extra.
        """
        self._ensure_alive()
        try:
            __import__("pandas")
        except ImportError as error:
            raise ImportError(
                "mapInPandas requires pandas (pip install 'repark[pandas]')"
            ) from error

        if not callable(func):
            raise PySparkTypeError(f"mapInPandas func must be callable, got {type(func).__name__}")

        return self.mapInArrow(
            functools.partial(_map_in_pandas_arrow_batches, user_func=func),
            schema,
        )

    map_in_pandas = mapInPandas

    # =========================================================================
    # Scalar pandas UDF projection rewrite.
    # =========================================================================

    def _select_with_pandas_udfs(self, items: list[Any]) -> DataFrame:
        """Rewrite scalar pandas UDF projections through one deferred ``mapInArrow`` bridge.

        Scalar UDFs run per batch; iterator UDFs receive batch iterators. Grouped aggregates require
        ``groupBy.agg`` or a window. The output schema is declared before action. Requires pandas
        at action time.
        """
        import pyarrow as pa

        from repark.spark.functions import PandasUDFColumn, PandasUDFType
        from repark.spark.types import _arrow_type_to_repark

        self._ensure_alive()

        #: windowed GROUPED_AGG must not enter the scalar mapInArrow path.
        if any(
            isinstance(item, PandasUDFColumn)
            and int(getattr(item, "_function_type", PandasUDFType.SCALAR))
            == PandasUDFType.GROUPED_AGG
            and getattr(item, "_window_spec", None) is not None
            for item in items
        ):
            return self._select_with_window_pandas_udfs(items)

        # ---- plan intermediate projections + final output slots -------------------------
        intermediate_columns: list[Column] = []
        output_slots: list[dict[str, Any]] = []
        seen_out_names: dict[str, int] = {}

        for item in items:
            if isinstance(item, PandasUDFColumn):
                function_type = int(getattr(item, "_function_type", PandasUDFType.SCALAR))
                if function_type == PandasUDFType.GROUPED_AGG:
                    raise AnalysisException(
                        "GROUPED_AGG pandas_udf cannot be used in select/withColumn without "
                        ".over(Window.partitionBy(...)); use groupBy(...).agg(pandas_udf(...)) "
                        "for non-window form, or attach an unbounded partition window via .over"
                    )
                if function_type not in {
                    PandasUDFType.SCALAR,
                    PandasUDFType.SCALAR_ITER,
                }:
                    raise UnsupportedOperationException(
                        f"pandas_udf functionType={function_type!r} is not supported in "
                        "select/withColumn (supported: SCALAR, SCALAR_ITER). "
                        "GROUPED_MAP / window pandas_udf are M6-class seeds."
                    )
                input_inter_names: list[str] = []
                for input_column in item._inputs:
                    # Partition transforms (years/months/days/hours/bucket) project
                    # literal(None) dummies outside partitionedBy — UDF would silently
                    # see all-null Series.
                    _reject_partition_transform(input_column)
                    # Generators as UDF inputs project the array without unnest and yield
                    # wrong cardinality/values — refuse loud.
                    if getattr(input_column, "_generator", None) is not None:
                        raise AnalysisException(
                            "pandas_udf input cannot be explode/posexplode generator; "
                            "unnest first, then apply the UDF on the expanded column "
                            f"(got generator on input to {item._function_name!r})"
                        )
                    if bool(getattr(input_column, "_is_aggregate", False)):
                        raise AnalysisException(
                            "pandas_udf input cannot be an aggregate expression; "
                            "aggregate first or apply the UDF before aggregation "
                            f"(got aggregate on input to {item._function_name!r})"
                        )
                    temp_name = f"__repark_pudf_in_{uuid.uuid4().hex}"
                    # Stable bare refs rebind to this frame; compounds keep their plan expr.
                    if input_column._stable_name and not input_column._is_aggregate:
                        bound = self._rebind_stable_name_column(input_column)
                    else:
                        bound = input_column
                    intermediate_columns.append(bound.alias(temp_name).for_select())
                    input_inter_names.append(temp_name)
                out_name = item.output_name()
                seen_out_names[out_name] = seen_out_names.get(out_name, 0) + 1
                output_slots.append(
                    {
                        "kind": "pudf",
                        "user_func": item._user_func,
                        "function_name": item._function_name,
                        "function_type": function_type,
                        "return_type_sql": item._return_type_sql,
                        "input_inter_names": input_inter_names,
                        "out_name": out_name,
                    }
                )
                continue

            column = self._column_of(item).for_select()
            if getattr(column, "_generator", None) is not None:
                raise AnalysisException(
                    "pandas_udf cannot mix with explode/posexplode generators in one select; "
                    "project generators and pandas_udf in separate steps"
                )
            if bool(column._is_aggregate):
                raise AnalysisException(
                    "pandas_udf cannot mix with aggregate expressions in one select; "
                    "aggregate first or apply the UDF before aggregation"
                )
            out_name = (
                column._projection_name
                if column._projection_name is not None
                else column.spark_display_part()
            )
            seen_out_names[out_name] = seen_out_names.get(out_name, 0) + 1
            intermediate_columns.append(column)
            output_slots.append(
                {
                    "kind": "pass",
                    "inter_name": out_name,
                    "out_name": out_name,
                }
            )

        duplicates = [name for name, count in seen_out_names.items() if count > 1]
        if duplicates:
            raise AnalysisException(
                f"select would produce duplicate column names {duplicates}; repark requires "
                "unique projection names (DataFusion). Use .alias(...) to disambiguate. "
                "(Live PySpark allows duplicate names — disclosed Group H divergence.)"
            )
        if not intermediate_columns:
            raise PySparkTypeError("pandas_udf select produced no intermediate columns")

        # Engine intermediate project — lazy plan-only; does not run UDF bodies or pull rows.
        intermediate = self._spawn(
            self._plan().select([column._inner for column in intermediate_columns])
        )

        # Pass-through types from the intermediate *analyzed* Arrow schema (metadata only —
        # no limit(0).to_arrow() action;), not collapsed logical_schema type_keys.
        # arrow_type_key maps float32→double, i16→int, binary→string, which then fails
        # mapInArrow type checks on withColumn/select siblings. Physical
        # pass-through Arrow types are kept as-is for mapInArrow validation —
        # Arrow→repark→Arrow drops timestamp timezone (timestamp[us,tz=UTC]→timestamp[us]),
        # which fails current_timestamp() / zoned siblings.
        inter_arrow_schema = intermediate._analyzed_arrow_schema()
        inter_arrow_by_name = {field.name: field for field in inter_arrow_schema}

        from repark.spark.functions import (
            _normalize_pandas_udf_return_type_sql,
            _pandas_udf_arrow_type_for_return,
        )

        struct_fields: list[StructField] = []
        expected_arrow_fields: list[Any] = []
        for slot in output_slots:
            out_name = slot["out_name"]
            if slot["kind"] == "pass":
                inter_field = inter_arrow_by_name.get(slot["inter_name"])
                if inter_field is None:
                    raise AnalysisException(
                        "pandas_udf intermediate projection missing pass-through column "
                        f"{slot['inter_name']!r}"
                    )
                data_type: DataType = _arrow_type_to_repark(inter_field.type)
                # Keep intermediate physical type (incl. tz) for batch validation.
                expected_arrow_fields.append(pa.field(out_name, inter_field.type, nullable=True))
            else:
                # Revalidate at bridge — marker ``_return_type_sql`` may be mutated after
                # construction, or a hostile PandasUDFColumn may bypass the decorator
                # ( of fail-open string).
                validated_sql = _normalize_pandas_udf_return_type_sql(slot["return_type_sql"])
                slot["return_type_sql"] = validated_sql
                data_type = DataType.fromDDL(validated_sql)
                expected_arrow_fields.append(
                    pa.field(
                        out_name,
                        _pandas_udf_arrow_type_for_return(data_type),
                        nullable=True,
                    )
                )
            struct_fields.append(StructField(out_name, data_type, True))
        result_schema = StructType(struct_fields)
        expected_arrow = pa.schema(expected_arrow_fields)

        needs_scalar_iter = any(
            slot["kind"] == "pudf" and slot.get("function_type") == PandasUDFType.SCALAR_ITER
            for slot in output_slots
        )

        # mapInArrow coerces StructType→Arrow and rebuilds logical schema via
        # ``struct_type_from_arrow`` — that drops timestamp tz on pass slots
        # and collapses ``timestamp_ntz`` / ``varchar(n)`` / ``char(n)`` to timestamp/string
        # . Patch both bridge halves with the identity we built above.
        result = intermediate.mapInArrow(
            functools.partial(
                _run_pandas_udf_arrow_batches,
                slots=output_slots,
                expected_arrow=expected_arrow,
                needs_scalar_iter=needs_scalar_iter,
            ),
            result_schema,
        )
        if result._map_bridge is not None:
            result._map_bridge["arrow_schema"] = expected_arrow
            result._map_bridge["schema"] = result_schema
        return result

    # =========================================================================
    # Classic scalar Python UDF projection rewrite.
    # =========================================================================
    #
    # Use mapInArrow for classic scalar Python UDFs; pandas UDFs use the bridge above.
    # Per-row Python: O(rows) calls — slower than pandas_udf by design (Spark parity).

    def _select_with_python_udfs(self, items: list[Any]) -> DataFrame:
        """Rewrite classic scalar UDF projections through a deferred ``mapInArrow`` bridge.

        The facade invokes the user function once per row and declares the output schema before
        action. This path requires PyArrow but not pandas.
        """
        import pyarrow as pa

        from repark.spark.functions import (
            PandasUDFColumn,
            PythonUDFColumn,
            _normalize_python_udf_return_type_sql,
            _python_udf_arrow_type_for_return,
        )
        from repark.spark.types import _arrow_type_to_repark

        self._ensure_alive()

        intermediate_columns: list[Column] = []
        output_slots: list[dict[str, Any]] = []
        seen_out_names: dict[str, int] = {}

        for item in items:
            if isinstance(item, PandasUDFColumn):
                raise UnsupportedOperationException(
                    "cannot mix classic udf and pandas_udf in one select/withColumn; "
                    "project them in separate steps"
                )
            if isinstance(item, PythonUDFColumn):
                input_inter_names: list[str] = []
                for input_column in item._inputs:
                    _reject_partition_transform(input_column)
                    if getattr(input_column, "_generator", None) is not None:
                        raise AnalysisException(
                            "udf input cannot be explode/posexplode generator; "
                            "unnest first, then apply the UDF on the expanded column "
                            f"(got generator on input to {item._function_name!r})"
                        )
                    if bool(getattr(input_column, "_is_aggregate", False)):
                        raise AnalysisException(
                            "udf input cannot be an aggregate expression; "
                            "aggregate first or apply the UDF before aggregation "
                            f"(got aggregate on input to {item._function_name!r})"
                        )
                    temp_name = f"__repark_udf_in_{uuid.uuid4().hex}"
                    if input_column._stable_name and not input_column._is_aggregate:
                        bound = self._rebind_stable_name_column(input_column)
                    else:
                        bound = input_column
                    intermediate_columns.append(bound.alias(temp_name).for_select())
                    input_inter_names.append(temp_name)
                out_name = item.output_name()
                seen_out_names[out_name] = seen_out_names.get(out_name, 0) + 1
                output_slots.append(
                    {
                        "kind": "udf",
                        "user_func": item._user_func,
                        "function_name": item._function_name,
                        "return_type_sql": item._return_type_sql,
                        "input_inter_names": input_inter_names,
                        "out_name": out_name,
                    }
                )
                continue

            column = self._column_of(item).for_select()
            if getattr(column, "_generator", None) is not None:
                raise AnalysisException(
                    "udf cannot mix with explode/posexplode generators in one select; "
                    "project generators and udf in separate steps"
                )
            if bool(column._is_aggregate):
                raise AnalysisException(
                    "udf cannot mix with aggregate expressions in one select; "
                    "aggregate first or apply the UDF before aggregation"
                )
            out_name = (
                column._projection_name
                if column._projection_name is not None
                else column.spark_display_part()
            )
            seen_out_names[out_name] = seen_out_names.get(out_name, 0) + 1
            intermediate_columns.append(column)
            output_slots.append(
                {
                    "kind": "pass",
                    "inter_name": out_name,
                    "out_name": out_name,
                }
            )

        duplicates = [name for name, count in seen_out_names.items() if count > 1]
        if duplicates:
            raise AnalysisException(
                f"select would produce duplicate column names {duplicates}; repark requires "
                "unique projection names (DataFusion). Use .alias(...) to disambiguate. "
                "(Live PySpark allows duplicate names — disclosed Group H divergence.)"
            )
        if not intermediate_columns:
            raise PySparkTypeError("udf select produced no intermediate columns")

        intermediate = self._spawn(
            self._plan().select([column._inner for column in intermediate_columns])
        )
        inter_arrow_schema = intermediate._analyzed_arrow_schema()
        inter_arrow_by_name = {field.name: field for field in inter_arrow_schema}

        struct_fields: list[StructField] = []
        expected_arrow_fields: list[Any] = []
        for slot in output_slots:
            out_name = slot["out_name"]
            if slot["kind"] == "pass":
                inter_field = inter_arrow_by_name.get(slot["inter_name"])
                if inter_field is None:
                    raise AnalysisException(
                        "udf intermediate projection missing pass-through column "
                        f"{slot['inter_name']!r}"
                    )
                data_type: DataType = _arrow_type_to_repark(inter_field.type)
                expected_arrow_fields.append(pa.field(out_name, inter_field.type, nullable=True))
            else:
                validated_sql = _normalize_python_udf_return_type_sql(slot["return_type_sql"])
                slot["return_type_sql"] = validated_sql
                data_type = DataType.fromDDL(validated_sql)
                expected_arrow_fields.append(
                    pa.field(
                        out_name,
                        _python_udf_arrow_type_for_return(data_type),
                        nullable=True,
                    )
                )
            struct_fields.append(StructField(out_name, data_type, True))
        result_schema = StructType(struct_fields)
        expected_arrow = pa.schema(expected_arrow_fields)
        result = intermediate.mapInArrow(
            functools.partial(
                _run_python_udf_arrow_batches,
                slots=output_slots,
                expected_arrow=expected_arrow,
            ),
            result_schema,
        )
        if result._map_bridge is not None:
            result._map_bridge["arrow_schema"] = expected_arrow
            result._map_bridge["schema"] = result_schema
        return result

    def _select_with_window_pandas_udfs(self, items: list[Any]) -> DataFrame:
        """Lower windowed ``GROUPED_AGG`` expressions through a deferred Arrow bridge.

        Partition-only windows use grouped aggregation. Ordered windows use rows-frame slices.
        Mixed partition-key sets and mixed windowed/non-windowed UDFs are refused.
        """
        from repark.spark.functions import PandasUDFColumn, PandasUDFType

        # Pandas UDF bridge over window frames.
        self._ensure_alive()

        windowed: list[PandasUDFColumn] = []
        plain_items: list[Any] = []
        for item in items:
            if isinstance(item, PandasUDFColumn):
                function_type = int(getattr(item, "_function_type", PandasUDFType.SCALAR))
                if (
                    function_type == PandasUDFType.GROUPED_AGG
                    and getattr(item, "_window_spec", None) is not None
                ):
                    windowed.append(item)
                elif function_type in {PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER}:
                    raise UnsupportedOperationException(
                        "mixing scalar/SCALAR_ITER pandas_udf with windowed GROUPED_AGG in "
                        "one select is not supported in repark v1; materialize window columns "
                        "first, then apply scalar pandas_udf"
                    )
                elif function_type == PandasUDFType.GROUPED_AGG:
                    raise AnalysisException(
                        "GROUPED_AGG pandas_udf in select requires .over(Window.partitionBy(...)) "
                        "or use groupBy(...).agg(...)"
                    )
                else:
                    raise UnsupportedOperationException(
                        f"pandas_udf functionType={function_type!r} is not supported in "
                        "windowed select (GROUPED_MAP is an M7-class seed)"
                    )
            else:
                plain_items.append(item)

        if not windowed:
            raise AnalysisException(
                "internal: _select_with_window_pandas_udfs with no windowed markers"
            )

        # All windowed markers must share the same partition key names (v1).
        partition_key_lists: list[list[str]] = []
        order_key_lists: list[list[str]] = []
        frame_bounds_list: list[tuple[int | None, int | None]] = []
        for marker in windowed:
            spec = marker._window_spec
            keys: list[str] = []
            for column in list(getattr(spec, "_partition_columns", []) or []):
                if column._stable_name and column._projection_name is not None:
                    keys.append(column._projection_name)
                else:
                    raise AnalysisException(
                        "windowed pandas_udf partitionBy requires simple column-name keys "
                        f"(got non-NamedExpression {column.spark_display_part()!r}); "
                        "project the expression first"
                    )
            if not keys:
                raise UnsupportedOperationException(
                    "windowed pandas_udf requires Window.partitionBy(...) with at least one key"
                )
            partition_key_lists.append(keys)
            order_keys: list[str] = []
            for column in list(getattr(spec, "_order_columns", []) or []):
                if column._stable_name and column._projection_name is not None:
                    order_keys.append(column._projection_name)
                else:
                    raise AnalysisException(
                        "windowed pandas_udf orderBy requires simple column-name keys "
                        f"(got non-NamedExpression {column.spark_display_part()!r}); "
                        "project the expression first"
                    )
            order_key_lists.append(order_keys)
            frame_bounds_list.append(_pandas_udf_window_frame_bounds(spec))
        first_keys = partition_key_lists[0]
        for keys in partition_key_lists[1:]:
            if keys != first_keys:
                raise UnsupportedOperationException(
                    "windowed pandas_udf markers in one select must share the same "
                    f"partitionBy keys (got {first_keys!r} vs {keys!r})"
                )
        key_names = list(first_keys)
        has_order = any(bool(order_keys) for order_keys in order_key_lists)
        if has_order and not all(bool(order_keys) for order_keys in order_key_lists):
            raise UnsupportedOperationException(
                "windowed pandas_udf markers in one select must all use orderBy or all omit it"
            )
        if has_order:
            first_order = order_key_lists[0]
            first_frame = frame_bounds_list[0]
            for order_keys, frame_bounds in zip(
                order_key_lists[1:], frame_bounds_list[1:], strict=True
            ):
                if order_keys != first_order or frame_bounds != first_frame:
                    raise UnsupportedOperationException(
                        "windowed pandas_udf markers in one select must share the same "
                        "orderBy keys and rows frame bounds"
                    )
            return self._select_with_ordered_window_pandas_udfs(
                items=items,
                windowed=windowed,
                plain_items=plain_items,
                key_names=key_names,
                order_names=list(first_order),
                frame_start=first_frame[0],
                frame_end=first_frame[1],
            )

        # ---- unbounded whole-partition path ----------------------------------------
        # Strip window and run pure GROUPED_AGG (plan-built applyInPandas).
        bare_markers: list[PandasUDFColumn] = []
        window_out_names: list[str] = []
        for marker in windowed:
            bare = PandasUDFColumn(
                marker._user_func,
                marker._return_type_sql,
                marker._inputs,
                marker._function_name,
                alias_name=marker._alias_name,
                function_type=marker._function_type,
                window_spec=None,
            )
            bare_markers.append(bare)
            window_out_names.append(bare.output_name())

        grouped = GroupedData(self, [self._bind_schema_column(name) for name in key_names])
        agg_frame = grouped._agg_via_pandas_udfs(tuple(bare_markers))

        # Source side: keep plain projections + partition keys for the join.
        # When plain_items is empty (withColumn-style single UDF), retain all source cols.
        if plain_items:
            # Ensure partition keys are present for the join even if not selected.
            source_projections: list[Any] = list(plain_items)
            plain_names: set[str] = set()
            for item in plain_items:
                if isinstance(item, str):
                    plain_names.add(item)
                elif isinstance(item, Column) and item._projection_name is not None:
                    plain_names.add(item._projection_name)
            for key_name in key_names:
                if key_name not in plain_names:
                    source_projections.append(self._bind_schema_column(key_name))
            left = self.select(*source_projections)
        else:
            left = self

        # Materialize both sides so the join sees unqualified field names (mapInArrow
        # bridge plans can otherwise qualify keys and trip schema ambiguity). Join with
        # ``IS NOT DISTINCT FROM`` so NULL partition keys match (Spark null-group parity;
        # name-list equi-join drops them). Final result is also materialized
        # so intermediate views can be dropped.
        session = self._session
        left_view = scratch_view_name(session, "__repark_win_l_")
        agg_view = scratch_view_name(session, "__repark_win_a_")
        out_view = scratch_view_name(session, "__repark_win_o_")
        try:
            left._prepare_for_plan()
            agg_frame._prepare_for_plan()
            session.materialize_as_temp_view(left_view, left._inner)
            session.materialize_as_temp_view(agg_view, agg_frame._inner)
            left_clean = self._spawn(session.sql(f"SELECT * FROM {left_view}"))
            agg_clean = self._spawn(session.sql(f"SELECT * FROM {agg_view}"))

            # Final select order matches caller items: non-UDF as given, UDF slots as out
            # names. **Last wins** on duplicate names so ``select("v", mean.alias("v"))`` /
            # withColumn-style overwrite keeps the window result — first-wins
            # skipped the UDF out and silently returned the source column.
            if plain_items:
                final_names: list[str] = []
                window_iter = iter(window_out_names)
                for item in items:
                    if isinstance(item, PandasUDFColumn):
                        name = next(window_iter)
                    elif isinstance(item, str):
                        name = item
                    elif isinstance(item, Column):
                        name = (
                            item._projection_name
                            if item._projection_name is not None
                            else item.spark_display_part()
                        )
                    else:
                        raise PySparkTypeError(
                            "select item type "
                            f"{type(item).__name__} unsupported with windowed pandas_udf"
                        )
                    final_names = [prior for prior in final_names if prior != name]
                    final_names.append(name)
            else:
                # select was only windowed UDF marker(s) — project just those columns.
                final_names = list(window_out_names)

            join_sql = _null_safe_equi_join_sql(
                left_view,
                agg_view,
                list(key_names),
                final_names,
                left_column_names=list(left_clean.columns),
                right_column_names=list(agg_clean.columns),
                prefer_right_names=set(window_out_names),
            )
            joined = self._spawn(session.sql(join_sql))
            joined._prepare_for_plan()
            session.materialize_as_temp_view(out_view, joined._inner)
            return self._spawn(session.sql(f"SELECT * FROM {out_view}"))
        finally:
            with contextlib.suppress(Exception):
                session.drop_temp_view(left_view)
            with contextlib.suppress(Exception):
                session.drop_temp_view(agg_view)

    def _select_with_ordered_window_pandas_udfs(
        self,
        *,
        items: list[Any],
        windowed: list[Any],
        plain_items: list[Any],
        key_names: list[str],
        order_names: list[str],
        frame_start: int | None,
        frame_end: int | None,
    ) -> DataFrame:
        """Apply a grouped aggregate over each ordered rows-frame partition.

        Frame offsets are relative to the current row (Spark ``rowsBetween``): ``None``
        means unbounded on that side; ``0`` is current row; negative is preceding.
        Default bounds are ``(None, 0)``: UNBOUNDED PRECEDING to CURRENT ROW. User UDF
        code runs over each frame slice.
        """
        from repark.spark.functions import (
            PandasUDFColumn,
            _normalize_pandas_udf_return_type_sql,
            _pandas_udf_arrow_type_for_return,
        )
        from repark.spark.types import DataType, StructField, StructType

        window_out_names: list[str] = []
        udf_specs: list[dict[str, Any]] = []
        for marker in windowed:
            out_name = marker.output_name()
            window_out_names.append(out_name)
            input_names: list[str] = []
            for column in marker._inputs:
                if column._stable_name and column._projection_name is not None:
                    input_names.append(column._projection_name)
                else:
                    raise AnalysisException(
                        "windowed pandas_udf inputs require simple column-name args "
                        f"(got {column.spark_display_part()!r}); project first"
                    )
            udf_specs.append(
                {
                    "user_func": marker._user_func,
                    "function_name": marker._function_name,
                    "return_type_sql": marker._return_type_sql,
                    "out_name": out_name,
                    "input_names": input_names,
                }
            )

        # Ensure partition + order + UDF inputs are present on the group frame.
        needed: list[str] = []
        for name in [*key_names, *order_names]:
            if name not in needed:
                needed.append(name)
        for spec in udf_specs:
            for name in spec["input_names"]:
                if name not in needed:
                    needed.append(name)
        # Keep all source columns so plain projections survive (withColumn / select *).
        source_cols = list(self.columns)
        for name in source_cols:
            if name not in needed:
                needed.append(name)
        projected = self.select(*[self._bind_schema_column(name) for name in needed])

        struct_fields: list[StructField] = []
        for field in projected.schema.fields:
            struct_fields.append(StructField(field.name, field.dataType, field.nullable))
        for spec in udf_specs:
            validated_sql = _normalize_pandas_udf_return_type_sql(spec["return_type_sql"])
            spec["return_type_sql"] = validated_sql
            data_type = DataType.fromDDL(validated_sql)
            _pandas_udf_arrow_type_for_return(data_type)
            # Overwrite same-name source columns (withColumn last-wins).
            struct_fields = [field for field in struct_fields if field.name != spec["out_name"]]
            struct_fields.append(StructField(spec["out_name"], data_type, True))
        result_schema = StructType(struct_fields)

        group_cols = [projected._bind_schema_column(name) for name in key_names]
        grouped = GroupedData(projected, group_cols)
        result = grouped.applyInPandas(
            functools.partial(
                _apply_ordered_window_pandas_udf,
                specs=udf_specs,
                order_cols=list(order_names),
                start_bound=frame_start,
                end_bound=frame_end,
                struct_fields=struct_fields,
            ),
            result_schema,
        )

        # Final projection: match select item order (last-wins on duplicate names).
        if plain_items:
            final_names: list[str] = []
            window_iter = iter(window_out_names)
            for item in items:
                if isinstance(item, PandasUDFColumn):
                    name = next(window_iter)
                elif isinstance(item, str):
                    name = item
                elif isinstance(item, Column):
                    name = (
                        item._projection_name
                        if item._projection_name is not None
                        else item.spark_display_part()
                    )
                else:
                    raise PySparkTypeError(
                        "select item type "
                        f"{type(item).__name__} unsupported with windowed pandas_udf"
                    )
                final_names = [prior for prior in final_names if prior != name]
                final_names.append(name)
            return result.select(*final_names)
        # withColumn-style: all source cols with window outs overwriting same names.
        return result

        # Cache and persist.

    # Cache materialization.

    def cache(self) -> DataFrame:
        """Mark this DataFrame for lazy MemTable materialization (PySpark ``cache``).

        Equivalent to ``persist()`` with the default MEMORY_AND_DISK_DESER level. The first
        action materializes; later actions on **this object** scan the MemTable.

        **Loud memory contract:** materialize is a full collect into an in-process
        MemTable — peak memory O(result). Despite the Spark default name
        ``MEMORY_AND_DISK_DESER``, repark does **not** spill to disk. Optional size guard:
        ``spark.conf.set("repark.cache.max_bytes", N)`` (or builder ``.config``) refuses
        materialize when collected Arrow array memory exceeds ``N`` bytes.

        **Object-identity only (not Spark plan-matching cache):** ``df.cache().filter(…).count()``
        does **not** materialize ``df`` or share a MemTable with the child — only actions on the
        same Python object after ``cache()`` trigger materialize. Two separately
        built identical plans never share a cache. Child-plan cache sharing is OUT (architectural).
        """
        from repark.spark.storage import StorageLevel

        return self.persist(StorageLevel.MEMORY_AND_DISK_DESER)

    def persist(self, storageLevel: Any = None) -> DataFrame:  # noqa: N803 — PySpark
        """Mark this DataFrame for lazy in-memory materialization (PySpark ``persist``).

        ``storageLevel`` is accepted and recorded on :attr:`storageLevel`. repark always
        materializes to a single-node MemTable when an action runs — **loud memory contract**
        Full collect, O(result) peak, no disk spill. Disk / off-heap / replication
        flags are signature parity only; the first time a level claims those behaviors in a
        session, a :class:`UserWarning` fires once. Optional
        ``repark.cache.max_bytes`` refuses oversized materialize.
        """
        from repark.spark.storage import StorageLevel

        self._ensure_alive()
        level = StorageLevel.MEMORY_AND_DISK_DESER if storageLevel is None else storageLevel
        if not isinstance(level, StorageLevel):
            raise PySparkTypeError(
                f"persist storageLevel must be StorageLevel, got {type(level).__name__}"
            )
        _warn_storage_level_cosmetic_once(self._alive_token, level, stacklevel=2)
        self._persist_requested = True
        self._storage_level = level
        _register_cache_frame(self._alive_token, self)
        return self

    def unpersist(self, blocking: bool = False) -> DataFrame:
        """Drop the MemTable cache for this object (PySpark ``unpersist``). Idempotent.

        For ``mapInArrow`` results, also clears ``_mia_plan_ready``: cache-era lineage is an
        action-ephemeral MemTable, not a durable plan-stable snapshot. Leaving the sticky
        ready flag would let later ``filter``/``select``/``groupBy`` reuse a stale or
        post-action-dangling ``_inner`` while parent actions re-run the bridge.

        ``blocking`` is accepted for signature parity and ignored (single-node drop is sync).
        """
        _ = blocking  # signature parity; single-node drop is always synchronous
        self._ensure_alive()
        if self._cache_view is not None:
            self._session.drop_temp_view(self._cache_view)
            self._cache_view = None
        if self._lineage_inner is not None:
            self._inner = self._lineage_inner
            self._lineage_inner = None
        self._persist_requested = False
        self._storage_level = None
        if self._map_bridge is not None:
            self._mia_plan_ready = False
        return self

    def localCheckpoint(  # noqa: N802 — PySpark method name
        self,
        eager: bool = True,
        storageLevel: Any = None,  # noqa: N803 — Spark arg name
    ) -> DataFrame:
        """Truncate lineage by materializing to a MemTable (PySpark ``localCheckpoint``).

        When ``eager`` is true (default), materializes immediately. Unlike ``cache``,
        checkpoint does **not** set :attr:`is_cached` (live Spark 4.1.2 oracle). Returns self.
        ``storageLevel`` is accepted for signature parity and ignored (always MemTable).
        """
        _ = storageLevel  # signature parity; single-node MemTable only
        self._ensure_alive()
        self._checkpoint_lazy = True
        self._persist_requested = False
        self._storage_level = None
        if eager:
            self._materialize_cache_if_needed()
        return self

    @property
    def is_cached(self) -> bool:
        """Whether this object has an active cache/persist mark (PySpark ``is_cached``)."""
        if self._checkpoint_lazy:
            return False
        return self._persist_requested or self._cache_view is not None

    @property
    def isStreaming(self) -> bool:  # noqa: N802 — PySpark property name
        """Whether this is a streaming DataFrame (PySpark ``DataFrame.isStreaming``).

           repark is batch-only in v1 — always ``False``. Apache suite probes this attribute
        after many function/column builders; exposing it unblocks that FAIL-MISSING wall
        without claiming streaming support (``readStream`` remains absent).
        """
        return False

    is_streaming = isStreaming

    def sameSemantics(self, other: DataFrame) -> bool:  # noqa: N802 — PySpark camelCase
        """Whether ``other`` has the same logical semantics (PySpark ``DataFrame.sameSemantics``).

        Type-gates non-DataFrame arguments with ``NOT_DATAFRAME`` (Apache
        ``test_same_semantics_error``). Positive path is **best-effort identity of the native
        handle** (``self._inner is other._inner``) — not Catalyst plan isomorphism and not
        plan-text equality (no stable plan printer on the native surface yet).
        """
        if not isinstance(other, DataFrame):
            raise PySparkTypeError(
                errorClass="NOT_DATAFRAME",
                messageParameters={
                    "arg_name": "other",
                    "arg_type": type(other).__name__,
                },
            )
        self._ensure_alive()
        other._ensure_alive()
        # Best-effort: same native PyDataFrame object only (not full semantic equality).
        return self._inner is other._inner

    same_semantics = sameSemantics

    @property
    def storageLevel(self) -> Any:  # noqa: N802 — PySpark property name
        """Return the storage level, or ``StorageLevel.NONE`` when not cached."""
        from repark.spark.storage import StorageLevel

        if self._storage_level is not None:
            return self._storage_level
        return StorageLevel.NONE

    storage_level = storageLevel

    @property
    def pl(self) -> Any:
        """Polars-style API wrapper (``import repark.polars as rp`` / ``df.pl``).

        Returns a :class:`repark.polars.PolarsFrame` over this plan. Does not import real polars
        until :meth:`repark.polars.PolarsFrame.collect`.
        """
        from repark.spark.polars import PolarsFrame

        return PolarsFrame(self)

    def create_or_replace_temp_view(self, name: str) -> None:
        """Register this DataFrame as a replaceable temporary view.

        Materialize pending bridges and cache requests so SQL scans real rows.
        """
        self._session.create_or_replace_temp_view(name, self._native_for_registration())

    # PySpark spells this ``createOrReplaceTempView``; expose both so the import swap just works.
    createOrReplaceTempView = create_or_replace_temp_view  # noqa: N815 — PySpark camelCase alias

    # Declared-sort registration.
    def declare_sorted(
        self,
        *cols: str,
        tightenNulls: bool = False,  # noqa: N803 — repark-extra camelCase keyword
    ) -> DataFrame:
        """Declare this source frame already sorted by ``cols`` — a repark **extension**.

        Not a PySpark API. Declares ASC NULLS LAST ordering (per key, in the order given)
        on the in-memory view backing a ``createDataFrame`` result, so DataFusion can drop
        the redundant ``SortExec`` a window over the same keys would otherwise plan — the
        sort is O(n log n) on *every* query, the verification below is O(n) once.

        The engine **always verifies** the claim with an adjacent-pair scan over the sort
        keys (across batch boundaries) before it records anything: an out-of-order pair
        raises :class:`~repark.errors.AnalysisException` naming the first offending row
        indices, and the view is left exactly as it was. There is no unverified fast path.

        Parameters
        ----------
        cols:
            Sort keys, in order. At least one is required.
        tightenNulls:
            Default ``False`` keeps the door a pure hint (schema unchanged). ``True``
            unlocks elision on the serving-shape window
            (``Window.partitionBy(...).orderBy(...)`` over the declared keys): after
            verify, a NULL in a declared key refuses (name the key; drop
            ``tightenNulls`` or clean the data); otherwise the in-engine schema of
            those keys becomes non-nullable
            (``df.schema`` / ``to_arrow()``). That is a plan property, not a data contract
            — Iceberg CREATE is refused when the SELECT would persist a
            non-nullable column (all-nullable projections are allowed). Internal
            ``repark.tighten_nulls`` tags are stripped from ``to_arrow()`` export.

        Returns
        -------
        DataFrame
            ``self``, so the call chains.

        Examples
        --------
        >>> from repark import ReparkSession
        >>> spark = ReparkSession.builder.appName("doctest-declare-sorted").getOrCreate()
        >>> bars = [("AAA", 1), ("AAA", 2), ("BBB", 1)]
        >>> frame = spark.createDataFrame(bars, ["symbol", "ts"]).declareSorted("symbol", "ts")
        >>> frame.columns
        ['symbol', 'ts']
        >>> tight = spark.createDataFrame(bars, ["symbol", "ts"]).declareSorted(
        ...     "symbol", "ts", tightenNulls=True
        ... )
        >>> tight.schema["ts"].nullable
        False
        >>> spark.stop()

        Valid only on a source frame — the frame ``createDataFrame`` handed back. Any
        transformed frame (``select`` / ``filter`` / join / agg output) refuses loudly;
        declare on the source, then transform. Names resolve case-insensitively through the
        same display→engine machinery as :meth:`select`, so a mixed-case column may be
        declared with any spelling; an unknown name refuses and lists the available columns.

        Replacing the underlying view drops the declaration (it lives on the registered
        table, not on this handle). Each call is a fresh verify-then-register: a later
        default-flag call after a tighten restores original key nullability **on that
        source frame**. Already-derived frames cannot be re-declared (source-frames
        only), so they keep the derived plan's nullability.
        """
        self._ensure_alive()
        if not cols:
            raise PySparkValueError(
                "declareSorted requires at least one column "
                "(the sort keys, in order — repark extension, not PySpark)"
            )
        if self._source_view_name is None:
            raise PySparkValueError(
                "declareSorted applies to source frames only — the frame createDataFrame "
                "returned, whose rows are already materialized in memory. This frame is a "
                "transform of one (or a cache/SQL result); declare on the source frame and "
                "transform afterwards."
            )
        # Caching redirects the scan to another view. Declare the source before caching.
        if self._cache_view is not None or self._persist_requested or self._checkpoint_lazy:
            raise PySparkValueError(
                "declareSorted must run before cache()/persist()/checkpoint on this frame "
                "— caching redirects the frame to a cache view, and declaring afterwards "
                "would detach it. Call declareSorted first, then cache."
            )
        engine_keys: list[str] = []
        for name in cols:
            if not isinstance(name, str):
                raise PySparkTypeError(
                    f"declareSorted column names must be str, got {type(name).__name__}"
                )
            # Same bind machinery select/explode use: case-insensitive canonicalization
            # (raises listing the available columns), then the display-to-engine overlay.
            canonical = self._resolve_getitem_column_name(name)
            engine_keys.append(self._engine_field_for_display(canonical))
        view = self._source_view_name
        self._session.declare_temp_view_sorted(view, engine_keys, tightenNulls)
        # The declaration re-registers the view's MemTable, but this frame's logical plan
        # still holds the table source captured when the scan was planned — re-resolve it,
        # or the frame that declared would be the one frame that never sees the elision.
        self._inner = self._session.sql(f"SELECT * FROM {view}")
        self._tighten_derived = tightenNulls
        return self

    def _refuse_tightened_iceberg_create(self) -> None:
        """Refuse Iceberg CREATE of a tighten-derived frame that would persist a required field.

        Skip when every output field is nullable; no required column would be written.
        """
        if not self._tighten_derived:
            return
        if not any(_output_field_would_persist_required(field) for field in self.schema.fields):
            return
        raise AnalysisException(
            "Iceberg CREATE of a frame declared with tightenNulls=True is refused until "
            "PR-D2 (the write-boundary relax). Drop tightenNulls or wait for the "
            "create-path relax."
        )

    # repark extension (no PySpark equivalent); camelCase is the disclosed repark spelling.
    declareSorted = declare_sorted  # noqa: N815 — repark-extra camelCase surface

    # ---- transform surface (PySpark DataFrame ops) ------------------------------------------

    def with_column(self, col_name: str, column: Column) -> DataFrame:
        """Add or replace a column (PySpark ``DataFrame.withColumn``).

        Returns a new :class:`DataFrame`; the original is unchanged (Spark DataFrames are
        immutable). Empty-string column names are rejected.
        """
        if not isinstance(col_name, str):
            raise PySparkTypeError(f"withColumn name must be str, got {type(col_name).__name__}")
        if col_name.strip() == "":
            raise AnalysisException(
                "withColumn column names must be non-empty "
                "(empty/whitespace names are rejected — Group F / octo r3)"
            )
        from repark.spark.functions import PandasUDFColumn, PythonUDFColumn

        # Scalar UDF markers use the withColumns→select bridge.
        if isinstance(column, (PandasUDFColumn, PythonUDFColumn)):
            return self.with_columns({col_name: column})
        if not isinstance(column, Column):
            raise PySparkTypeError(
                f"withColumn value must be Column, udf result, or pandas_udf result, "
                f"got {type(column).__name__}"
            )
        _reject_partition_transform(column)
        # Aggregates only lower via select/agg — withColumn→native would fail engine-side
        # (or withColumns→select pure_global would collapse N→1 rows). Spark rejects
        # Aggregates are rejected in withColumn.
        _reject_aggregate_in_with_column(column, surface="withColumn")
        # Window, random, and stratified-sampling validation.
        _reject_non_numeric_range_order(self, column)
        # Generators must go through the select unnest rewrite — native with_column would
        # project the array placeholder without multiplying rows.
        if getattr(column, "_generator", None) is not None:
            return self.with_columns({col_name: column})
        # Plan-collapse.
        # Route the ordinary path through with_columns→select so alias-chain squash and
        # adjacent same-spec window merge apply to both withColumn and withColumns.
        # Multi-name identity is handled on the with_columns/select path.
        return self.with_columns({col_name: column})

    # PySpark spells this ``withColumn``; expose both.
    withColumn = with_column  # noqa: N815 — deliberate PySpark-compatible camelCase alias

    def with_columns(self, colsMap: dict[str, Column]) -> DataFrame:  # noqa: N803 — PySpark camelCase
        """Add or replace multiple columns atomically (PySpark ``DataFrame.withColumns``).

        Expressions over **existing** column names are evaluated against the original frame
        (not a running fold). Live PySpark 4.1.2 probe:
        ``df.withColumns({"a": col("b")+1, "b": col("a")+100})`` on ``(a=1, b=10)`` yields
        ``(a=11, b=101)`` — both sides see the pre-update values. A naive sequential
        ``withColumn`` fold would yield ``b=111``. New names append in dict order.

        Spark resolves a **new** name
        referencing an **earlier new** name via lateral column aliases
        (``{"x": a+1, "y": col("x")}`` → ``y == x``; the reverse order raises). repark has no
        lateral-alias resolution and raises ``AnalysisException`` in both orders — pinned in
        ``test_dropin_disclosure.py``.
        """
        from repark.spark.functions import PandasUDFColumn, PythonUDFColumn

        if not isinstance(colsMap, dict):
            raise PySparkTypeError(
                f"colsMap should be a dict of column name to Column, got {type(colsMap).__name__}"
            )
        # Validate keys + values before any `.alias` so bad maps raise TypeError early
        # Validate keys and values before aliasing.
        for name, column in colsMap.items():
            if not isinstance(name, str):
                raise PySparkTypeError(
                    f"withColumns keys must be str column names, got {type(name).__name__}"
                )
            if name.strip() == "":
                raise AnalysisException(
                    "withColumns column names must be non-empty "
                    "(empty/whitespace names are rejected — Group F / octo r3)"
                )
            if isinstance(column, (PandasUDFColumn, PythonUDFColumn)):
                continue
            if not isinstance(column, Column):
                raise PySparkTypeError(
                    f"withColumns values must be Column, udf result, or pandas_udf result, "
                    f"got {type(column).__name__} for {name!r}"
                )
            # Aggregates only lower via select/agg — withColumns always select(*) and
            # pure_global would collapse N rows → 1 for all-agg/foldable maps (Spark
            # rejects aggregates in withColumns.
            _reject_aggregate_in_with_column(column, surface="withColumns")
        # Adjacent same-spec window merge.
        # Only when the immediately-prior layer (sticky meta on this frame) used the same
        # structural window AND no new column may read a name defined in that prior layer.
        # filter/drop/select never copy sticky meta → intervening ops block merge.
        # When in doubt, fall through to a new stacked layer.
        merged = self._try_merge_adjacent_window_layer(colsMap)
        if merged is not None:
            return merged
        # Select accepts Column and scalar UDF markers.
        projected: list[Any] = []
        # Multi-name frames iterate engine/display bindings.
        seen_display: set[str] = set()
        for bound in self._iter_bound_columns():
            display = bound._projection_name or bound.spark_display_part()
            seen_display.add(display)
            if display in colsMap:
                replacement = colsMap[display]
                if isinstance(replacement, Column):
                    replacement = self._rebind_origin_column(replacement)
                # Preserve origin on replacement when multi-name so select keeps identity.
                aliased = replacement.alias(display)
                if (
                    isinstance(replacement, Column)
                    and replacement._origin_plan_id is not None
                    and bound._origin_plan_id is not None
                ):
                    projected.append(
                        Column(
                            aliased._inner,
                            spark_display=display,
                            projection_name=display,
                            stable_name=True,
                            has_free_attribute=True,
                            origin_plan_id=bound._origin_plan_id,
                            origin_field=bound._origin_field,
                            join_sql_expr=replacement._join_sql_expr,
                            sql_expr=aliased._sql_expr,
                            window_spec=getattr(replacement, "_window_spec", None),
                        )
                    )
                else:
                    projected.append(aliased)
            else:
                projected.append(bound)
        for name, column in colsMap.items():
            if name not in seen_display:
                if isinstance(column, Column):
                    projected.append(self._rebind_origin_column(column).alias(name))
                else:
                    projected.append(column.alias(name))
        child = self.select(*projected)
        # Sticky layer meta for a subsequent adjacent same-spec merge.
        child._collapse_base = self
        child._layer_map = dict(colsMap)
        child._layer_defined = frozenset(colsMap.keys())
        child._layer_window_key = _uniform_window_key_from_map(colsMap)
        return child

    # PySpark spells this ``withColumns``.
    withColumns = with_columns  # noqa: N815 — deliberate PySpark-compatible camelCase alias

    def _try_merge_adjacent_window_layer(self, cols_map: dict[str, Any]) -> DataFrame | None:
        """Stage (b): merge into prior withColumn(s) layer when safe; else ``None``.

        Requires sticky meta from the immediately-prior withColumn(s) (``_collapse_base`` +
        matching ``_layer_window_key``). Dependency analysis must not see any prior-layer
        defined name in the new expressions (ETR5 reading ``tr`` keeps stacking).
        """
        base = self._collapse_base
        prior_key = self._layer_window_key
        prior_map = self._layer_map
        prior_defined = self._layer_defined
        if base is None or prior_key is None or prior_map is None or prior_defined is None:
            return None
        # Do not merge past a cache mark; that would orphan the intermediate MemTable pin.
        if self._persist_requested or self._cache_view is not None:
            return None
        new_key = _uniform_window_key_from_map(cols_map)
        if new_key is None or new_key != prior_key:
            return None
        for _name, column in cols_map.items():
            if not isinstance(column, Column):
                return None
            if _column_may_reference_names(column, prior_defined):
                return None
        # Replay both maps on the pre-layer frame → one WindowAggr (DataFusion fuses).
        combined: dict[str, Any] = {**prior_map, **cols_map}
        return base.with_columns(combined)

    def filter(self, condition: Column | str) -> DataFrame:
        """Keep only rows matching ``condition`` (PySpark ``DataFrame.filter``).

        ``condition`` is a boolean :class:`Column` or a SQL-string predicate (``"a > 1"``).
        Partition transforms are valid only inside :meth:`DataFrameWriterV2.partitionedBy`. SQL
        predicates quote schema-bound identifiers.
        so a requested-spelling projection (``select("X")`` → field ``"X"``) still filters under
        DataFusion's case-sensitive unquoted fold. Live PySpark 4.1.2 keeps
        ``filter("X > 0")`` working).

        **Case-collision refusal — SQL-string form only.** In a bare SQL predicate, a token
        naming a column that collides only by case with another (``id`` / ``ID``) raises
        :class:`~repark.errors.AnalysisException` with Spark's ``[AMBIGUOUS_REFERENCE]``
        condition tag; naming any unambiguous column of that same frame still works. Two accepted
        spellings **bypass** that refusal and diverge from live PySpark 4.1.2, which raises
        ``AMBIGUOUS_REFERENCE`` for both (verified against the live oracle, disclosed not fixed —
        pinned in ``test_filter_predicate_rewrite.py`` and re-checked by the live tier's
        ``filter_case_collision_bypasses`` disclosure):

        * the :class:`Column` form — ``df.filter(df["id"] > 0)`` resolves **exact-case-first** and
        returns rows (``df["ID"]`` binds the other column) instead of refusing;
        * an explicitly double-quoted ident is passed through untouched and
          DataFusion resolves it case-sensitively.
          Spark reads ``"ID"`` as a string *literal*,
        not an identifier, so the two engines disagree about that span regardless of collisions.)
        """
        if isinstance(condition, Column):
            _reject_partition_transform(condition)
            # Generators only lower via select unnest — filter on a generator would
            # A generator predicate would target the array placeholder.
            condition._reject_nested_generator("filter")
            # Compounds clear origin but keep join_sql QCOL
            # tokens — rewrite to local engine fields and use filter_sql (native Column
            # path cannot re-apply ops without stored children).
            join_sql = condition.join_sql_part()
            if "__REPARK_QCOL_" in join_sql and self._origin_map is not None:
                local_sql = _rewrite_qcol_tokens_local(join_sql, self)
                if "__REPARK_QCOL_" not in local_sql:
                    return self._spawn_preserving_identity(self._plan().filter_sql(local_sql))
            # Pure origin Columns rebind to engine fields before native filter.
            predicate = self._rebind_origin_column(condition)
            return self._spawn_preserving_identity(self._plan().filter(predicate._inner))
        if isinstance(condition, str):
            quoted = self._quote_filter_sql_identifiers(condition)
            return self._spawn_preserving_identity(self._plan().filter_sql(quoted))
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={
                "arg_name": "condition",
                "arg_type": type(condition).__name__,
            },
        )

    # PySpark aliases ``where`` to ``filter``.
    where = filter

    def select(self, *cols: Column | str) -> DataFrame:
        """Project to the given columns (PySpark ``DataFrame.select``).

        Each argument is a :class:`Column` or a bare column name (which becomes ``col(name)``).
        ``select("*")`` projects every column (wildcard); a bare ``"*"`` among other args
        expands the same way (live PySpark ``select("*", expr)``). Partition-transform Columns
        (``F.years`` / ``F.months`` / ``F.days`` / ``F.hours``) are valid only inside
        :meth:`DataFrameWriterV2.partitionedBy` and raise here (Spark
        ``PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY`` parity).

        Non-aggregate projection names match live PySpark: compound expressions
        use the facade ``_projection_name`` (``(x + 1)``, ``negative(x)``, …) rather than
        DataFusion's ``t.x + Int64(1)`` text. An explicit ``.alias(...)`` always wins.
        Plain casts of named attributes keep the child name (``df.x.cast("double")`` →
        ``"x"``); cast of a compound expression uses ``CAST(...)``. Bare string / ``col``
        refs keep the **requested** spelling (``select("X")`` → ``"X"`` when the schema
        column is ``x`` — live PySpark 4.1.2 under case-insensitive resolution). Schema
        binds use **quoted** native identifiers so a subsequent ``select("X")`` /
        ``filter`` after a requested-spelling projection still resolves.

        An **all-aggregate** select list (every column carries aggregate metadata via
        ``Column._is_aggregate`` / ``_agg_name``) is Spark's global aggregate and routes through
        :meth:`agg`. Aggregates composed with ``cast`` / arithmetic /
        scalar wrappers keep sticky ``_is_aggregate`` and still route here. Spark also allows
        **foldable** constants (``F.lit``, ``current_timestamp()``) beside aggregates — those
        use the SQL global-agg path. A **mixed** list (free attribute, nested free attr e.g.
        ``sum(x) + id``, or non-foldable non-agg companion e.g. ``row_number().over(...)``)
        without ``groupBy`` raises :class:`~repark.errors.AnalysisException` with Spark's
        ``[MISSING_GROUP_BY]`` tag. The projection must not silently group free attributes.
        """
        from repark.spark.functions import PandasUDFColumn, PythonUDFColumn

        expanded: list[Any] = []
        for item in cols:
            if isinstance(item, str) and item == "*":
                # Multi-name frames cannot re-resolve bare display strings (duplicate
                # "b" → AMBIGUOUS_REFERENCE). Expand via engine fields + display identity.
                if self._display_names is not None and self._engine_names is not None:
                    for display, engine in zip(
                        self._display_names, self._engine_names, strict=True
                    ):
                        expanded.append(self._bind_engine_display_column(display, engine))
                else:
                    expanded.extend(self.columns)
            else:
                expanded.append(item)
        # Scalar UDF markers rewrite before Column projection.
        has_pandas_udf = any(isinstance(item, PandasUDFColumn) for item in expanded)
        has_python_udf = any(isinstance(item, PythonUDFColumn) for item in expanded)
        if has_pandas_udf and has_python_udf:
            raise UnsupportedOperationException(
                "cannot mix classic udf and pandas_udf in one select; "
                "project them in separate steps"
            )
        if has_pandas_udf:
            for item in expanded:
                if isinstance(item, Column):
                    _reject_partition_transform(item)
            return self._select_with_pandas_udfs(expanded)
        if has_python_udf:
            for item in expanded:
                if isinstance(item, Column):
                    _reject_partition_transform(item)
            return self._select_with_python_udfs(expanded)
        for item in expanded:
            if isinstance(item, Column):
                _reject_partition_transform(item)
                # Validate window, random, and stratified-sampling markers.
                # Range markers are sticky pre-for_select Column attrs — validate on the
                # raw inputs here; for_select (deferred below) drops the sticky attrs.
                _reject_non_numeric_range_order(self, item)
        # Rebind origin Columns before projection.
        # for_select is deferred until multi-name disambiguation assigns unique aliases.
        projected = [self._rebind_origin_column(self._column_of(item)) for item in expanded]
        generators = [column for column in projected if getattr(column, "_generator", None)]
        if len(generators) > 1:
            raise AnalysisException(
                "Only one generator allowed per select list "
                "(Spark: only one explode/posexplode family generator)"
            )
        # DataFusion requires unique *engine* projection names; live PySpark allows duplicate
        # *display* names (join both sides / select(x, x.cast(...))). Origin-qualified
        # duplicates keep bare display names via the facade identity map. Non-origin duplicates
        # (cast / year / compound same display) use the same multi-name map with synthetic
        # engine aliases — DataFusion never sees colliding field names.
        projection_names = [
            (
                column._projection_name
                if column._projection_name is not None
                else column.spark_display_part()
            )
            for column in projected
        ]
        seen: dict[str, int] = {}
        duplicates: list[str] = []
        for name in projection_names:
            seen[name] = seen.get(name, 0) + 1
            if seen[name] == 2:
                duplicates.append(name)
        h1_multi_name = False
        h1_display_names: list[str] | None = None
        h1_engine_names: list[str] | None = None
        h1_origin_map: dict[tuple[str, str], str] | None = None
        if duplicates:
            # Join identity and multi-name select share display/engine maps.
            dup_set = set(duplicates)
            h1_multi_name = True
            h1_display_names = []
            h1_engine_names = []
            h1_origin_map = {}
            name_counts: dict[str, int] = {}
            rewritten: list[Column] = []
            for name, column in zip(projection_names, projected, strict=True):
                name_counts[name] = name_counts.get(name, 0) + 1
                if name in dup_set:
                    if column._origin_plan_id is not None and column._origin_field is not None:
                        engine = (
                            f"__repark_sel_{column._origin_plan_id}_"
                            f"{column._origin_field}_{name_counts[name]}"
                        )
                    else:
                        # Non-origin columns use a synthetic engine id.
                        engine = f"__repark_sel_h2_{len(h1_engine_names)}_{name_counts[name]}"
                    rewritten.append(
                        Column(
                            column._inner.alias(engine),
                            spark_display=name,
                            projection_name=engine,
                            stable_name=True,
                            has_free_attribute=column._has_free_attribute,
                            is_aggregate=column._is_aggregate,
                            is_foldable=column._is_foldable,
                            has_ungroupable=column._has_ungroupable,
                            is_aggregate_function=column._is_aggregate_function,
                            origin_plan_id=column._origin_plan_id,
                            origin_field=column._origin_field,
                            # Keep composed join_sql (fillna coalesce / cast) so the
                            # QCOL SQL select path does not fall back to a bare leaf
                            # token without stripping the operation.
                            join_sql_expr=column._join_sql_expr,
                            sql_expr=column._sql_expr,
                        )
                    )
                    h1_display_names.append(name)
                    h1_engine_names.append(engine)
                    if column._origin_plan_id is not None and column._origin_field is not None:
                        h1_origin_map[(column._origin_plan_id, column._origin_field)] = engine
                else:
                    # Identity alias squash.
                    rewritten.append(_collapse_identity_projection_alias(column))
                    engine_name = (
                        column._projection_name
                        if column._projection_name is not None
                        else column.spark_display_part()
                    )
                    h1_display_names.append(name)
                    h1_engine_names.append(engine_name)
                    if column._origin_plan_id is not None and column._origin_field is not None:
                        h1_origin_map[(column._origin_plan_id, column._origin_field)] = engine_name
            projected = rewritten
            if not h1_origin_map:
                h1_origin_map = None
        else:
            # Identity alias squash.
            projected = [_collapse_identity_projection_alias(column) for column in projected]
        # All-aggregate or aggregate-plus-foldable select lists use the global aggregate path.
        # Classify projections from Column metadata, not expression text.
        # Mixed aggregate and generator projections raise MISSING_GROUP_BY because they cannot
        # share one grouping stage.
        aggregate_flags = [bool(column._is_aggregate) for column in projected]
        if aggregate_flags and any(aggregate_flags):
            # A generator and aggregate cannot share one projection grouping stage.
            if generators:
                raise AnalysisException(
                    "[MISSING_GROUP_BY] The query does not include a GROUP BY clause. "
                    "Add GROUP BY or turn it into the window functions using OVER clauses."
                )
            # Pure global: every projection is aggregate and/or foldable, with no free
            # attributes and no sticky ungroupable. ``all(not free)`` alone was incomplete —
            # ``row_number().over(...)`` is neither free nor foldable nor aggregate and must
            # raise. Nested ``sum+over`` / ``coalesce(sum,window)`` need
            # ``_has_ungroupable``; ``F.rand`` is non-foldable.
            pure_global = all(
                (bool(column._is_aggregate) or bool(column._is_foldable))
                and not bool(column._has_free_attribute)
                and not bool(column._has_ungroupable)
                for column in projected
            )
            if pure_global:
                # Pure bare aggregates use the native aggregate
                # path for name/type fidelity with ``df.agg``. Composed post-agg ops
                # (``sum(x)+1``, ``cast``, ``abs(sum)``) and non-agg companions need SQL —
                # DataFusion's ``DataFrame.aggregate`` rejects non-AggregateFunction exprs
                # and bare literals.
                if all(aggregate_flags) and all(
                    _is_native_pure_global_aggregate(column) for column in projected
                ):
                    child = self.group_by().agg(*projected)
                else:
                    child = self._select_global_aggregate_sql(projected)
                # Multi-name rewrite assigns unique engines before
                # this early return — attach the display/engine overlay so ``sum,sum``
                # surfaces Spark-legal ``sum(v)`` x2 (not ``__repark_sel_h2_*`` leaks).
                if h1_multi_name and h1_display_names is not None and h1_engine_names is not None:
                    child._display_names = list(h1_display_names)
                    child._engine_names = list(h1_engine_names)
                    child._origin_map = dict(h1_origin_map) if h1_origin_map is not None else None
                return child
            # Mixed aggregate and free companion without GROUP BY — Spark
            # ``[MISSING_GROUP_BY]`` (live PySpark 4.1.2).
            raise AnalysisException(
                "[MISSING_GROUP_BY] The query does not include a GROUP BY clause. "
                "Add GROUP BY or turn it into the window functions using OVER clauses."
            )
        if len(generators) == 1:
            # Duplicate display names cannot pass through the generator SQL rewrite because
            # engine aliases would become ambiguous. Keep this refusal explicit.
            if h1_multi_name:
                raise AnalysisException(
                    "select would produce duplicate column names alongside a generator "
                    "(explode/posexplode); duplicate display names are not supported on the "
                    "generator rewrite path. Use .alias(...) to make names unique."
                )
            return self._select_with_generator(projected, generators[0])
        # Compounds that still carry QCOL tokens (cast / arithmetic of parent Columns)
        # cannot use unrebound native exprs on multi-name frames — SQL-project via rewrite.
        if any("__REPARK_QCOL_" in column.join_sql_part() for column in projected):
            sql_child = self._select_via_qcol_sql(
                projected,
                h1_display_names=h1_display_names if h1_multi_name else None,
                h1_engine_names=h1_engine_names if h1_multi_name else None,
                h1_origin_map=h1_origin_map if h1_multi_name else None,
            )
            if sql_child is not None:
                return sql_child
        natives = [column._inner for column in projected]
        child = self._spawn(self._plan().select(natives))
        if h1_multi_name and h1_display_names is not None:
            child._display_names = h1_display_names
            child._engine_names = h1_engine_names
            child._origin_map = h1_origin_map
        return child

    def _select_global_aggregate_sql(self, projected: list[Column]) -> DataFrame:
        """Global-agg ``select`` via SQL for composed aggregates and non-agg companions.

        Used when sticky ``_is_aggregate`` / free-attribute metadata classify the list as
        Spark global aggregate but the native ``aggregate`` API cannot accept the
        expressions (``sum(x)+1``, ``CAST(sum(x) AS DOUBLE)``, ``sum(x), lit(1)``,
        ``sum(x), current_timestamp()``). Pure AggregateFunction columns are rebound via
        :meth:`GroupedData._rebind_simple_name_aggregate` so case-preserved schema binds
        match the native ``groupBy().agg`` path.

        Materialize one plan-stable :meth:`_plan` snapshot for ``mapInArrow``. Action-path
        registration would prepare a non-idempotent UDF twice and could change results.
        Structural checks alone do not cover that value drift. Build the rebind host without
        a second preparation.
        """
        from repark.spark._idents import quote_ident as _quote_ident

        self._ensure_alive()
        # One plan-stable snapshot for uncached mapInArrow (and no-op for ordinary frames).
        plan = self._plan()
        view = scratch_view_name(self._session, "__repark_select_agg_")
        # Register the prepared plan — never the empty MIA placeholder (raw ``_inner``
        # before prepare) and never a second action re-run via DF createOrReplaceTempView.
        self._session.create_or_replace_temp_view(view, plan)
        try:
            # Empty group-by only for the shared rebind helper (schema bind). Already
            # prepared above — do not call ``self.group_by()`` (second ``_prepare_for_plan``).
            rebind_host = GroupedData(self, [])
            parts: list[str] = []
            for column in projected:
                # Case-preserving rebind for bare AF builders (``F.sum("X")`` + lit),
                # including post-``.alias`` pure AFs that clear ``_agg_name`` but keep
                # structural ``sql_expr``.
                if column._is_aggregate_function and (
                    column._agg_name is not None or column._sql_expr is not None
                ):
                    column = rebind_host._rebind_simple_name_aggregate(column)
                expression_sql, output_name = _global_agg_sql_parts(column)
                parts.append(f"{expression_sql} AS {_quote_ident(output_name)}")
            sql = f"SELECT {', '.join(parts)} FROM {view}"
            return self._spawn(self._session.sql(sql))
        finally:
            self._session.drop_temp_view(view)

    def _select_with_generator(
        self,
        projected: list[Column],
        generator: Column,
    ) -> DataFrame:
        """Lower explode and explode_outer through guarded SQL unnest.

        The rewrite has two phases:

        1. **Native** project siblings + the array under a private temp name so compounds
        (``order + 0``, mixed-case idents), scalar helpers (``size`` → engine
        ``cardinality``), and ColumnOrName tokens never re-embed Spark pretty names or
        free SQL text into the unnest statement.
        2. SQL ``unnest`` / WHERE / outer CASE against that intermediate view using only
        double-quoted identifiers.

        Empty-array guards use top-level ``array_length`` (not multi-dim ``cardinality``):
        nested ``[[]]`` / ``[[],[1]]`` have product cardinality 0 and would be falsely
        treated as empty (silent drop / null rewrite). DF empty-array
        ``array_length`` is 0; null stays NULL (``coalesce(..., 0)``).
        """
        kind = generator._generator
        if kind not in {"explode", "explode_outer", "explode_keep_null"}:
            raise UnsupportedOperationException(
                f"generator {kind!r} is not supported on the guarded-unnest path"
            )
        self._ensure_alive()
        out_name = generator._projection_name or "col"
        # Private array field — uuid so it cannot collide with user projection names.
        array_temp = f"__repark_arr_{uuid.uuid4().hex}"

        mid_natives: list[Any] = []
        for column in projected:
            if column is generator or getattr(column, "_generator", None):
                # Array expression only (cast after unnest via _generator_cast).
                mid_natives.append(_bound_generator_array(self, generator).alias(array_temp))
            else:
                # for_select already applied Spark projection names on the native expr.
                mid_natives.append(column._inner)
        # Project from ``_plan()`` (not raw ``_inner``) so uncached ``mapInArrow`` parents
        # materialize the bridge before unnest — raw ``_inner`` is the empty schema
        # placeholder and would silently yield zero rows. Ordinary select/filter also use
        # ``_plan()``.
        mid = self._spawn(self._plan().select(mid_natives))

        # The second SQL projection refers only to quoted identifiers from the intermediate schema.
        array_sql = _quote_ident_sql(array_temp)
        # Top-level length only (not multi-dim cardinality product) —.
        length_expr = f"coalesce(array_length({array_sql}), 0)"
        if kind == "explode":
            # Drop null/empty arrays (Spark explode). Element type is not needed —
            # do not call outer-type resolution (struct arrays are legal;).
            where = f"({array_sql}) IS NOT NULL AND {length_expr} > 0"
            unnest_expr = f"unnest({array_sql})"
        else:
            # explode_outer / explode_keep_null: CASE + NULL element.
            # Type is taken from the intermediate field (covers coalesce/compounds —
            # ); never fail-open to BIGINT. Void / Null elements have
            # Keep make_array(NULL) untyped so the engine infers its element type.
            element_sql_type = mid._array_element_sql_type(array_sql, generator)
            if element_sql_type == _UNTYPED_NULL_ELEMENT:
                null_array_sql = "make_array(NULL)"
            else:
                null_array_sql = f"make_array(CAST(NULL AS {element_sql_type}))"
            if kind == "explode_keep_null":
                # NULL list → one null-element row; EMPTY list stays empty and drops.
                guarded = (
                    f"CASE WHEN ({array_sql}) IS NULL THEN {null_array_sql} ELSE ({array_sql}) END"
                )
                where = f"({array_sql}) IS NULL OR {length_expr} > 0"
            else:
                # explode_outer: null/empty → single-element array of NULL of element type.
                guarded = (
                    f"CASE WHEN ({array_sql}) IS NULL OR {length_expr} = 0 "
                    f"THEN {null_array_sql} "
                    f"ELSE ({array_sql}) END"
                )
                where = None
            unnest_expr = f"unnest({guarded})"
        # Element cast after unnest (explode(...).cast(...)) — sticky via _generator_cast.
        # Re-validate each Spark token before SQL embed (defense-in-depth; Column.cast already
        # allowlists — /). A tuple is a cast *chain* (innermost first)
        # from chained ``.cast().cast()`` — apply nested CAST wrappers.
        element_cast = getattr(generator, "_generator_cast", None)
        if element_cast is not None:
            from repark.spark.column import _require_allowlisted_spark_cast_token

            if isinstance(element_cast, str):
                cast_tokens: tuple[str, ...] = (element_cast,)
            else:
                cast_tokens = tuple(element_cast)
            for cast_token in cast_tokens:
                safe_cast = _require_allowlisted_spark_cast_token(cast_token)
                unnest_expr = f"CAST({unnest_expr} AS {safe_cast})"

        select_parts: list[str] = []
        for column in projected:
            if column is generator or getattr(column, "_generator", None):
                select_parts.append(f"{unnest_expr} AS {_quote_ident_sql(out_name)}")
            else:
                name = column._projection_name or column.spark_display_part()
                quoted = _quote_ident_sql(name)
                select_parts.append(f"{quoted} AS {quoted}")

        view = scratch_view_name(mid._session, "__repark_expl_")
        mid._session.create_or_replace_temp_view(view, mid._inner)
        try:
            sql = f"SELECT {', '.join(select_parts)} FROM {view}"
            if where is not None:
                sql = f"{sql} WHERE {where}"
            return self._spawn(mid._session.sql(sql))
        finally:
            mid._session.drop_temp_view(view)

    def _array_element_sql_type(self, array_sql: str, generator: Column) -> str:
        """SQL type for NULL element inside explode_outer guard array.

        Bind by **field name only** (exact or casefold unique) — never substring
        ``name in display``, which lets a short sibling list name (e.g. ``a`` inside
        ``data``, or a column literally named ``explode``) steal the CASE element type
        . Case folding covers ColumnOrName and mixed
        spelling after quoting.

        Unresolved / unmapped types raise — never fail-open to ``BIGINT`` (corrupts
        VARCHAR/TIMESTAMP null guards under CASE unification; /).
        """
        _ = generator  # bind uses array_sql only (no display substring match)
        try:
            fields = self._inner.logical_schema_fields()
        except Exception:
            fields = []
        bare = _sql_ident_bare_name(array_sql.strip())
        if bare is None:
            raise AnalysisException(
                "explode_outer cannot resolve SQL element type for a non-identifier "
                "array expression; project the array to a named column first"
            )
        matches = [
            (name, type_key)
            for name, type_key, _nullable in fields
            if name == bare or name.casefold() == bare.casefold()
        ]
        if not matches:
            raise AnalysisException(
                f"explode_outer cannot resolve array column {bare!r} in the frame schema"
            )
        # Prefer exact spelling; otherwise require a unique casefold hit.
        exact = [(name, type_key) for name, type_key in matches if name == bare]
        chosen = exact[0] if exact else (matches[0] if len(matches) == 1 else None)
        if chosen is None:
            raise AnalysisException(
                f"explode_outer array column {bare!r} is ambiguous among case-insensitive "
                f"schema matches: {[name for name, _type in matches]}"
            )
        _name, type_key = chosen
        parsed = _parse_list_element_sql_type(type_key)
        if parsed is not None:
            return parsed
        # Field bound but element type unsupported (map / nested-void / …).
        raise AnalysisException(
            f"explode_outer cannot resolve SQL element type for array column {bare!r} "
            f"(engine type {type_key!r}); cast the array or use a supported element type"
        )

    # Smart CSV diagnostics.
    def describe_ingest(self) -> dict[str, Any]:
        """Return smartCsv ingest diagnostics (repark extension; empty dict if not smart-loaded).

        Surfaces skipped preamble lines, header row index, delimiter, BOM strip, ragged-row
        padding counts, and per-column resolved protocol type + fallback counts. Silent magic
        is a defect — every heuristic decision is listed here.
        """
        if self._ingest_report is None:
            return {}
        return dict(self._ingest_report)

    @property
    def columns(self) -> list[str]:
        """Column names in order (PySpark ``DataFrame.columns``) — metadata only, no execution.

        After a condition join retains Spark-legal duplicate display names, this
        returns the facade display list (bare names), not the unique engine field names.
        """
        self._ensure_alive()
        if self._display_names is not None:
            return list(self._display_names)
        if self._map_bridge is not None:
            return list(self._map_bridge["schema"].names)
        from repark import _native

        return _native.logical_column_names(self._inner)

    def _display_overlay_names(self) -> list[str] | None:
        """Return display names when they differ from engine field names, else None."""
        if self._display_names is None or self._engine_names is None:
            return None
        if self._display_names == self._engine_names:
            return None
        return list(self._display_names)

    def __getattr__(self, name: str) -> Column:
        """Attribute access to a column (PySpark ``DataFrame.__getattr__`` → ``df.x``).

        Only reached when normal attribute lookup fails, so methods and properties
        (``count``, ``columns``, ``schema``, …) always win over a same-named column
        (live PySpark 4.1.2). Missing names raise
        :class:`~repark.errors.PySparkAttributeError` with Spark's ``[ATTRIBUTE_NOT_SUPPORTED]``
        message shape with PySpark's matching exception class.
        Column resolution is **case-sensitive** on this surface (``df.X`` fails when the
        column is ``x``) — unlike :meth:`__getitem__`, which follows the Spark analyzer's
        default case-insensitive name resolution. Underscore names work when present
        (``df._x``). Existing type dunders (``__class__``, ``__repr__``,...) resolve on the
        type and never hit this method; a missing dunder still falls through here and raises
        ``ATTRIBUTE_NOT_SUPPORTED`` (membership-only, same as live PySpark classic).
        """
        # Half-built instances (copy/pickle protocols create the object before filling
        # __dict__) must not recurse: `_ensure_alive` reads `self._inner`, which re-enters
        # Bail to a plain AttributeError for half-built instances.
        try:
            object.__getattribute__(self, "_inner")
        except AttributeError:
            # A bare AttributeError handles copy, pickle, and hasattr probes before initialization.
            # not user misuse of a DataFrame attribute — PySpark's PySparkAttributeError models
            # User attribute misses use the classified error below.
            raise AttributeError(name) from None
        self._ensure_alive()
        # Permanent out-of-scope surfaces use named errors.
        _oos = {
            "rdd": "RDD is out of scope for repark (use DataFrame API / Arrow collect)",
            "writeStream": "Structured Streaming is out of scope (batch DataFrame writes only)",
            "withWatermark": "watermarks require streaming (out of scope for repark v1)",
            "foreach": "foreach is out of scope until the UDF campaign (use collect + Python)",
            "foreachPartition": (
                "foreachPartition is out of scope until the UDF campaign (use to_arrow / to_polars)"
            ),
        }
        if name in _oos:
            raise UnsupportedOperationException(f"DataFrame.{name} is not supported: {_oos[name]}")
        if name not in self.columns:
            raise PySparkAttributeError(
                f"[ATTRIBUTE_NOT_SUPPORTED] Attribute `{name}` is not supported."
            )
        # Exact membership only (case-sensitive, like PySpark attr). Quoted bind so
        # non-lowercase schema fields remain re-selectable.
        return self._bind_schema_column(name)

    def _resolve_getitem_column_name(self, item: str) -> str:
        """Resolve a getitem str key to a canonical schema column name.

        Prefer exact membership first (``df["x"]`` with column ``x``). On exact miss, fall
        back to case-insensitive match against :attr:`columns` — matching Spark classic
        ``Dataset.apply`` under ``spark.sql.caseSensitive=false`` (the default). Exactly one
        case-insensitive hit returns that canonical name for ``col(...)``; zero hits raise
        :class:`~repark.errors.AnalysisException`; multiple hits raise for ambiguity.

        Exact duplicate display names (post-join Spark multi-name output) raise
        ``[AMBIGUOUS_REFERENCE]`` — the 4.1.2 class for ``joined["x"]`` when both sides
        contributed ``x``.
        """
        names = self.columns
        exact_hits = [name for name in names if name == item]
        if len(exact_hits) > 1:
            could_be = ", ".join(f"`{name}`" for name in exact_hits)
            raise AnalysisException(
                f"[AMBIGUOUS_REFERENCE] Reference `{item}` is ambiguous, could be: [{could_be}]."
            )
        if item in names:
            return item
        item_folded = item.casefold()
        matches = [name for name in names if name.casefold() == item_folded]
        # De-dupe preserving order for case-insensitive multi-hit reporting.
        unique_matches: list[str] = []
        for match in matches:
            if match not in unique_matches:
                unique_matches.append(match)
        if len(matches) > 1 and len(unique_matches) == 1:
            # Same display repeated (join dup) already handled above; casefold multi.
            could_be = ", ".join(f"`{name}`" for name in matches)
            raise AnalysisException(
                f"[AMBIGUOUS_REFERENCE] Reference `{item}` is ambiguous, could be: [{could_be}]."
            )
        if len(unique_matches) == 1:
            return unique_matches[0]
        if len(unique_matches) == 0:
            raise AnalysisException(
                f"A column with name `{item}` cannot be resolved; available columns: {names}"
            )
        raise AnalysisException(
            f"A column with name `{item}` is ambiguous among case-insensitive matches: "
            f"{unique_matches}; available columns: {names}"
        )

    def _engine_field_for_display(self, display: str) -> str:
        """Map a unique display name to the engine field name on this frame."""
        if self._display_names is None or self._engine_names is None:
            return display
        matches = [
            engine
            for name, engine in zip(self._display_names, self._engine_names, strict=True)
            if name == display
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            could_be = ", ".join(f"`{display}`" for _ in matches)
            raise AnalysisException(
                f"[AMBIGUOUS_REFERENCE] Reference `{display}` is ambiguous, could be: [{could_be}]."
            )
        return display

    def _select_via_qcol_sql(
        self,
        projected: list[Column],
        *,
        h1_display_names: list[str] | None,
        h1_engine_names: list[str] | None,
        h1_origin_map: dict[tuple[str, str], str] | None,
    ) -> DataFrame | None:
        """Project Columns whose ``join_sql_part`` still has QCOL tokens.

        Registers this frame as a temp view, rewrites tokens to quoted engine fields, runs
        ``SELECT … FROM view``, drops the view. Returns ``None`` if any token cannot be
        resolved (caller falls through / fails engine-side).
        """
        from repark.spark._idents import quote_ident as _quote_ident

        # Token resolution requires a post-join origin map.
        if self._origin_map is None:
            return None

        proj_parts: list[str] = []
        display_names: list[str] = []
        engine_names: list[str] = []
        origin_map: dict[tuple[str, str], str] = {}
        name_counts: dict[str, int] = {}
        for column in projected:
            expr_sql = column.join_sql_part()
            if "__REPARK_QCOL_" in expr_sql:
                expr_sql = _rewrite_qcol_tokens_local(expr_sql, self)
                if "__REPARK_QCOL_" in expr_sql:
                    return None
            display = (
                column._projection_name
                if column._projection_name is not None
                else column.spark_display_part()
            )
            name_counts[display] = name_counts.get(display, 0) + 1
            # Prefer multi-name engine aliases when the outer select already assigned them.
            if h1_engine_names is not None and len(engine_names) < len(h1_engine_names):
                engine = h1_engine_names[len(engine_names)]
                display = (
                    h1_display_names[len(engine_names)] if h1_display_names is not None else display
                )
            elif name_counts[display] > 1 or display in {
                name for name, count in name_counts.items() if count > 1
            }:
                engine = f"__repark_sel_q_{len(engine_names)}_{display}"
            else:
                # Unique display — use as engine name when safe; CAST display needs alias.
                if display.startswith("CAST(") or any(
                    ch in display for ch in (" ", "(", ")", "+", "-", "*", "/")
                ):
                    engine = f"__repark_sel_q_{len(engine_names)}"
                else:
                    engine = display
            proj_parts.append(f"({expr_sql}) AS {_quote_ident(engine)}")
            display_names.append(display)
            engine_names.append(engine)
            if column._origin_plan_id is not None and column._origin_field is not None:
                origin_map[(column._origin_plan_id, column._origin_field)] = engine

        view = scratch_view_name(self._session, "_repark_h1_sel_")
        self._session.create_or_replace_temp_view(view, self._plan())
        try:
            planned = self._session.sql(f"SELECT {', '.join(proj_parts)} FROM {view}")
            child = self._spawn(planned)
            if h1_display_names is not None:
                child._display_names = h1_display_names
                child._engine_names = h1_engine_names
                child._origin_map = h1_origin_map
            else:
                pairs = zip(display_names, engine_names, strict=True)
                needs_identity = len(display_names) != len(set(display_names)) or any(
                    display != engine for display, engine in pairs
                )
                if needs_identity:
                    child._display_names = display_names
                    child._engine_names = engine_names
                    child._origin_map = origin_map or None
            return child
        finally:
            self._session.drop_temp_view(view)

    def _bind_engine_display_column(self, display: str, engine: str) -> Column:
        """Bind a multi-name display and engine pair without ambiguous lookup.

        Used by ``select("*")`` expansion and other positional projections on frames that
        carry Spark-legal duplicate display names.
        """
        from repark import _native
        from repark.spark._idents import quote_ident as _quote_ident

        quoted = _quote_ident(engine)
        native = _native.PyColumn.column(quoted)
        origin_plan_id = self._plan_id
        origin_field = display
        if self._origin_map is not None:
            for (plan_id, field), mapped in self._origin_map.items():
                if mapped == engine:
                    origin_plan_id = plan_id
                    origin_field = field
                    break
        return Column(
            native.alias(display),
            spark_display=display,
            projection_name=display,
            stable_name=True,
            has_free_attribute=True,
            sql_expr=quoted,
            origin_plan_id=origin_plan_id,
            origin_field=origin_field,
        )

    def _iter_bound_columns(self) -> list[Column]:
        """Bind every column by position, preserving duplicate display names.

        Multi-name frames use engine/display pairs; ordinary frames bind by name.
        """
        if self._display_names is not None and self._engine_names is not None:
            return [
                self._bind_engine_display_column(display, engine)
                for display, engine in zip(self._display_names, self._engine_names, strict=True)
            ]
        names = self.columns
        if len(set(names)) != len(names):
            return [self._bind_schema_column(name) for name in names]
        return [self._bind_schema_column(name, name) for name in names]

    def _origin_plan_ids(self) -> frozenset[str]:
        """Plan ids this frame can still attribute (own id + nested origin-map keys)."""
        ids = {self._plan_id}
        if self._origin_map is not None:
            ids.update(plan_id for plan_id, _field in self._origin_map)
        return frozenset(ids)

    def _remember_unemitted_right_origins(
        self, left: DataFrame, right: DataFrame, *, left_only: bool = True
    ) -> None:
        """Record (semi/anti) or forget (emitting join) exclusive right plan ids.

        ``left_only=True`` unions exclusive right ids into :attr:`_origin_not_emitted`.
        ``left_only=False`` removes them after an emitting join.
        """
        exclusive = right._origin_plan_ids() - left._origin_plan_ids()
        if exclusive:
            self._origin_not_emitted = (
                self._origin_not_emitted | exclusive
                if left_only
                else self._origin_not_emitted - exclusive
            )

    def _raise_if_origin_not_emitted(self, plan_id: str | None, field: str | None) -> None:
        """Raise Spark 4.1.2 ``MISSING_ATTRIBUTES`` when ``plan_id`` was not emitted."""
        if plan_id is None or plan_id not in self._origin_not_emitted:
            return
        name = field if field is not None else "<unknown>"
        available = ", ".join(f'"{column}"' for column in self.columns)
        quoted = f'"{name}"'
        if name in self.columns:
            raise AnalysisException(
                f"[MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION] "
                f"Resolved attribute(s) {quoted} missing from {available} in operator "
                f"!Project. Attribute(s) with the same name appear in the operation: "
                f"{quoted}. Please check if the right attribute(s) are used."
            )
        raise AnalysisException(
            f"[MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT] "
            f"Resolved attribute(s) {quoted} missing from {available} in operator !Project."
        )

    def _raise_unemitted_qcol_tokens(self, join_sql: str) -> None:
        """Refuse QCOL tokens whose plan id is in :attr:`_origin_not_emitted`."""
        if not self._origin_not_emitted or "__REPARK_QCOL_" not in join_sql:
            return
        for match in _QCOL_TOKEN_RE.finditer(join_sql):
            self._raise_if_origin_not_emitted(match.group(1), _decode_qcol_field(match.group(2)))

    def _rebind_origin_column(self, column: Column) -> Column:
        """Rebind a parent-origin Column to this frame's engine field.

        Preserve compound expressions. A right-side origin excluded by semi or anti raises
        ``MISSING_ATTRIBUTES`` instead of falling back to the left side.
        """
        self._raise_if_origin_not_emitted(column._origin_plan_id, column._origin_field)
        join_sql = column._join_sql_expr
        if join_sql is not None:
            self._raise_unemitted_qcol_tokens(join_sql)
        if (
            column._origin_plan_id is None
            or column._origin_field is None
            or self._origin_map is None
        ):
            return column
        # Only pure leaf refs: join_sql is absent, a bare QCOL token, or a quoted ident.
        # ``coalesce(...)`` / ``CAST(...)`` / binary ops keep native + origin for select.
        if join_sql is not None:
            stripped = join_sql.strip()
            pure_qcol = stripped.startswith("__REPARK_QCOL_") and stripped.endswith("__")
            pure_quoted = (
                stripped.startswith('"')
                and stripped.endswith('"')
                and "(" not in stripped
                and " " not in stripped
            )
            if not pure_qcol and not pure_quoted:
                return column
        key = (column._origin_plan_id, column._origin_field)
        engine = self._origin_map.get(key)
        if engine is None:
            return column
        from repark import _native
        from repark.spark._idents import quote_ident as _quote_ident

        quoted = _quote_ident(engine)
        native = _native.PyColumn.column(quoted)
        display = column._projection_name or column._origin_field
        return Column(
            native.alias(display),
            spark_display=display,
            projection_name=display,
            stable_name=True,
            has_free_attribute=True,
            sql_expr=quoted,
            origin_plan_id=column._origin_plan_id,
            origin_field=column._origin_field,
            # Keep join rewrite tokens so further composition (filter compounds built
            # *before* rebind) is not required; pure rebound is already engine-local.
            join_sql_expr=quoted,
            # Preserve sort markers through origin rebind (orderBy(parent.col.desc())).
            sort_ascending=column._sort_ascending,
            sort_nulls_first=column._sort_nulls_first,
        )

    def _bind_schema_column(self, name: str, canonical: str | None = None) -> Column:
        """Bind a name case-insensitively and quote its canonical engine identifier.

        Preserve the requested display spelling and attach origin metadata for joins.
        """
        from repark import _native
        from repark.spark._idents import quote_ident as _quote_ident

        canonical = self._resolve_getitem_column_name(name) if canonical is None else canonical
        engine_field = self._engine_field_for_display(canonical)
        quoted = _quote_ident(engine_field)
        native = _native.PyColumn.column(quoted)
        return Column(
            native.alias(name),
            spark_display=name,
            projection_name=name,
            stable_name=True,
            has_free_attribute=True,
            # Quote the *engine* schema field for free-SQL embeds.
            # Join ON rewrite uses origin_plan_id and origin_field, not this fragment.
            sql_expr=quoted,
            origin_plan_id=self._plan_id,
            origin_field=canonical,
        )

    def _quote_filter_sql_identifiers(self, sql: str) -> str:
        """Quote schema-bound identifiers in a SQL filter predicate.

        DataFusion lowercases unquoted identifiers. This rewrite quotes case-insensitive
        schema matches so case-preserved fields remain resolvable. It ignores single-quoted
        literals and double-quoted spans.

        Backtick-quoted identifiers are not protected. The rewrite can quote their contents
        again and make a valid Spark predicate fail in DataFusion.

        A case-fold collision fails only when the predicate names the ambiguous field. The
        error lists the conflicting field names and omits Spark's SQLSTATE suffix.
        """
        columns = self.columns
        if not columns:
            return sql
        columns_by_fold: dict[str, list[str]] = {}
        for column in columns:
            columns_by_fold.setdefault(column.casefold(), []).append(column)
        # Do not rewrite function names or SQL boolean and null literals.
        ident_pattern = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\()")

        # Protect single-quoted SQL string literals, then double-quoted idents inside the rest.
        pieces = re.split(r"('(?:[^']|'')*')", sql)
        rebuilt: list[str] = []
        for piece in pieces:
            if piece.startswith("'"):
                rebuilt.append(piece)
                continue
            subpieces = re.split(r'("(?:[^"]|"")*")', piece)
            for subpiece in subpieces:
                if subpiece.startswith('"'):
                    rebuilt.append(subpiece)
                else:
                    rebuilt.append(
                        _quote_filter_idents_in_fragment(
                            subpiece,
                            ident_pattern=ident_pattern,
                            columns_by_fold=columns_by_fold,
                        )
                    )
        return "".join(rebuilt)

    def _rebind_stable_name_column(self, column: Column) -> Column:
        """Rebind a bare NamedExpression against this frame (covers ``F.col`` at select).

        Only pure name refs (``spark_display == projection_name`` and ``stable_name``) are
        rebound — casts (``CAST(...)`` display), true user aliases (``x AS z``), and
        compounds keep their existing plan. Missing names fall through to the engine.
        Sort markers (``asc``/``desc``) from the original column are preserved so
        ``orderBy(df.x.desc())`` still sorts after schema binding.

        Origin-qualified Columns (parent ``df1["x"]`` / ``select("*")`` engine binds)
        must not re-resolve by bare display name — multi-name frames raise
        ``AMBIGUOUS_REFERENCE`` on that path.
        """
        if not column._stable_name:
            return column
        # Origin pins a specific side/engine field — skip bare-name rebind.
        if column._origin_plan_id is not None and column._origin_field is not None:
            return column
        name = column._projection_name
        if name is None or name == "" or name == "*":
            return column
        if column._spark_display != name:
            return column
        try:
            bound = self._bind_schema_column(name)
        except AnalysisException:
            return column
        if column._sort_ascending is None and column._sort_nulls_first is None:
            return bound
        # Sort markers force a new Column: preserve sticky bits like ``Column.asc`` /
        # ``desc`` (sql_expr / generator / is_aggregate_function). Prefer bound's
        # schema-quoted ``sql_expr`` so cube/rollup free-SQL SELECT keeps reserved
        # Names such as ``order`` are quoted.
        return Column(
            bound._inner,
            sort_ascending=column._sort_ascending,
            sort_nulls_first=column._sort_nulls_first,
            spark_display=bound._spark_display,
            projection_name=bound._projection_name,
            stable_name=bound._stable_name,
            agg_name=column._agg_name,
            is_aggregate=column._is_aggregate,
            is_foldable=column._is_foldable,
            has_free_attribute=bound._has_free_attribute or column._has_free_attribute,
            has_ungroupable=bound._has_ungroupable or column._has_ungroupable,
            is_aggregate_function=column._is_aggregate_function,
            partition_transform=column._partition_transform,
            sql_expr=bound._sql_expr if bound._sql_expr is not None else column._sql_expr,
            generator=column._generator,
            generator_cast=column._generator_cast,
            when_pairs=column._when_pairs,
            # Keep origin and join_sql through sort-marker rebind.
            origin_plan_id=bound._origin_plan_id or column._origin_plan_id,
            origin_field=bound._origin_field or column._origin_field,
            join_sql_expr=bound._join_sql_expr or column._join_sql_expr,
        )

    def __getitem__(
        self,
        item: str | int | Column | list[Any] | tuple[Any, ...],
    ) -> Column | DataFrame:
        """Item access (PySpark ``DataFrame.__getitem__``).

               Live PySpark 4.1.2 forms:

               * ``str`` → a :class:`Column` for that name. Name resolution prefers an exact
               schema match, then a single case-insensitive match (Spark analyzer default
               ``spark.sql.caseSensitive=false`` — so ``df["X"]`` succeeds when the column is
               ``x``). The returned column is a NamedExpression with the **requested** spelling
               (``spark_display`` / projection ``X``, same as ``F.col("X")``) — not an
               ``Alias(canonical AS requested)`` — so compounds match live Spark
               (``df.select(df["X"] + 1).columns == ["(X + 1)"]``). Native bind is quoted so
               mixed-case fields remain resolvable on later hops. An eager miss raises
               :class:`~repark.errors.AnalysisException`; multiple case-insensitive matches raise
               ambiguity.
               :meth:`__getattr__` remains case-sensitive like PySpark ``df.X``.
        * ``int`` → column by position; out of range raises :class:`IndexError`.
               * :class:`Column` → :meth:`filter` (``df[df.x > 1]``)
               * ``list`` / ``tuple`` → :meth:`select` of the items (``df[["x", "y"]]``)
        """
        self._ensure_alive()
        if isinstance(item, str):
            # Star projection token used by count(df["*"]) and select(df["*"]).
            if item == "*":
                from repark.spark.functions import col as col_fn

                return col_fn("*")
            # Live PySpark 4.1.2: CI getitem is a NamedExpression with the *requested*
            # spelling (same display identity as F.col("X")), not Alias(canonical AS item)
            # text pollution. Quoted schema bind also keeps the field
            # re-selectable after a non-lowercase projection.
            return self._bind_schema_column(item)
        if isinstance(item, Column):
            return self.filter(item)
        if isinstance(item, (list, tuple)):
            return self.select(*item)
        if isinstance(item, int):
            return self._bind_schema_column(self.columns[item])
        raise PySparkTypeError(
            f"DataFrame indices must be str, int, Column, list, or tuple, not {type(item).__name__}"
        )

    def _analyzed_arrow_schema(self) -> Any:
        """Post-analysis physical Arrow schema — **analysis only**, no plan execution or row pull.

        Wraps native ``PyDataFrame.analyzed_arrow_schema`` (Arrow C schema capsule). Prefer this
        over ``limit(0).to_arrow().schema`` for plan-time type inspection (pandas_udf
        pass-through;). Map-bridge frames return the declared bridge Arrow schema.
        """
        import pyarrow as pa

        self._ensure_alive()
        if self._map_bridge is not None:
            return self._map_bridge["arrow_schema"]
        capsule = self._inner.analyzed_arrow_schema()
        return pa.Schema._import_from_c_capsule(capsule)

    @property
    def schema(self) -> StructType:
        """Return the analyzed logical schema without executing the plan."""
        self._ensure_alive()
        if self._map_bridge is not None:
            return self._map_bridge["schema"]
        from repark.spark.types import (
            BinaryType,
            BooleanType,
            ByteType,
            DateType,
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            NullType,
            ShortType,
            StringType,
        )
        from repark.spark.types import (
            DataType as ReparkDataType,
        )

        fields: list[StructField] = []
        for name, type_key, nullable in self._inner.logical_schema_fields():
            data_type: DataType
            if type_key == "int":
                data_type = IntegerType()
            elif type_key == "long":
                data_type = LongType()
            elif type_key == "short":
                data_type = ShortType()
            elif type_key == "byte":
                data_type = ByteType()
            elif type_key == "double":
                data_type = DoubleType()
            elif type_key == "float":
                data_type = FloatType()
            elif type_key == "boolean":
                data_type = BooleanType()
            elif type_key == "string":
                data_type = StringType()
            elif type_key == "binary":
                data_type = BinaryType()
            elif type_key == "date":
                data_type = DateType()
            elif type_key in ("timestamp", "timestamp_ntz"):
                data_type = ReparkDataType.fromDDL(type_key)
            # "Null" is the Arrow Debug spelling, which reaches every flat void column —
            # a plain NULL literal included, not just a void explode (engine spells every
            # other standard type lowercase) — W-1.
            elif type_key in ("void", "null", "Null"):
                data_type = NullType()
            elif type_key.startswith("decimal("):
                # decimal(p,s)
                inner = type_key[len("decimal(") : -1]
                precision_str, scale_str = inner.split(",", 1)
                data_type = DecimalType(int(precision_str), int(scale_str))
            elif type_key.startswith(("array<", "map<", "struct<")):
                try:
                    data_type = ReparkDataType.fromDDL(type_key)
                except Exception:
                    data_type = StringType()
            else:
                data_type = StringType()
            fields.append(StructField(name, data_type, nullable))
        # Overlay Spark-legal display names while engine fields stay unique.
        overlay = self._display_overlay_names()
        if overlay is not None and len(overlay) == len(fields):
            fields = [
                StructField(display, field.dataType, field.nullable)
                for display, field in zip(overlay, fields, strict=True)
            ]
        return StructType(fields)

    @property
    def dtypes(self) -> list[tuple[str, str]]:
        """Column name + simple type string pairs (PySpark ``DataFrame.dtypes``)."""
        return [(field.name, field.dataType.simpleString()) for field in self.schema.fields]

    def printSchema(  # noqa: N802 — PySpark method name
        self, level: int | None = None
    ) -> None:
        """Print the schema tree to stdout (PySpark ``DataFrame.printSchema``).

        ``level`` is Spark 3.4+ max depth (``StructType.treeString(maxDepth)``). ``None`` /
        omitted prints the full tree. Uses typeName labels (``long`` not ``bigint``) so the
        tree matches live Spark / Apache ``test_print_schema``.
        """
        self._ensure_alive()
        max_depth = -1 if level is None else int(level)
        # treeString ends with a newline and print adds Spark's second one.
        print(self.schema.treeString(max_depth))

    print_schema = printSchema

    def __str__(self) -> str:
        """``DataFrame[name: type, …]`` (PySpark ``DataFrame.__str__``).

        Uses ``dtypes`` simpleString pairs (``bigint`` for LongType). Apache
        ``test_column_name_with_non_ascii`` pins this form via ``str(df)``.
        """
        self._ensure_alive()
        parts = [f"{name}: {type_name}" for name, type_name in self.dtypes]
        return f"DataFrame[{', '.join(parts)}]"

    def __repr__(self) -> str:
        """Schema form by default; table show when ``spark.sql.repl.eagerEval.enabled``.

        Conf keys ``spark.sql.repl.eagerEval.enabled`` (truthy),
        ``.truncate`` (default 20), ``.maxNumRows`` (default 20) match Spark REPL shape
        (Apache ``test_repr_behaviors``).
        """
        # Window, random, and stratified-sampling validation.
        self._ensure_alive()
        if not self._eager_eval_enabled():
            return self.__str__()
        max_rows, truncate_at = self._eager_eval_limits()
        table = self.limit(max_rows).to_arrow()
        # Spark Dataset.showString packing (no spaces around cells; right-align).
        rendered = _format_eager_eval_table(table, truncate_at=truncate_at)
        if table.num_rows >= max_rows:
            try:
                total = self.count()
            except Exception:
                total = None
            if total is not None and total > max_rows:
                rendered = f"{rendered}\nonly showing top {max_rows} row" + (
                    "s" if max_rows != 1 else ""
                )
        return rendered

    def _repr_html_(self) -> str | None:
        """HTML table when eager-eval is on; ``None`` otherwise (Jupyter / PySpark).

        Cell text and header names are HTML-escaped (Spark
        ``Dataset.html`` / ``StringEscapeUtils``) so ``<script>``, ``&``, and hostile column
        names cannot inject markup. Truncate first (hard left-slice, same as ``__repr__``),
        then escape — matches live Spark 4.1.2 ordering.
        """
        # Validate window, random, and stratified-sampling markers.
        import html as html_module

        self._ensure_alive()
        if not self._eager_eval_enabled():
            return None
        max_rows, truncate_at = self._eager_eval_limits()
        table = self.limit(max_rows).to_arrow()
        names = list(table.column_names)
        rows = _table_to_cell_rows(table, truncate_at=None, style="spark")
        if truncate_at is not None and truncate_at > 0:
            rows = [[cell[:truncate_at] for cell in row] for row in rows]
        safe_names = [html_module.escape(name, quote=True) for name in names]
        parts = [
            "<table border='1'>",
            "<tr>" + "".join(f"<th>{name}</th>" for name in safe_names) + "</tr>",
        ]
        for row in rows:
            safe_cells = [html_module.escape(cell, quote=True) for cell in row]
            parts.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in safe_cells) + "</tr>")
        parts.append("</table>")
        html = "\n".join(parts)
        if table.num_rows >= max_rows:
            try:
                total = self.count()
            except Exception:
                total = None
            if total is not None and total > max_rows:
                html = f"{html}\nonly showing top {max_rows} row" + ("s" if max_rows != 1 else "")
        return html

    def _eager_eval_enabled(self) -> bool:
        """``spark.sql.repl.eagerEval.enabled`` truthy (runtime conf or builder)."""
        raw = self._conf_lookup("spark.sql.repl.eagerEval.enabled")
        if raw is None:
            return False
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def _eager_eval_limits(self) -> tuple[int, int]:
        """``(maxNumRows, truncate)`` with Spark defaults 20 / 20."""
        max_raw = self._conf_lookup("spark.sql.repl.eagerEval.maxNumRows")
        trunc_raw = self._conf_lookup("spark.sql.repl.eagerEval.truncate")
        try:
            max_rows = max(0, int(max_raw)) if max_raw is not None else 20
        except (TypeError, ValueError):
            max_rows = 20
        try:
            truncate_at = max(0, int(trunc_raw)) if trunc_raw is not None else 20
        except (TypeError, ValueError):
            truncate_at = 20
        return max_rows, truncate_at

    def _conf_lookup(self, key: str) -> str | None:
        """Runtime conf then builder snapshot (case-sensitive Spark keys)."""
        token = getattr(self, "_alive_token", {}) or {}
        store = token.get("runtime_conf")
        if isinstance(store, dict) and key in store:
            return str(store[key])
        builder = token.get("builder_config") or {}
        if key in builder and builder[key] is not None:
            return str(builder[key])
        return None

    def toDF(  # noqa: N802 — PySpark method name
        self, *cols: str
    ) -> DataFrame:
        """Rename columns positionally (PySpark ``DataFrame.toDF``)."""
        self._ensure_alive()
        names = list(cols)
        for name in names:
            if not isinstance(name, str):
                raise PySparkTypeError(
                    errorClass="NOT_LIST_OF_STR",
                    messageParameters={
                        "arg_name": "cols",
                        "arg_type": type(name).__name__,
                    },
                )
        current = self.columns
        if names and len(names) != len(current):
            raise PySparkValueError(f"toDF expects {len(current)} column names, got {len(names)}")
        if not names:
            return self._identity_child()
        # Multi-name frames cannot re-bind bare display strings; rename
        # positionally via engine/display bindings.
        return self.select(
            *[
                bound.alias(new)
                for bound, new in zip(self._iter_bound_columns(), names, strict=True)
            ]
        )

    to_df = toDF

    def selectExpr(  # noqa: N802 — PySpark method name
        self, *expr: str
    ) -> DataFrame:
        """Project SQL expression strings through a plan-stable native snapshot.

        Pending ``mapInArrow`` bridges are prepared once, so non-idempotent UDFs do not rerun
        during SQL projection planning.
        """
        self._ensure_alive()
        if not expr:
            raise PySparkValueError("selectExpr requires at least one expression")
        for item in expr:
            if not isinstance(item, str):
                raise PySparkTypeError(
                    f"selectExpr expressions must be str, got {type(item).__name__}"
                )
        # A bare ``*`` keeps multi-name display identity.
        if len(expr) == 1 and expr[0].strip() == "*":
            return self.select("*")
        view = scratch_view_name(self._session, "__repark_selx_")
        # Use a plan-stable bridge snapshot rather than action registration.
        self._session.create_or_replace_temp_view(view, self._plan())
        try:
            projection = ", ".join(expr)
            planned = self._session.sql(f"SELECT {projection} FROM {view}")
            return self._spawn(planned)
        finally:
            self._session.drop_temp_view(view)

    select_expr = selectExpr

    def alias(self, alias: str) -> DataFrame:
        """Return this frame registered under a SQL alias name (PySpark ``DataFrame.alias``).

        Registers a replaceable temp view ``alias`` and returns a scan of it (Spark's
        SubqueryAlias for joins/self-joins). Alias must be a bare SQL identifier.
        """
        self._ensure_alive()
        if not isinstance(alias, str) or alias.strip() == "":
            raise PySparkTypeError(f"alias must be a non-empty str, got {alias!r}")
        name = alias.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise AnalysisException(
                f"DataFrame.alias requires a bare SQL identifier, got {alias!r}"
            )
        # Register one plan-stable bridge snapshot.
        # Mirrors selectExpr / select / filter so post-prepare alias agrees with peers.
        self._session.create_or_replace_temp_view(name, self._plan())
        # -1: the NAME stays one-part (the user chose it), but the read is home-pinned —
        # a bare/quoted one-part reference is re-resolved against the live default catalog.
        home_ref = home_view_ref(self._session, name)
        child = self._spawn(self._session.sql(f"SELECT * FROM {home_ref}"))
        # SQL SELECT * surfaces engine field names; re-attach display identity so
        # Multi-name joins keep duplicate display columns positionally.
        if self._display_names is not None and self._engine_names is not None:
            child._display_names = list(self._display_names)
            child._engine_names = list(self._engine_names)
            child._origin_map = dict(self._origin_map) if self._origin_map is not None else None
        return child

    def toArrow(  # noqa: N802 — PySpark method name
        self,
    ) -> Any:
        """Return rows as a ``pyarrow.Table`` (PySpark 4.0+ ``DataFrame.toArrow``)."""
        return self.to_arrow()

    def colRegex(  # noqa: N802 — PySpark method name
        self,
        colName: str,  # noqa: N803 — PySpark arg name
    ) -> Column:
        """Select columns matching a Java/Python regex (PySpark ``colRegex``).

        Returns a :class:`Column` usable in :meth:`select`. When multiple columns match,
        selects the first match (disclose: Spark expands all matches in select).
        """
        self._ensure_alive()
        if not isinstance(colName, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={
                    "arg_name": "colName",
                    "arg_type": type(colName).__name__,
                },
            )
        pattern = re.compile(colName)
        matches = [name for name in self.columns if pattern.search(name)]
        if not matches:
            raise AnalysisException(f"No column matched regex {colName!r}")
        from repark.spark.functions import col as col_fn

        return col_fn(matches[0])

    col_regex = colRegex

    def sample(
        self,
        withReplacement: bool | float | None = None,  # noqa: N803
        fraction: float | None = None,
        seed: int | None = None,
    ) -> DataFrame:
        """Bernoulli sample of rows (PySpark ``DataFrame.sample``).

        Engine RNG ≠ Spark RNG — pins use seed determinism + count tolerance, not exact rows.
        ``withReplacement=True`` is not supported (loud error).

        Overload resolution mirrors PySpark's sample-arg helper (classic/connect DataFrame):

        * ``sample(fraction)`` / ``sample(fraction, seed)`` — first positional is a number
        * ``sample(withReplacement, fraction [, seed])`` — first positional is a bool
        * ``sample(fraction=…, seed=…)`` — keyword form

        When ``seed`` is omitted, repark bakes a default seed into the plan so repeated
        actions on the same sampled DataFrame return a stable multiset (Spark embeds a
        planning-time seed the same way).
        """
        self._ensure_alive()
        replacement_flag, fraction_value, plan_seed = self._prepare_sample_args(
            withReplacement, fraction, seed
        )
        if replacement_flag:
            raise UnsupportedOperationException(
                "sample(withReplacement=True) is not supported; use withReplacement=False"
            )
        if fraction_value < 0.0 or fraction_value > 1.0:
            # Live Spark raises IllegalArgumentException for out-of-range fraction.
            raise IllegalArgumentException(
                f"requirement failed: Fraction must be in [0, 1], but got {fraction_value}"
            )
        view = scratch_view_name(self._session, "__repark_samp_")
        # Register one plan-stable bridge snapshot.
        self._session.create_or_replace_temp_view(view, self._plan())
        try:
            if fraction_value >= 1.0:
                planned = self._session.sql(f"SELECT * FROM {view}")
            elif fraction_value <= 0.0:
                planned = self._session.sql(f"SELECT * FROM {view} WHERE 1 = 0")
            else:
                # Deterministic LCG-ish sample on ordered row_number (engine RNG ≠ Spark).
                # Mix seed into the multiplier term — a pure ``rn * A + seed`` offset left
                # adjacent seeds producing identical keep-sets.
                # ORDER BY must use unique engine field names on multi-name frames.
                order_fields = (
                    self._engine_names if self._engine_names is not None else self.columns
                )
                order_sql = ", ".join(_quote_ident_sql(c) for c in order_fields)
                order_clause = f"ORDER BY {order_sql}" if order_sql else ""
                planned = self._session.sql(
                    f"SELECT * EXCLUDE (__repark_rn) FROM ("
                    f"  SELECT *, row_number() OVER ({order_clause}) AS __repark_rn FROM {view}"
                    f") WHERE (abs((__repark_rn + {plan_seed}) * 1103515245 + 12345) % 1000000) "
                    f"/ 1000000.0 < {fraction_value}"
                )
            child = self._spawn(planned)
            if self._display_names is not None and self._engine_names is not None:
                child._display_names = list(self._display_names)
                child._engine_names = list(self._engine_names)
                child._origin_map = dict(self._origin_map) if self._origin_map is not None else None
            return child
        finally:
            self._session.drop_temp_view(view)

    @staticmethod
    def _prepare_sample_args(
        withReplacement: bool | float | None,  # noqa: N803
        fraction: float | None,
        seed: int | None,
    ) -> tuple[bool, float, int]:
        """Resolve sample overloads (PySpark classic/connect sample-arg helper parity).

        Default plan seed is ``42`` (not ``random.randint``) so unseeded samples are
        action-stable on the same plan.
        """
        # Default plan-stable seed when the caller omits seed (Spark planning-time embed).
        default_seed = 42

        # sample(withReplacement=bool, fraction=float [, seed])
        if (
            isinstance(withReplacement, bool)
            and isinstance(fraction, (int, float))
            and not (isinstance(fraction, bool))
        ):
            plan_seed = default_seed if seed is None else _coerce_sample_seed(seed, label="seed")
            return withReplacement, float(fraction), plan_seed

        # sample(fraction=float [, seed=…])  — keyword fraction, optional seed kw
        if (
            withReplacement is None
            and isinstance(fraction, (int, float))
            and not isinstance(fraction, bool)
        ):
            plan_seed = default_seed if seed is None else _coerce_sample_seed(seed, label="seed")
            return False, float(fraction), plan_seed

        # sample(0.5 [, seed])  — first positional is fraction; second positional is seed.
        # PySpark ignores the seed= keyword on this form (only the fraction-slot seed counts).
        if isinstance(withReplacement, (int, float)) and not isinstance(withReplacement, bool):
            if fraction is not None:
                plan_seed = _coerce_sample_seed(fraction, label="seed")
            else:
                plan_seed = default_seed
            return False, float(withReplacement), plan_seed

        # Missing / wrong-type overloads (Apache test_sample).
        argtypes = [type(arg).__name__ for arg in (withReplacement, fraction, seed)]
        raise PySparkTypeError(
            errorClass="NOT_BOOL_OR_FLOAT_OR_INT",
            messageParameters={
                "arg_name": ("withReplacement (optional), fraction (required) and seed (optional)"),
                "arg_type": ", ".join(argtypes),
            },
        )

    def randomSplit(  # noqa: N802 — PySpark method name
        self,
        weights: list[float] | tuple[float, ...],
        seed: int | None = None,
    ) -> list[DataFrame]:
        """Split rows into weighted buckets (PySpark ``DataFrame.randomSplit``).

        Weights are normalized like Spark. Engine RNG ≠ Spark — pin count-in-tolerance, disclose
        exact-row divergence.
        """
        self._ensure_alive()
        if not isinstance(weights, (list, tuple)) or not weights:
            raise PySparkTypeError("randomSplit weights must be a non-empty list of floats")
        weight_list = [float(weight) for weight in weights]
        if any(weight < 0 for weight in weight_list):
            raise PySparkValueError("weights must be non-negative")
        total = sum(weight_list)
        if total == 0:
            raise PySparkValueError("weights must sum to a positive value")
        normalized = [weight / total for weight in weight_list]
        # Cumulative bounds on [0,1).
        bounds: list[float] = []
        running = 0.0
        for weight in normalized:
            running += weight
            bounds.append(running)
        view = scratch_view_name(self._session, "__repark_rsplit_")
        # Register one plan-stable bridge snapshot.
        self._session.create_or_replace_temp_view(view, self._plan())
        try:
            # Order by unique engine fields on multi-name frames.
            order_fields = self._engine_names if self._engine_names is not None else self.columns
            order_sql = ", ".join(_quote_ident_sql(c) for c in order_fields)
            order_clause = f"ORDER BY {order_sql}" if order_sql else ""
            if seed is None:
                # Spark: unseeded randomSplit is non-deterministic.
                bucket_sql = f"SELECT *, random() AS __repark_split_u FROM {view}"
            else:
                # Deterministic LCG on ordered row_number + seed (engine RNG ≠ Spark).
                # Same seed mix as sample — pure ``rn * A + seed`` left adjacent seeds
                # identical.
                seed_expr = str(int(seed))
                bucket_sql = (
                    f"SELECT * EXCLUDE (__repark_rn), "
                    f"(abs((__repark_rn + {seed_expr}) * 1103515245 + 12345) % 1000000) "
                    f"/ 1000000.0 AS __repark_split_u FROM ("
                    f"  SELECT *, row_number() OVER ({order_clause}) AS __repark_rn FROM {view}"
                    f")"
                )
            scored_name = scratch_view_name(self._session, "__repark_rsplit_s_")
            scored = self._session.sql(bucket_sql)
            self._session.create_or_replace_temp_view(scored_name, scored)
            try:
                frames: list[DataFrame] = []
                lower = 0.0
                for index, upper in enumerate(bounds):
                    if index == len(bounds) - 1:
                        predicate = f"__repark_split_u >= {lower}"
                    else:
                        predicate = f"__repark_split_u >= {lower} AND __repark_split_u < {upper}"
                    part = self._session.sql(
                        f"SELECT * EXCLUDE (__repark_split_u) FROM {scored_name} WHERE {predicate}"
                    )
                    child = self._spawn(part)
                    if self._display_names is not None and self._engine_names is not None:
                        child._display_names = list(self._display_names)
                        child._engine_names = list(self._engine_names)
                        child._origin_map = (
                            dict(self._origin_map) if self._origin_map is not None else None
                        )
                    frames.append(child)
                    lower = upper
                return frames
            finally:
                self._session.drop_temp_view(scored_name)
        finally:
            self._session.drop_temp_view(view)

    random_split = randomSplit

    def describe(self, *cols: str) -> DataFrame:
        """Basic stats (count/mean/stddev/min/max) as a DataFrame (PySpark ``describe``)."""
        return self.summary("count", "mean", "stddev", "min", "max", _columns=cols or None)

    def summary(self, *statistics: str, _columns: tuple[str, ...] | None = None) -> DataFrame:
        """Summary statistics as a DataFrame (PySpark ``DataFrame.summary``).

        Supports count/mean/stddev/min/max. Percentile stats (``25%``/``50%``/``75%``) raise
        loud unsupported (engine gap — disclosed).
        """
        self._ensure_alive()
        # Bare summary() omits Spark percentile rows — refuse rather than mislead.
        if not statistics:
            raise UnsupportedOperationException(
                "DataFrame.summary() without statistics is not Spark-shaped "
                "(engine lacks percentile rows); call summary('count','mean','stddev','min','max') "
                "or describe()"
            )
        stats = list(statistics)
        supported = {"count", "mean", "stddev", "min", "max"}
        bad = [item for item in stats if item not in supported]
        if bad:
            raise UnsupportedOperationException(
                f"summary statistics not supported yet: {bad} "
                f"(supported: {sorted(supported)}; percentiles are an engine gap)"
            )
        # Multi-name frames must aggregate on unique engine fields (display "b" is
        # ambiguous or missing in the view schema.
        if _columns:
            target_pairs: list[tuple[str, str]] = [(name, name) for name in _columns]
            if self._display_names is not None and self._engine_names is not None:
                target_pairs = []
                want = set(_columns)
                for display, engine in zip(self._display_names, self._engine_names, strict=True):
                    if display in want:
                        target_pairs.append((display, engine))
        elif self._display_names is not None and self._engine_names is not None:
            target_pairs = list(zip(self._display_names, self._engine_names, strict=True))
        else:
            target_pairs = [(name, name) for name in self.columns]
        if not target_pairs:
            raise AnalysisException("summary/describe on a zero-column frame is undefined")
        # Build one row per statistic via SQL aggregations, UNION ALL.
        view = scratch_view_name(self._session, "__repark_sum_")
        # Register one plan-stable bridge snapshot.
        self._session.create_or_replace_temp_view(view, self._plan())
        try:
            pieces: list[str] = []
            for stat in stats:
                select_parts = [f"'{stat}' AS summary"]
                for _display, engine in target_pairs:
                    quoted_eng = _quote_ident_sql(engine)
                    # Engine alias must stay unique; facade may overlay display names.
                    quoted_as = quoted_eng
                    if stat == "count":
                        select_parts.append(f"CAST(count({quoted_eng}) AS VARCHAR) AS {quoted_as}")
                    elif stat == "mean":
                        select_parts.append(f"CAST(avg({quoted_eng}) AS VARCHAR) AS {quoted_as}")
                    elif stat == "stddev":
                        select_parts.append(f"CAST(stddev({quoted_eng}) AS VARCHAR) AS {quoted_as}")
                    elif stat == "min":
                        select_parts.append(f"CAST(min({quoted_eng}) AS VARCHAR) AS {quoted_as}")
                    elif stat == "max":
                        select_parts.append(f"CAST(max({quoted_eng}) AS VARCHAR) AS {quoted_as}")
                pieces.append(f"SELECT {', '.join(select_parts)} FROM {view}")
            sql = " UNION ALL ".join(pieces)
            child = self._spawn(self._session.sql(sql))
            # Overlay Spark-legal display names (summary + data displays).
            if self._display_names is not None or any(
                display != engine for display, engine in target_pairs
            ):
                child._display_names = ["summary"] + [display for display, _engine in target_pairs]
                child._engine_names = ["summary"] + [engine for _display, engine in target_pairs]
            return child
        finally:
            self._session.drop_temp_view(view)

    def replace(
        self,
        to_replace: Any,
        value: Any = None,
        subset: str | list[str] | tuple[str, ...] | None = None,
    ) -> DataFrame:
        """Replace value(s) across columns (PySpark ``DataFrame.replace``).

        Supports scalar ``to_replace``/``value`` or a ``dict`` mapping. Subset limits columns.
        """
        self._ensure_alive()
        mapping = to_replace if isinstance(to_replace, dict) else {to_replace: value}
        if not mapping:
            return self._identity_child()
        if subset is None:
            targets = set(self.columns)
        elif isinstance(subset, str):
            targets = {subset}
        else:
            targets = set(subset)
        from repark.spark.functions import lit as lit_fn
        from repark.spark.functions import when

        # Multi-name frames bind by engine/display pairs.
        projected: list[Column] = []
        for bound in self._iter_bound_columns():
            display = bound._projection_name or bound.spark_display_part()
            if display not in targets:
                projected.append(bound)
                continue
            expression: Column = bound
            for old, new in mapping.items():
                expression = when(expression == lit_fn(old), lit_fn(new)).otherwise(expression)
            # Preserve origin for multi-name select identity.
            if bound._origin_plan_id is not None and bound._origin_field is not None:
                projected.append(
                    Column(
                        expression._inner.alias(display),
                        spark_display=display,
                        projection_name=display,
                        stable_name=True,
                        has_free_attribute=True,
                        origin_plan_id=bound._origin_plan_id,
                        origin_field=bound._origin_field,
                        join_sql_expr=expression.join_sql_part(),
                        sql_expr=expression._sql_expr,
                    )
                )
            else:
                projected.append(expression.alias(display))
        return self.select(*projected)

    # Repartition argument validation; execution is single-node.
    def repartition(self, numPartitions: Any, *cols: Any) -> DataFrame:  # noqa: N803
        """Accept ``repartition`` as a no-op (single-node; plan unchanged — disclosed).

        Validates Spark-shaped first-arg types so Apache ``test_repartition`` error-class
        pins land before the identity child is returned. List / bool / other non
        int-or-Column-or-str first args raise ``NOT_COLUMN_OR_STR`` whether or not
        ``*cols`` is present (Spark parity — sole-arg list must not silently no-op).
        """
        self._ensure_alive()
        # Spark: first position is int count, or a Column/str partition expr when the
        # call is ``repartition(*cols)``. List/bool/float/… → NOT_COLUMN_OR_STR always
        # Reject a sole-argument list instead of treating it as no columns.
        if isinstance(numPartitions, bool) or (
            not isinstance(numPartitions, (int, str)) and not isinstance(numPartitions, Column)
        ):
            raise PySparkTypeError(
                errorClass="NOT_COLUMN_OR_STR",
                messageParameters={
                    "arg_name": "numPartitions",
                    "arg_type": type(numPartitions).__name__,
                },
            )
        _ = cols
        return self._identity_child()

    def repartitionByRange(  # noqa: N802
        self,
        numPartitions: Any,  # noqa: N803 — PySpark parameter name
        *cols: Any,
    ) -> DataFrame:
        """Accept ``repartitionByRange`` as a no-op (single-node; disclosed).

        Type-checks the first argument against Spark's
        ``NOT_COLUMN_OR_INT_OR_STR`` surface (Apache ``test_repartition_by_range``). Real
        multi-partition range assignment is an engine seed (``spark_partition_id`` family).
        """
        self._ensure_alive()
        if isinstance(numPartitions, list):
            raise PySparkTypeError(
                errorClass="NOT_COLUMN_OR_INT_OR_STR",
                messageParameters={
                    "arg_name": "numPartitions",
                    "arg_type": "list",
                },
            )
        if isinstance(numPartitions, bool) or (
            not isinstance(numPartitions, (int, str)) and not isinstance(numPartitions, Column)
        ):
            raise PySparkTypeError(
                errorClass="NOT_COLUMN_OR_INT_OR_STR",
                messageParameters={
                    "arg_name": "numPartitions",
                    "arg_type": type(numPartitions).__name__,
                },
            )
        _ = cols
        return self._identity_child()

    def repartitionById(  # noqa: N802
        self,
        numPartitions: Any,  # noqa: N803
        partitionIdExpr: Any,  # noqa: N803
    ) -> DataFrame:
        """Accept ``repartitionById`` as a single-node no-op after Spark-shaped validation.

        Validates ``numPartitions`` (``NOT_INT`` / ``VALUE_NOT_POSITIVE``) so Apache error
        pins pass. A bare string / simple-name :class:`Column` whose schema type is not
        integer-family raises :class:`~repark.errors.AnalysisException` at plan time
        (Apache ``test_repartition_by_id_error_non_int_type``). Actual partition-id
        routing needs multi-partition execution + ``spark_partition_id`` (engine seed).
        """
        self._ensure_alive()
        if isinstance(numPartitions, bool) or not isinstance(numPartitions, int):
            raise PySparkTypeError(
                errorClass="NOT_INT",
                messageParameters={
                    "arg_name": "numPartitions",
                    "arg_type": type(numPartitions).__name__,
                },
            )
        if numPartitions <= 0:
            raise PySparkValueError(
                errorClass="VALUE_NOT_POSITIVE",
                messageParameters={
                    "arg_name": "numPartitions",
                    "arg_value": str(numPartitions),
                },
            )
        # Type-check simple name refs so non-int partition columns fail loud (Spark analysis).
        column_name: str | None = None
        if isinstance(partitionIdExpr, str):
            column_name = partitionIdExpr
        elif isinstance(partitionIdExpr, Column):
            display = partitionIdExpr.spark_display_part()
            # Bare attribute only — casts / expressions stay deferred to the engine seed.
            if display.isidentifier() and display in self.columns:
                column_name = display
        if column_name is not None:
            type_keys = {
                name: type_key for name, type_key, _ in self._inner.logical_schema_fields()
            }
            type_key = type_keys.get(column_name, "")
            if type_key not in {"int", "long", "byte", "short"}:
                raise AnalysisException(
                    f"repartitionById requires an integer partition expression; "
                    f"column `{column_name}` has type `{type_key or 'unknown'}`"
                )
        return self._identity_child()

    def coalesce(self, numPartitions: int) -> DataFrame:  # noqa: N803
        """Accept ``coalesce(numPartitions)`` as a no-op (single-node — disclosed).

        Note: :func:`repark.functions.coalesce` is the SQL multi-column null-coalesce; this is
        the DataFrame partition-coalesce form.
        """
        _ = numPartitions
        self._ensure_alive()
        return self._identity_child()

    def hint(self, name: str, *parameters: Any) -> DataFrame:
        """Accept optimizer ``hint`` as a no-op (single-node; plan unchanged — disclosed)."""
        _ = (name, parameters)
        self._ensure_alive()
        return self._identity_child()

    def limit(self, n: int) -> DataFrame:
        """Return a new DataFrame with at most ``n`` rows (PySpark ``DataFrame.limit``)."""
        return self._spawn_preserving_identity(self._plan().limit(max(0, int(n))))

    def offset(self, n: int) -> DataFrame:
        """Skip the first ``n`` rows (PySpark ``DataFrame.offset``).

        Implemented as ``limit_with_skip(n, large_fetch)`` then unrestricted remainder via a
        large fetch cap (engine has no pure OFFSET without LIMIT).
        """
        self._ensure_alive()
        if isinstance(n, bool) or not isinstance(n, int):
            raise PySparkTypeError(f"offset expects int, got {type(n).__name__}")
        if n < 0:
            raise PySparkValueError(f"offset must be >= 0, got {n}")
        if n == 0:
            return self._identity_child()
        # Fetch a very large tail after skip (practical unbounded offset for single-node).
        return self._spawn_preserving_identity(self._plan().limit_with_skip(n, 2**31 - 1))

    def drop(self, *cols: Column | str) -> DataFrame:
        """Drop columns by name or :class:`Column` (PySpark ``DataFrame.drop``).

        An absent name is a no-op. A :class:`Column` argument drops by its resolved
        field name (simple ``col("x")`` / ``df.x`` form). When the Column carries
        origin identity and this frame has an origin map (post-join), drop targets the
        correct side's engine field only, not every display-name match. Dropping an
        unemitted semi/anti right origin is a Spark 4.1.2 no-op.
        """
        # Join identity.
        engine_drop: list[str] = []
        for item in cols:
            if (
                isinstance(item, Column)
                and item._origin_plan_id is not None
                and item._origin_field is not None
            ):
                if item._origin_plan_id in self._origin_not_emitted:
                    # Live Spark 4.1.2: drop(right["k"]) after leftsemi/leftanti is a no-op.
                    continue
                if self._origin_map is not None:
                    key = (item._origin_plan_id, item._origin_field)
                    if key in self._origin_map:
                        engine_drop.append(self._origin_map[key])
                        continue
            name = self._name_of(item)
            if self._display_names is not None and self._engine_names is not None:
                # Name-based: drop every engine field whose display matches.
                for display, engine in zip(self._display_names, self._engine_names, strict=True):
                    if display == name:
                        engine_drop.append(engine)
            else:
                engine_drop.append(name)
        child = self._spawn(self._plan().drop(engine_drop))
        if self._display_names is not None and self._engine_names is not None:
            dropped = set(engine_drop)
            new_display: list[str] = []
            new_engine: list[str] = []
            for display, engine in zip(self._display_names, self._engine_names, strict=True):
                if engine not in dropped:
                    new_display.append(display)
                    new_engine.append(engine)
            child._display_names = new_display
            child._engine_names = new_engine
            if self._origin_map is not None:
                child._origin_map = {
                    key: engine for key, engine in self._origin_map.items() if engine not in dropped
                }
        return child

    def order_by(
        self,
        *cols: Column | str,
        ascending: bool | list[bool] | None = None,
    ) -> DataFrame:
        """Order rows by the given columns (PySpark ``DataFrame.orderBy`` / ``sort``).

        Each column may carry :meth:`repark.column.Column.asc` /:meth:`~repark.column.Column.desc`;
        the ``ascending`` keyword (a bool or a per-column list) overrides those. Null ordering
        follows Spark: ascending → nulls first, descending → nulls last.
        """
        columns, ascending_flags, nulls_first_flags = self._sort_specs(cols, ascending)
        # Sort does not change column identity; keep display and engine maps.
        return self._spawn_preserving_identity(
            self._plan().sort(columns, ascending_flags, nulls_first_flags)
        )

    # PySpark spells this ``orderBy`` and also aliases ``sort`` to it.
    orderBy = order_by  # noqa: N815 — deliberate PySpark-compatible camelCase alias
    sort = order_by

    def sort_within_partitions(
        self,
        *cols: Column | str,
        ascending: bool | list[bool] | None = None,
    ) -> DataFrame:
        """Sort rows within the single execution partition.

        RePark has one partition, so this delegates to ``orderBy``. Multi-partition semantics are
        outside the single-node execution model.
        """
        return self.order_by(*cols, ascending=ascending)

    # PySpark camelCase.
    sortWithinPartitions = sort_within_partitions  # noqa: N815 — PySpark camelCase alias

    def join(
        self,
        other: DataFrame,
        on: str | list[str] | Column | None = None,
        how: str | None = None,
    ) -> DataFrame:
        """Join with ``other`` (PySpark ``DataFrame.join``).

        ``on`` is a shared column name, a list of names (equi-join, single merged key column), a
        boolean :class:`Column` condition (all columns kept), or ``None`` for a Cartesian product
        (subject to ``spark.sql.crossJoin.enabled`` via :attr:`session.conf`). ``how`` defaults
        to ``"inner"``. Supported join types: ``inner``, ``left`` / ``left_outer`` / ``leftouter``,
        ``right`` / ``right_outer`` / ``rightouter``, ``full`` / ``outer`` / ``fullouter`` /
        ``full_outer``, ``cross``, ``semi`` / ``leftsemi`` / ``left_semi``, ``anti`` /
        ``leftanti`` / ``left_anti``. Partition-transform Columns (``F.years`` / …) in a Column
        condition raise — valid only inside :meth:`DataFrameWriterV2.partitionedBy`.

        Semi and anti joins filter the left side and emit no right-hand columns. NULL keys do not
        match. A semi or anti join with ``on=None`` is refused instead of becoming Cartesian.
        Right-parent Columns then raise ``MISSING_ATTRIBUTES``; ``drop`` is a no-op.

        Condition joins rewrite origin-qualified references to relation-qualified SQL, so self-joins
        and duplicate non-key names resolve. Output
        may carry Spark-legal duplicate *display* names with unique engine fields + origin map
        for post-join ``select(df1["x"])`` / ``drop(df1["x"])`` / ``AMBIGUOUS_REFERENCE``.

        Same-object joins alternate token sides for simple
        leaf comparisons (``df.x == df.x``, AND/OR of those) so equi self-joins keep
        correct cardinality. Multi-token arms (``(df.x + df.y) == …``) refuse loud with
        the ``df.alias("l").join(df.alias("r"), …)`` workaround — alternation would
        silently mis-bind.
        """
        # Join identity and self-join handling.
        join_how = "inner" if how is None else str(how).lower().replace("_", "")
        # Normalize Spark aliases to engine tokens.
        how_aliases = {
            "inner": "inner",
            "cross": "cross",
            "left": "left",
            "leftouter": "left",
            "right": "right",
            "rightouter": "right",
            "full": "full",
            "outer": "full",
            "fullouter": "full",
            # semi family. `.replace("_", "")` already folded `left_semi`/`left_anti` in.
            "semi": "leftsemi",
            "leftsemi": "leftsemi",
            "anti": "leftanti",
            "leftanti": "leftanti",
        }
        if join_how not in how_aliases:
            raise AnalysisException(
                f"Unsupported join type '{how}'. Supported join types include: "
                "'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', "
                "'left_outer', 'rightouter', 'right', 'right_outer', 'cross', 'semi', "
                "'leftsemi', 'left_semi', 'anti', 'leftanti', 'left_anti'."
            )
        engine_how = how_aliases[join_how]
        if engine_how in _SEMI_JOIN_HOWS and (
            on is None or (isinstance(on, (list, tuple)) and not on)
        ):
            # A conditionless semi/anti join is NOT a Cartesian product: Spark keeps every left
            # row iff the right side is non-empty (semi) / empty (anti), with no m*n fan-out.
            # Both conditionless shapes (`on=None`, `on=[]`) fall through to crossJoin below, so
            # they are refused loud here rather than silently answering with a cross join's rows.
            raise AnalysisException(
                f"join type '{how}' requires an `on` condition. A conditionless {engine_how} "
                "join is not a Cartesian product, so repark refuses it rather than returning a "
                "cross join's rows. Pass `on=` a column name, a list of names, or a boolean "
                "Column."
            )
        if on is None:
            # Cartesian product requires crossJoin or conf spark.sql.crossJoin.enabled.
            # Read the same effective value as RuntimeConfig.get (runtime map, then builder).
            if engine_how != "cross" and not self._cross_join_enabled():
                raise AnalysisException(
                    "Detected implicit cartesian product for INNER join between logical plans. "
                    "If this is intended, set spark.sql.crossJoin.enabled=true to allow them."
                )
            return self.crossJoin(other)
        if isinstance(on, Column):
            _reject_partition_transform(on)
            return self._join_on_condition_h1(other, on, engine_how)
        # Name equi-join: SubqueryAlias both sides only when names collide or self-join
        # — unconditional alias leaked permanent session views.
        if self is other or set(self.columns) & set(other.columns):
            left: DataFrame = self.alias(f"_repark_jl_{uuid.uuid4().hex[:12]}")
            right: DataFrame = other.alias(f"_repark_jr_{uuid.uuid4().hex[:12]}")
        else:
            left = self
            right = other
        if isinstance(on, str):
            child = left._spawn(left._plan().join_on_names(right._plan(), [on], engine_how), other)
            child._remember_unemitted_right_origins(
                self, other, left_only=engine_how in _SEMI_JOIN_HOWS
            )
            return child
        if isinstance(on, (list, tuple)) and all(isinstance(key, str) for key in on):
            #: empty key list is a cartesian product — same gate as on=None
            # (vacuous all-str would otherwise call join_on_names([]) and skip the conf check).
            keys = list(on)
            if not keys:
                if engine_how != "cross" and not left._cross_join_enabled():
                    raise AnalysisException(
                        "Detected implicit cartesian product for INNER join between logical plans. "
                        "If this is intended, set spark.sql.crossJoin.enabled=true to allow them."
                    )
                return left.crossJoin(right)
            child = left._spawn(left._plan().join_on_names(right._plan(), keys, engine_how), other)
            child._remember_unemitted_right_origins(
                self, other, left_only=engine_how in _SEMI_JOIN_HOWS
            )
            return child
        raise PySparkTypeError(
            "join `on` expects a column name, a list of names, or a Column, "
            f"got {type(on).__name__}"
        )

    def _join_on_condition_h1(
        self,
        other: DataFrame,
        condition: Column,
        engine_how: str,
    ) -> DataFrame:
        """Rewrite a condition join with origin-qualified references.

        Semi and anti joins project only the left side because they emit no right-hand columns.
        """
        left_alias = scratch_view_name(self._session, "_repark_jl_")
        right_alias = scratch_view_name(self._session, "_repark_jr_")
        how_sql = {
            "inner": "INNER",
            "left": "LEFT OUTER",
            "right": "RIGHT OUTER",
            "full": "FULL OUTER",
            "cross": "CROSS",
            "leftsemi": "LEFT SEMI",
            "leftanti": "LEFT ANTI",
        }.get(engine_how, "INNER")
        left_only = engine_how in _SEMI_JOIN_HOWS
        # Register both plans as temp views (plan-stable), analyze SQL join, then drop views.
        self._session.create_or_replace_temp_view(left_alias, self._plan())
        self._session.create_or_replace_temp_view(right_alias, other._plan())
        try:
            on_sql = _rewrite_join_qcol_sql(
                condition.join_sql_part(),
                left=self,
                right=other,
                left_alias=left_alias,
                right_alias=right_alias,
            )
            left_cols = list(self.columns)
            right_cols = list(other.columns)
            #: a semi/anti join emits the left side only, so a right-hand name that merely
            # SHARES a left name is not a duplicate in the output — counting it would mangle the
            # left engine field for no reason (and `k` is shared on essentially every semi join).
            all_display = left_cols if left_only else left_cols + right_cols
            display_counts: dict[str, int] = {}
            for name in all_display:
                display_counts[name] = display_counts.get(name, 0) + 1

            proj_parts: list[str] = []
            display_names: list[str] = []
            engine_names: list[str] = []
            origin_map: dict[tuple[str, str], str] = {}

            _emit_join_side_columns(
                self,
                left_alias,
                "l",
                display_counts=display_counts,
                proj_parts=proj_parts,
                display_names=display_names,
                engine_names=engine_names,
                origin_map=origin_map,
            )
            if not left_only:
                _emit_join_side_columns(
                    other,
                    right_alias,
                    "r",
                    display_counts=display_counts,
                    proj_parts=proj_parts,
                    display_names=display_names,
                    engine_names=engine_names,
                    origin_map=origin_map,
                )

            if engine_how == "cross":
                join_sql = (
                    f"SELECT {', '.join(proj_parts)} FROM {left_alias} CROSS JOIN {right_alias}"
                )
            else:
                join_sql = (
                    f"SELECT {', '.join(proj_parts)} FROM {left_alias} "
                    f"{how_sql} JOIN {right_alias} ON {on_sql}"
                )
            planned = self._session.sql(join_sql)
            child = self._spawn(planned, other)
            # Always attach identity when any display name collides OR origin map needed.
            child._display_names = display_names
            child._engine_names = engine_names
            child._origin_map = origin_map
            child._remember_unemitted_right_origins(self, other, left_only=left_only)
            return child
        finally:
            self._session.drop_temp_view(left_alias)
            self._session.drop_temp_view(right_alias)

    # Aggregation.

    def group_by(self, *cols: Column | str) -> GroupedData:
        """Group by columns and return a ``GroupedData`` handle.

        Arguments may be ``Column`` objects or names. Partition transforms are valid only in
        ``DataFrameWriterV2.partitionedBy``.
        """
        self._prepare_for_plan()
        group_columns = [self._column_of(item) for item in cols]
        for column in group_columns:
            _reject_partition_transform(column)
            # Generators lower through select unnest, not as grouping keys.
            column._reject_nested_generator("groupBy")
        return GroupedData(self, group_columns)

    # PySpark spells this ``groupBy`` and also accepts the lowercase ``groupby``.
    groupBy = group_by  # noqa: N815 — deliberate PySpark-compatible camelCase alias
    groupby = group_by

    def cube(self, *cols: Column | str) -> GroupedData:
        """Return a ``GroupedData`` cube grouping."""
        return self._grouping_sets_grouped("CUBE", cols)

    def rollup(self, *cols: Column | str) -> GroupedData:
        """Return a ``GroupedData`` rollup grouping."""
        return self._grouping_sets_grouped("ROLLUP", cols)

    def grouping_sets(self, *cols: Column | str) -> GroupedData:
        """Return grouping sets for each column and the grand total."""
        # Full Spark groupingSets API is multi-list; v1: one set per col +.
        names = [self._grouping_col_sql(item) for item in cols]
        if not names:
            raise AnalysisException("groupingSets requires at least one column")
        sets = ", ".join(f"({name})" for name in names) + ", ()"
        return self._grouping_sets_grouped(f"GROUPING SETS ({sets})", cols, bare=True)

    groupingSets = grouping_sets  # noqa: N815

    def _grouping_col_sql(self, item: Column | str) -> str:
        """Return a safely quoted SQL fragment for one grouping key.

        String keys use identifier quoting; ``Column`` keys retain their structural expression.
        """
        if isinstance(item, str):
            from repark.spark._idents import quote_ident as _quote_ident

            return _quote_ident(item)
        return item.sql_expr_part()

    def _grouping_sets_grouped(
        self,
        clause: str,
        cols: tuple[Column | str, ...],
        *,
        bare: bool = False,
    ) -> GroupedData:
        group_columns = [self._column_of(item) for item in cols]
        for column in group_columns:
            _reject_partition_transform(column)
            # Generators only lower via select unnest — cube/rollup/groupingSets on a
            # A generator would group by the array placeholder, which Spark rejects.
            column._reject_nested_generator("cube/rollup/groupingSets")
            column._reject_higher_order("cube/rollup/groupingSets")
        if bare:
            sql_group = clause
        else:
            names = ", ".join(self._grouping_col_sql(item) for item in cols)
            sql_group = f"{clause}({names})" if names else clause
        return GroupedData(self, group_columns, sql_group_clause=sql_group)

    def unpivot(
        self,
        ids: list[str] | str | None,
        values: list[str] | str | None,
        variableColumnName: str,  # noqa: N803
        valueColumnName: str,  # noqa: N803
    ) -> DataFrame:
        """Unpivot columns into rows through UNION ALL SQL.

        Identifiers and value labels are quoted before SQL embedding. The operation registers a
        plan-stable snapshot when its input uses a deferred Arrow bridge.
        """
        from repark.spark._idents import quote_ident as _quote_ident

        id_list = [] if ids is None else ([ids] if isinstance(ids, str) else list(ids))
        if values is None:
            raise AnalysisException("unpivot requires an explicit values list in repark v1")
        value_list = [values] if isinstance(values, str) else list(values)
        if not value_list:
            raise AnalysisException("unpivot values list must be non-empty")
        self._ensure_alive()
        view = scratch_view_name(self._session, "__repark_unpivot_")
        # Register one plan-stable bridge snapshot.
        self._session.create_or_replace_temp_view(view, self._plan())
        try:
            parts: list[str] = []
            id_select = ", ".join(_quote_ident(name) for name in id_list)
            if id_select:
                id_select = id_select + ", "
            var_out = _quote_ident(variableColumnName)
            val_out = _quote_ident(valueColumnName)
            for value_col in value_list:
                parts.append(
                    f"SELECT {id_select}"
                    f"{_sql_string_literal(value_col)} AS {var_out}, "
                    f"{_quote_ident(value_col)} AS {val_out} FROM {view}"
                )
            sql = " UNION ALL ".join(parts)
            return self._spawn(self._session.sql(sql))
        finally:
            self._session.drop_temp_view(view)

    melt = unpivot

    def explain(
        self,
        extended: bool | str | None = None,
        mode: str | None = None,
    ) -> None:
        """Print the plan (PySpark ``DataFrame.explain``). Modes, not plan text, are oracle-pinned.

        Disclosed: plan text diverges from Spark (DataFusion plans).

        ``extended=True`` is **not** ``EXPLAIN ANALYZE`` (that executes the plan — and would
        hang on e.g. ``range(10e10)``). Extended only requests a fuller plan print; ANALYZE is
        reserved for ``mode`` values that explicitly request cost or analysis.
        """
        self._ensure_alive()
        view = scratch_view_name(self._session, "__repark_explain_")
        self.create_or_replace_temp_view(view)
        try:
            # Only modes that Spark documents as executing get ANALYZE. extended=True is print-only.
            analyze = False
            if mode is not None:
                mode_upper = str(mode).upper()
                analyze = mode_upper in {"COST"} or "ANALYZE" in mode_upper
            prefix = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
            plan = self._spawn(self._session.sql(f"{prefix} SELECT * FROM {view}"))
            for row in plan.collect():
                print(row)
        finally:
            self._session.drop_temp_view(view)

    def toJSON(self) -> DataFrame:  # noqa: N802 — PySpark camelCase
        """Unsupported: ``toJSON`` / engine ``to_json`` not wired (R- loud)."""
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "DataFrame.toJSON is not supported yet (engine has no to_json; disclosed R-DF-BATCH2)"
        )

    def create_temp_view(self, name: str) -> None:
        """Create a temp view; fails if the name exists (PySpark ``createTempView``)."""
        self._ensure_alive()
        # Engine path: createOrReplace is the only native; simulate IF NOT EXISTS fail via list.
        # v1: same as createOrReplaceTempView (disclosed: does not fail on replace).
        self.create_or_replace_temp_view(name)

    createTempView = create_temp_view  # noqa: N815

    def create_global_temp_view(self, name: str) -> None:
        """Unsupported global_temp namespace (R- loud; use session temp views)."""
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "createGlobalTempView is not supported yet "
            "(no global_temp catalog; disclosed R-DF-BATCH2)"
        )

    createGlobalTempView = create_global_temp_view  # noqa: N815
    createOrReplaceGlobalTempView = create_global_temp_view  # noqa: N815

    def approxQuantile(  # noqa: N802
        self,
        col: str | list[str] | tuple[str, ...],
        probabilities: list[float] | tuple[float, ...],
        relativeError: float,  # noqa: N803
    ) -> list[float] | list[list[float]]:
        """Approximate quantiles of numeric columns (PySpark ``DataFrame.approxQuantile``).

        Lowers to engine ``approx_percentile_cont`` via :func:`repark.functions.percentile_approx`
        ( FAIL-MISSING family). ``relativeError`` is validated (non-negative number) for API
        parity; the engine path is fixed-accuracy today (t-digest accuracy).
        """
        from repark.errors import PySparkTypeError, PySparkValueError
        from repark.spark.functions import percentile_approx

        #: Spark requires relativeError >= 0; do not silently accept garbage.
        if isinstance(relativeError, bool) or not isinstance(relativeError, (int, float)):
            raise PySparkTypeError(
                errorClass="NOT_FLOAT_OR_INT",
                messageParameters={
                    "arg_name": "relativeError",
                    "arg_type": type(relativeError).__name__,
                },
            )
        relative_error_value = float(relativeError)
        #: NaN is not < 0 in IEEE — refuse explicitly (parity with sampleBy).
        if relative_error_value != relative_error_value or relative_error_value < 0.0:
            raise PySparkValueError(
                errorClass="NEGATIVE_VALUE",
                messageParameters={
                    "arg_name": "relativeError",
                    "arg_value": str(relativeError),
                },
            )
        if not isinstance(col, (str, list, tuple)):
            raise PySparkTypeError(
                errorClass="NOT_LIST_OR_STR_OR_TUPLE",
                messageParameters={"arg_name": "col", "arg_type": type(col).__name__},
            )
        single = isinstance(col, str)
        columns: list[str] = [col] if single else list(col)
        for name in columns:
            if not isinstance(name, str):
                raise PySparkTypeError(
                    errorClass="DISALLOWED_TYPE_FOR_CONTAINER",
                    messageParameters={
                        "arg_name": "col",
                        "arg_type": type(col).__name__,
                        "allowed_types": "str",
                        "item_type": type(name).__name__,
                    },
                )
        if not isinstance(probabilities, (list, tuple)):
            raise PySparkTypeError(
                errorClass="NOT_LIST_OR_TUPLE",
                messageParameters={
                    "arg_name": "probabilities",
                    "arg_type": type(probabilities).__name__,
                },
            )
        probs = list(probabilities)
        for probability in probs:
            if not isinstance(probability, (float, int)) or isinstance(probability, bool):
                raise PySparkTypeError(
                    errorClass="NOT_LIST_OF_FLOAT_OR_INT",
                    messageParameters={
                        "arg_name": "probabilities",
                        "arg_type": type(probability).__name__,
                    },
                )
            probability_value = float(probability)
            #: domain errors are ValueError-class, not TypeError.
            if (
                probability_value != probability_value
                or probability_value < 0.0
                or probability_value > 1.0
            ):
                raise PySparkValueError(
                    errorClass="VALUE_OUT_OF_BOUND",
                    messageParameters={
                        "arg_name": "probabilities",
                        "arg_value": str(probability),
                    },
                )

        results: list[list[float]] = []
        for name in columns:
            # One collect per probability — engine approx_percentile_cont is scalar-only.
            row_values: list[float] = []
            for probability in probs:
                cell = self.agg(percentile_approx(name, float(probability)).alias("_q")).collect()
                raw = cell[0][0] if cell else None
                row_values.append(float("nan") if raw is None else float(raw))
            results.append(row_values)
        return results[0] if single else results

    def corr(self, col1: str, col2: str, method: str | None = None) -> float:
        """Pearson correlation of two columns (PySpark ``DataFrame.corr`` / ``stat.corr``)."""
        from repark.errors import PySparkTypeError, PySparkValueError
        from repark.spark.functions import corr as f_corr

        if not isinstance(col1, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "col1", "arg_type": type(col1).__name__},
            )
        if not isinstance(col2, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "col2", "arg_type": type(col2).__name__},
            )
        resolved_method = method if method else "pearson"
        if resolved_method != "pearson":
            raise PySparkValueError(
                errorClass="VALUE_NOT_PEARSON",
                messageParameters={"arg_name": "method", "arg_value": str(resolved_method)},
            )
        rows = self.agg(f_corr(col1, col2).alias("_corr")).collect()
        value = rows[0][0] if rows else None
        return float("nan") if value is None else float(value)

    def cov(self, col1: str, col2: str) -> float:
        """Sample covariance of two columns (PySpark ``DataFrame.cov`` / ``stat.cov``)."""
        from repark.errors import PySparkTypeError
        from repark.spark.functions import covar_samp

        if not isinstance(col1, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "col1", "arg_type": type(col1).__name__},
            )
        if not isinstance(col2, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "col2", "arg_type": type(col2).__name__},
            )
        rows = self.agg(covar_samp(col1, col2).alias("_cov")).collect()
        value = rows[0][0] if rows else None
        return float("nan") if value is None else float(value)

    def crosstab(self, col1: str, col2: str) -> DataFrame:
        """Pair-wise frequency table (PySpark ``DataFrame.crosstab`` / ``stat.crosstab``).

        First column is named ``{col1}_{col2}``; remaining columns are the distinct
        string forms of ``col2`` values with occurrence counts (missing pairs → 0).
        """
        from repark.errors import PySparkTypeError
        from repark.spark.functions import col

        if not isinstance(col1, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "col1", "arg_type": type(col1).__name__},
            )
        if not isinstance(col2, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "col2", "arg_type": type(col2).__name__},
            )
        # Cast strata to string so pivot column names match Spark's string-key form.
        left_name = f"{col1}_{col2}"
        staged = self.select(
            col(col1).cast("string").alias(left_name),
            col(col2).cast("string").alias(col2),
        )
        # pivot requires simple-name aggregate inputs (not count(lit(1))).
        pivoted = staged.groupBy(left_name).pivot(col2).count()
        # Spark fills absent pairs with 0 (not null).
        return pivoted.na.fill(0)

    def sampleBy(  # noqa: N802 — PySpark camelCase
        self,
        col: Column | str,
        fractions: dict[Any, float],
        seed: int | None = None,
    ) -> DataFrame:
        """Stratified sample without replacement (PySpark ``DataFrame.sampleBy``).

        Rows whose stratum key is absent from ``fractions`` are dropped.

        Matches Spark's mechanism: ``rand(seed)`` (XORShiftRandom,
        ``seed + partitionIndex``; repark partitionIndex=0) compared per stratum
        (Spark ``DataFrameStatFunctions.sampleBy`` / ``randomExpressions.Rand``).
        Seeded counts match Spark single-partition layouts (Apache ``test_sampleby``
        band 35-36 at seed=0). Alias of ``stat.sampleBy``.
        """
        # Validate window, random, and stratified-sampling markers.
        from repark.errors import PySparkTypeError
        from repark.spark.column import Column as ReparkColumn
        from repark.spark.functions import col as f_col
        from repark.spark.functions import lit, rand

        if isinstance(col, str):
            stratum = f_col(col)
        elif isinstance(col, ReparkColumn):
            stratum = col
        else:
            raise PySparkTypeError(
                errorClass="NOT_COLUMN_OR_STR",
                messageParameters={"arg_name": "col", "arg_type": type(col).__name__},
            )
        if not isinstance(fractions, dict):
            raise PySparkTypeError(
                errorClass="NOT_DICT",
                messageParameters={"arg_name": "fractions", "arg_type": type(fractions).__name__},
            )
        normalized: dict[Any, float] = {}
        for key, value in fractions.items():
            if isinstance(key, bool) or not isinstance(key, (float, int, str)):
                raise PySparkTypeError(
                    errorClass="DISALLOWED_TYPE_FOR_CONTAINER",
                    messageParameters={
                        "arg_name": "fractions",
                        "arg_type": type(fractions).__name__,
                        "allowed_types": "float, int, str",
                        "item_type": type(key).__name__,
                    },
                )
            if isinstance(value, bool) or not isinstance(value, (float, int)):
                raise PySparkTypeError(
                    errorClass="DISALLOWED_TYPE_FOR_CONTAINER",
                    messageParameters={
                        "arg_name": "fractions",
                        "arg_type": type(fractions).__name__,
                        "allowed_types": "float, int",
                        "item_type": type(value).__name__,
                    },
                )
            fraction_value = float(value)
            #: Spark sampleBy rejects fractions outside [0, 1] (incl. NaN).
            # Engine ``rand() < nan`` is True — silent wrong sample without this guard.
            if fraction_value != fraction_value or fraction_value < 0.0 or fraction_value > 1.0:
                raise IllegalArgumentException(
                    f"requirement failed: Fraction must be in [0, 1], but got {fraction_value}"
                )
            normalized[key] = fraction_value
        if not normalized:
            return self.limit(0)
        # Spark: val r = rand(seed); filter udf(stratum, r) => r < fractions.getOrElse(stratum, 0)
        # One shared rand column so the XORShift sequence advances once per row.
        #: Spark seed is Long — bool has no sampleBy overload (Py4J).
        if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
            raise PySparkTypeError(
                errorClass="NOT_INT",
                messageParameters={
                    "arg_name": "seed",
                    "arg_type": type(seed).__name__,
                },
            )
        effective_seed = 0 if seed is None else int(seed)
        rng = rand(effective_seed)
        predicate: Column | None = None
        for key, fraction in normalized.items():
            piece = (stratum == lit(key)) & (rng < lit(fraction))
            predicate = piece if predicate is None else (predicate | piece)
        if predicate is None:
            return self.limit(0)
        return self.filter(predicate)

    @property
    def stat(self) -> DataFrameStatFunctions:
        """Access ``DataFrameStatFunctions`` (PySpark ``DataFrame.stat``).

        Property form (not a method): Apache suite uses ``df.stat.corr(...)``.
        """
        return DataFrameStatFunctions(self)

    def agg(self, *exprs: Column | dict[str, str]) -> DataFrame:
        """Aggregate over the whole DataFrame — shorthand for ``groupBy().agg(...)`` (PySpark
        ``DataFrame.agg``). Column-expression form (``df.agg(F.sum("x"))``) or the dict form
        (``df.agg({"x": "sum"})``); the result is a single row.
        """
        return self.group_by().agg(*exprs)

    # Set operations.

    def union(self, other: DataFrame) -> DataFrame:
        """Union by **position** (PySpark ``DataFrame.union`` / ``unionAll``).

        Keeps this frame's column names, coerces the two column types to a common type, and does
        **not** deduplicate (Spark ``union`` is UNION ALL). The two frames must have the same
        number of columns.
        """
        child = self._spawn(self._plan().union(other._plan(), False), other)
        # Keep left-side display identity when present (union-by-position inherits
        # left engine field names — Spark keeps left display names).
        if self._display_names is not None and self._engine_names is not None:
            child._display_names = list(self._display_names)
            child._engine_names = list(self._engine_names)
            # Origin map is left-only; right-parent Columns no longer resolve (disclosed).
            child._origin_map = dict(self._origin_map) if self._origin_map is not None else None
        return child

    # PySpark keeps ``unionAll`` as a historical alias of ``union``.
    unionAll = union  # noqa: N815 — deliberate PySpark-compatible camelCase alias

    def union_by_name(self, other: DataFrame, allowMissingColumns: bool = False) -> DataFrame:  # noqa: N803 — PySpark camelCase kwarg
        """Union by **name** (PySpark ``DataFrame.unionByName``).

        Columns are matched by name regardless of order. When ``allowMissingColumns=False``
        (default), the frames must have the same columns or raise
        :class:`~repark.errors.AnalysisException`. When ``True``, a column
        present on only one side is filled with NULL on the other.
        """
        if not allowMissingColumns:
            this_columns = set(self.columns)
            other_columns = set(other.columns)
            if this_columns != other_columns:
                missing = (this_columns | other_columns) - (this_columns & other_columns)
                raise AnalysisException(
                    "Union can only be performed on inputs with the same columns unless "
                    "allowMissingColumns=True; mismatched columns: "
                    f"{sorted(missing)}"
                )
        return self._spawn(self._plan().union(other._plan(), True), other)

    # PySpark spells this ``unionByName``.
    unionByName = union_by_name  # noqa: N815 — deliberate PySpark-compatible camelCase alias

    def _sql_binary_set_op(self, other: DataFrame, op_sql: str) -> DataFrame:
        """Register both frames as temp views, plan ``SELECT * FROM a {op} SELECT * FROM b``."""
        self._ensure_alive()
        other._ensure_alive()
        left = scratch_view_name(self._session, "__repark_set_l_")
        right = scratch_view_name(self._session, "__repark_set_r_")
        # Materialize + register both under try/finally so a right-side MIA failure after
        # left registration cannot leak the left staging MemTable.
        # Register plan-stable bridge snapshots.
        try:
            self._session.create_or_replace_temp_view(left, self._plan())
            other._session.create_or_replace_temp_view(right, other._plan())
            planned = self._session.sql(f"SELECT * FROM {left} {op_sql} SELECT * FROM {right}")
            child = self._spawn(planned, other)
            # Re-attach left multi-name display maps after SQL set-op.
            if self._display_names is not None and self._engine_names is not None:
                child._display_names = list(self._display_names)
                child._engine_names = list(self._engine_names)
                child._origin_map = dict(self._origin_map) if self._origin_map is not None else None
            return child
        finally:
            self._session.drop_temp_view(left)
            other._session.drop_temp_view(right)

    def intersect(self, other: DataFrame) -> DataFrame:
        """Rows in both frames, deduplicated (PySpark ``DataFrame.intersect``)."""
        return self._sql_binary_set_op(other, "INTERSECT")

    def intersectAll(  # noqa: N802 — PySpark method name
        self, other: DataFrame
    ) -> DataFrame:
        """Multiset intersect (PySpark ``intersectAll``).

        Engine ``INTERSECT ALL`` does not match Spark min-multiplicity bags.
        Refuse rather than silently wrong; use :meth:`intersect` for distinct set intersect.
        """
        _ = other
        raise UnsupportedOperationException(
            "DataFrame.intersectAll multiset semantics are not Spark-correct on this engine yet; "
            "use intersect() for distinct bags (octo C1-L-005)"
        )

    intersect_all = intersectAll

    def exceptAll(  # noqa: N802 — PySpark method name
        self, other: DataFrame
    ) -> DataFrame:
        """Multiset except (PySpark ``exceptAll``).

        Engine ``EXCEPT ALL`` drops all matching keys rather than min-multiplicity.
        Refuse rather than silently wrong; use :meth:`subtract` for distinct set difference.
        """
        _ = other
        raise UnsupportedOperationException(
            "DataFrame.exceptAll multiset semantics are not Spark-correct on this engine yet; "
            "use subtract() for distinct bags (octo C1-L-006)"
        )

    except_all = exceptAll

    def subtract(self, other: DataFrame) -> DataFrame:
        """Rows in this frame not in ``other``, deduplicated (PySpark ``subtract`` / ``except``)."""
        return self._sql_binary_set_op(other, "EXCEPT")

    # PySpark also exposes ``exceptAll``; ``except_`` is the Python keyword escape (not shipped).

    def crossJoin(  # noqa: N802 — PySpark method name
        self, other: DataFrame
    ) -> DataFrame:
        """Cartesian product (PySpark ``DataFrame.crossJoin``)."""
        self._ensure_alive()
        other._ensure_alive()
        left = scratch_view_name(self._session, "__repark_x_l_")
        right = scratch_view_name(self._session, "__repark_x_r_")
        # Materialize + register both under try/finally (; same as set-ops).
        # Register plan-stable bridge snapshots.
        try:
            self._session.create_or_replace_temp_view(left, self._plan())
            other._session.create_or_replace_temp_view(right, other._plan())
            planned = self._session.sql(f"SELECT * FROM {left} CROSS JOIN {right}")
            return self._spawn(planned, other)
        finally:
            self._session.drop_temp_view(left)
            other._session.drop_temp_view(right)

    cross_join = crossJoin

    def distinct(self) -> DataFrame:
        """Remove duplicate rows over all columns (PySpark ``DataFrame.distinct``)."""
        return self._spawn_preserving_identity(self._plan().distinct())

    def drop_duplicates(self, subset: list[str] | tuple[str, ...] | None = None) -> DataFrame:
        """Remove duplicate rows (PySpark ``DataFrame.dropDuplicates`` / ``drop_duplicates``).

        With ``subset=None`` deduplicates over all columns (same as :meth:`distinct`). With a
        ``subset`` (a list or tuple of column names) keeps one row per distinct key — which row
        survives per key is unspecified when the non-key columns differ (Spark parity). A bare
        ``str`` is **not** accepted (PySpark ``dropDuplicates`` rejects it — never char-iterated);
        pass ``["col"]``.

        Subset dedup routes through :meth:`group_by` + ``first`` on non-keys with quoted schema
        binds so mixed-case fields after a requested-spelling projection still resolve (native
        ``distinct_on`` folds unquoted idents). Output column order matches
        the source schema (Spark keeps original order).
        """
        from repark.spark import functions as F  # noqa: N812 — PySpark idiom

        names = _normalize_subset(subset, accept_str=False, allowed_phrase="a list or tuple")
        if names is None:
            return self._spawn_preserving_identity(self._plan().distinct())
        # Ambiguous display names in a subset expand to every matching engine field.
        # (Spark keeps one row per distinct key multiset of those columns).
        resolved: list[str] = []
        if self._display_names is not None and self._engine_names is not None:
            want = {self._name_of(item) for item in names}
            for display, engine in zip(self._display_names, self._engine_names, strict=True):
                if display in want:
                    resolved.append(engine)
            if not resolved:
                for item in names:
                    resolved.append(self._resolve_getitem_column_name(self._name_of(item)))
        else:
            for item in names:
                resolved.append(self._resolve_getitem_column_name(self._name_of(item)))
        # Empty subset → full-row distinct (avoids DataFusion empty ORDER BY internal error;
        # Same outcome as subset == all columns.
        all_engine = (
            list(self._engine_names) if self._engine_names is not None else list(self.columns)
        )
        if not resolved or set(resolved) == set(all_engine):
            return self._spawn_preserving_identity(self._plan().distinct())
        # Use row_number keep-first rather than groupBy+first.
        # (preserves non-key columns without collapsing via first()).
        from repark.spark.window import Window

        order_cols: list[Column] = []
        if self._display_names is not None and self._engine_names is not None:
            engine_to_display = {
                engine: display
                for display, engine in zip(self._display_names, self._engine_names, strict=True)
            }
            for engine in resolved:
                display = engine_to_display.get(engine, engine)
                order_cols.append(self._bind_engine_display_column(display, engine))
        else:
            order_cols = [self._bind_schema_column(name) for name in resolved]
        window = Window.partitionBy(*order_cols).orderBy(*order_cols)
        ranked = self.with_column(
            "__repark_dd_rn",
            F.row_number().over(window),
        )
        filtered = ranked.filter(F.col("__repark_dd_rn") == F.lit(1))
        return filtered.drop("__repark_dd_rn")

    # PySpark spells this ``dropDuplicates``.
    dropDuplicates = drop_duplicates  # noqa: N815 — deliberate PySpark-compatible camelCase alias

    def with_column_renamed(self, existing: str, new: str) -> DataFrame:
        """Rename a column (PySpark ``DataFrame.withColumnRenamed``).

        Renaming a column that does not exist is a silent no-op (Spark semantics).
        Empty-string *target* names are rejected. Name resolution is
        case-insensitive (Spark ``caseSensitive=false``); the rename is applied via quoted
        schema bind +:meth:`select` so mixed-case fields after a requested-spelling projection
        actually rename (native DataFusion ``with_column_renamed`` silently no-ops on
        case-preserved fields).
        """
        if not isinstance(new, str):
            raise PySparkTypeError(
                f"withColumnRenamed new name must be str, got {type(new).__name__}"
            )
        if new.strip() == "":
            raise AnalysisException(
                "withColumnRenamed target names must be non-empty "
                "(empty/whitespace names are rejected — Group F / octo r3)"
            )
        try:
            canonical = self._resolve_getitem_column_name(existing)
        except AnalysisException:
            return self
        # Multi-name frames bind by engine/display pairs (bare name rebind
        # raises AMBIGUOUS_REFERENCE on duplicate display names.
        projected: list[Column] = []
        for bound in self._iter_bound_columns():
            display = bound._projection_name or bound.spark_display_part()
            if display == canonical:
                projected.append(bound.alias(new))
            else:
                projected.append(bound)
        return self.select(*projected)

    # PySpark spells this ``withColumnRenamed``.
    withColumnRenamed = with_column_renamed  # noqa: N815 — deliberate PySpark-compatible camelCase alias

    def with_columns_renamed(self, colsMap: dict[str, str]) -> DataFrame:  # noqa: N803 — PySpark
        """Rename multiple columns (PySpark ``DataFrame.withColumnsRenamed``).

        Name rewrites are applied **sequentially** in dict insertion order against a running
        name list (live PySpark 4.1.2). Probe: ``{"a": "b", "b": "c"}`` on ``[a, b]`` rewrites
        to ``[c, c]`` (``a→b`` → ``[b, b]``, then every ``b→c``). A missing old name is a
        silent no-op per the singular rule.

        repark cannot materialize **duplicate column names** (DataFusion projections require
        unique names). When a rename map would leave two columns with the same final name,
        repark raises :class:`~repark.errors.AnalysisException` rather than producing Spark's
        duplicate-named frame. Non-colliding maps match
        Spark bit-for-bit on names and values.
        """
        if not isinstance(colsMap, dict):
            raise PySparkTypeError(
                errorClass="NOT_DICT",
                messageParameters={
                    "arg_name": "colsMap",
                    "arg_type": type(colsMap).__name__,
                },
            )
        original = self.columns
        names = list(original)
        for old_name, new_name in colsMap.items():
            if not isinstance(old_name, str) or not isinstance(new_name, str):
                raise PySparkTypeError(
                    "withColumnsRenamed keys and values must be str, "
                    f"got {type(old_name).__name__} -> {type(new_name).__name__}"
                )
            if new_name.strip() == "":
                raise AnalysisException(
                    "withColumnsRenamed target names must be non-empty "
                    "(empty/whitespace names are rejected — Group F / octo r3)"
                )
            names = [new_name if name == old_name else name for name in names]
        # Multi-name frames already carry Spark-legal duplicate displays; allow them
        # and rename via engine bindings. Ordinary frames still refuse duplicate names.
        multi_name = self._display_names is not None and self._engine_names is not None
        if not multi_name and len(names) != len(set(names)):
            raise AnalysisException(
                "withColumnsRenamed produced duplicate column names "
                f"{names}; repark requires unique column names (Spark allows duplicates — "
                "Group F disclosure)"
            )
        projected: list[Column] = []
        for bound, final in zip(self._iter_bound_columns(), names, strict=True):
            display = bound._projection_name or bound.spark_display_part()
            if final == display:
                projected.append(bound)
                continue
            # Keep origin so multi-name select identity survives the rename.
            projected.append(
                Column(
                    bound._inner.alias(final),
                    spark_display=final,
                    projection_name=final,
                    stable_name=True,
                    has_free_attribute=True,
                    sql_expr=bound._sql_expr,
                    origin_plan_id=bound._origin_plan_id,
                    origin_field=bound._origin_field,
                    join_sql_expr=bound._join_sql_expr,
                )
            )
        return self.select(*projected)

    # PySpark spells this ``withColumnsRenamed``.
    withColumnsRenamed = with_columns_renamed  # noqa: N815 — deliberate PySpark-compatible camelCase

    def transform(
        self,
        func: Callable[..., DataFrame],
        *args: Any,
        **kwargs: Any,
    ) -> DataFrame:
        """Apply ``func(self, *args, **kwargs)`` and return the result (PySpark ``transform``).

        Signature mirrors live PySpark 4.1.2
        ``(func: Callable[..., DataFrame], *args, **kwargs) -> DataFrame``. The callable must
        return a :class:`DataFrame`; a non-DataFrame return raises :class:`AssertionError` with
        Spark's message shape (``Func returned an instance of type [...], should have been
        DataFrame.``).
        """
        self._ensure_alive()
        result = func(self, *args, **kwargs)
        if not isinstance(result, DataFrame):
            raise AssertionError(
                f"Func returned an instance of type [{type(result)}], should have been DataFrame."
            )
        return result

    def dynamicFlatten(  # noqa: N802 — repark-extra camelCase surface
        self,
        *,
        separator: str = "_",
        explode_lists: bool = True,
        drop_null_lists: bool = True,
        empty_as_null: bool = True,
        max_depth: int = 100,
    ) -> DataFrame:
        """Recursively flatten structs and optionally explode lists.

        Native kernel: ``repark_core::dynamic_flatten`` and DataFusion ``Unnest``.
        ``separator`` joins parent and child names. ``explode_lists`` controls list expansion.
        ``drop_null_lists`` drops null-element lists. ``empty_as_null`` preserves parent rows for
        empty lists when true. ``max_depth`` bounds rewrite passes and raises if nesting remains.

        The rewrite is schema-only and lazy. Collisions raise ``AnalysisException``. An unchanged
        schema preserves display and origin identity; an expanding rewrite creates a new child.
        """
        self._ensure_alive()
        if not isinstance(separator, str):
            raise PySparkTypeError(
                f"separator must be str, got {type(separator).__name__}",
            )
        if isinstance(explode_lists, bool) is False:
            raise PySparkTypeError(
                f"explode_lists must be bool, got {type(explode_lists).__name__}",
            )
        if isinstance(drop_null_lists, bool) is False:
            raise PySparkTypeError(
                f"drop_null_lists must be bool, got {type(drop_null_lists).__name__}",
            )
        if isinstance(empty_as_null, bool) is False:
            raise PySparkTypeError(
                f"empty_as_null must be bool, got {type(empty_as_null).__name__}",
            )
        if isinstance(max_depth, bool) or not isinstance(max_depth, int):
            raise PySparkTypeError(
                f"max_depth must be int, got {type(max_depth).__name__}",
            )
        if max_depth < 0:
            raise PySparkValueError(f"max_depth must be >= 0, got {max_depth}")

        planned = self._plan()
        inner = planned.dynamic_flatten(
            separator,
            explode_lists,
            drop_null_lists,
            empty_as_null,
            max_depth,
        )
        if self._display_names is not None and planned.column_names() == inner.column_names():
            return self._spawn_preserving_identity(inner)
        return self._spawn(inner)

    dynamic_flatten = dynamicFlatten

    # Null handling.

    @property
    def na(self) -> DataFrameNaFunctions:
        """The missing-data surface (PySpark ``DataFrame.na``): ``fill`` / ``drop``."""
        return DataFrameNaFunctions(self)

    def fillna(
        self,
        value: Any,
        subset: str | list[str] | tuple[str, ...] | None = None,
    ) -> DataFrame:
        """Replace NULLs (PySpark ``DataFrame.fillna`` — an alias for ``df.na.fill``)."""
        return self.na.fill(value, subset)

    def dropna(
        self,
        how: str = "any",
        thresh: int | None = None,
        subset: str | list[str] | tuple[str, ...] | None = None,
    ) -> DataFrame:
        """Drop rows containing NULLs; this aliases ``df.na.drop``."""
        return self.na.drop(how, thresh, subset)

    # Write surfaces.

    @property
    def write(self) -> DataFrameWriter:
        """The write surface (PySpark ``DataFrame.write``): a :class:`DataFrameWriter`."""
        self._ensure_alive()
        return DataFrameWriter(self)

    def writeTo(self, table: str) -> DataFrameWriterV2:  # noqa: N802 — PySpark method name
        """Create a V2 table writer (PySpark ``DataFrame.writeTo``).

        Returns a :class:`DataFrameWriterV2` bound to ``table``. Routes only over the engine's
        existing CTAS / ``CREATE OR REPLACE`` / ``INSERT INTO`` / ``INSERT OVERWRITE`` paths
        (no new commit machinery).
        """
        self._ensure_alive()
        if not isinstance(table, str) or table.strip() == "":
            raise PySparkTypeError(f"writeTo table must be a non-empty str, got {table!r}")
        return DataFrameWriterV2(self, table)

    write_to = writeTo

    def mergeInto(  # noqa: N802 — PySpark method name
        self,
        table: str,
        condition: Column | str,
    ) -> MergeIntoWriter:
        """Build a ``MERGE INTO`` against ``table`` (PySpark 4.0+ ``DataFrame.mergeInto``).

        Accumulates ``whenMatched`` / ``whenNotMatched`` / ``whenNotMatchedBySource`` clauses
        on the returned :class:`~repark.merge.MergeIntoWriter`; :meth:`MergeIntoWriter.merge`
        registers this frame as a generated temp view (``__repark_merge_src_<uuid>``), runs the
        rendered SQL through :meth:`ReparkSession.sql`, and drops the view. Zero new engine
        code — the existing SQL MERGE path executes the statement.

        ``condition`` may be a :class:`Column` (rendered via Spark display text) or a bare
        column-name ``str`` (equi-join sugar: ``target.<name> = source.<name>``). Prefer
        qualified names in Column form when both sides share column names.

        ``whenNotMatchedBySource`` DELETE and UPDATE execute; ``updateAll`` emits
        ``UPDATE SET *``, which Spark parse-fails on this arm.
        """
        self._ensure_alive()
        from repark.spark.merge import MergeIntoWriter

        return MergeIntoWriter(self, table, condition)

    merge_into = mergeInto

    def _column_of(self, item: Column | str) -> Column:
        """Coerce a column-name-or-Column into a :class:`Column` bound to this frame.

        String names resolve against the frame schema (case-insensitive) with a quoted
        native identifier. Bare ``F.col(...)`` NamedExpressions
        are rebound the same way at the select/group/sort boundary so a later hop after
        ``select("X")`` still finds field ``"X"``. Casts, true aliases, and compounds pass
        through unchanged.
        """
        if isinstance(item, Column):
            # Stable-name rebind (F.col / requested spelling) then origin rebind so
            # orderBy/groupBy/select parent Columns hit the correct post-join engine field.
            return self._rebind_origin_column(self._rebind_stable_name_column(item))
        if isinstance(item, str):
            return self._bind_schema_column(item)
        raise PySparkTypeError(f"expected a column name (str) or Column, got {type(item).__name__}")

    def _cross_join_enabled(self) -> bool:
        """Return the effective cross-join setting from runtime or builder configuration."""
        store = self._alive_token.get("runtime_conf")
        if isinstance(store, dict) and "spark.sql.crossJoin.enabled" in store:
            raw = store["spark.sql.crossJoin.enabled"]
            return str(raw).lower() in {"1", "true", "yes", "on"}
        builder = self._alive_token.get("builder_config")
        if isinstance(builder, dict) and "spark.sql.crossJoin.enabled" in builder:
            raw = builder.get("spark.sql.crossJoin.enabled")
            if raw is not None:
                return str(raw).lower() in {"1", "true", "yes", "on"}
        # Spark default is true (Cartesian allowed unless conf disables).
        return True

    @staticmethod
    def _name_of(item: Column | str) -> str:
        """Return a column name for ``drop`` from a str or simple :class:`Column`."""
        if isinstance(item, str):
            return item
        if isinstance(item, Column):
            if item._projection_name is not None:
                return str(item._projection_name)
            if item._spark_display is not None:
                return str(item._spark_display)
            try:
                return str(item._inner.display_name())
            except Exception as error:
                raise PySparkTypeError(
                    f"drop expects a named Column, got unresolved expression ({error})"
                ) from error
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={
                "arg_name": "cols",
                "arg_type": type(item).__name__,
            },
        )

    def _sort_specs(
        self,
        cols: tuple[Column | str, ...],
        ascending: bool | list[bool] | None,
    ) -> tuple[list[Any], list[bool], list[bool]]:
        """Build the parallel vectors (native columns, ascending, nulls_first) for ``sort``."""
        if not cols:
            raise PySparkValueError("orderBy/sort requires at least one column")
        columns: list[Any] = []
        ascending_flags: list[bool] = []
        order_columns: list[Any] = []
        for item in cols:
            column = self._column_of(item)
            _reject_partition_transform(column)
            # Generators lower through select unnest; ordering by the placeholder is invalid.
            # ``.asc()`` and ``.desc()`` keep the sticky generator marker.
            column._reject_nested_generator("orderBy")
            is_ascending = True if column._sort_ascending is None else column._sort_ascending
            columns.append(column._inner)
            ascending_flags.append(is_ascending)
            order_columns.append(column)
        # PySpark's `DataFrame._sort_cols`:
        #     if isinstance(ascending, (bool, int)):
        #         if not ascending: jcols = [jc.desc() for jc in jcols]
        #     elif isinstance(ascending, list):
        #         jcols = [jc if asc else jc.desc() for asc, jc in zip(ascending, jcols)]
        # A FALSY entry replaces that column's marker with `desc()` — descending, nulls last. A
        # TRUTHY entry is a NO-OP: the column keeps whatever it arrived carrying, marker and all.
        # Falsy entries apply descending markers. RePark rejects a short list instead of silently
        # truncating it. Tuples are accepted as a sequence for compatibility.
        remark = self._ascending_remark_flags(len(order_columns), ascending)
        directions: list[bool] = []
        nulls_first_flags: list[bool] = []
        for column, is_ascending, remarked in zip(
            order_columns, ascending_flags, remark, strict=True
        ):
            if remarked:
                # `.desc()` — descending, nulls last.
                directions.append(False)
                nulls_first_flags.append(False)
            else:
                directions.append(is_ascending)
                nulls_first_flags.append(sort_nulls_first_for(column, is_ascending))
        return columns, directions, nulls_first_flags

    @staticmethod
    def _ascending_remark_flags(count: int, ascending: bool | list[bool] | None) -> list[bool]:
        """Which positions the ``ascending`` keyword re-marks as ``desc()`` — falsy ones only."""
        if ascending is None:
            return [False] * count
        if isinstance(ascending, bool | int):
            return [not ascending] * count
        if isinstance(ascending, (list, tuple)):
            if len(ascending) != count:
                raise PySparkValueError(
                    "ascending list length must match the number of sort columns "
                    f"({len(ascending)} != {count})"
                )
            return [not flag for flag in ascending]
        # PySpark raises NOT_BOOL_OR_LIST here, which is a `PySparkTypeError`; a wrong TYPE for
        # the keyword must not arrive as a value error.
        raise PySparkTypeError(
            f"ascending must be a bool or a list of bools, got {type(ascending).__name__}"
        )

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """Expose the Arrow C stream capsule so ``pyarrow``/``polars`` can read rows zero-copy.

        Implementing this dunder makes a ``DataFrame`` itself a valid Arrow stream source:
        ``pyarrow.table(df)`` and ``polars.from_arrow(df)`` consume it directly.
        """
        self._ensure_alive()
        return self._action_inner().__arrow_c_stream__(requested_schema)

    def count(self) -> int:
        """Return the number of rows (PySpark ``DataFrame.count``)."""
        self._ensure_alive()
        return self._action_inner().count()

    def show(
        self,
        n: int = 20,
        truncate: bool | int = True,
        vertical: bool = False,
    ) -> None:
        """Print up to ``n`` rows as a text table.

        The Spark style limits before collecting. Polars and DuckDB styles show head and tail
        rows and run an extra count. ``truncate`` controls cell width; ``vertical`` applies only
        to the Spark style. INFO logs contain counts, while row data is DEBUG-only.
        """
        self._ensure_alive()
        if self._map_bridge is not None and not (
            self._persist_requested or self._checkpoint_lazy or self._cache_view is not None
        ):
            # mapInArrow peek: re-run bridge but only materialize up to ``n`` output rows
            # ( — avoid full IPC MemTable for a head peek).
            n, cap_m, vertical = self._normalize_show_args(n, truncate, vertical)
            limit = max(0, n)
            table = self._consume_map_in_arrow_batches(max_output_rows=limit)
            if vertical:
                # Peek path does not count the full multiset (bounded materialize).
                rendered = _format_show_vertical(
                    table, truncate_at=cap_m, n=limit, total_rows=table.num_rows
                )
            else:
                rendered = _format_show_table(table, truncate_at=cap_m)
            print(rendered)
            return
        self._materialize_cache_if_needed()
        n, cap, vertical = self._normalize_show_args(n, truncate, vertical)
        style = self._resolve_display_style()
        if style == "spark":
            limit = max(0, n)
            table = self.limit(limit).to_arrow()
            if vertical:
                # Render the Spark vertical layout.
                # "only showing top N" needs a total count only when the limit may have truncated.
                total_rows: int | None = None
                if max(0, n) > 0 and table.num_rows >= max(0, n):
                    try:
                        total_rows = self.count()
                    except Exception:
                        total_rows = None
                rendered = _format_show_vertical(
                    table, truncate_at=cap, n=max(0, n), total_rows=total_rows
                )
            else:
                rendered = _format_show_table(table, truncate_at=cap)
            shown_rows = table.num_rows
        else:
            if vertical:
                warnings.warn(
                    "DataFrame.show(vertical=True) is only rendered under repark.display.style="
                    "'spark'; styled polars/duckdb shows stay horizontal.",
                    UserWarning,
                    stacklevel=2,
                )
            rendered, shown_rows = self._render_styled_show(style, n=max(0, n), truncate_at=cap)
        print(rendered)
        # SEC-008: keep only a row-count breadcrumb at INFO; the full rendered table can carry row
        # data / PII, so it goes to DEBUG (opt-in), never INFO.
        logger.info("show(%s rows)", shown_rows)
        logger.debug("show(%s rows):\n%s", shown_rows, rendered)

    def _normalize_show_args(
        self,
        n: int,
        truncate: bool | int | float | str,
        vertical: bool,
    ) -> tuple[int, int | None, bool]:
        """Validate ``show`` arguments; return ``(n, truncate_cap, vertical)``.

        Mirrors Spark 4.1.2 diagnostics used by Apache ``test_df_show`` (NOT_INT / NOT_BOOL).
        Digit-only string ``truncate`` values (e.g. ``\"1\"``) are accepted as width caps.
        """
        # bool is an int subclass: int(False)=0 / int(True)=1 would silently empty or shrink.
        if not isinstance(n, int) or isinstance(n, bool):
            raise PySparkTypeError(
                errorClass="NOT_INT",
                messageParameters={"arg_name": "n", "arg_type": type(n).__name__},
            )
        if not isinstance(vertical, bool):
            raise PySparkTypeError(
                errorClass="NOT_BOOL",
                messageParameters={
                    "arg_name": "vertical",
                    "arg_type": type(vertical).__name__,
                },
            )
        if truncate is True:
            return n, 20, vertical
        if truncate is False:
            return n, None, vertical
        if isinstance(truncate, (int, float)) and not isinstance(truncate, bool):
            width = int(truncate)
            return n, (width if width > 0 else None), vertical
        if isinstance(truncate, str) and truncate.isdigit():
            width = int(truncate)
            return n, (width if width > 0 else None), vertical
        # Spark labels non-bool non-int truncate as NOT_BOOL (live 4.1.2 oracle).
        raise PySparkTypeError(
            errorClass="NOT_BOOL",
            messageParameters={
                "arg_name": "truncate",
                "arg_type": type(truncate).__name__,
            },
        )

    def _resolve_display_style(self) -> str:
        """Return the session display style (``spark`` / ``polars`` / ``duckdb``), default spark."""
        style = self._alive_token.get("display_style", "spark")
        if isinstance(style, str) and style in {"spark", "polars", "duckdb"}:
            return style
        return "spark"

    def _preview_tail_rows(self, n: int, *, total_rows: int) -> Any:
        """Return the last ``n`` rows for display without collecting the full result."""
        import pyarrow as pa

        fetch = max(0, int(n))
        total = max(0, int(total_rows))
        if fetch == 0 or total == 0:
            return self.limit(0).to_arrow()
        # total < fetch would make skip = total - fetch negative (native skip is usize).
        # total == fetch is skip 0 — still use limit(total) so the short-frame path never
        # depends on limit_with_skip for a full-window preview.
        if total <= fetch:
            return self.limit(total).to_arrow()
        skip = total - fetch
        limited = self._spawn(self._plan().limit_with_skip(skip, fetch))
        table = limited.to_arrow()
        if not isinstance(table, pa.Table):
            return pa.table(table)
        return table

    def _render_styled_show(
        self,
        style: str,
        *,
        n: int,
        truncate_at: int | None,
    ) -> tuple[str, int]:
        """Render a styled preview and return its text and row count.

        The renderer counts once, then collects only the head and tail windows.
        """
        total_rows = self.count()
        col_names = list(self.columns)
        if style == "polars":
            # Polars default look: all rows when ≤10; else first 5 + last 5. ``n`` caps the
            # keep-set (show(0)/show(k) must not over-show) but never enlarges edges past 5.
            edge = 5
            if n <= 0:
                head_n, tail_n, use_ellipsis = 0, 0, False
            elif total_rows <= min(n, 10):
                # Entire frame fits within both n and the polars "≤10 show all" window.
                head_n, tail_n, use_ellipsis = total_rows, 0, False
            elif total_rows <= 10:
                # Frame ≤10 but n is smaller → first n rows only (no middle ellipsis).
                head_n, tail_n, use_ellipsis = n, 0, False
            else:
                # total > 10: head + … + tail; edges ≤5; total shown ≤ min(n, 10).
                keep = min(n, 2 * edge)
                head_n = min(edge, (keep + 1) // 2)
                tail_n = min(edge, keep - head_n)
                # Ellipsis only when a non-empty tail follows (show(1) → head only, no bare …).
                use_ellipsis = tail_n > 0
        else:
            # duckdb: show up to n rows; when total > n, head+tail of the window with middle dots.
            # Prefer at least one head row so show(1) shows the first row, not only the last.
            if n <= 0:
                head_n, tail_n, use_ellipsis = 0, 0, False
            elif total_rows <= n:
                head_n, tail_n, use_ellipsis = total_rows, 0, False
            else:
                head_n = n // 2
                if head_n == 0:
                    head_n = 1
                tail_n = n - head_n
                #: show(1) → head_n=1, tail_n=0 — no middle · rows with empty tail.
                use_ellipsis = tail_n > 0

        head_table = self.limit(head_n).to_arrow() if head_n > 0 else self.limit(0).to_arrow()
        # When head and tail would overlap (small frames already handled above), skip tail.
        tail_table = self._preview_tail_rows(tail_n, total_rows=total_rows) if tail_n > 0 else None
        # Precise Arrow types from the head schema (logical_schema_fields collapses i8/i16/).
        type_labels = _display_type_labels_from_arrow(head_table, style=style)

        head_rows = _table_to_cell_rows(head_table, truncate_at=truncate_at, style=style)
        tail_rows = (
            _table_to_cell_rows(tail_table, truncate_at=truncate_at, style=style)
            if tail_table is not None
            else []
        )
        shown = len(head_rows) + len(tail_rows)
        if style == "polars":
            rendered = _format_polars_show(
                col_names,
                type_labels,
                head_rows,
                tail_rows if use_ellipsis else [],
                total_rows=total_rows,
                show_ellipsis=use_ellipsis,
            )
        else:
            # Always pass the keep-set size for the footer: when shown < total (including
            # show(0) → 0), ``_format_duckdb_show`` emits ``(K shown)`` even without middle
            # ellipsis dots. Passing ``total_rows`` on the non-ellipsis path hid show(0)
            # truncation.
            rendered = _format_duckdb_show(
                col_names,
                type_labels,
                head_rows,
                tail_rows if use_ellipsis else [],
                total_rows=total_rows,
                shown_rows=shown,
                show_ellipsis=use_ellipsis,
            )
        # Log the keep-set size actually rendered (show(0) → 0), not total_rows.
        return rendered, shown

    # Arrow export errors use the facade exception taxonomy and display names stay positional.

    def _apply_export_display_names(self, table: Any) -> Any:
        """Apply display names at the Arrow boundary while preserving duplicate positions."""
        table = _strip_internal_tighten_metadata(table)
        if self._display_names is None or self._engine_names is None:
            return table
        display = list(self._display_names)
        if len(display) != table.num_columns:
            return table
        if list(table.column_names) == display:
            return table
        return table.rename_columns(display)

    def collect(self) -> list[Row]:
        """Materialize all rows as a list of ``Row`` objects.

        Map cells become dictionaries. Calendar intervals raise ``PySparkNotImplementedError``.
        The result uses O(rows) Python memory; use ``toLocalIterator`` for streaming.
        """
        # Convert batches directly so collect does not hold a second full Arrow table.
        rows: list[Row] = []
        for batch in self.to_arrow_batches():
            rows.extend(DataFrame._rows_from_arrow_table(batch))
        return rows

    def take(self, num: int) -> list[Row]:
        """Return the first ``num`` rows as a list.

        Negative values raise ``AnalysisException`` with Spark's invalid-limit error class.
        Pending cache or persist requests materialize before the limited action.
        """
        if self._map_bridge is not None and not (
            self._persist_requested or self._checkpoint_lazy or self._cache_view is not None
        ):
            # Re-run map bridge but only keep ``num`` output rows.
            limit_count = self._require_non_negative_limit(num)
            if limit_count == 0:
                return []
            table = self._consume_map_in_arrow_batches(max_output_rows=limit_count)
            names = table.column_names
            rows: list[Row] = []
            for mapping in table.to_pylist():
                ordered = {name: mapping.get(name) for name in names}
                for value in ordered.values():
                    _refuse_calendar_interval_python_value(value)
                rows.append(Row.from_mapping(ordered))
            return rows
        self._materialize_cache_if_needed()
        limit_count = self._require_non_negative_limit(num)
        return self.limit(limit_count).collect()

    @overload
    def head(self) -> Row | None:
        """Typing overload: no argument → one :class:`~repark.row.Row`, or ``None`` if empty."""

    @overload
    def head(self, n: int) -> list[Row]:
        """Typing overload: an ``int`` argument → a ``list`` of at most ``n`` rows."""

    def head(self, n: int | None = None) -> Row | list[Row] | None:
        """Return the first row, or the first ``n`` rows (PySpark ``DataFrame.head``).

        * ``head()`` (no argument) → a single :class:`~repark.row.Row`, or ``None`` when empty.
        * ``head(n)`` → a ``list`` of :class:`~repark.row.Row` of length ``n`` (or fewer if the
          frame is shorter). ``head(0)`` → ``[]``.

        The result is loaded into driver memory. Negative ``n`` raises ``AnalysisException``.
        """
        if n is None:
            rows = self.head(1)
            return rows[0] if rows else None
        return self.take(n)

    def first(self) -> Row | None:
        """Return the first row as a :class:`~repark.row.Row`, or ``None`` if empty.

        PySpark ``DataFrame.first``. Equivalent to :meth:`head` with no argument (live
        PySpark 4.1.2: ``return self.head()``).
        """
        return self.head()

    def tail(self, num: int) -> list[Row]:
        """Return the last ``num`` rows as a list.

        The full result is collected before slicing. Non-positive values return an empty list.
        """
        # Live PySpark routes ``tail`` through JVM ``tailToPython`` and accepts a negative as
        # empty (unlike ``take``/``head``/``limit``, which raise AnalysisException). Match that.
        if isinstance(num, bool) or not isinstance(num, int):
            raise PySparkTypeError(f"Argument `num` should be a int, got {type(num).__name__}.")
        # Must gate stopped sessions even when num<=0 short-circuits (take(0)/isEmpty fail loud
        # via limit/collect; returning [] after stop would be a silent wrong lifecycle outcome).
        self._ensure_alive()
        if num <= 0:
            return []
        rows = self.collect()
        if num >= len(rows):
            return rows
        return rows[-num:]

    def isEmpty(self) -> bool:  # noqa: N802 — PySpark camelCase surface
        """Return ``True`` when the DataFrame has no rows.

        The check limits the plan to one row and materializes pending cache requests.
        """
        if self._map_bridge is not None and not (
            self._persist_requested or self._checkpoint_lazy or self._cache_view is not None
        ):
            # Stop after the first output row.
            return self._consume_map_in_arrow_batches(max_output_rows=1).num_rows == 0
        self._materialize_cache_if_needed()
        return self.limit(1).count() == 0

    # Snake_case alias — not a PySpark name; convenient for Python call sites.
    is_empty = isEmpty

    def toLocalIterator(  # noqa: N802 — PySpark camelCase surface
        self,
        prefetchPartitions: bool = False,  # noqa: N803 — PySpark parameter spelling
    ) -> Iterator[Row]:
        """Return a lazy iterator of ``Row`` objects.

        Arrow memory stays O(batch) while the iterator is consumed. Converting it to a list uses
        O(rows) Python memory. ``prefetchPartitions`` is accepted and ignored.
        """
        del prefetchPartitions  # signature parity only
        # Honest streaming: pull RecordBatches via the C-stream, convert one batch at a time.
        yield from self._iter_rows_streaming()

    # Snake_case alias — not a PySpark name; convenient for Python call sites.
    to_local_iterator = toLocalIterator

    def _iter_rows_streaming(self) -> Iterator[Row]:
        """Yield ``Row``s from the Arrow C stream without materializing the full table (P2b)."""
        for batch in self.to_arrow_batches():
            yield from self._iter_rows_from_record_batch(batch)

    @staticmethod
    def _iter_rows_from_record_batch(batch: Any) -> Iterator[Row]:
        """Convert one ``pyarrow.RecordBatch`` into :class:`~repark.row.Row` (collect parity)."""
        # Collect rows directly from each batch.
        # RecordBatch shares column/schema APIs with Table — skip Table.from_batches wrap.
        yield from DataFrame._rows_from_arrow_table(batch)

    @staticmethod
    def _iter_rows_from_arrow_table(table: Any) -> Iterator[Row]:
        """Yield rows from an Arrow ``Table`` / ``RecordBatch`` (stream + profile entry point)."""
        yield from DataFrame._rows_from_arrow_table(table)

    @staticmethod
    def _arrow_type_needs_spark_python_convert(arrow_type: Any) -> bool:
        """True when cells need ``_arrow_cell_to_spark_python`` (map / tz-aware timestamp)."""
        from repark.spark.dataframe.rows_export import arrow_type_needs_spark_python_convert

        return arrow_type_needs_spark_python_convert(arrow_type)

    @staticmethod
    def _arrow_type_may_hold_calendar_interval(arrow_type: Any) -> bool:
        """Return whether an Arrow type can contain a calendar interval."""
        from repark.spark.dataframe.rows_export import arrow_type_may_hold_calendar_interval

        return arrow_type_may_hold_calendar_interval(arrow_type)

    @staticmethod
    def _rows_from_arrow_table(table: Any) -> list[Row]:
        """Convert an Arrow table or batch to ``Row`` objects by column position.

        Map values become dictionaries, and calendar intervals are refused.
        """
        from repark.spark.dataframe.rows_export import rows_from_arrow_table

        return rows_from_arrow_table(table)

    @staticmethod
    def _require_non_negative_limit(num: int) -> int:
        """Validate a take/head count; raise Spark-shaped AnalysisException if negative."""
        if isinstance(num, bool) or not isinstance(num, int):
            raise PySparkTypeError(f"Argument `num` should be a int, got {type(num).__name__}.")
        if num < 0:
            # Live PySpark 4.1.2 (zulu-17): AnalysisException
            # [INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE] The limit like expression "-1" is
            # invalid. The limit expression must be equal to or greater than 0, but got -1.
            # SQLSTATE: 42K0E; + a plan dump. repark drops SQLSTATE and the plan dump (no repark
            # error carries SQLSTATE; plan text is engine-internal).
            raise AnalysisException(
                f"[INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE] The limit like expression "
                f'"{num}" is invalid. The limit expression must be equal to or greater than 0, '
                f"but got {num}."
            )
        return num

    def to_arrow(self) -> pa.Table:
        """Return the full result as a ``pyarrow.Table``.

        Mid-stream engine errors become ``PySparkException`` with the engine message. Plan-time
        errors keep their original classification. Use ``to_arrow_batches`` for O(batch) memory.
        """
        self._ensure_alive()
        import pyarrow as pa

        try:
            table = pa.table(self)
        except pa.lib.ArrowException as arrow_error:
            raise _export_engine_error(arrow_error) from arrow_error
            raise PySparkException(str(arrow_error)) from arrow_error
        return self._apply_export_display_names(table)

    def to_arrow_batches(self) -> Iterator[Any]:
        """Yield Arrow record batches lazily.

        Memory stays O(batch), and mid-stream errors become ``PySparkException``. Empty streams
        yield one zero-row batch with the declared schema. This is a RePark extension.
        """
        self._ensure_alive()
        import pyarrow as pa

        try:
            reader = pa.RecordBatchReader.from_stream(self)
        except pa.lib.ArrowException as arrow_error:
            raise _export_engine_error(arrow_error) from arrow_error
        # Capture schema before drain — empty streams yield no batches from the reader, but the
        # C-stream still declares a schema (same source :meth:`to_arrow` uses).
        stream_schema = reader.schema
        yielded_batch = False
        try:
            for batch in reader:
                yielded_batch = True
                yield self._apply_export_display_names(batch)
        except pa.lib.ArrowException as arrow_error:
            raise _export_engine_error(arrow_error) from arrow_error
        if not yielded_batch:
            # Preserve the declared schema when the stream has no rows.
            empty = pa.RecordBatch.from_pylist([], schema=stream_schema)
            yield self._apply_export_display_names(empty)

    # CamelCase alias for the repark batch iterator (disclosed extension; not PySpark).
    toArrowBatches = to_arrow_batches  # noqa: N815 — deliberate camelCase twin of to_arrow_batches

    def to_polars(self) -> pl.DataFrame:
        """Return the rows as a Polars DataFrame through Arrow.

        Requires the optional ``polars`` extra. Duplicate display names receive occurrence
        suffixes because Polars requires unique names.
        """
        import polars as pl

        table = self.to_arrow()
        names = list(table.column_names)
        if len(names) != len(set(names)):
            seen: dict[str, int] = {}
            unique: list[str] = []
            for name in names:
                occurrence = seen.get(name, 0)
                seen[name] = occurrence + 1
                if occurrence == 0:
                    unique.append(name)
                else:
                    unique.append(f"{name}__{occurrence}")
            table = table.rename_columns(unique)
        return pl.from_arrow(table)

    def to_pandas(self) -> pd.DataFrame:
        """Return the rows as a :class:`pandas.DataFrame` (PySpark ``DataFrame.toPandas``).

        Conversion goes through Arrow (:meth:`to_arrow`, then ``pyarrow.Table.to_pandas``) — the
        same path PySpark takes with ``spark.sql.execution.arrow.pyspark.enabled=true``, so dtypes
        match Arrow-enabled PySpark rather than the legacy row-based converter. Requires the
        optional ``pandas`` extra (``pip install 'repark[pandas]'``).
        """
        return self.to_arrow().to_pandas()

    # PySpark spells this ``toPandas``; expose both so the one-line import swap just works.
    toPandas = to_pandas  # noqa: N815 — deliberate PySpark-compatible camelCase alias

    def to_numpy(self) -> np.ndarray:
        """Return the rows as a 2-D :class:`numpy.ndarray` (a RePark extension; not a PySpark API).

        Built for the feed-the-model path: each column converts via Arrow and the columns are
        stacked with ``numpy.column_stack``, promoting to a common dtype — an all-numeric frame
        yields a numeric matrix, mixed types promote to ``object``. Numeric columns containing
        nulls convert to ``float64`` with ``NaN``. Requires the optional ``numpy`` extra
        (``pip install 'repark[numpy]'``; already present if pandas or pyarrow<18 is installed).
        """
        import numpy as np

        table = self.to_arrow()
        if table.num_columns == 0:
            return np.empty((table.num_rows, 0))
        return np.column_stack([column.to_numpy(zero_copy_only=False) for column in table.columns])


# Re-export bindings. Keep plan_collapse first because sibling modules import its helpers.
from repark.spark.dataframe.plan_collapse import (  # noqa: E402, I001
    _G2_RANGE_NUMERIC_DTYPES,
    _global_agg_sql_parts,
    _is_native_pure_global_aggregate,
    _pandas_udf_window_frame_bounds,
    _parse_count_distinct_simple_names,
    _reject_aggregate_in_with_column,
    _reject_partition_transform,
    _QCOL_SIDE_BOUNDARY_RE,
    _QCOL_TOKEN_RE,
    _arrow_debug_type_to_sql,
    _arrow_pa_type_label,
    _cell_text,
    _collapse_identity_projection_alias,
    _column_may_reference_names,
    _column_widths,
    _column_window_spec,
    _data_type_has_required_child,
    _decode_qcol_field,
    _display_type_labels_from_arrow,
    _format_duckdb_show,
    _format_eager_eval_table,
    _format_polars_show,
    _format_show_table,
    _format_show_vertical,
    _g2_dtype_is_range_numeric,
    _is_compound_sql_expr,
    _list_field_element_debug,
    _null_safe_equi_join_sql,
    _output_field_would_persist_required,
    _parse_list_element_sql_type,
    _reject_non_numeric_range_order,
    _rewrite_join_qcol_sql,
    _rewrite_qcol_tokens_local,
    _same_object_qcol_alternation_safe,
    _spark_array_element_to_sql,
    _UNTYPED_NULL_ELEMENT,
    _sql_embed_expr_fragment,
    _sql_ident_bare_name,
    _sql_string_literal,
    _strip_internal_tighten_metadata,
    _style_type_label,
    _table_to_cell_rows,
    _uniform_window_key_from_map,
    _window_spec_structural_key,
)
from repark.spark.dataframe.actions_export import DataFrameNaFunctions  # noqa: E402
from repark.spark.dataframe.joins_columns import (  # noqa: E402
    GroupedData,
    _pivot_agg_output_suffix,
    _pivot_aggregate_builder,
    _pivot_aggregate_input,
    _pivot_column_engine_type,
    _pivot_count_one_is_row_count,
    _pivot_is_count_distinct_name,
    _pivot_is_typed_scalar_inner,
    _pivot_max_values,
    _pivot_native_shows_typed_literal,
    _pivot_recover_agg_name,
    _pivot_sort_discovered_values,
    _pivot_value_column_name,
)
from repark.spark.dataframe.writer_readwriter import (  # noqa: E402
    DataFrameStatFunctions,
    DataFrameWriter,
    DataFrameWriterV2,
    _merge_path_write_tree,
    _normalize_parquet_write_compression,
    _normalize_write_compression,
    _resolve_writer_table,
    _sql_option_escape,
)

__all__ = [
    "DataFrame",
    "DataFrameNaFunctions",
    "DataFrameStatFunctions",
    "DataFrameWriter",
    "DataFrameWriterV2",
    "GroupedData",
]
