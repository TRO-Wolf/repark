"""Plan-collapse / show-format / qcol-rewrite region — module-level helpers (r27 T0b).

Extracted from ``core.py`` (SE-1 PR-B headroom): the r23b N2 plan-collapse
helpers plus the show/eager-eval formatters, Arrow type labels, generator SQL-type
mapping and the H1 join-qcol rewriters that trailed them. ``core.py`` re-exports
every name from its tail bind block, so ``repark.spark.dataframe.core`` /
``repark.spark.dataframe`` import paths are unchanged (Q7 import freeze).
**SE-1 R-3:** also owns ``_strip_internal_tighten_metadata`` (export-boundary
strip of ``repark.tighten_nulls``).

Nothing here imports ``core`` at module scope (the region modules' circular-import rule);
the one ``core``-side type it needs is a ``TYPE_CHECKING`` annotation only.
"""

from __future__ import annotations

import functools
import logging
import re
from typing import TYPE_CHECKING, Any

from repark.errors import AnalysisException
from repark.spark._idents import quote_ident as _quote_ident_sql
from repark.spark.column import Column

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame

logger = logging.getLogger("repark.spark.dataframe")


def _output_field_would_persist_required(field: Any) -> bool:
    """True when this field or a nested child would persist Iceberg-required."""
    if not field.nullable:
        return True
    return _data_type_has_required_child(field.dataType)


def _data_type_has_required_child(data_type: Any) -> bool:
    """Walk Struct / Array / Map the way the engine ``field_or_child`` helper does."""
    children = getattr(data_type, "fields", None)
    if children is not None:
        return any(_output_field_would_persist_required(child) for child in children)
    element = getattr(data_type, "elementType", None)
    if element is not None:
        if not getattr(data_type, "containsNull", True):
            return True
        return _data_type_has_required_child(element)
    value_type = getattr(data_type, "valueType", None)
    if value_type is not None:
        if not getattr(data_type, "valueContainsNull", True):
            return True
        return _data_type_has_required_child(value_type)
    return False


def _strip_tighten_nulls_field(
    field: Any,
    *,
    key: bytes,
    depth: int = 0,
) -> tuple[Any, bool]:
    """Walk one Arrow field, dropping ``repark.tighten_nulls`` metadata (depth-bounded)."""
    if depth > 32:
        return field, False
    changed = False
    meta = field.metadata
    if meta and key in meta:
        meta = {k: v for k, v in meta.items() if k != key}
        changed = True
    new_type, type_changed = _strip_tighten_nulls_type(field.type, key=key, depth=depth)
    changed = changed or type_changed
    if not changed:
        return field, False
    return field.with_type(new_type).with_metadata(meta), True


def _strip_tighten_nulls_type(data_type: Any, *, key: bytes, depth: int) -> tuple[Any, bool]:
    """Walk one Arrow type, stripping tighten-nulls metadata from nested fields."""
    import pyarrow as pa

    if pa.types.is_struct(data_type):
        fields = []
        changed = False
        for index in range(data_type.num_fields):
            child, child_changed = _strip_tighten_nulls_field(
                data_type.field(index), key=key, depth=depth + 1
            )
            fields.append(child)
            changed = changed or child_changed
        return (pa.struct(fields) if changed else data_type, changed)
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        child, child_changed = _strip_tighten_nulls_field(
            data_type.value_field, key=key, depth=depth + 1
        )
        if not child_changed:
            return data_type, False
        if pa.types.is_large_list(data_type):
            return pa.large_list(child), True
        return pa.list_(child), True
    if pa.types.is_map(data_type):
        value_field, value_changed = _strip_tighten_nulls_field(
            data_type.item_field, key=key, depth=depth + 1
        )
        if not value_changed:
            return data_type, False
        return pa.map_(data_type.key_field, value_field), True
    return data_type, False


def _strip_internal_tighten_metadata(table: Any) -> Any:
    """Drop ``repark.tighten_nulls`` from user-visible Arrow export (not a data column)."""
    import pyarrow as pa

    key = b"repark.tighten_nulls"
    fields = []
    changed = False
    for field in table.schema:
        new_field, field_changed = _strip_tighten_nulls_field(field, key=key)
        fields.append(new_field)
        changed = changed or field_changed
    schema_meta = table.schema.metadata
    if schema_meta and key in schema_meta:
        schema_meta = {k: v for k, v in schema_meta.items() if k != key}
        changed = True
    if not changed:
        return table
    new_schema = pa.schema(fields, metadata=schema_meta)
    return type(table).from_arrays(list(table.columns), schema=new_schema)


# ==================================================================================================
# r23b N2: plan-collapse helpers (alias-chain squash + adjacent same-spec window merge)
# ==================================================================================================


def _collapse_identity_projection_alias(column: Column) -> Column:
    """Stage (a) + r25 T3: peel nested identity Alias, then skip redundant for_select.

    ``withColumns`` passthrough binds via ``native.alias(name)`` then ``select`` used to call
    ``for_select`` which re-aliased again → logical ``x AS x AS x`` chains. N2 skipped
    ``for_select`` when ``display_name`` already matched the projection name.

    Residual (r25 T3 / greylit Q7): re-aliasing still re-entered as *nested native Alias*
    nodes when a path (or user ``.alias(...).alias(...)``) stacked aliases before select —
    ``display_name()`` only surfaces the outer name, so the N2 gate returned the column
    unchanged while the plan still showed ``… AS x AS x`` or ``… AS a AS b``. Extend **this**
    helper only (no second collapse path): peel nested Alias chains on the native expr via
    ``PyColumn.collapse_identity_aliases`` (outermost name, single Alias), then apply the
    original for_select gate.

    Case renames (engine ``x``, projection ``X``) still hold a single alias from bind and are
    not re-aliased here. Compounds / casts whose native display differs still take
    ``for_select``. H1 multi-name synthetic engines are already uniquely named before this.
    """
    if column._projection_name is None:
        return column
    # Peel same-name nested Alias on the native expr (single collapse path — Q7).
    try:
        peeled_inner = column._inner.collapse_identity_aliases()
        column = Column(
            peeled_inner,
            sort_ascending=column._sort_ascending,
            sort_nulls_first=column._sort_nulls_first,
            when_pairs=column._when_pairs,
            agg_name=column._agg_name,
            is_aggregate=column._is_aggregate,
            is_foldable=column._is_foldable,
            has_free_attribute=column._has_free_attribute,
            has_ungroupable=column._has_ungroupable,
            is_aggregate_function=column._is_aggregate_function,
            generator=column._generator,
            generator_cast=column._generator_cast,
            spark_display=column._spark_display,
            projection_name=column._projection_name,
            stable_name=column._stable_name,
            partition_transform=column._partition_transform,
            sql_expr=column._sql_expr,
            origin_plan_id=column._origin_plan_id,
            origin_field=column._origin_field,
            join_sql_expr=column._join_sql_expr,
            g2_range_order_names=column._g2_range_order_names,
            window_spec=column._window_spec,
        )
    except AttributeError:
        # Native peel unavailable (older native module) — keep N2 for_select gate only.
        pass
    except Exception:
        # A fenced! engine failure is a real error, not "unavailable" — surface it in logs
        # instead of silently keeping the stacked aliases the peel exists to remove
        # (r25 morning critic).
        logger.debug("native identity-alias peel failed; keeping unpeeled expr", exc_info=True)
    if not column._stable_name:
        return column.for_select()
    try:
        current_name = column._inner.display_name()
    except Exception:
        return column.for_select()
    if current_name == column._projection_name:
        return column
    return column.for_select()


def _window_spec_structural_key(spec: Any) -> tuple[Any, ...] | None:
    """Structural equality key for a facade ``WindowSpec`` (partition / order / frame)."""
    try:
        partitions = tuple(
            column._projection_name or column.spark_display_part()
            for column in spec._partition_columns
        )
        orders = tuple(
            (
                column._projection_name or column.spark_display_part(),
                True if column._sort_ascending is None else bool(column._sort_ascending),
                # Null placement is part of the spec's identity: two window specs that differ
                # only in it are different windows, and merging them silently reorders rows.
                column._sort_nulls_first,
            )
            for column in spec._order_columns
        )
        frame = (spec._frame_units, spec._frame_start, spec._frame_end)
        return (partitions, orders, frame)
    except Exception:
        return None


def _column_window_spec(column: Column) -> Any | None:
    """Facade WindowSpec retained after ``Column.over`` (and alias/round wraps)."""
    return getattr(column, "_window_spec", None)


def _uniform_window_key_from_map(cols_map: dict[str, Any]) -> tuple[Any, ...] | None:
    """Structural window key when every value is a same-spec window (or alias/round wrap).

    Returns ``None`` when the map is empty, contains non-Column values (UDF markers), has
    any non-window Column, has mixed window specs, or a spec cannot be keyed. When in doubt
    the caller must not merge (Q16).
    """
    if not cols_map:
        return None
    keys: list[tuple[Any, ...]] = []
    for value in cols_map.values():
        if not isinstance(value, Column):
            return None
        spec = _column_window_spec(value)
        if spec is None:
            return None
        key = _window_spec_structural_key(spec)
        if key is None:
            return None
        keys.append(key)
    first = keys[0]
    if any(key != first for key in keys[1:]):
        return None
    return first


def _column_may_reference_names(column: Column, names: frozenset[str]) -> bool:
    """True when ``column`` may read any name in ``names`` (exact-enough dep gate for N2).

    Prefer over-merge refusal: if the expression text cannot be inspected, return True so
    the caller blocks the merge (Q16). Word-boundary matching avoids ``tr`` ⊆ ``trange``.
    """
    if not names:
        return False
    if (
        column._has_free_attribute
        and column._stable_name
        and column._projection_name is not None
        and column._projection_name in names
    ):
        return True
    texts: list[str] = []
    for attr in ("_spark_display", "_sql_expr", "_projection_name", "_join_sql_expr"):
        value = getattr(column, attr, None)
        if value is not None:
            texts.append(str(value))
    try:
        texts.append(str(column._inner.display_name()))
    except Exception:
        return True
    blob = " ".join(texts)
    for name in names:
        if not name:
            continue
        if f'"{name}"' in blob:
            return True
        if re.search(rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])", blob):
            return True
    return False


# Spark dtypes that satisfy RANGE value-offset ORDER BY (NUMERIC / INTERVAL family).
_G2_RANGE_NUMERIC_DTYPES = frozenset(
    {
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "float",
        "double",
        "decimal",
        "byte",
        "short",
        "long",
        "integer",
    }
)


def _g2_dtype_is_range_numeric(type_name: str) -> bool:
    """True when a Spark-style dtype string is legal for RANGE value-offset ORDER BY."""
    normalized = type_name.strip().lower()
    if normalized in _G2_RANGE_NUMERIC_DTYPES:
        return True
    if normalized.startswith("decimal"):
        return True
    return "interval" in normalized


def _reject_non_numeric_range_order(frame: DataFrame, column: Column) -> None:
    """Refuse value-offset RANGE windows whose ORDER BY is non-numeric (r20 G2 octo C1-Q-002).

    Spark ``DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE``. Peer-only RANGE
    frames do not set ``_g2_range_order_names`` and skip this check.
    """
    # === r20 G2: window/rand/sampleBy ===
    names = getattr(column, "_g2_range_order_names", None)
    if not names:
        return
    dtype_by_name = dict(frame.dtypes)
    for order_name in names:
        type_name = dtype_by_name.get(order_name)
        if type_name is None:
            # Expression ORDER BY without a bare schema name — engine residual.
            continue
        if not _g2_dtype_is_range_numeric(type_name):
            raise AnalysisException(
                "[DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE] Cannot resolve "
                "RANGE window frame due to data type mismatch: The data type of the order "
                f"key {order_name!r} ({type_name!r}) does not match the expected data type "
                '("NUMERIC" or "INTERVAL"). SQLSTATE: 42K09'
            )


def _sql_string_literal(value: str) -> str:
    """Single-quote a SQL string literal, doubling embedded quotes."""
    return "'" + value.replace("'", "''") + "'"


def _show_grid_row(cells: list[str], widths: list[int]) -> str:
    """Render one ``| a | b |`` Spark-show grid line, each cell left-padded to its column width."""
    return "| " + " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(cells)) + " |"


def _eager_eval_grid_row(cells: list[str], widths: list[int]) -> str:
    """Render one abutted ``|a|b|`` REPL line, each cell right-aligned to its column width."""
    return "|" + "|".join(cell.rjust(widths[i]) for i, cell in enumerate(cells)) + "|"


def _format_show_table(table: Any, *, truncate_at: int | None) -> str:
    """Render a small Arrow table as a PySpark-style ASCII grid."""
    names = list(table.column_names)
    raw_rows = _table_to_cell_rows(table, truncate_at=truncate_at, style="spark")
    widths = [len(name) for name in names]
    for row in raw_rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    separator = "+-" + "-+-".join("-" * width for width in widths) + "-+"
    lines = [separator, _show_grid_row(names, widths), separator]
    for row in raw_rows:
        lines.append(_show_grid_row(row, widths))
    lines.append(separator)
    return "\n".join(lines)


def _format_eager_eval_table(table: Any, *, truncate_at: int | None) -> str:
    """Spark ``Dataset.showString`` packing for ``spark.sql.repl.eagerEval`` (r20 G2).

    Matches Apache ``test_repr_behaviors``: ``|`` abut cells (no spaces), cells right-aligned
    to the column max width, separator ``+---+`` without the spaced ``+- -+`` form used by
    :func:`_format_show_table` (kept for ``DataFrame.show`` stability). Truncation is a hard
    left-slice (Spark ``StringUtils.left``-style), not the ``…`` ellipsis used by ``show``.
    """
    names = list(table.column_names)
    # Hard-slice truncate for REPL parity (ellipsis would widen cells past Spark's pin).
    raw_rows = _table_to_cell_rows(table, truncate_at=None, style="spark")
    if truncate_at is not None and truncate_at > 0:
        raw_rows = [[cell[:truncate_at] for cell in row] for row in raw_rows]
    widths = [len(name) for name in names]
    for row in raw_rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    separator = "+" + "+".join("-" * width for width in widths) + "+"
    lines = [separator, _eager_eval_grid_row(names, widths), separator]
    for row in raw_rows:
        lines.append(_eager_eval_grid_row(row, widths))
    lines.append(separator)
    return "\n".join(lines)


def _format_show_vertical(
    table: Any,
    *,
    truncate_at: int | None,
    n: int,
    total_rows: int | None,
) -> str:
    """Render Arrow rows as live PySpark ``show(vertical=True)`` layout (R-PARITY3).

    Shape (oracle 4.1.2)::

        -RECORD 0---------------
         a   | 1
         b   | hello_world_long
        -RECORD 1---------------
         a   | 2
         b   | y
        only showing top 1 row   # when total_rows > n
    """
    names = list(table.column_names)
    raw_rows = _table_to_cell_rows(table, truncate_at=truncate_at, style="spark")
    name_width = max((len(name) for name in names), default=0)
    # Body line: " name | value" with name left-justified to name_width.
    body_widths: list[int] = []
    for row in raw_rows:
        for name, cell in zip(names, row, strict=True):
            body_widths.append(len(f" {name.ljust(name_width)} | {cell} "))
    content_width = max(body_widths, default=len("-RECORD 0-"))
    lines: list[str] = []
    for row_index, row in enumerate(raw_rows):
        header = f"-RECORD {row_index}-"
        if len(header) < content_width:
            header = header + ("-" * (content_width - len(header)))
        lines.append(header)
        for name, cell in zip(names, row, strict=True):
            lines.append(f" {name.ljust(name_width)} | {cell}")
    if total_rows is not None and n >= 0 and total_rows > n and n > 0:
        unit = "row" if n == 1 else "rows"
        lines.append(f"only showing top {n} {unit}")
    # Spark ends with a trailing newline when printing; print() adds one, so no extra here.
    return "\n".join(lines)


def _cell_text(value: Any, *, style: str, truncate_at: int | None) -> str:
    """Format one cell for a show style (null/NaN/bool spellings differ by style).

    Booleans use lowercase ``true``/``false`` (Spark / polars / duckdb oracles) — never Python
    ``True``/``False``. ``bool`` is checked before other numeric branches because ``bool`` is an
    ``int`` subclass.
    """
    if value is None:
        text = "null" if style == "polars" else "NULL"
    elif isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, float) and value != value:  # NaN
        text = "NaN" if style == "polars" else "nan"
    else:
        text = str(value)
    # Only positive caps truncate (Spark: truncate>0). Zero/negative would blank or chop (C6-L-001).
    if truncate_at is not None and truncate_at > 0 and len(text) > truncate_at:
        text = text[: max(0, truncate_at - 3)] + "..." if truncate_at >= 3 else text[:truncate_at]
    return text


def _table_to_cell_rows(
    table: Any,
    *,
    truncate_at: int | None,
    style: str,
) -> list[list[str]]:
    """Convert an Arrow table to string cell rows for a show style."""
    names = list(table.column_names)
    rows: list[list[str]] = []
    for mapping in table.to_pylist():
        rows.append(
            [_cell_text(mapping.get(name), style=style, truncate_at=truncate_at) for name in names]
        )
    return rows


def _display_type_labels_from_arrow(table: Any, *, style: str) -> list[str]:
    """Short dtype/type-row labels from a collected Arrow table's precise field types.

    Prefer this over ``logical_schema_fields`` for styled show: the native ``arrow_type_key``
    collapses Int8/Int16→``int`` and Float32→``double`` for the coarse Spark ``StructType``
    surface, which would mislabel TINYINT/SMALLINT/FLOAT as i32/f64 (int32/double).
    """
    return [_arrow_pa_type_label(field.type, style=style) for field in table.schema]


def _arrow_pa_type_label(arrow_type: Any, *, style: str) -> str:
    """Map a ``pyarrow.DataType`` to a polars- or duckdb-style display label."""
    import pyarrow.types as pat

    if pat.is_int8(arrow_type):
        return "i8" if style == "polars" else "int8"
    if pat.is_int16(arrow_type):
        return "i16" if style == "polars" else "int16"
    if pat.is_int32(arrow_type):
        return "i32" if style == "polars" else "int32"
    if pat.is_int64(arrow_type):
        return "i64" if style == "polars" else "int64"
    if pat.is_uint8(arrow_type):
        return "u8" if style == "polars" else "uint8"
    if pat.is_uint16(arrow_type):
        return "u16" if style == "polars" else "uint16"
    if pat.is_uint32(arrow_type):
        return "u32" if style == "polars" else "uint32"
    if pat.is_uint64(arrow_type):
        return "u64" if style == "polars" else "uint64"
    if pat.is_float16(arrow_type) or pat.is_float32(arrow_type):
        return "f32" if style == "polars" else "float"
    if pat.is_float64(arrow_type):
        return "f64" if style == "polars" else "double"
    if pat.is_boolean(arrow_type):
        return "bool" if style == "polars" else "boolean"
    if (
        pat.is_string(arrow_type)
        or pat.is_large_string(arrow_type)
        or getattr(pat, "is_string_view", lambda _t: False)(arrow_type)
    ):
        return "str" if style == "polars" else "varchar"
    if pat.is_date(arrow_type):
        return "date"
    if pat.is_timestamp(arrow_type):
        return "datetime[μs]" if style == "polars" else "timestamp"
    if pat.is_decimal(arrow_type):
        precision = arrow_type.precision
        scale = arrow_type.scale
        return f"decimal({precision},{scale})"
    # Fallback: reuse the coarse logical-key mapper on a stringified type.
    return _style_type_label(str(arrow_type), style=style)


def _style_type_label(type_key: str, *, style: str) -> str:
    """Map a logical type key (or Arrow type string) to a short polars/duckdb display label."""
    key = type_key.lower()
    if style == "polars":
        if key in {"int8", "byte", "tinyint"}:
            return "i8"
        if key in {"int16", "short", "smallint"}:
            return "i16"
        if key in {"int", "integer", "int32"}:
            return "i32"
        if key in {"long", "bigint", "int64"}:
            return "i64"
        if key in {"double", "float64"}:
            return "f64"
        if key in {"float", "float32", "real"}:
            return "f32"
        if key in {"string", "varchar", "utf8", "large_string", "string_view"}:
            return "str"
        if key in {"boolean", "bool"}:
            return "bool"
        if key.startswith("decimal"):
            return key  # decimal(p,s)
        if key in {"date"}:
            return "date"
        if key.startswith("timestamp"):
            return "datetime[μs]"
        return key
    # duckdb-ish
    if key in {"int8", "byte", "tinyint"}:
        return "int8"
    if key in {"int16", "short", "smallint"}:
        return "int16"
    if key in {"int", "integer", "int32"}:
        return "int32"
    if key in {"long", "bigint", "int64"}:
        return "int64"
    if key in {"double", "float64"}:
        return "double"
    if key in {"float", "float32", "real"}:
        return "float"
    if key in {"string", "varchar", "utf8", "large_string", "string_view"}:
        return "varchar"
    if key in {"boolean", "bool"}:
        return "boolean"
    if key.startswith("decimal"):
        return key
    if key in {"date"}:
        return "date"
    if key.startswith("timestamp"):
        return "timestamp"
    return key


def _column_widths(
    names: list[str],
    type_labels: list[str],
    *row_groups: list[list[str]],
    min_width: int = 1,
) -> list[int]:
    """Compute per-column display widths from headers, types, and data rows."""
    widths = [
        max(min_width, len(name), len(type_labels[i] if i < len(type_labels) else ""))
        for i, name in enumerate(names)
    ]
    for rows in row_groups:
        for row in rows:
            for index, cell in enumerate(row):
                if index < len(widths):
                    widths[index] = max(widths[index], len(cell))
    return widths


def _box_rule(
    segment_widths: list[int],
    left: str,
    mid: str,
    right: str,
    fill: str = "─",
) -> str:
    """Join ``fill * width`` segments with ``mid``, capped by ``left`` / ``right`` glyphs."""
    return left + mid.join(fill * width for width in segment_widths) + right


def _polars_row_line(cells: list[str], widths: list[int], *, align: str = "left") -> str:
    """Render one polars body line; ``align="center"`` centres each cell in its column."""
    parts: list[str] = []
    for index, width in enumerate(widths):
        cell = cells[index] if index < len(cells) else ""
        if align == "center":
            parts.append(f" {cell.center(width)} ")
        else:
            parts.append(f" {cell.ljust(width)} ")
    return "│" + "┆".join(parts) + "│"


def _duckdb_cell_is_numeric(text: str) -> bool:
    """Report whether a rendered cell should right-align (a number, not NULL / NaN / a dot)."""
    if text in {"NULL", "null", "nan", "NaN", "…", "·"}:
        return False
    try:
        float(text)
        return True
    except ValueError:
        return False


def _duckdb_row_line(cells: list[str], widths: list[int], *, center: bool = False) -> str:
    """Render one DuckDB body line; headers centre, numeric cells right-align."""
    parts: list[str] = []
    for index, width in enumerate(widths):
        cell = cells[index] if index < len(cells) else ""
        if center:
            parts.append(f" {cell.center(width)} ")
        elif _duckdb_cell_is_numeric(cell):
            parts.append(f" {cell.rjust(width)} ")
        else:
            parts.append(f" {cell.ljust(width)} ")
    return "│" + "│".join(parts) + "│"


def _format_polars_show(
    names: list[str],
    type_labels: list[str],
    head_rows: list[list[str]],
    tail_rows: list[list[str]],
    *,
    total_rows: int,
    show_ellipsis: bool,
) -> str:
    """Render a polars-style preview (shape header, dtype row, optional … separator).

    Exact rendering is pinned by ``test_display_styles.py`` goldens — approximation of polars
    1.x box-drawing with ``┆`` column separators.
    """
    if not names:
        return f"shape: ({total_rows}, 0)\n┌┐\n└┘"
    widths = _column_widths(names, type_labels, head_rows, tail_rows)
    # polars pads cells with one space each side inside the box.
    inner_widths = [width + 2 for width in widths]

    lines = [
        f"shape: ({total_rows}, {len(names)})",
        _box_rule(inner_widths, "┌", "┬", "┐"),
        _polars_row_line(names, widths),
        _polars_row_line(["---"] * len(names), widths),
        _polars_row_line(type_labels, widths),
        _box_rule(inner_widths, "╞", "╪", "╡", fill="═"),
    ]
    for row in head_rows:
        lines.append(_polars_row_line(row, widths))
    if show_ellipsis:
        lines.append(_polars_row_line(["…"] * len(names), widths))
        for row in tail_rows:
            lines.append(_polars_row_line(row, widths))
    lines.append(_box_rule(inner_widths, "└", "┴", "┘"))
    return "\n".join(lines)


def _format_duckdb_show(
    names: list[str],
    type_labels: list[str],
    head_rows: list[list[str]],
    tail_rows: list[list[str]],
    *,
    total_rows: int,
    shown_rows: int,
    show_ellipsis: bool,
) -> str:
    """Render a duckdb-style box-drawing table with a type row and row-count footer.

    Exact rendering is pinned by ``test_display_styles.py`` goldens — approximation of DuckDB
    1.x ``Relation.show()`` (``│`` / ``├`` / centered headers, right-aligned integers).
    """
    if not names:
        return f"┌┐\n│ {total_rows} rows │\n└┘"
    widths = _column_widths(names, type_labels, head_rows, tail_rows)
    # Ensure footer text fits: " N rows " / " (N shown) "
    footer_main = f" {total_rows} rows "
    # Emit ``(K shown)`` whenever the keep-set is smaller than the frame — including
    # show(0) (empty body, no middle dots) where ``show_ellipsis`` is False (C4-L-001).
    footer_shown = f" ({shown_rows} shown) " if shown_rows != total_rows else ""
    # Widen first column if needed so the footer can sit under the table.
    table_inner = sum(widths) + 3 * (len(widths) - 1)  # cells + " │ " between
    footer_need = max(len(footer_main), len(footer_shown))
    if footer_need > table_inner and widths:
        widths[0] += footer_need - table_inner
        table_inner = footer_need

    padded_widths = [width + 2 for width in widths]
    lines = [
        _box_rule(padded_widths, "┌", "┬", "┐"),
        _duckdb_row_line(names, widths, center=True),
        _duckdb_row_line(type_labels, widths, center=True),
    ]
    has_body = bool(head_rows) or show_ellipsis or bool(tail_rows)
    if has_body:
        lines.append(_box_rule(padded_widths, "├", "┼", "┤"))
        for row in head_rows:
            lines.append(_duckdb_row_line(row, widths))
        if show_ellipsis:
            lines.append(_duckdb_row_line(["·"] * len(names), widths, center=True))
            lines.append(_duckdb_row_line(["·"] * len(names), widths, center=True))
            lines.append(_duckdb_row_line(["·"] * len(names), widths, center=True))
            for row in tail_rows:
                lines.append(_duckdb_row_line(row, widths))
        # Footer: collapse column dividers into a single spanning cell.
        lines.append("├" + "┴".join("─" * (width + 2) for width in widths) + "┤")
    else:
        # Empty body: one separator under the type row, then the footer (duckdb-style).
        lines.append(_box_rule(padded_widths, "├", "┼", "┤"))
    span = table_inner
    lines.append(f"│{footer_main.center(span + 2)}│")
    if footer_shown:
        lines.append(f"│{footer_shown.center(span + 2)}│")
    lines.append("└" + "─" * (span + 2) + "┘")
    return "\n".join(lines)


def _parse_list_element_sql_type(type_key: str) -> str | None:
    """Map engine list type_key text to a SQL cast target for NULL elements.

    Accepts:
    * Spark simpleString ``array<element>`` (E2 ``arrow_type_key`` List path)
    * legacy Arrow debug ``List(Field { data_type: …, nullable: … })``

    Parses the **outer** list element only (not a substring hunt across nested content)
    so nested arrays do not steal the wrong element type (octo C2-L-001 / C2-Q-003).
    """
    text = type_key.strip()
    # E2: logical_schema_fields emits array<…> for List types.
    if text.startswith("array<") and text.endswith(">"):
        return _spark_array_element_to_sql(text[len("array<") : -1].strip())
    element = _list_field_element_debug(text)
    if element is None:
        return None
    return _arrow_debug_type_to_sql(element)


def _split_angle_csv(text: str) -> list[str]:
    """Split a comma-separated Spark type-arg list.

    Honors nested ``<>`` / ``()`` and backticks so ``decimal(10,2)`` commas
    do not split fields.
    """
    parts: list[str] = []
    start = 0
    depth = 0
    in_backtick = False
    for index, char in enumerate(text):
        if char == "`":
            in_backtick = not in_backtick
        elif not in_backtick:
            if char in "<(":
                depth += 1
            elif char in ">)":
                depth -= 1
            elif char == "," and depth == 0:
                parts.append(text[start:index].strip())
                start = index + 1
    tail = text[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _split_struct_field(field: str) -> tuple[str, str] | None:
    """Split one ``name:type`` field at the first colon outside ``<>`` / ``()`` / backticks."""
    depth = 0
    in_backtick = False
    for index, char in enumerate(field):
        if char == "`":
            in_backtick = not in_backtick
        elif not in_backtick:
            if char in "<(":
                depth += 1
            elif char in ">)":
                depth -= 1
            elif char == ":" and depth == 0:
                name = field[:index].strip()
                type_text = field[index + 1 :].strip()
                if name and type_text:
                    return name, type_text
                return None
    return None


_SIMPLE_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_DECIMAL_SQL_RE = re.compile(r"decimal\(\d+,\s*\d+\)", re.IGNORECASE)

# Sentinel: void / Null elements have no CAST spelling. explode_outer emits
# make_array(NULL) and must never interpolate this token into CAST(... AS ...).
_UNTYPED_NULL_ELEMENT = "__repark_untyped_null__"


def _sql_array_of(inner: str) -> str:
    """Spell "array of ``inner``" as ``array<inner>``, never postfix ``inner[]``.

    G3b (GA4 ``items[].item_params[]``): the postfix form binds to the *innermost*
    field when ``inner`` ends in ``>``. Measured against the engine parser via
    ``SELECT make_array(CAST(NULL AS <spelling>))``::

        struct<item_id:VARCHAR,item_params:struct<key:VARCHAR,value:struct<sv:VARCHAR>>[]>
          parses as  item_params: struct<key, value: array<struct<sv>>>   <- [] migrated
        struct<item_id:VARCHAR,item_params:array<struct<key:VARCHAR,value:struct<sv:VARCHAR>>>>
          parses as  item_params: array<struct<key, value: struct<sv>>>   <- exact

    The mis-parse made the ``CASE WHEN`` arms of the explode_outer rewrite disagree, so
    dynamic_flatten / explode_outer refused an array-of-struct nested inside an
    array-element struct. The angle form round-trips exactly for scalar inners too
    (``array<BIGINT>`` == ``BIGINT[]``), so it is used uniformly — one honest spelling
    rather than a shape-dependent pair.
    """
    return f"array<{inner}>"


def _struct_field_name_for_cast(name: str) -> str | None:
    """Allowlist a struct field name before embedding it in CAST SQL.

    Hostile names (``:``, spaces, comments) are not quoted into the CAST
    target — explode_outer refuses loud, same class as an unmapped type.
    """
    text = name.strip()
    if text.startswith("`") and text.endswith("`") and len(text) >= 3:
        text = text[1:-1]
    if _SIMPLE_IDENT_RE.fullmatch(text):
        return text
    return None


def _spark_struct_element_to_sql(raw: str) -> str | None:
    """Map Spark ``struct<field:type,…>`` to a CAST-accepted struct spelling.

    Engine SQL accepts Spark-style ``struct<name:TYPE>`` (including nested
    ``array<…>`` / ``struct<…>``). Field names keep their original case. ``map<…>``
    fields have no CAST spelling — refuse (same message class as other unmapped
    element types). ``timestamp_ntz`` rewrites to ``TIMESTAMP``.
    """
    inner = raw.strip()[len("struct<") : -1]
    fields = _split_angle_csv(inner)
    if not fields:
        return None
    parts: list[str] = []
    for field in fields:
        split = _split_struct_field(field)
        if split is None:
            return None
        name, type_text = split
        safe_name = _struct_field_name_for_cast(name)
        mapped = _spark_array_element_to_sql(type_text)
        if safe_name is None or mapped is None or mapped == _UNTYPED_NULL_ELEMENT:
            return None
        parts.append(f"{safe_name}:{mapped}")
    return "struct<" + ",".join(parts) + ">"


def _spark_array_element_to_sql(element: str) -> str | None:
    """Map Spark simpleString array element token → DataFusion SQL cast target."""
    raw = element.strip()
    token = raw.lower()
    if token.startswith("array<") and token.endswith(">"):
        inner = _spark_array_element_to_sql(raw[len("array<") : -1].strip())
        # Nested array<void> has no CAST spelling (leaf void uses make_array(NULL)).
        if inner is None or inner == _UNTYPED_NULL_ELEMENT:
            return None
        return _sql_array_of(inner)
    if token.startswith("struct<") and token.endswith(">"):
        return _spark_struct_element_to_sql(raw)
    if token.startswith("map<"):
        return None
    if token in {"null", "void"}:
        return _UNTYPED_NULL_ELEMENT
    mapping = {
        "tinyint": "TINYINT",
        "byte": "TINYINT",
        "smallint": "SMALLINT",
        "short": "SMALLINT",
        "int": "INT",
        "integer": "INT",
        "bigint": "BIGINT",
        "long": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "string": "VARCHAR",
        "binary": "BYTEA",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "timestamp_ntz": "TIMESTAMP",
    }
    if token in mapping:
        return mapping[token]
    if token.startswith("decimal"):
        if _DECIMAL_SQL_RE.fullmatch(token):
            return token.upper()
        return None
    return None


def _list_field_element_debug(type_key: str) -> str | None:
    """Extract ``data_type`` text from ``List(Field { data_type: …, nullable: … })``."""
    text = type_key.strip()
    prefix = "List(Field { data_type: "
    if not text.startswith(prefix):
        return None
    rest = text[len(prefix) :]
    marker = ", nullable: "
    index = rest.rfind(marker)
    if index < 0:
        return None
    return rest[:index].strip()


def _arrow_debug_type_to_sql(element: str) -> str | None:
    """Map one Arrow ``DataType`` debug string to a DataFusion SQL cast target."""
    text = element.strip()
    if text.startswith("List("):
        inner = _parse_list_element_sql_type(text)
        # Nested void has no CAST spelling (same refuse as simpleString array<null>).
        if inner is None or inner == _UNTYPED_NULL_ELEMENT:
            return None
        return _sql_array_of(inner)
    if text.startswith("Timestamp"):
        return "TIMESTAMP"
    if text.startswith("Date32") or text.startswith("Date64"):
        return "DATE"
    if text.startswith("Time32") or text.startswith("Time64"):
        return "TIME"
    if text in {"Int64", "UInt64"} or text.startswith("Int64"):
        return "BIGINT"
    if text in {"Int32", "UInt32"} or text.startswith("Int32"):
        return "INT"
    if text in {"Int16", "UInt16", "Int8", "UInt8"}:
        return "INT"
    if text in {"Float64"} or text.startswith("Float64"):
        return "DOUBLE"
    if text in {"Float32"} or text.startswith("Float32"):
        return "FLOAT"
    if text.startswith("Utf8") or text.startswith("LargeUtf8") or text == "Utf8View":
        return "VARCHAR"
    if text.startswith("Boolean"):
        return "BOOLEAN"
    if text.startswith("Decimal"):
        # Arrow debug: Decimal128(p, s) / Decimal256(p, s) — preserve precision/scale
        # for explode_outer NULL guard (octo C5 residual S2; was hard-coded DECIMAL(38,18)).
        decimal_match = re.fullmatch(r"Decimal(?:128|256)?\((\d+),\s*(\d+)\)", text)
        if decimal_match is not None:
            return f"DECIMAL({decimal_match.group(1)}, {decimal_match.group(2)})"
        return "DECIMAL(38, 18)"
    if text.startswith("Null"):
        return _UNTYPED_NULL_ELEMENT
    # Struct / Map / Union / Dictionary — unsupported for make_array(NULL) guard.
    return None


def _null_safe_equi_join_sql(
    left_view: str,
    right_view: str,
    key_names: list[str],
    select_names: list[str],
    *,
    left_column_names: list[str],
    right_column_names: list[str],
    prefer_right_names: set[str] | None = None,
) -> str:
    """``INNER JOIN`` SQL with ``IS NOT DISTINCT FROM`` on every key (null-safe equi-join).

    Spark ``groupBy`` / ``Window.partitionBy`` treat ``NULL`` as a real group key. Name-list
    equi-joins use ``=``, so ``NULL = NULL`` is unknown and those groups/rows silently
    disappear (octo M6 C1). Keys are projected from the left view. Non-key columns resolve
    from left then right unless listed in ``prefer_right_names`` (window UDF outs that may
    share a name with a source column — last-wins / withColumn overwrite, octo M6 C2).
    """
    if not key_names:
        raise ValueError("_null_safe_equi_join_sql requires at least one key")
    # Parenthesize each IS NOT DISTINCT FROM — unparenthesized multi-key AND binds the
    # second key into the first comparison (DataFusion: Utf8 AND Boolean) (octo M6 C1).
    on_clause = " AND ".join(
        f"({left_view}.{_quote_ident_sql(key)} IS NOT DISTINCT FROM "
        f"{right_view}.{_quote_ident_sql(key)})"
        for key in key_names
    )
    left_set = set(left_column_names)
    right_set = set(right_column_names)
    key_set = set(key_names)
    prefer_right = prefer_right_names or set()
    select_parts: list[str] = []
    for name in select_names:
        quoted = _quote_ident_sql(name)
        if name in key_set:
            select_parts.append(f"{left_view}.{quoted} AS {quoted}")
        elif name in prefer_right and name in right_set:
            # Window/UDF out that overwrites a same-named source column (octo M6 C2).
            select_parts.append(f"{right_view}.{quoted} AS {quoted}")
        elif name in left_set:
            select_parts.append(f"{left_view}.{quoted} AS {quoted}")
        elif name in right_set:
            select_parts.append(f"{right_view}.{quoted} AS {quoted}")
        else:
            raise AnalysisException(
                f"null-safe equi-join select name {name!r} is not present on either side "
                f"(left={list(left_column_names)!r}, right={list(right_column_names)!r})"
            )
    return (
        f"SELECT {', '.join(select_parts)} FROM {left_view} INNER JOIN {right_view} ON {on_clause}"
    )


# === r20 H1: join/identity helpers ============================================================

# Join-ON tokens from Column.join_sql_part: __REPARK_QCOL_{plan_id}__{field_enc}__
_QCOL_TOKEN_RE = re.compile(r"__REPARK_QCOL_([0-9a-f]+)__(.+?)__")

# Separators that mark a side boundary between consecutive QCOL tokens for same-object
# self-join alternation (comparison ops + boolean connectors). Arithmetic/func glue alone
# means multi-token arm → refuse (critic-octo H2 C1-001).
_QCOL_SIDE_BOUNDARY_RE = re.compile(r"(?i)(<=>|<=|>=|<>|!=|=|<|>|\bAND\b|\bOR\b)")


def _same_object_qcol_alternation_safe(join_sql: str) -> bool:
    """True when even/odd QCOL alternation preserves per-side binding for ``df.join(df, …)``.

    Safe for simple leaf comparisons and AND/OR chains of them (``df.x == df.x``,
    ``(df.a == df.b) & (df.b == df.a)``). Unsafe when two QCOL tokens are separated only by
    arithmetic/function glue — e.g. ``(x + y) = (x + y)`` — because alternation would bind
    ``L.x + R.y`` instead of ``L.x + L.y`` (silent wrong cardinality / predicate).
    """
    matches = list(_QCOL_TOKEN_RE.finditer(join_sql))
    if len(matches) < 2:
        return True
    for index in range(len(matches) - 1):
        between = join_sql[matches[index].end() : matches[index + 1].start()]
        if _QCOL_SIDE_BOUNDARY_RE.search(between) is None:
            return False
    return True


def _decode_qcol_field(field_enc: str) -> str:
    """Decode a ``join_sql_part`` field payload (inverse of Column encoding)."""
    return field_enc.replace("\\_\\_", "__").replace("\\n", "\n").replace("\\\\", "\\")


def _replace_local_qcol_token(
    match: re.Match[str],
    *,
    origin_map: dict[tuple[str, str], str],
    frame: DataFrame,
) -> str:
    """Rewrite one ``__REPARK_QCOL_*`` token against a single frame's origin map."""
    plan_id = match.group(1)
    field = _decode_qcol_field(match.group(2))
    frame._raise_if_origin_not_emitted(plan_id, field)
    engine = origin_map.get((plan_id, field))
    if engine is None:
        return match.group(0)
    return _quote_ident_sql(engine)


def _rewrite_qcol_tokens_local(join_sql: str, frame: DataFrame) -> str:
    """Rewrite ``__REPARK_QCOL_*`` tokens to quoted engine fields on *one* post-join frame.

    Used by :meth:`DataFrame.filter` for comparison / null-check compounds built from
    parent-origin Columns (``left.b > 1``, ``left.b.isNotNull()``) where origin bits were
    cleared by the op but ``join_sql_expr`` still carries side tokens.
    """
    origin_map = frame._origin_map
    if origin_map is None:
        return join_sql
    return _QCOL_TOKEN_RE.sub(
        functools.partial(_replace_local_qcol_token, origin_map=origin_map, frame=frame),
        join_sql,
    )


def _rewrite_join_qcol_sql(
    join_sql: str,
    *,
    left: DataFrame,
    right: DataFrame,
    left_alias: str,
    right_alias: str,
) -> str:
    """Rewrite ``__REPARK_QCOL_*`` tokens in a join ON clause to ``"alias"."engine"``.

    Tokens are produced by :meth:`Column.join_sql_part` for origin-bound Columns. Plan ids
    that match neither side (or unknown fields) are left unchanged so DataFusion reports a
    clear analysis error rather than a silent wrong-side bind.

    H2 same-object self-join (``df.join(df, df.x == df.x)``): both sides share one
    ``_plan_id``, so plan-id matching alone would bind every token to the left alias and
    turn the ON into a tautology (cartesian). When ``left is right``, alternate token
    occurrences left/right for **simple** leaf comparisons so equi self-joins get correct
    cardinality. Multi-token comparison arms (arithmetic compounds) refuse loud — even/odd
    alternation would silently mis-bind (``L.x + R.y`` instead of ``L.x + L.y``; critic-octo
    C1-001). Prefer ``df.alias("l").join(df.alias("r"), …)`` for compounds and when
    post-join parent-Column origin identity is required (same-object origin map cannot
    split one plan_id across two sides).
    """
    # H2: same Python object → same plan_id on both sides; alternate token sides when safe.
    same_object = left is right
    if same_object and not _same_object_qcol_alternation_safe(join_sql):
        raise AnalysisException(
            "same-object self-join condition has multi-token comparison arms that cannot "
            "be disambiguated by alternating left/right QCOL sides (would silently "
            'mis-bind columns). Use df.alias("l").join(df.alias("r"), …) so each side '
            "has a distinct plan id."
        )
    rewriter = _JoinQcolRewriter(
        left=left,
        right=right,
        left_alias=left_alias,
        right_alias=right_alias,
        same_object=same_object,
    )
    return _QCOL_TOKEN_RE.sub(rewriter, join_sql)


def _join_side_engine(frame: DataFrame, field: str) -> str:
    """Resolve a join-ON field name to the engine column on ``frame``."""
    # Prefer origin map (nested), else display→engine, else bare field.
    if frame._origin_map is not None:
        for (plan_id, origin_field), engine in frame._origin_map.items():
            if origin_field == field and plan_id == frame._plan_id:
                return engine
        # Any origin entry for this field on the frame (chained).
        for (_plan_id, origin_field), engine in frame._origin_map.items():
            if origin_field == field:
                return engine
    if frame._display_names is not None and frame._engine_names is not None:
        matches = [
            engine
            for name, engine in zip(frame._display_names, frame._engine_names, strict=True)
            if name == field
        ]
        if len(matches) == 1:
            return matches[0]
    return field


class _JoinQcolRewriter:
    """Rewrite ``__REPARK_QCOL_*`` tokens in a join ON clause; holds the occurrence counter."""

    def __init__(
        self,
        *,
        left: DataFrame,
        right: DataFrame,
        left_alias: str,
        right_alias: str,
        same_object: bool,
    ) -> None:
        self.left = left
        self.right = right
        self.left_alias = left_alias
        self.right_alias = right_alias
        self.same_object = same_object
        self.token_index = 0

    def __call__(self, match: re.Match[str]) -> str:
        plan_id = match.group(1)
        field_enc = match.group(2)
        field = field_enc.replace("\\_\\_", "__").replace("\\n", "\n").replace("\\\\", "\\")
        left = self.left
        right = self.right
        if self.same_object and plan_id == left._plan_id:
            # Occurrence 0,2,… → left alias; 1,3,… → right (equi self-join sugar).
            side_alias = self.left_alias if (self.token_index % 2 == 0) else self.right_alias
            self.token_index += 1
            engine = _join_side_engine(left, field)
            return f"{side_alias}.{_quote_ident_sql(engine)}"
        if plan_id == left._plan_id or (
            left._origin_map is not None and any(pid == plan_id for pid, _field in left._origin_map)
        ):
            # Prefer direct left plan_id; also accept nested origins that live on left.
            if plan_id == left._plan_id:
                engine = _join_side_engine(left, field)
                return f"{self.left_alias}.{_quote_ident_sql(engine)}"
            if left._origin_map is not None and (plan_id, field) in left._origin_map:
                engine = left._origin_map[(plan_id, field)]
                return f"{self.left_alias}.{_quote_ident_sql(engine)}"
        if plan_id == right._plan_id or (
            right._origin_map is not None
            and any(pid == plan_id for pid, _field in right._origin_map)
        ):
            if plan_id == right._plan_id:
                engine = _join_side_engine(right, field)
                return f"{self.right_alias}.{_quote_ident_sql(engine)}"
            if right._origin_map is not None and (plan_id, field) in right._origin_map:
                engine = right._origin_map[(plan_id, field)]
                return f"{self.right_alias}.{_quote_ident_sql(engine)}"
        # Unknown plan_id — leave token (should not happen for well-formed conditions).
        return match.group(0)


def _sql_ident_bare_name(fragment: str) -> str | None:
    """Return the bare identifier if ``fragment`` is one double-quoted or bare name.

    Complex SQL expressions (casts, binary ops, function calls) return ``None``.
    """
    text = fragment.strip()
    if not text:
        return None
    if text.startswith('"'):
        # Single double-quoted identifier (``""`` escapes a quote).
        if not text.endswith('"') or len(text) < 2:
            return None
        inner = text[1:-1]
        if '"' in inner.replace('""', ""):
            return None
        return inner.replace('""', '"')
    if text.startswith("`") and text.endswith("`") and len(text) >= 2:
        return text[1:-1].replace("``", "`")
    # Facade-built expressions always use parentheses / CAST / CASE / fn( — leave them.
    if _is_compound_sql_expr(text):
        return None
    # Bare name or hostile ColumnOrName token (quote later) — treat as identifier text.
    return text


def _is_compound_sql_expr(text: str) -> bool:
    """True when ``text`` looks like a facade-built SQL expression, not a raw identifier."""
    stripped = text.strip()
    if stripped.startswith("(") or stripped.startswith("CAST(") or stripped.startswith("CASE "):
        return True
    # function-call style: name(...)
    if len(stripped) > 1 and "(" in stripped:
        head = stripped.split("(", 1)[0].strip()
        if head.isidentifier() or head.lower() in {
            "coalesce",
            "make_array",
            "unnest",
            "cardinality",
            "not",
        }:
            return True
    return False


def _sql_embed_expr_fragment(fragment: str) -> str:
    """Embed a column SQL fragment into generator rewrite SQL.

    Identifier-like fragments (schema fields, ``F.col`` / ColumnOrName, reserved words,
    mixed-case, hostile names) are always double-quoted so they cannot change SELECT/FROM
    shape (octo C2-SEC-001/002, C2-Q-002, C2-L-002). Compound expressions pass through.
    """
    bare = _sql_ident_bare_name(fragment)
    if bare is not None:
        return _quote_ident_sql(bare)
    return fragment.strip()


# ==================================================================================================
# CEIL-1 (D1 #173): global-agg / partition-transform / pandas-UDF-frame gate helpers
# moved VERBATIM from ``core.py``'s tail block (move-only; T0b precedent). ``core.py``
# re-exports all six from its tail bind block, so every import path is unchanged (Q7 freeze).
# ==================================================================================================


def _is_native_pure_global_aggregate(column: Column) -> bool:
    """True when ``DataFrame.aggregate`` can accept this column as an aggregate arg.

    Bare ``F.sum``/… builders set ``_is_aggregate_function``; ``.alias`` / ``for_select``
    preserve it. Cast / binary / unary / scalar wrappers clear it and need the SQL
    global-agg path (metadata only — no display-string sniff; octo C2-Q-002 fallout).

    Compound AF arguments (``sum((X + 1))``) keep nested parentheses in structural
    ``sql_expr``; the native pure path cannot rebind those case-preserved leaves, so they
    take the free-SQL global-agg path instead (octo C6-L-002).
    """
    if not (column._is_aggregate and column._is_aggregate_function):
        return False
    sql_text = column._sql_expr
    if sql_text is None:
        return True
    open_paren = sql_text.find("(")
    if open_paren < 0:
        return True
    # Nested ``(`` after the outer AF call → compound arg; free-SQL path keeps quotes.
    return "(" not in sql_text[open_paren + 1 :]


def _parse_count_distinct_simple_names(text: str) -> list[str] | None:
    """Extract simple leaf names from a ``count(DISTINCT …)`` display/sql fragment.

    Supports bare/quoted simple names (``count(DISTINCT a, b)``, ``count(DISTINCT "A")``)
    and the multi-col null-if-any pack form
    ``count(DISTINCT CASE WHEN … THEN struct("a", "b") END)`` (octo C5-L-001). Compounds
    (``count(DISTINCT (x + 1))``) return ``None`` so rebind leaves them alone.
    """
    stripped = text.strip()
    if not stripped.startswith("count(DISTINCT ") or not stripped.endswith(")"):
        return None
    body = stripped[len("count(DISTINCT ") : -1].strip()
    # Multi-col SQL pack: only the struct field list carries recoverable simple names.
    case_match = re.fullmatch(
        r"CASE WHEN .+ THEN struct\((.+)\) END",
        body,
        flags=re.DOTALL,
    )
    if case_match is not None:
        body = case_match.group(1).strip()
    # Comma-separated simple identifiers, each optionally double-quoted.
    token = r'"?([A-Za-z_][A-Za-z0-9_]*)"?'
    if re.fullmatch(token, body) is not None:
        match = re.fullmatch(token, body)
        return [match.group(1)] if match is not None else None
    multi = re.fullmatch(
        rf"(?:{token}\s*,\s*)+{token}",
        body,
    )
    if multi is None:
        return None
    return re.findall(r'"?([A-Za-z_][A-Za-z0-9_]*)"?', body)


def _global_agg_sql_parts(column: Column) -> tuple[str, str]:
    """``(expression_sql, output_name)`` for the SQL global-agg select path.

    Expression SQL comes from structural ``Column._sql_expr`` chains (aggregate builders
    quote identifiers; ``alias`` does not embed ``AS name`` — octo C3-SEC-001 / C3-002).
    Output names are always quoted by the caller via ``_quote_ident``.
    """
    if column._projection_name is not None:
        output_name = column._projection_name
    elif column._agg_name is not None:
        output_name = column._agg_name
    else:
        output_name = column.spark_display_part()
    return column.sql_expr_part(), output_name


def _pandas_udf_window_frame_bounds(spec: Any) -> tuple[int | None, int | None]:
    """Resolve rows-frame offsets for windowed GROUPED_AGG (M7).

    Returns ``(start, end)`` relative to the current row: ``None`` = unbounded on that
    side; ``0`` = current row. When G2 has not set ``_frame_start`` / ``_frame_end`` on
    the :class:`~repark.window.WindowSpec`, ordered windows default to Spark's
    ``ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`` → ``(None, 0)``.
    """
    from repark.spark.window import _JVM_LONG_MAX, _JVM_LONG_MIN

    order_columns = list(getattr(spec, "_order_columns", []) or [])
    if not order_columns:
        return (None, None)
    start = getattr(spec, "_frame_start", None)
    end = getattr(spec, "_frame_end", None)
    if start is None and end is None:
        # G2's WindowSpec always declares the attrs (None until rowsBetween sets ints);
        # ordered window with no explicit frame keeps the Spark default.
        return (None, 0)
    # G2 normalizes ±unbounded to JVM long sentinels — map back to None (unbounded side).
    if start is not None:
        start = None if int(start) <= _JVM_LONG_MIN else int(start)
    if end is not None:
        end = None if int(end) >= _JVM_LONG_MAX else int(end)
    return (start, end)


def _reject_partition_transform(column: Column) -> None:
    """Raise if ``column`` is an ``F.years``/``months``/``days``/``hours`` partition transform.

    Those expressions are valid only inside :meth:`DataFrameWriterV2.partitionedBy` (live PySpark
    4.1.2: ``PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY``).
    """
    transform = getattr(column, "_partition_transform", None)
    if transform is not None:
        raise AnalysisException(
            f"[PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY] The expression "
            f"{transform!r} must be inside 'partitionedBy'."
        )


def _reject_aggregate_in_with_column(column: Column, *, surface: str) -> None:
    """Refuse sticky aggregates on ``withColumn`` / ``withColumns`` (combine octo C3-001).

    Spark rejects aggregate expressions outside ``select`` / ``agg`` / ``groupBy``. Without
    this gate, ``withColumns`` projects via :meth:`DataFrame.select` and F1 pure-global
    routing silently collapses every row to one global-agg row.
    """
    if bool(getattr(column, "_is_aggregate", False)):
        raise AnalysisException(
            f"[INVALID_USAGE_OF_AGGREGATE] Aggregate expressions are not allowed in "
            f"{surface} (use select/agg for global aggregates; Spark rejects "
            f"aggregates in withColumn/withColumns)."
        )
