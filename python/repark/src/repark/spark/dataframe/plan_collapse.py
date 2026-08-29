"""Plan simplification, display formatting, SQL type mapping, and join rewrites.

The module exports helpers bound by ``core`` and strips internal tighten metadata at export.
"""

from __future__ import annotations

import functools
import logging
import re
from typing import TYPE_CHECKING, Any

from repark.errors import AnalysisException
from repark.spark._idents import quote_ident as _quote_ident_sql
from repark.spark._idents import sql_string_literal as _sql_string_literal
from repark.spark.column import Column

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame

logger = logging.getLogger("repark.spark.dataframe")


def _output_field_would_persist_required(field: Any) -> bool:
    """Return whether this field or a nested child remains Iceberg-required."""
    if not field.nullable:
        return True
    return _data_type_has_required_child(field.dataType)


def _data_type_has_required_child(data_type: Any) -> bool:
    """Return whether a nested Struct, Array, or Map child is required."""
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
    """Remove tighten-null metadata from one Arrow field up to the depth limit."""
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
    """Remove tighten-null metadata from nested Arrow fields."""
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
    """Remove internal tighten-null metadata from user-visible Arrow output."""
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


def _collapse_identity_projection_alias(column: Column) -> Column:
    """Collapse nested identity aliases before applying the projection alias gate."""
    if column._projection_name is None:
        return column
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
        pass
    except Exception:
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
    """Return a structural key including partition, order, null placement, and frame."""
    try:
        partitions = tuple(
            column._projection_name or column.spark_display_part()
            for column in spec._partition_columns
        )
        orders = tuple(
            (
                column._projection_name or column.spark_display_part(),
                True if column._sort_ascending is None else bool(column._sort_ascending),
                column._sort_nulls_first,
            )
            for column in spec._order_columns
        )
        frame = (spec._frame_units, spec._frame_start, spec._frame_end)
        return (partitions, orders, frame)
    except Exception:
        return None


def _column_window_spec(column: Column) -> Any | None:
    """Return the window specification retained by a column, if any."""
    return getattr(column, "_window_spec", None)


def _uniform_window_key_from_map(cols_map: dict[str, Any]) -> tuple[Any, ...] | None:
    """Return a shared structural key when all mapped columns use one window spec."""
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
    """Return whether a column may reference any name in ``names``."""
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
    """Return whether a Spark dtype is legal for value-offset RANGE ordering."""
    normalized = type_name.strip().lower()
    if normalized in _G2_RANGE_NUMERIC_DTYPES:
        return True
    if normalized.startswith("decimal"):
        return True
    return "interval" in normalized


def _reject_non_numeric_range_order(frame: DataFrame, column: Column) -> None:
    """Reject non-numeric value-offset RANGE windows with Spark's analysis error class."""
    names = getattr(column, "_g2_range_order_names", None)
    if not names:
        return
    dtype_by_name = dict(frame.dtypes)
    for order_name in names:
        type_name = dtype_by_name.get(order_name)
        if type_name is None:
            continue
        if not _g2_dtype_is_range_numeric(type_name):
            raise AnalysisException(
                "[DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE] Cannot resolve "
                "RANGE window frame due to data type mismatch: The data type of the order "
                f"key {order_name!r} ({type_name!r}) does not match the expected data type "
                '("NUMERIC" or "INTERVAL"). SQLSTATE: 42K09'
            )


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
    """Render the compact grid used by ``spark.sql.repl.eagerEval``."""
    names = list(table.column_names)
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
    """Render Arrow rows in the PySpark ``show(vertical=True)`` layout."""
    names = list(table.column_names)
    raw_rows = _table_to_cell_rows(table, truncate_at=truncate_at, style="spark")
    name_width = max((len(name) for name in names), default=0)
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
    return "\n".join(lines)


def _cell_text(value: Any, *, style: str, truncate_at: int | None) -> str:
    """Format one cell with the null, NaN, boolean, and truncation spellings for ``style``."""
    if value is None:
        text = "null" if style == "polars" else "NULL"
    elif isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, float) and value != value:
        text = "NaN" if style == "polars" else "nan"
    else:
        text = str(value)
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
    """Return display labels from precise Arrow fields, preserving narrow numeric types."""
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
            return key
        if key in {"date"}:
            return "date"
        if key.startswith("timestamp"):
            return "datetime[μs]"
        return key
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
    """Render the Polars-style preview with shape, dtypes, and optional ellipsis."""
    if not names:
        return f"shape: ({total_rows}, 0)\n┌┐\n└┘"
    widths = _column_widths(names, type_labels, head_rows, tail_rows)
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
    """Render the DuckDB-style table with dtypes and a row-count footer."""
    if not names:
        return f"┌┐\n│ {total_rows} rows │\n└┘"
    widths = _column_widths(names, type_labels, head_rows, tail_rows)
    footer_main = f" {total_rows} rows "
    footer_shown = f" ({shown_rows} shown) " if shown_rows != total_rows else ""
    table_inner = sum(widths) + 3 * (len(widths) - 1)
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
        lines.append("├" + "┴".join("─" * (width + 2) for width in widths) + "┤")
    else:
        lines.append(_box_rule(padded_widths, "├", "┼", "┤"))
    span = table_inner
    lines.append(f"│{footer_main.center(span + 2)}│")
    if footer_shown:
        lines.append(f"│{footer_shown.center(span + 2)}│")
    lines.append("└" + "─" * (span + 2) + "┘")
    return "\n".join(lines)


def _parse_list_element_sql_type(type_key: str) -> str | None:
    """Map an engine list type key to a SQL cast target for null elements."""
    text = type_key.strip()
    if text.startswith("array<") and text.endswith(">"):
        return _spark_array_element_to_sql(text[len("array<") : -1].strip())
    element = _list_field_element_debug(text)
    if element is None:
        return None
    return _arrow_debug_type_to_sql(element)


def _split_angle_csv(text: str) -> list[str]:
    """Split Spark type arguments while honoring nested brackets and backticks."""
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

_UNTYPED_NULL_ELEMENT = "__repark_untyped_null__"


def _sql_array_of(inner: str) -> str:
    """Return the unambiguous angle-bracket spelling for an array type."""
    return f"array<{inner}>"


def _struct_field_name_for_cast(name: str) -> str | None:
    """Return a safe struct field name for CAST SQL, or ``None`` for hostile text."""
    text = name.strip()
    if text.startswith("`") and text.endswith("`") and len(text) >= 3:
        text = text[1:-1]
    if _SIMPLE_IDENT_RE.fullmatch(text):
        return text
    return None


def _spark_struct_element_to_sql(raw: str) -> str | None:
    """Map a Spark struct type to a CAST-compatible struct spelling."""
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
    """Map a Spark array element token to a DataFusion SQL cast target."""
    raw = element.strip()
    token = raw.lower()
    if token.startswith("array<") and token.endswith(">"):
        inner = _spark_array_element_to_sql(raw[len("array<") : -1].strip())
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
        decimal_match = re.fullmatch(r"Decimal(?:128|256)?\((\d+),\s*(\d+)\)", text)
        if decimal_match is not None:
            return f"DECIMAL({decimal_match.group(1)}, {decimal_match.group(2)})"
        return "DECIMAL(38, 18)"
    if text.startswith("Null"):
        return _UNTYPED_NULL_ELEMENT
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
    """Build a null-safe equi-join that preserves null keys and selected output names."""
    if not key_names:
        raise ValueError("_null_safe_equi_join_sql requires at least one key")
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


_QCOL_TOKEN_RE = re.compile(r"__REPARK_QCOL_([0-9a-f]+)__(.+?)__")

_QCOL_SIDE_BOUNDARY_RE = re.compile(r"(?i)(<=>|<=|>=|<>|!=|=|<|>|\bAND\b|\bOR\b)")


def _same_object_qcol_alternation_safe(join_sql: str) -> bool:
    """Return whether QCOL alternation can preserve sides in a same-object self-join.

    Reject compound arms because alternation could bind a field to the wrong side.
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
    """Rewrite QCOL tokens to quoted engine fields on one post-join frame."""
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
    """Rewrite join QCOL tokens to quoted fields on the matching side.

    Unknown tokens remain unchanged so the engine reports an analysis error.
    """
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
    if frame._origin_map is not None:
        for (plan_id, origin_field), engine in frame._origin_map.items():
            if origin_field == field and plan_id == frame._plan_id:
                return engine
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
    """Rewrite join QCOL tokens while tracking their occurrence order."""

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
            side_alias = self.left_alias if (self.token_index % 2 == 0) else self.right_alias
            self.token_index += 1
            engine = _join_side_engine(left, field)
            return f"{side_alias}.{_quote_ident_sql(engine)}"
        if plan_id == left._plan_id or (
            left._origin_map is not None and any(pid == plan_id for pid, _field in left._origin_map)
        ):
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
        return match.group(0)


def _sql_ident_bare_name(fragment: str) -> str | None:
    """Return one quoted or bare identifier; complex SQL expressions return ``None``."""
    text = fragment.strip()
    if not text:
        return None
    if text.startswith('"'):
        if not text.endswith('"') or len(text) < 2:
            return None
        inner = text[1:-1]
        if '"' in inner.replace('""', ""):
            return None
        return inner.replace('""', '"')
    if text.startswith("`") and text.endswith("`") and len(text) >= 2:
        return text[1:-1].replace("``", "`")
    if _is_compound_sql_expr(text):
        return None
    return text


def _is_compound_sql_expr(text: str) -> bool:
    """True when ``text`` looks like a facade-built SQL expression, not a raw identifier."""
    stripped = text.strip()
    if stripped.startswith("(") or stripped.startswith("CAST(") or stripped.startswith("CASE "):
        return True
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
    """Quote identifier-like fragments before embedding them in generator SQL."""
    bare = _sql_ident_bare_name(fragment)
    if bare is not None:
        return _quote_ident_sql(bare)
    return fragment.strip()


def _is_native_pure_global_aggregate(column: Column) -> bool:
    """Return whether a column can use the native pure global-aggregate path."""
    if not (column._is_aggregate and column._is_aggregate_function):
        return False
    sql_text = column._sql_expr
    if sql_text is None:
        return True
    open_paren = sql_text.find("(")
    if open_paren < 0:
        return True
    return "(" not in sql_text[open_paren + 1 :]


def _parse_count_distinct_simple_names(text: str) -> list[str] | None:
    """Extract simple leaf names from a count-distinct SQL fragment, if possible."""
    stripped = text.strip()
    if not stripped.startswith("count(DISTINCT ") or not stripped.endswith(")"):
        return None
    body = stripped[len("count(DISTINCT ") : -1].strip()
    case_match = re.fullmatch(
        r"CASE WHEN .+ THEN struct\((.+)\) END",
        body,
        flags=re.DOTALL,
    )
    if case_match is not None:
        body = case_match.group(1).strip()
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
    """Return expression SQL and output name for the global-aggregate select path."""
    if column._projection_name is not None:
        output_name = column._projection_name
    elif column._agg_name is not None:
        output_name = column._agg_name
    else:
        output_name = column.spark_display_part()
    return column.sql_expr_part(), output_name


def _pandas_udf_window_frame_bounds(spec: Any) -> tuple[int | None, int | None]:
    """Return rows-frame offsets, using Spark's ordered-window default when unset."""
    from repark.spark.window import _JVM_LONG_MAX, _JVM_LONG_MIN

    order_columns = list(getattr(spec, "_order_columns", []) or [])
    if not order_columns:
        return (None, None)
    start = getattr(spec, "_frame_start", None)
    end = getattr(spec, "_frame_end", None)
    if start is None and end is None:
        return (None, 0)
    if start is not None:
        start = None if int(start) <= _JVM_LONG_MIN else int(start)
    if end is not None:
        end = None if int(end) >= _JVM_LONG_MAX else int(end)
    return (start, end)


def _reject_partition_transform(column: Column) -> None:
    """Reject partition transforms outside ``DataFrameWriterV2.partitionedBy``."""
    transform = getattr(column, "_partition_transform", None)
    if transform is not None:
        raise AnalysisException(
            f"[PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY] The expression "
            f"{transform!r} must be inside 'partitionedBy'."
        )


def _reject_aggregate_in_with_column(column: Column, *, surface: str) -> None:
    """Reject aggregate expressions on ``withColumn`` and ``withColumns``."""
    if bool(getattr(column, "_is_aggregate", False)):
        raise AnalysisException(
            f"[INVALID_USAGE_OF_AGGREGATE] Aggregate expressions are not allowed in "
            f"{surface} (use select/agg for global aggregates; Spark rejects "
            f"aggregates in withColumn/withColumns)."
        )
