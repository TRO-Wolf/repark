"""Polars-style cell and type spellings for show previews."""

from __future__ import annotations

from typing import Any

_POLARS_NESTED_DEPTH_CAP = 8

_POLARS_SCIENTIFIC_BOUND = 999999.0


def _polars_short_sci(negative: bool, int_part: str, int_digits: int) -> str:
    """Spell an integer float in shortest scientific form (``5e8``, ``1.5e8``)."""
    digits = int_part.lstrip("0").rstrip("0") or "0"
    mantissa = digits[0] + ("." + digits[1:] if len(digits) > 1 else "")
    sign = "-" if negative else ""
    return f"{sign}{mantissa}e{int_digits - 1}"


def _polars_padded_sci(value: float) -> str:
    """Spell a float in four-decimal scientific form without padded exponents."""
    mantissa, _, exponent = f"{value:.4e}".partition("e")
    sign = "-" if exponent.startswith("-") else ""
    trimmed = exponent.lstrip("+-").lstrip("0") or "0"
    return f"{mantissa}e{sign}{trimmed}"


def _polars_round_six(value: float) -> str:
    """Round a float to six decimals, trimming spare zeros but keeping one."""
    text = f"{value:.6f}".rstrip("0")
    if text.endswith("."):
        text += "0"
    return text


def _polars_float_text(value: float) -> str:
    """Spell a float the way polars mixed mode spells it, from the shortest expansion."""
    if value != value:
        return "NaN"
    if value == float("inf"):
        return "inf"
    if value == float("-inf"):
        return "-inf"
    from decimal import Decimal

    fixed = format(Decimal(repr(value)), "f")
    negative = fixed.startswith("-")
    unsigned = fixed[1:] if negative else fixed
    int_part, _, frac_part = unsigned.partition(".")
    magnitude = abs(value)
    if not frac_part.strip("0"):
        if magnitude < _POLARS_SCIENTIFIC_BOUND:
            return fixed
        if len(int_part) + int(negative) <= 9:
            return _polars_short_sci(negative, int_part, len(int_part))
        return _polars_padded_sci(value)
    if len(fixed) > 9:
        if magnitude < 1e-6 or magnitude > _POLARS_SCIENTIFIC_BOUND:
            return _polars_padded_sci(value)
        return _polars_round_six(value)
    return fixed


def _polars_nested_text(value: Any, arrow_type: Any | None, *, truncate_at: int | None) -> str:
    """Spell a struct or list cell the way polars spells it, recursing with a depth cap."""
    return _polars_nested_at_depth(value, arrow_type, truncate_at=truncate_at, depth=0)


def _polars_nested_at_depth(
    value: Any,
    arrow_type: Any | None,
    *,
    truncate_at: int | None,
    depth: int,
) -> str:
    """Spell one nested value; past the depth cap values fall back to plain text."""
    import pyarrow.types as pat

    if value is None:
        return "null"
    if depth > _POLARS_NESTED_DEPTH_CAP:
        return str(value)
    if isinstance(value, str):
        if truncate_at is not None and truncate_at > 0 and len(value) > truncate_at:
            value = value[:truncate_at] + "…"
        return '"' + value + '"'
    if isinstance(value, dict) and arrow_type is not None and pat.is_struct(arrow_type):
        parts = [
            _polars_nested_at_depth(
                value.get(field.name), field.type, truncate_at=truncate_at, depth=depth + 1
            )
            for field in arrow_type
        ]
        return "{" + ",".join(parts) + "}"
    if (
        isinstance(value, (list, tuple))
        and arrow_type is not None
        and (
            pat.is_list(arrow_type)
            or pat.is_large_list(arrow_type)
            or pat.is_fixed_size_list(arrow_type)
        )
    ):
        element_type: Any | None = arrow_type.value_field.type
        items = [
            _polars_nested_at_depth(item, element_type, truncate_at=truncate_at, depth=depth + 1)
            for item in value
        ]
        if len(items) > 3:
            return "[" + ", ".join(items[:2]) + ", … " + items[-1] + "]"
        return "[" + ", ".join(items) + "]"
    return _cell_text(value, style="polars", truncate_at=truncate_at)


def _cell_text(
    value: Any,
    *,
    style: str,
    truncate_at: int | None,
    arrow_type: Any | None = None,
) -> str:
    """Format one cell with the null, NaN, boolean, and truncation spellings for ``style``."""
    if value is None:
        text = "null" if style == "polars" else "NULL"
    elif isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, float):
        if style == "polars":
            text = _polars_float_text(value)
        elif value != value:
            text = "nan"
        else:
            text = str(value)
    elif style == "polars" and arrow_type is not None and isinstance(value, (dict, list, tuple)):
        text = _polars_nested_text(value, arrow_type, truncate_at=None)
        if truncate_at is not None and truncate_at > 0 and len(text) > truncate_at:
            text = text[:truncate_at] + "…"
        return text
    else:
        text = str(value)
    if truncate_at is not None and truncate_at > 0 and len(text) > truncate_at:
        if style == "polars":
            text = text[:truncate_at] + "…"
        elif truncate_at >= 3:
            text = text[: max(0, truncate_at - 3)] + "..."
        else:
            text = text[:truncate_at]
    return text


def _table_to_cell_rows(
    table: Any,
    *,
    truncate_at: int | None,
    style: str,
) -> list[list[str]]:
    """Convert an Arrow table to string cell rows for a show style."""
    import pyarrow as pa
    import pyarrow.types as pat

    names = list(table.column_names)
    column_types = [field.type for field in table.schema]
    narrow_float = [
        pat.is_float32(column_type) or pat.is_float16(column_type) for column_type in column_types
    ]
    rows: list[list[str]] = []
    for mapping in table.to_pylist():
        cells: list[str] = []
        for name, column_type, is_narrow in zip(names, column_types, narrow_float, strict=True):
            raw = mapping.get(name)
            if is_narrow and isinstance(raw, float):
                raw = pa.scalar(raw, type=column_type).as_py()
            cells.append(
                _cell_text(raw, style=style, truncate_at=truncate_at, arrow_type=column_type)
            )
        rows.append(cells)
    return rows


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
        if style != "polars":
            return "timestamp"
        unit_labels = {"s": "ms", "ms": "ms", "us": "μs", "ns": "ns"}
        unit = unit_labels.get(arrow_type.unit, arrow_type.unit)
        if arrow_type.tz is not None:
            return f"datetime[{unit}, {arrow_type.tz}]"
        return f"datetime[{unit}]"
    if (pat.is_time32(arrow_type) or pat.is_time64(arrow_type)) and style == "polars":
        return "time"
    if pat.is_decimal(arrow_type):
        precision = arrow_type.precision
        scale = arrow_type.scale
        if style == "polars":
            return f"decimal[{precision},{scale}]"
        return f"decimal({precision},{scale})"
    if style == "polars" and pat.is_struct(arrow_type):
        return f"struct[{arrow_type.num_fields}]"
    if style == "polars" and (
        pat.is_list(arrow_type)
        or pat.is_large_list(arrow_type)
        or pat.is_fixed_size_list(arrow_type)
    ):
        inner = _arrow_pa_type_label(arrow_type.value_field.type, style=style)
        return f"list[{inner}]"
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


def _polars_column_gap(
    names: list[str],
    type_labels: list[str],
    head_rows: list[list[str]],
    tail_rows: list[list[str]],
    max_cols: int | None,
) -> tuple[list[str], list[str], list[list[str]], list[list[str]], int | None]:
    """Hide columns past ``max_cols`` behind one gap column; ``None`` when everything fits."""
    if max_cols is None or len(names) <= max_cols:
        return names, type_labels, head_rows, tail_rows, None
    head_cols = (max_cols + 1) // 2
    tail_cols = max_cols - head_cols
    keep = list(range(head_cols)) + list(range(len(names) - tail_cols, len(names)))
    gap_names = [names[index] for index in keep]
    gap_labels = [type_labels[index] for index in keep]
    gap_head = [[row[index] for index in keep] for row in head_rows]
    gap_tail = [[row[index] for index in keep] for row in tail_rows]
    gap_names.insert(head_cols, "…")
    gap_labels.insert(head_cols, "")
    gap_head = [[*row[:head_cols], "…", *row[head_cols:]] for row in gap_head]
    gap_tail = [[*row[:head_cols], "…", *row[head_cols:]] for row in gap_tail]
    return gap_names, gap_labels, gap_head, gap_tail, head_cols
