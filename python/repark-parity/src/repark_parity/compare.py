"""Null-aware, order-insensitive comparison of Apache Arrow tables.

This is the core of the parity harness and the one piece of it with real behavior, so it carries
its own tests. It compares two :class:`pyarrow.Table` values by schema, row count, and cell values,
treating row order as insignificant by default (Spark result sets are unordered unless an
``ORDER BY`` pins them). Nulls at matching positions are equal; values are compared bit-exactly.

The schema signature is ``(name, type, nullable)`` per field: **field nullability is part of the
parity contract**. Spark carries a real nullability guarantee (e.g. ``coalesce`` with a non-null
fallback, or ``row_number``, are non-nullable), and the engine reproduces it, so a nullability
divergence is a genuine parity failure — not a decorative annotation. (This closes the residual that
the comparison core ignored nullability; verified empirically to keep every existing parity case
green.)

**Nested columns (list / struct / map).** Arrow's ``Table.sort_by`` rejects nested types, so the
order-insensitive path cannot sort every column when the schema is nested. The nested path (see
:func:`_sorted_by_all_columns`) builds a **total, deterministic** per-row sort key via recursive
canonical encoding, reorders with ``Table.take``, then compares with Arrow ``equals`` as before.
Flat-only schemas keep the historical ``sort_by`` path so existing corpora do not re-record.
Map entry order is normalized (keys sorted) before sort+equals so equal maps with different
storage order still match — list element order remains significant.
"""

from __future__ import annotations

import struct
from decimal import Decimal
from typing import Any

import pyarrow as pa


class FrameMismatchError(AssertionError):
    """Raised when two Arrow tables differ in schema, row count, or cell values."""


def assert_frames_equal(
    actual: pa.Table,
    expected: pa.Table,
    *,
    order_sensitive: bool = False,
) -> None:
    """Assert that ``actual`` matches ``expected``, raising :class:`FrameMismatchError` otherwise.

    Args:
        actual: The table produced by repark.
        expected: The reference table (a recorded Spark golden, or another engine's output).
        order_sensitive: When ``False`` (default) both tables are sorted by all columns first, so
            row order is ignored. Set ``True`` to also pin row order (e.g. when an ``ORDER BY`` is
            under test).
    """
    actual_signature = _schema_signature(actual)
    expected_signature = _schema_signature(expected)
    if actual_signature != expected_signature:
        raise FrameMismatchError(
            f"schema mismatch:\n  actual  : {actual_signature}\n  expected: {expected_signature}"
        )

    if actual.num_rows != expected.num_rows:
        raise FrameMismatchError(
            f"row count mismatch: actual={actual.num_rows} expected={expected.num_rows}"
        )

    left = actual if order_sensitive else _sorted_by_all_columns(actual)
    right = expected if order_sensitive else _sorted_by_all_columns(expected)

    # Arrow `equals` treats nulls at matching positions as equal and compares values bit-exactly.
    if not left.equals(right):
        raise FrameMismatchError(f"value mismatch at {_first_difference(left, right)}")


def _schema_signature(table: pa.Table) -> list[tuple[str, str, bool]]:
    """Return a comparable ``(name, type, nullable)`` signature of a table's schema (metadata
    ignored). Field nullability is part of the signature — see the module docstring."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _is_nested_type(arrow_type: pa.DataType) -> bool:
    """True when ``arrow_type`` is list, large_list, fixed_size_list, struct, or map."""
    return bool(
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_struct(arrow_type)
        or pa.types.is_map(arrow_type)
    )


def _schema_has_nested(schema: pa.Schema) -> bool:
    """True when any top-level field has a nested Arrow type."""
    return any(_is_nested_type(field.type) for field in schema)


def _sorted_by_all_columns(table: pa.Table) -> pa.Table:
    """Sort a table so two equal row-multisets become directly comparable via ``equals``.

    **Flat schemas** keep the historical ``Table.sort_by`` path (all columns ascending). That
    preserves the exact row order existing goldens and the facade suite already rely on.

    **Nested schemas** (any list/struct/map column) cannot use ``sort_by``. The nested path:

    1. Normalizes map entry order (recursive key sort) so equal maps with different storage
       order compare equal under Arrow ``equals``.
    2. Builds a total, deterministic per-row sort key by recursive canonical encoding of every
       cell (nulls first; list order significant; struct fields in schema order; map entries
       sorted by key).
    3. Reorders with ``Table.take`` and returns the permuted table.

    Rejected alternatives (recorded in the unit ledger): sort-by-flat-columns-only (not total when
    nested values distinguish rows); always-Counter-of-pylist (drops Arrow ``equals`` and changes
    the difference message path); JSON sort keys (not bit-exact for float/decimal).
    """
    if table.num_columns == 0 or table.num_rows == 0:
        return table
    if not _schema_has_nested(table.schema):
        keys = [(name, "ascending") for name in table.column_names]
        return table.sort_by(keys)
    normalized = _normalize_maps(table)
    row_keys = [_row_canonical_key(row, normalized.schema) for row in normalized.to_pylist()]
    # Stable argsort: equal keys keep relative order (Python's sorted is stable).
    indices = sorted(range(len(row_keys)), key=lambda index: row_keys[index])
    return normalized.take(pa.array(indices, type=pa.int64()))


def _normalize_maps(table: pa.Table) -> pa.Table:
    """Return a table whose map-typed values have key-value pairs sorted by key (recursive).

    List element order is left alone. Struct fields keep schema order. Non-map nested columns
    that contain no map anywhere are passed through without a pylist round-trip.
    """
    if not any(_type_contains_map(field.type) for field in table.schema):
        return table
    arrays: list[pa.Array] = []
    for field in table.schema:
        column = table.column(field.name)
        if _type_contains_map(field.type):
            values = [_normalize_map_value(cell, field.type) for cell in column.to_pylist()]
            arrays.append(pa.array(values, type=field.type))
        else:
            arrays.append(column.combine_chunks())
    return pa.Table.from_arrays(arrays, schema=table.schema)


def _type_contains_map(arrow_type: pa.DataType) -> bool:
    """True when ``arrow_type`` is a map or nests a map somewhere inside."""
    if pa.types.is_map(arrow_type):
        return True
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return _type_contains_map(arrow_type.value_type)
    if pa.types.is_struct(arrow_type):
        return any(_type_contains_map(field.type) for field in arrow_type)
    return False


def _normalize_map_value(value: object, arrow_type: pa.DataType) -> object:
    """Recursively sort map entries by key; descend into list/struct containers."""
    if value is None:
        return None
    if pa.types.is_map(arrow_type):
        # to_pylist maps → list[tuple[key, value]]
        assert isinstance(value, (list, tuple))
        key_type = arrow_type.key_type
        item_type = arrow_type.item_type
        pairs: list[tuple[object, object]] = [
            (entry[0], _normalize_map_value(entry[1], item_type)) for entry in value
        ]
        pairs.sort(key=lambda pair: _cell_canonical_key(pair[0], key_type))
        return pairs
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        assert isinstance(value, list)
        child = arrow_type.value_type
        return [_normalize_map_value(item, child) for item in value]
    if pa.types.is_struct(arrow_type):
        assert isinstance(value, dict)
        return {
            field.name: _normalize_map_value(value.get(field.name), field.type)
            for field in arrow_type
        }
    return value


def _row_canonical_key(row: dict[str, Any], schema: pa.Schema) -> tuple[Any, ...]:
    """Total, deterministic sort key for one pylist row under ``schema``."""
    return tuple(_cell_canonical_key(row.get(field.name), field.type) for field in schema)


def _cell_canonical_key(value: object, arrow_type: pa.DataType) -> tuple[Any, ...]:
    """Recursive canonical key: nulls first; nested shapes encoded as nested tuples.

    List element order is significant (Spark arrays are ordered). Map entries are sorted by
    key so two equal maps with different storage order share one key. Struct fields follow
    schema field order.
    """
    if value is None:
        return (0,)
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        assert isinstance(value, list)
        child = arrow_type.value_type
        return (1, tuple(_cell_canonical_key(item, child) for item in value))
    if pa.types.is_struct(arrow_type):
        assert isinstance(value, dict)
        return (
            1,
            tuple(_cell_canonical_key(value.get(field.name), field.type) for field in arrow_type),
        )
    if pa.types.is_map(arrow_type):
        assert isinstance(value, (list, tuple))
        key_type = arrow_type.key_type
        item_type = arrow_type.item_type
        pairs = [
            (
                _cell_canonical_key(entry[0], key_type),
                _cell_canonical_key(entry[1], item_type),
            )
            for entry in value
        ]
        pairs.sort()
        return (1, tuple(pairs))
    return (1, _scalar_sort_payload(value))


def _scalar_sort_payload(value: object) -> Any:
    """Hashable, totally ordered payload for a non-null scalar leaf.

    Floats use big-endian IEEE-754 bytes so NaN / -0.0 are bit-deterministic and comparable.
    Decimals use their coefficient tuple so scale-normalized Arrow values still order stably.
    """
    if isinstance(value, bool):
        # bool is a subclass of int; pin a distinct branch before the int case.
        return (0, int(value))
    if isinstance(value, int):
        return (1, value)
    if isinstance(value, float):
        return (2, struct.pack(">d", value))
    if isinstance(value, str):
        return (3, value)
    if isinstance(value, (bytes, bytearray)):
        return (4, bytes(value))
    if isinstance(value, Decimal):
        return (5, value.as_tuple())
    # date / datetime / time and other Arrow pylist leaves: fall back to their natural order
    # when comparable; otherwise to a type-tagged repr (total, deterministic for a given class).
    try:
        # Probe whether the value is totally ordered against itself (NaN-like rejects).
        # Intentional self-equality probe: NaN-like values reject `value == value`.
        if value == value:
            return (6, type(value).__qualname__, value)
    except TypeError:
        pass
    return (7, type(value).__qualname__, repr(value))


def _first_difference(left: pa.Table, right: pa.Table) -> str:
    """Describe the first row where two equal-length tables disagree (for the error message)."""
    for index, (left_row, right_row) in enumerate(
        zip(left.to_pylist(), right.to_pylist(), strict=True)
    ):
        if left_row != right_row:
            return f"row {index}:\n  actual  : {left_row}\n  expected: {right_row}"
    return "no row-level difference found (likely a type- or metadata-only difference)"
