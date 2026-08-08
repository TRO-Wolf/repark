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
"""

from __future__ import annotations

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


def _sorted_by_all_columns(table: pa.Table) -> pa.Table:
    """Sort a table by every column so two row-sets become directly comparable.

    Nested-typed columns are not sortable; the harness corpus uses flat schemas, matching the
    DataFrame shapes the conformance scripts emit.
    """
    if table.num_columns == 0 or table.num_rows == 0:
        return table
    keys = [(name, "ascending") for name in table.column_names]
    return table.sort_by(keys)


def _first_difference(left: pa.Table, right: pa.Table) -> str:
    """Describe the first row where two equal-length tables disagree (for the error message)."""
    for index, (left_row, right_row) in enumerate(
        zip(left.to_pylist(), right.to_pylist(), strict=True)
    ):
        if left_row != right_row:
            return f"row {index}:\n  actual  : {left_row}\n  expected: {right_row}"
    return "no row-level difference found (likely a type- or metadata-only difference)"
