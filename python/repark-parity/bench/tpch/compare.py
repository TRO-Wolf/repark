"""Sorted-row TPC-H result comparison.

Correctness oracle = DuckDB result set. WRONG-RESULT is first-class and must never
be massaged away.

**Equality rules (disclosed):**

- Pure integers: **exact** equality.
- Integral-valued floats and Decimals (including after Decimal→normalize): **exact**
  integer equality — relative ``1e-6`` must not mask off-by-one at large magnitude
  (octo C1-Q-001 / C2-L-001).
- Non-integral floats: relative tolerance ``1e-6`` with floor scale ``1e-12``.
- Mixed int/float: promote int → float rules when the float is non-integral; exact
  when the float is integral-valued.
- NULL/None must match. Dates/timestamps normalize via ``isoformat()``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

RELATIVE_TOLERANCE: float = 1e-6


@dataclass(frozen=True)
class CompareResult:
    """Outcome of comparing repark vs DuckDB result sets."""

    equal: bool
    message: str
    repark_rows: int
    duckdb_rows: int


def compare_result_sets(
    repark_rows: list[tuple[Any, ...]],
    duckdb_rows: list[tuple[Any, ...]],
    *,
    column_names: list[str] | None = None,
    subject_label: str = "repark",
) -> CompareResult:
    """Compare two unordered result sets after sorting rows.

    ``subject_label`` names the left (subject) engine in mismatch messages so Sail
    WRONG-RESULT rows do not mislabel the subject as ``repark`` (C1-H-001).
    """
    left = [_normalize_row(row) for row in repark_rows]
    right = [_normalize_row(row) for row in duckdb_rows]
    left_name = subject_label or "repark"

    if len(left) != len(right):
        return CompareResult(
            equal=False,
            message=f"row count mismatch: {left_name}={len(left)} duckdb={len(right)}",
            repark_rows=len(left),
            duckdb_rows=len(right),
        )

    left_sorted = sorted(left, key=_sort_key)
    right_sorted = sorted(right, key=_sort_key)

    for index, (left_row, right_row) in enumerate(zip(left_sorted, right_sorted, strict=True)):
        if len(left_row) != len(right_row):
            return CompareResult(
                equal=False,
                message=(
                    f"column count mismatch at sorted row {index}: "
                    f"{left_name}={len(left_row)} duckdb={len(right_row)}"
                ),
                repark_rows=len(left),
                duckdb_rows=len(right),
            )
        for col_index, (left_cell, right_cell) in enumerate(zip(left_row, right_row, strict=True)):
            if not _cells_equal(left_cell, right_cell):
                col_label = (
                    column_names[col_index]
                    if column_names is not None and col_index < len(column_names)
                    else str(col_index)
                )
                return CompareResult(
                    equal=False,
                    message=(
                        f"value mismatch at sorted row {index} col {col_label}: "
                        f"{left_name}={left_cell!r} duckdb={right_cell!r}"
                    ),
                    repark_rows=len(left),
                    duckdb_rows=len(right),
                )

    return CompareResult(
        equal=True,
        message="ok",
        repark_rows=len(left),
        duckdb_rows=len(right),
    )


def _normalize_row(row: tuple[Any, ...] | list[Any]) -> tuple[Any, ...]:
    return tuple(_normalize_cell(cell) for cell in row)


def _normalize_cell(cell: Any) -> Any:
    if cell is None:
        return None
    if isinstance(cell, Decimal):
        # Keep integral Decimals as int so exact equality applies (C2-L-001).
        if cell == cell.to_integral_value():
            return int(cell)
        return float(cell)
    if isinstance(cell, bool):
        return cell
    if isinstance(cell, int) and not isinstance(cell, bool):
        return cell
    if isinstance(cell, float):
        if math.isnan(cell):
            return float("nan")
        # Integral-valued float → int for exact key/count equality.
        if cell.is_integer():
            return int(cell)
        return cell
    iso = getattr(cell, "isoformat", None)
    if callable(iso):
        return iso()
    return cell


def _cells_equal(left: Any, right: Any) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False

    # Bools: never `True == 1` (E1-L-004).
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left is right

    # Integrals (incl. integral-valued floats promoted in normalize): exact only.
    if _is_integral(left) and _is_integral(right):
        return int(left) == int(right)

    # Mixed int / non-integral float: promote int and use float rules.
    if _is_integral(left) and isinstance(right, float):
        return _floats_equal(float(left), right)
    if isinstance(left, float) and _is_integral(right):
        return _floats_equal(left, float(right))

    if isinstance(left, float) and isinstance(right, float):
        return _floats_equal(left, right)

    return left == right


def _floats_equal(left: float, right: float) -> bool:
    if math.isnan(left) and math.isnan(right):
        return True
    if math.isnan(left) or math.isnan(right):
        return False
    if left == right:
        return True
    # Defensive: integral-valued floats that slipped past normalize.
    if left.is_integer() and right.is_integer():
        return int(left) == int(right)
    scale = max(abs(left), abs(right), 1e-12)
    return abs(left - right) / scale <= RELATIVE_TOLERANCE


def _is_integral(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _sort_key(row: tuple[Any, ...]) -> tuple[Any, ...]:
    """Stable sort key: None last; keep ints as ints for exact key ordering."""
    keys: list[Any] = []
    for cell in row:
        if cell is None:
            keys.append((3, ""))
        elif isinstance(cell, bool):
            keys.append((0, int(cell)))
        elif isinstance(cell, int) and not isinstance(cell, bool):
            keys.append((1, cell))
        elif isinstance(cell, float):
            if math.isnan(cell):
                keys.append((2, float("inf")))
            else:
                keys.append((1, cell))
        else:
            keys.append((1, str(cell)))
    return tuple(keys)
