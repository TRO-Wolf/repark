"""Sorted- or ordered-row result comparison for the SQL fuzzer.

Same bar as TPC-H/TPC-DS (``bench/tpch/compare.py``):

- Pure integers: **exact** equality.
- Integral-valued floats and Decimals: **exact** integer equality.
- Non-integral Decimals: **exact** ``Decimal`` equality (not collapsed via ``float()``).
- Non-integral floats: relative tolerance ``1e-6`` with floor scale ``1e-12``.
- Mixed int/float: float rules (TPC-H bar; integral floats already → int).
- NULL/None must match. Dates/timestamps normalize via ``isoformat()``.
- Order-insensitive multiset unless the query has ``ORDER BY`` (then ordered).

Kept local (not imported from tpch) so the fuzzer package stays self-contained
and can grow ORDER BY semantics without coupling scoreboard harness changes.
"""

from __future__ import annotations

import datetime
import math
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict

RELATIVE_TOLERANCE: float = 1e-6


class CompareResult(BaseModel):
    """Outcome of comparing repark vs DuckDB result sets."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    equal: bool
    message: str
    repark_rows: int
    duckdb_rows: int


def compare_result_sets(
    repark_rows: list[tuple[Any, ...]],
    duckdb_rows: list[tuple[Any, ...]],
    *,
    column_names: list[str] | None = None,
    order_sensitive: bool = False,
) -> CompareResult:
    """Compare two result sets; sort both unless ``order_sensitive`` is True."""
    left = [_normalize_row(row) for row in repark_rows]
    right = [_normalize_row(row) for row in duckdb_rows]

    if len(left) != len(right):
        return CompareResult(
            equal=False,
            message=f"row count mismatch: repark={len(left)} duckdb={len(right)}",
            repark_rows=len(left),
            duckdb_rows=len(right),
        )

    if order_sensitive:
        left_view = left
        right_view = right
    else:
        left_view = sorted(left, key=_sort_key)
        right_view = sorted(right, key=_sort_key)

    for index, (left_row, right_row) in enumerate(zip(left_view, right_view, strict=True)):
        if len(left_row) != len(right_row):
            return CompareResult(
                equal=False,
                message=(
                    f"column count mismatch at row {index}: "
                    f"repark={len(left_row)} duckdb={len(right_row)}"
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
                        f"value mismatch at row {index} col {col_label}: "
                        f"repark={left_cell!r} duckdb={right_cell!r}"
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
        # Integral Decimals → int (exact). Non-integral Decimals stay Decimal so
        # distinct high-precision values are not collapsed via float() (C1-L-003).
        if cell == cell.to_integral_value():
            return int(cell)
        return cell.normalize()
    if isinstance(cell, bool):
        return cell
    if isinstance(cell, int) and not isinstance(cell, bool):
        return cell
    if isinstance(cell, float):
        if math.isnan(cell):
            return float("nan")
        if cell.is_integer():
            return int(cell)
        return cell
    # TZ-4 PR-2: LTZ export is tz-aware UTC; DuckDB is naive. Same instant.
    if isinstance(cell, datetime.datetime) and cell.tzinfo is not None:
        cell = cell.astimezone(datetime.UTC).replace(tzinfo=None)
    iso = getattr(cell, "isoformat", None)
    if callable(iso):
        return iso()
    return cell


def _cells_equal(left: Any, right: Any) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False

    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left is right

    if _is_integral(left) and _is_integral(right):
        return int(left) == int(right)

    # Decimal exactness (both sides Decimal after normalize) — C1-L-003.
    if isinstance(left, Decimal) and isinstance(right, Decimal):
        return left == right
    if isinstance(left, Decimal) and isinstance(right, float):
        return _floats_equal(float(left), right)
    if isinstance(left, float) and isinstance(right, Decimal):
        return _floats_equal(left, float(right))
    if isinstance(left, Decimal) and _is_integral(right):
        return left == Decimal(int(right))
    if _is_integral(left) and isinstance(right, Decimal):
        return Decimal(int(left)) == right

    # Mixed int/float: promote via float rules (TPC-H bar; integral floats already
    # normalized to int). Relative 1e-6 is intentional — not a silent widen.
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
    if left.is_integer() and right.is_integer():
        return int(left) == int(right)
    scale = max(abs(left), abs(right), 1e-12)
    return abs(left - right) / scale <= RELATIVE_TOLERANCE


def _is_integral(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _sort_key(row: tuple[Any, ...]) -> tuple[Any, ...]:
    """Stable sort key: None last; typed tags so int/Decimal/float never cross-compare."""
    keys: list[Any] = []
    for cell in row:
        if cell is None:
            keys.append((3, 0, ""))
        elif isinstance(cell, bool):
            keys.append((0, 0, int(cell)))
        elif isinstance(cell, int) and not isinstance(cell, bool):
            keys.append((1, 0, cell))
        elif isinstance(cell, Decimal):
            # Tag 1 separates Decimal from int/float payload compares (C1-L-003).
            keys.append((1, 1, str(cell)))
        elif isinstance(cell, float):
            if math.isnan(cell):
                keys.append((2, 0, float("inf")))
            else:
                keys.append((1, 2, cell))
        else:
            keys.append((1, 3, str(cell)))
    return tuple(keys)
