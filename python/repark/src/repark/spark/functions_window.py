"""Window facade wrappers (FN-W).

Public names are re-exported from ``functions.py``. PySpark signatures live
here; Rust ``PyColumn`` builds the DataFusion window UDWF with no IntegerType
cast. ``ignoreNulls`` is an honest cut (not exposed).
"""

from __future__ import annotations

from repark import _native
from repark.spark.column import Column, Scalar
from repark.spark.functions import _column_argument, lit


def _window_column(
    inner: object,
    *,
    display: str,
    sql_expr: str,
) -> Column:
    return Column(
        inner,
        spark_display=display,
        projection_name=display,
        sql_expr=sql_expr,
        is_aggregate=False,
        is_foldable=False,
        has_ungroupable=True,
    )


def _require_int(name: str, value: int, *, positive: bool) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or (positive and value <= 0):
        from repark.errors import IllegalArgumentException

        kind = "a positive integer" if positive else "an integer"
        raise IllegalArgumentException(f"{name} requires {kind}, got {value!r}")
    return int(value)


def lag(col: Column | str, offset: int = 1, default: Scalar | Column = None) -> Column:
    """Preceding-row value (PySpark ``functions.lag``). Requires ``.over(...)``."""
    offset = _require_int("lag offset", offset, positive=False)
    column = _column_argument(col)
    default_column = default if isinstance(default, Column) else lit(default)
    display = f"lag({column.spark_wrap_display_part()}, {offset})"
    return _window_column(
        _native.PyColumn.lag([column._inner, lit(offset)._inner, default_column._inner]),
        display=display,
        sql_expr=f"lag({column.sql_expr_part()}, {offset})",
    )


def lead(col: Column | str, offset: int = 1, default: Scalar | Column = None) -> Column:
    """Following-row value (PySpark ``functions.lead``). Requires ``.over(...)``."""
    offset = _require_int("lead offset", offset, positive=False)
    column = _column_argument(col)
    default_column = default if isinstance(default, Column) else lit(default)
    display = f"lead({column.spark_wrap_display_part()}, {offset})"
    return _window_column(
        _native.PyColumn.lead([column._inner, lit(offset)._inner, default_column._inner]),
        display=display,
        sql_expr=f"lead({column.sql_expr_part()}, {offset})",
    )


def nth_value(col: Column | str, offset: int) -> Column:
    """1-based nth value in the frame (PySpark ``functions.nth_value``). Requires ``.over(...)``."""
    offset = _require_int("nth_value offset", offset, positive=True)
    column = _column_argument(col)
    display = f"nth_value({column.spark_wrap_display_part()}, {offset})"
    return _window_column(
        _native.PyColumn.nth_value([column._inner, lit(offset)._inner]),
        display=display,
        sql_expr=f"nth_value({column.sql_expr_part()}, {offset})",
    )


def percent_rank() -> Column:
    """Relative rank ``(rank-1)/(n-1)`` (PySpark ``functions.percent_rank``).

    Requires ``.over(...)``.
    """
    return _window_column(
        _native.PyColumn.percent_rank(),
        display="percent_rank()",
        sql_expr="percent_rank()",
    )


def cume_dist() -> Column:
    """Cumulative distribution (PySpark ``functions.cume_dist``). Requires ``.over(...)``."""
    return _window_column(
        _native.PyColumn.cume_dist(),
        display="cume_dist()",
        sql_expr="cume_dist()",
    )
