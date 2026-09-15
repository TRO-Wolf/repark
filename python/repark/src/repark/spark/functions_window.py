"""Window function wrappers for the Spark facade."""

from __future__ import annotations

from typing import Any

from repark import _native
from repark.errors import PySparkTypeError
from repark.spark.column import Column, Scalar
from repark.spark.functions import _column_argument, _scalar, lit

INSTALL_NAMES: tuple[str, ...] = ("window", "window_time", "session_window")


def install_into(namespace: dict[str, Any], exported: list[str]) -> None:
    """Copy this module's tail-installed names onto the canonical functions module."""
    for name in INSTALL_NAMES:
        namespace[name] = globals()[name]
        if name not in exported:
            exported.append(name)


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


def _duration_argument(name: str, value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise PySparkTypeError(f"{name} must be a duration string, got {type(value).__name__}")
    return value


def window(
    timeColumn: Column | str,  # noqa: N803 — PySpark parameter name
    windowDuration: str,  # noqa: N803 — PySpark parameter name
    slideDuration: str | None = None,  # noqa: N803 — PySpark parameter name
    startTime: str | None = None,  # noqa: N803 — PySpark parameter name
) -> Column:
    """Bucket rows into time windows (PySpark ``functions.window``)."""
    time = _column_argument(timeColumn)
    time._reject_nested_generator("function window")
    window_text = _duration_argument("windowDuration", windowDuration)
    slide_text = _duration_argument("slideDuration", slideDuration)
    start_text = _duration_argument("startTime", startTime)
    if window_text is None:
        raise PySparkTypeError("windowDuration must be a duration string, got None")
    inner = _native.PyColumnParts.time_window(time._inner, window_text, slide_text, start_text)
    display = f"window({time.spark_wrap_display_part()}, '{window_text}'"
    sql_text = f"window({time.sql_expr_part()}, '{window_text}'"
    if slide_text is not None:
        display += f", '{slide_text}'"
        sql_text += f", '{slide_text}'"
    if start_text is not None:
        display += f", '{start_text}'"
        sql_text += f", '{start_text}'"
    display += ")"
    sql_text += ")"
    return Column(
        inner,
        spark_display=display,
        projection_name=display,
        sql_expr=sql_text,
        is_aggregate=False,
        is_foldable=False,
        has_ungroupable=False,
    )


def window_time(windowColumn: Column | str) -> Column:  # noqa: N803 — PySpark parameter name
    """Event time of a window bucket (PySpark ``functions.window_time``)."""
    return _scalar("window_time", windowColumn)


def session_window(
    timeColumn: Column | str,  # noqa: N803 — PySpark parameter name
    gapDuration: Column | str,  # noqa: N803 — PySpark parameter name
) -> Column:
    """Group rows into sessions split by inactivity gaps (PySpark ``functions.session_window``)."""
    time = _column_argument(timeColumn)
    time._reject_nested_generator("function session_window")
    if isinstance(gapDuration, Column):
        gap: Column = gapDuration
        gap._reject_nested_generator("function session_window")
    elif isinstance(gapDuration, str):
        gap = lit(gapDuration)
    else:
        raise PySparkTypeError(
            f"gapDuration must be a duration string or a Column, got {type(gapDuration).__name__}"
        )
    inner = _native.PyColumnParts.session_window(time._inner, gap._inner)
    display = f"session_window({time.spark_wrap_display_part()}, {gap.spark_wrap_display_part()})"
    sql_text = f"session_window({time.sql_expr_part()}, {gap.sql_expr_part()})"
    return Column(
        inner,
        spark_display=display,
        projection_name=display,
        sql_expr=sql_text,
        is_aggregate=False,
        is_foldable=False,
        has_ungroupable=False,
    )
