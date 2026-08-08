"""The :class:`Window` / :class:`WindowSpec` facade — PySpark's ``pyspark.sql.window`` surface.

Build a window specification the same way as PySpark::

    from repark import Window
    from repark import functions as F

    spec = Window.partitionBy("group").orderBy(F.col("ts"))
    df.withColumn("rn", F.row_number().over(spec))

    # r20 G2: framed windows
    w = Window.partitionBy("g").orderBy("k").rowsBetween(Window.currentRow, 1)
    df.withColumn("mx", F.max("k").over(w))

A :class:`WindowSpec` is immutable — ``partitionBy`` / ``orderBy`` / ``rowsBetween`` /
``rangeBetween`` each return a new spec — and is consumed by :meth:`repark.column.Column.over`.
"""

from __future__ import annotations

import math
import sys
from typing import TYPE_CHECKING

from repark.column import Column
from repark.errors import AnalysisException, PySparkTypeError

if TYPE_CHECKING:
    from typing import Any

# === r20 G2: window/rand/sampleBy ===
# JVM long bounds used by PySpark Window (pyspark.util.JVM_LONG_MIN/MAX).
_JVM_LONG_MIN = -9223372036854775808
_JVM_LONG_MAX = 9223372036854775807


def _as_column(item: Column | str) -> Column:
    """Coerce a column-name-or-Column into a :class:`Column` (names via ``functions.col``)."""
    if isinstance(item, Column):
        return item
    if isinstance(item, str):
        from repark.functions import col

        return col(item)
    raise PySparkTypeError(
        f"window column expects a name (str) or Column, got {type(item).__name__}"
    )


def _window_column(item: Column | str) -> Column:
    """Coerce a window partition/order column and reject Group I partition transforms.

    ``F.years`` / ``months`` / ``days`` / ``hours`` are valid only inside
    ``DataFrameWriterV2.partitionedBy`` — using them as ``Window.partitionBy`` /
    ``orderBy`` keys would silently evaluate the dummy null literal (octo r3).

    Generators only lower via select unnest — ``Window.partitionBy`` /
    ``orderBy`` on a generator would window over the array placeholder
    (octo C7-Q-002; Spark ``UNSUPPORTED_GENERATOR``).
    """
    column = _as_column(item)
    transform = getattr(column, "_partition_transform", None)
    if transform is not None:
        raise AnalysisException(
            f"[PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY] The expression "
            f"{transform!r} must be inside 'partitionedBy'."
        )
    column._reject_nested_generator("window partitionBy/orderBy")
    return column


def _normalize_frame_bound(value: int | float, *, is_start: bool) -> int:
    """Clamp Spark frame bounds (accepts ``float('-inf')`` / ``float('inf')`` only among floats).

    **r20 G2 octo C1-Q-004:** Spark's JVM ``Window.rowsBetween(long, long)`` rejects finite
    ``Double`` bounds (no overload). Finite non-int floats must not silently ``int()``-truncate
    (``1.9 → 1``). Only integers and ±infinity (mapped to JVM long extremes, matching PySpark's
    threshold clamp for ``float('-inf')`` / ``float('inf')``) are accepted.
    """
    if isinstance(value, float):
        if math.isinf(value):
            return _JVM_LONG_MIN if value < 0 else _JVM_LONG_MAX
        if math.isnan(value):
            raise PySparkTypeError("window frame bound must be int (or ±inf), got float NaN")
        # Finite float — refuse rather than truncate (Spark has no Double overload).
        raise PySparkTypeError(
            f"window frame bound must be int (or ±inf), got finite float {value!r}"
        )
    if not isinstance(value, int) or isinstance(value, bool):
        raise PySparkTypeError(f"window frame bound must be int, got {type(value).__name__}")
    # PySpark Window.rowsBetween / rangeBetween clamps past JVM long thresholds.
    preceding_threshold = max(-sys.maxsize, _JVM_LONG_MIN)
    following_threshold = min(sys.maxsize, _JVM_LONG_MAX)
    if value <= preceding_threshold:
        return _JVM_LONG_MIN
    if value >= following_threshold:
        return _JVM_LONG_MAX
    # Quiet unused for symmetric API (start/end both clamp the same way).
    del is_start
    return int(value)


def _reject_inverted_frame_bounds(start: int, end: int) -> None:
    """Refuse start > end (Spark / DataFusion invalid window frame; octo C3-Q-001)."""
    if start > end:
        raise AnalysisException(
            "Invalid window frame: start bound cannot be larger than end bound "
            f"(got start={start}, end={end})"
        )


def _range_bound_is_value_offset(bound: int) -> bool:
    """True when a RANGE bound is a finite value offset (needs numeric ORDER BY).

    Peer-only RANGE frames allowed on non-numeric ORDER BY (Spark 4.1.2 oracle):
    unbounded↔current, current↔current, current↔unbounded, unbounded↔unbounded.
    Any finite non-zero offset requires NUMERIC / INTERVAL order key
    (``DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE``).
    """
    return bound not in (_JVM_LONG_MIN, _JVM_LONG_MAX, 0)


class WindowSpec:
    """An immutable window specification (near-drop-in for ``pyspark.sql.window.WindowSpec``).

    Holds partition columns, ordering columns (each may carry ``asc``/``desc``), and an
    optional frame (``rowsBetween`` / ``rangeBetween``). Consumed by
    :meth:`repark.column.Column.over`.
    """

    __slots__ = (
        "_frame_end",
        "_frame_start",
        "_frame_units",
        "_order_columns",
        "_partition_columns",
    )

    def __init__(
        self,
        partition_columns: list[Column],
        order_columns: list[Column],
        *,
        frame_units: str | None = None,
        frame_start: int | None = None,
        frame_end: int | None = None,
    ) -> None:
        """Store the (already-coerced) partition, ordering, and optional frame."""
        self._partition_columns = partition_columns
        self._order_columns = order_columns
        self._frame_units = frame_units
        self._frame_start = frame_start
        self._frame_end = frame_end

    def partitionBy(self, *cols: Column | str) -> WindowSpec:  # noqa: N802 — PySpark camelCase
        """Return a new spec partitioned by ``cols`` (PySpark ``WindowSpec.partitionBy``)."""
        return WindowSpec(
            [_window_column(item) for item in cols],
            self._order_columns,
            frame_units=self._frame_units,
            frame_start=self._frame_start,
            frame_end=self._frame_end,
        )

    def orderBy(self, *cols: Column | str) -> WindowSpec:  # noqa: N802 — PySpark camelCase
        """Return a new spec ordered by ``cols`` (PySpark ``WindowSpec.orderBy``)."""
        return WindowSpec(
            self._partition_columns,
            [_window_column(item) for item in cols],
            frame_units=self._frame_units,
            frame_start=self._frame_start,
            frame_end=self._frame_end,
        )

    def rowsBetween(self, start: int | float, end: int | float) -> WindowSpec:  # noqa: N802
        """Row-based frame from ``start`` to ``end`` (inclusive; Spark relative offsets)."""
        frame_start = _normalize_frame_bound(start, is_start=True)
        frame_end = _normalize_frame_bound(end, is_start=False)
        _reject_inverted_frame_bounds(frame_start, frame_end)
        return WindowSpec(
            self._partition_columns,
            self._order_columns,
            frame_units="rows",
            frame_start=frame_start,
            frame_end=frame_end,
        )

    def rangeBetween(self, start: int | float, end: int | float) -> WindowSpec:  # noqa: N802
        """Range-based frame from ``start`` to ``end`` (inclusive; Spark relative offsets).

        RANGE with a value-offset bound on a non-numeric ``orderBy`` is refused loud when the
        window is applied (``Column.over`` / plan select — Spark
        ``DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE``). Peer-only frames
        (unbounded/current) stay legal on string ORDER BY. RANGE without ORDER BY raises
        ``RANGE_FRAME_WITHOUT_ORDER``.
        """
        frame_start = _normalize_frame_bound(start, is_start=True)
        frame_end = _normalize_frame_bound(end, is_start=False)
        _reject_inverted_frame_bounds(frame_start, frame_end)
        return WindowSpec(
            self._partition_columns,
            self._order_columns,
            frame_units="range",
            frame_start=frame_start,
            frame_end=frame_end,
        )

    def _range_needs_numeric_order(self) -> bool:
        """True when this is a RANGE frame with a finite value offset (Spark numeric ORDER BY)."""
        if self._frame_units != "range":
            return False
        if self._frame_start is None or self._frame_end is None:
            return False
        return _range_bound_is_value_offset(self._frame_start) or _range_bound_is_value_offset(
            self._frame_end
        )

    def _validate_at_over(self) -> None:
        """Loud Spark-class checks applied when the spec is consumed by ``Column.over``.

        * RANGE without ORDER BY → ``DATATYPE_MISMATCH.RANGE_FRAME_WITHOUT_ORDER``.
        * Value-offset RANGE with **multiple** ORDER BY expressions →
          ``DATATYPE_MISMATCH.RANGE_FRAME_MULTI_ORDER`` (Spark 4.1.2 oracle; octo C2-Q-001).
        * Value-offset RANGE marks ``_g2_range_order_names`` on the result Column (set by
          ``Column.over``) so select/withColumn can refuse non-numeric order types against
          the DataFrame schema.
        """
        if self._frame_units != "range":
            return
        if not self._order_columns:
            raise AnalysisException(
                "[DATATYPE_MISMATCH.RANGE_FRAME_WITHOUT_ORDER] Cannot resolve RANGE window "
                "frame: A range window frame cannot be used in an unordered window "
                "specification. SQLSTATE: 42K09"
            )
        if self._range_needs_numeric_order() and len(self._order_columns) > 1:
            raise AnalysisException(
                "[DATATYPE_MISMATCH.RANGE_FRAME_MULTI_ORDER] Cannot resolve RANGE window "
                "frame: A range window frame with value boundaries cannot be used in a "
                "window specification with multiple order by expressions. SQLSTATE: 42K09"
            )

    # PySpark exposes only the camelCase spellings on WindowSpec; add snake_case for new code.
    partition_by = partitionBy
    order_by = orderBy
    rows_between = rowsBetween
    range_between = rangeBetween

    def _partition_natives(self) -> list[Any]:
        """The native ``PyColumn`` handles for the partition columns."""
        return [column._inner for column in self._partition_columns]

    def _order_specs(self) -> tuple[list[Any], list[bool], list[bool]]:
        """Parallel (native order columns, ascending, nulls_first) vectors for the native ``over``.

        Each ordering column's direction comes from its ``asc()`` / ``desc()`` marker (defaulting to
        ascending); Spark's null ordering follows the direction (ascending → nulls first, descending
        → nulls last).
        """
        natives: list[Any] = []
        ascending: list[bool] = []
        for column in self._order_columns:
            is_ascending = True if column._sort_ascending is None else column._sort_ascending
            natives.append(column._inner)
            ascending.append(is_ascending)
        nulls_first = list(ascending)
        return natives, ascending, nulls_first

    def _frame_args(self) -> tuple[str | None, int | None, int | None]:
        """``(units, start, end)`` for native ``over``; all None when no frame was set."""
        return self._frame_units, self._frame_start, self._frame_end


class Window:
    """Entry point for building a :class:`WindowSpec` (near-drop-in for ``pyspark.sql.window``).

    ``Window.partitionBy(...)`` / ``Window.orderBy(...)`` / ``Window.rowsBetween(...)`` /
    ``Window.rangeBetween(...)`` start a spec; chain the other methods on the returned
    :class:`WindowSpec`.
    """

    # === r20 G2: window/rand/sampleBy ===
    # PySpark camelCase constants (N815: surface is the Spark API).
    unboundedPreceding: int = _JVM_LONG_MIN  # noqa: N815
    unboundedFollowing: int = _JVM_LONG_MAX  # noqa: N815
    currentRow: int = 0  # noqa: N815

    # snake_case aliases (PySpark only has camelCase constants).
    unbounded_preceding = unboundedPreceding
    unbounded_following = unboundedFollowing
    current_row = currentRow

    @staticmethod
    def partitionBy(*cols: Column | str) -> WindowSpec:  # noqa: N802 — PySpark camelCase
        """Start a spec partitioned by ``cols`` (PySpark ``Window.partitionBy``)."""
        return WindowSpec([_window_column(item) for item in cols], [])

    @staticmethod
    def orderBy(*cols: Column | str) -> WindowSpec:  # noqa: N802 — PySpark camelCase
        """Start a spec ordered by ``cols`` (PySpark ``Window.orderBy``)."""
        return WindowSpec([], [_window_column(item) for item in cols])

    @staticmethod
    def rowsBetween(start: int | float, end: int | float) -> WindowSpec:  # noqa: N802
        """Start a spec with a row frame only (PySpark ``Window.rowsBetween``)."""
        frame_start = _normalize_frame_bound(start, is_start=True)
        frame_end = _normalize_frame_bound(end, is_start=False)
        _reject_inverted_frame_bounds(frame_start, frame_end)
        return WindowSpec(
            [],
            [],
            frame_units="rows",
            frame_start=frame_start,
            frame_end=frame_end,
        )

    @staticmethod
    def rangeBetween(start: int | float, end: int | float) -> WindowSpec:  # noqa: N802
        """Start a spec with a range frame only (PySpark ``Window.rangeBetween``)."""
        frame_start = _normalize_frame_bound(start, is_start=True)
        frame_end = _normalize_frame_bound(end, is_start=False)
        _reject_inverted_frame_bounds(frame_start, frame_end)
        return WindowSpec(
            [],
            [],
            frame_units="range",
            frame_start=frame_start,
            frame_end=frame_end,
        )

    # snake_case aliases for new code (PySpark exposes only the camelCase spellings).
    partition_by = partitionBy
    order_by = orderBy
    rows_between = rowsBetween
    range_between = rangeBetween


__all__ = ["Window", "WindowSpec"]
