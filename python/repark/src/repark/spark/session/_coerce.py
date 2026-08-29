"""Session helpers parked here because ``session_core.py`` sits at its exact file-size baseline."""

from __future__ import annotations


def range_bound_as_int(name: str, value: int | float | None) -> int:
    """Coerce a ``SparkSession.range`` bound or step; ``bool`` is rejected."""
    from repark.errors import PySparkTypeError

    if value is None:
        raise PySparkTypeError(f"range {name} must not be None")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PySparkTypeError(f"range {name} must be int or float, got {type(value).__name__}")
    return int(value)


def sql_clause_end_after(
    start: int | None,
    later: tuple[int | None, ...],
    body_length: int,
) -> int:
    """Next top-level SQL clause start after ``start``, or ``body_length``."""
    if start is None:
        return body_length
    ends = [index for index in (*later, body_length) if index is not None and index > start]
    return min(ends) if ends else body_length
