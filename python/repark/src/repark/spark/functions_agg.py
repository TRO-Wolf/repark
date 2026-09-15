"""Aggregate function aliases for the Spark facade."""

from __future__ import annotations

import warnings
from typing import Any

from repark.spark.column import Column
from repark.spark.functions import col, count, first, last, lit, max, min
from repark.spark.functions_expr import approx_count_distinct, stddev, when


def first_value(col: Column | str, ignorenulls: bool = False) -> Column:
    """First value in a group (PySpark ``functions.first_value``; alias of ``first``)."""
    return first(col, ignorenulls)


def last_value(col: Column | str, ignorenulls: bool = False) -> Column:
    """Last value in a group (PySpark ``functions.last_value``; alias of ``last``)."""
    return last(col, ignorenulls)


def std(col: Column | str) -> Column:
    """Sample standard deviation (PySpark ``functions.std``; alias of ``stddev``)."""
    return stddev(col)


def count_if(condition: Column | str) -> Column:
    """Count rows where ``condition`` is true (PySpark ``functions.count_if``).

    SHIM: ``count(when(condition, lit(1)))``. False and NULL conditions become
    NULL and are not counted.
    """
    predicate = condition if isinstance(condition, Column) else col(condition)
    return count(when(predicate, lit(1)))


def bool_and(col: Column | str) -> Column:
    """Boolean AND of a group (PySpark ``functions.bool_and``).

    SHIM over ``min``: for boolean values, ``min`` is NULL-skipping AND
    (any False → False; all-NULL → NULL; otherwise True).
    """
    return min(col)


def every(col: Column | str) -> Column:
    """Boolean AND of a group (PySpark ``functions.every``; alias of ``bool_and``)."""
    return bool_and(col)


def bool_or(col: Column | str) -> Column:
    """Boolean OR of a group (PySpark ``functions.bool_or``).

    SHIM over ``max``: for boolean values, ``max`` is NULL-skipping OR
    (any True → True; all-NULL → NULL; otherwise False).
    """
    return max(col)


def some(col: Column | str) -> Column:
    """Boolean OR of a group (PySpark ``functions.some``; alias of ``bool_or``)."""
    return bool_or(col)


INSTALL_NAMES: tuple[str, ...] = ("approxCountDistinct",)


def approxCountDistinct(col: Column | str, rsd: float | None = None) -> Column:  # noqa: N802
    """Approximate distinct count (PySpark ``functions.approxCountDistinct``; deprecated)."""
    warnings.warn(
        "Deprecated in 2.1, use approx_count_distinct instead.",
        FutureWarning,
        stacklevel=2,
    )
    return approx_count_distinct(col, rsd)


def install_into(namespace: dict[str, Any], exported: list[str]) -> None:
    """Copy this module's deprecated alias onto the canonical functions module."""
    for name in INSTALL_NAMES:
        namespace[name] = globals()[name]
        if name not in exported:
            exported.append(name)
