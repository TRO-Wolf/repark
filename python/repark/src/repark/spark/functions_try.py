"""Spark ``try_*`` facade wrappers (FNP-7a/7b).

Installed onto ``functions.py`` so that module stays at its 1985-line ceiling.
pins: fnp-7-try-inversions/C-013, C-016
"""

from __future__ import annotations

from typing import Any

from repark.spark.column import Column
from repark.spark.functions import _aggregate_argument, _scalar, _thread_origin


def try_divide(left: Column | str | float, right: Column | str | float) -> Column:
    """NULL on divide-by-zero or overflow (PySpark ``functions.try_divide``)."""
    return _scalar("try_divide", left, right)


def try_mod(left: Column | str | float, right: Column | str | float) -> Column:
    """NULL on remainder-by-zero (PySpark ``functions.try_mod``)."""
    return _scalar("try_mod", left, right)


def try_add(left: Column | str | float, right: Column | str | float) -> Column:
    """NULL on overflow (PySpark ``functions.try_add``)."""
    return _scalar("try_add", left, right)


def try_subtract(left: Column | str | float, right: Column | str | float) -> Column:
    """NULL on overflow (PySpark ``functions.try_subtract``)."""
    return _scalar("try_subtract", left, right)


def try_multiply(left: Column | str | float, right: Column | str | float) -> Column:
    """NULL on overflow (PySpark ``functions.try_multiply``)."""
    return _scalar("try_multiply", left, right)


def try_element_at(
    col: Column | str,
    extraction: Column | str | int,
) -> Column:
    """1-based array element or map value; NULL on OOB / missing key.

    Index ``0`` still raises ``INVALID_INDEX_OF_ZERO``.
    """
    return _scalar(
        "try_element_at",
        col,
        extraction,
        lit_indices=frozenset({} if isinstance(extraction, Column) else {1}),
    )


def try_to_date(col: Column | str, format: str | None = None) -> Column:
    """Parse a date; NULL on malformed input (PySpark ``functions.try_to_date``)."""
    if format is None:
        return _scalar("try_to_date", col)
    return _scalar("try_to_date", col, format, lit_indices=frozenset({1}))


def try_to_number(col: Column | str, format: str) -> Column:
    """Parse a number with a format string; NULL on mismatch.

    A malformed format string itself raises ``INVALID_FORMAT``.
    """
    return _scalar("try_to_number", col, format, lit_indices=frozenset({1}))


def try_to_binary(col: Column | str, format: str | None = None) -> Column:
    """Decode bytes; NULL on failure. Default format is hex."""
    if format is None:
        return _scalar("try_to_binary", col)
    return _scalar("try_to_binary", col, format, lit_indices=frozenset({1}))


def try_to_time(col: Column | str, format: str | None = None) -> Column:
    """Spark 4.1.2 raises ``UNSUPPORTED_TIME_TYPE``; this wrapper matches that."""
    if format is None:
        return _scalar("try_to_time", col)
    return _scalar("try_to_time", col, format, lit_indices=frozenset({1}))


def try_sum(col: Column | str) -> Column:
    """Sum of a group; NULL on accumulator overflow (PySpark ``functions.try_sum``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"try_sum({part})"
    return Column(
        column._inner.aggregate("try_sum", False),
        agg_name=agg_name,
        sql_expr=f"try_sum({column.sql_expr_part()})",
        join_sql_expr=f"try_sum({column.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def try_avg(col: Column | str) -> Column:
    """Mean of a group as double (PySpark ``functions.try_avg``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"try_avg({part})"
    return Column(
        column._inner.aggregate("try_avg", False),
        agg_name=agg_name,
        sql_expr=f"try_avg({column.sql_expr_part()})",
        join_sql_expr=f"try_avg({column.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


TRY_EXPORTS: tuple[str, ...] = (
    "try_add",
    "try_avg",
    "try_divide",
    "try_element_at",
    "try_mod",
    "try_multiply",
    "try_subtract",
    "try_sum",
    "try_to_binary",
    "try_to_date",
    "try_to_number",
    "try_to_time",
)


def install_into(namespace: dict[str, Any], exported: list[str]) -> None:
    """Copy the FNP-7 try_* names onto the canonical functions module."""
    for name in TRY_EXPORTS:
        namespace[name] = globals()[name]
        if name not in exported:
            exported.append(name)
