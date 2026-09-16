"""FNP-AGG-1 step-2 aggregates: any_value, max_by, min_by, product."""

from __future__ import annotations

from typing import Any

from repark.errors import AnalysisException, PySparkTypeError
from repark.spark.column import Column
from repark.spark.functions import _aggregate_argument, lit
from repark.spark.functions_expr import _binary_aggregate

FNPAGG1_EXPORTS: tuple[str, ...] = (
    "any_value",
    "max_by",
    "min_by",
    "product",
)


def install_into(namespace: dict[str, Any], all: list[str]) -> None:
    """Expose the step-2 aggregate names on ``repark.spark.functions``."""
    from repark.spark import functions_agg_1 as module

    for name in FNPAGG1_EXPORTS:
        namespace[name] = getattr(module, name)
    all.extend(FNPAGG1_EXPORTS)


def _fold_flag(name: str, flag: Column) -> bool:
    """Fold a literal boolean flag Column to bool, mirroring bucket."""
    text = flag.sql_expr_part().strip() if bool(flag._is_foldable) else ""
    if text == "TRUE":
        return True
    if text == "FALSE":
        return False
    raise AnalysisException(f"{name} flag must be a boolean literal, got {text!r}")


def any_value(col: Column | str, ignoreNulls: bool | Column | None = None) -> Column:  # noqa: N803 — PySpark arg name
    """An arbitrary value of a group (PySpark ``functions.any_value``)."""
    if ignoreNulls is None:
        ignore = False
    elif isinstance(ignoreNulls, bool):
        ignore = ignoreNulls
    elif isinstance(ignoreNulls, Column):
        ignore = _fold_flag("any_value ignoreNulls", ignoreNulls)
    else:
        raise PySparkTypeError(
            f"any_value ignoreNulls must be bool, Column or None, got {type(ignoreNulls).__name__}"
        )
    column, part = _aggregate_argument(col)
    agg_name = f"any_value({part})"
    sql_expr = f"any_value({column.sql_expr_part()})"
    if ignore:
        sql_expr = f"any_value({column.sql_expr_part()}, TRUE)"
    return Column(
        column._inner.aggregate("first", ignore),
        agg_name=agg_name,
        sql_expr=sql_expr,
        join_sql_expr=f"first_value({column.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def max_by(col: Column | str, ord: Column | str) -> Column:
    """Value of ``col`` at the greatest ``ord`` (PySpark ``functions.max_by``)."""
    return _binary_aggregate("max_by", col, ord)


def min_by(col: Column | str, ord: Column | str) -> Column:
    """Value of ``col`` at the smallest ``ord`` (PySpark ``functions.min_by``)."""
    return _binary_aggregate("min_by", col, ord)


def product(col: Column | str) -> Column:
    """Product of a group as ``DoubleType`` (PySpark ``functions.product``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"product({part})"
    return Column(
        column._inner.aggregate("product", False),
        agg_name=agg_name,
        sql_expr=f"product({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def _mode_deterministic(col: Column | str) -> Column:
    """Deterministic ``mode`` lowered as a two-argument aggregate."""
    column, part = _aggregate_argument(col)
    flag = lit(True)
    agg_name = f"mode() WITHIN GROUP (ORDER BY {part} DESC)"
    return Column(
        column._inner.aggregate_binary("mode", flag._inner),
        agg_name=agg_name,
        sql_expr=f"mode({column.sql_expr_part()}, {flag.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )
