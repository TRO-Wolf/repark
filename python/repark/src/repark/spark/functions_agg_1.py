"""FNP-AGG-1 step-2 aggregates: any_value, max_by, min_by, product, percentile, count_min_sketch."""

from __future__ import annotations

import random
from typing import Any

from repark.errors import PySparkTypeError
from repark.spark.column import Column
from repark.spark.functions import _aggregate_argument, _thread_origin, lit
from repark.spark.functions_expr import _binary_aggregate, array

FNPAGG1_EXPORTS: tuple[str, ...] = (
    "any_value",
    "count_min_sketch",
    "grouping_id",
    "listagg_distinct",
    "max_by",
    "histogram_numeric",
    "min_by",
    "percentile",
    "product",
    "string_agg_distinct",
)


def install_into(namespace: dict[str, Any], all: list[str]) -> None:
    """Expose the step-2 aggregate names on ``repark.spark.functions``."""
    from repark.spark import functions_agg_1 as module

    for name in FNPAGG1_EXPORTS:
        namespace[name] = getattr(module, name)
    all.extend(FNPAGG1_EXPORTS)


def any_value(col: Column | str, ignoreNulls: bool | Column | None = None) -> Column:  # noqa: N803 — PySpark arg name
    """An arbitrary value of a group (PySpark ``functions.any_value``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"any_value({part})"
    if ignoreNulls is None:
        return Column(
            column._inner.aggregate("any_value", False),
            agg_name=agg_name,
            sql_expr=f"any_value({column.sql_expr_part()})",
            join_sql_expr=f"any_value({column.join_sql_part()})",
            spark_display=agg_name,
            projection_name=agg_name,
            partition_transform=column._partition_transform,
            **_thread_origin(column),
        )
    if isinstance(ignoreNulls, bool):
        flag = lit(ignoreNulls)
    elif isinstance(ignoreNulls, Column):
        flag = ignoreNulls
    else:
        raise PySparkTypeError(
            f"any_value ignoreNulls must be bool, Column or None, got {type(ignoreNulls).__name__}"
        )
    return Column(
        column._inner.aggregate_binary("any_value", [flag._inner]),
        agg_name=agg_name,
        sql_expr=f"any_value({column.sql_expr_part()}, {flag.sql_expr_part()})",
        join_sql_expr=f"any_value({column.join_sql_part()}, {flag.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def max_by(col: Column | str, ord: Column | str) -> Column:
    """Value of ``col`` at the greatest ``ord`` (PySpark ``functions.max_by``)."""
    return _binary_aggregate("max_by", col, ord)


def min_by(col: Column | str, ord: Column | str) -> Column:
    """Value of ``col`` at the smallest ``ord`` (PySpark ``functions.min_by``)."""
    return _binary_aggregate("min_by", col, ord)


def percentile(
    col: Column | str,
    percentage: Column | float | list[float],
    frequency: Column | int = 1,
) -> Column:
    """Exact percentile with linear interpolation (PySpark ``functions.percentile``)."""
    if isinstance(percentage, bool) or not isinstance(
        percentage, (int, float, list, tuple, Column)
    ):
        raise PySparkTypeError(
            "percentile percentage must be a number, a list of numbers or a Column, "
            f"got {type(percentage).__name__}"
        )
    if isinstance(percentage, (int, float)):
        pct: Column = lit(percentage)
        pct_part = repr(percentage)
    elif isinstance(percentage, (list, tuple)):
        pct = array(*[lit(item) for item in percentage])
        pct_part = f"array({', '.join(repr(item) for item in percentage)})"
    else:
        pct = percentage
        pct_part = percentage.spark_display_part()
    if isinstance(frequency, bool) or not isinstance(frequency, (int, Column)):
        raise PySparkTypeError(
            f"percentile frequency must be an int or a Column, got {type(frequency).__name__}"
        )
    column, part = _aggregate_argument(col)
    if isinstance(frequency, Column):
        freq: Column = frequency
        freq_part = frequency.spark_display_part()
        args = [pct._inner, freq._inner]
    elif frequency == 1:
        freq_part = "1"
        args = [pct._inner]
    else:
        freq = lit(frequency)
        freq_part = repr(frequency)
        args = [pct._inner, freq._inner]
    args_sql = [pct.sql_expr_part()] + ([freq.sql_expr_part()] if len(args) > 1 else [])
    args_join = [pct.join_sql_part()] + ([freq.join_sql_part()] if len(args) > 1 else [])
    agg_name = f"percentile({part}, {pct_part}, {freq_part})"
    return Column(
        column._inner.aggregate_binary("percentile", args),
        agg_name=agg_name,
        sql_expr=f"percentile({column.sql_expr_part()}, {', '.join(args_sql)})",
        join_sql_expr=f"percentile({column.join_sql_part()}, {', '.join(args_join)})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def _string_distinct(
    base: str, internal: str, col: Column | str, delimiter: Column | str | bytes | None
) -> Column:
    """Distinct values joined by ``delimiter`` (``listagg_distinct`` / ``string_agg_distinct``)."""
    if delimiter is None:
        delim: Column = lit(None)
        delim_part = "NULL"
    elif isinstance(delimiter, str):
        delim = lit(delimiter)
        delim_part = delimiter
    elif isinstance(delimiter, bytes):
        try:
            decoded = delimiter.decode("utf-8")
        except UnicodeDecodeError:
            raise PySparkTypeError(
                "listagg_distinct delimiter must be str, bytes, Column or None, "
                f"got {type(delimiter).__name__}"
            ) from None
        delim = lit(decoded)
        delim_part = decoded
    elif isinstance(delimiter, Column):
        delim = delimiter
        delim_part = delimiter.spark_display_part()
    else:
        raise PySparkTypeError(
            "listagg_distinct delimiter must be str, bytes, Column or None, "
            f"got {type(delimiter).__name__}"
        )
    column, part = _aggregate_argument(col)
    agg_name = f"{base}(DISTINCT {part}, {delim_part})"
    return Column(
        column._inner.aggregate_binary(internal, [delim._inner]),
        agg_name=agg_name,
        sql_expr=f"__repark_{internal}({column.sql_expr_part()}, {delim.sql_expr_part()})",
        join_sql_expr=f"__repark_{internal}({column.join_sql_part()}, {delim.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def listagg_distinct(col: Column | str, delimiter: Column | str | bytes | None = None) -> Column:
    """Distinct values joined by ``delimiter`` (PySpark ``functions.listagg_distinct``)."""
    return _string_distinct("listagg", "listagg_distinct", col, delimiter)


def string_agg_distinct(col: Column | str, delimiter: Column | str | bytes | None = None) -> Column:
    """Distinct values joined by ``delimiter`` (PySpark ``functions.string_agg_distinct``)."""
    return _string_distinct("string_agg", "string_agg_distinct", col, delimiter)


def histogram_numeric(col: Column | str, nBins: Column | int) -> Column:  # noqa: N803 — PySpark arg name
    """Histogram of ``col`` with ``nBins`` bins (PySpark ``functions.histogram_numeric``)."""
    if isinstance(nBins, bool) or not isinstance(nBins, (int, Column)):
        raise PySparkTypeError(
            f"histogram_numeric nBins must be an int or a Column, got {type(nBins).__name__}"
        )
    column, part = _aggregate_argument(col)
    if isinstance(nBins, Column):
        bins: Column = nBins
        bins_part = nBins.spark_display_part()
    else:
        bins = lit(nBins)
        bins_part = repr(nBins)
    agg_name = f"histogram_numeric({part}, {bins_part})"
    return Column(
        column._inner.aggregate_binary("histogram_numeric", [bins._inner]),
        agg_name=agg_name,
        sql_expr=f"histogram_numeric({column.sql_expr_part()}, {bins.sql_expr_part()})",
        join_sql_expr=f"histogram_numeric({column.join_sql_part()}, {bins.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def grouping_id(*cols: Column | str) -> Column:
    """Grouping-set bitmask of a row (PySpark ``functions.grouping_id``)."""
    from repark import _native

    columns: list[Column] = []
    parts: list[str] = []
    sql_parts: list[str] = []
    join_parts: list[str] = []
    for item in cols:
        column, part = _aggregate_argument(item)
        columns.append(column)
        parts.append(part)
        sql_parts.append(column.sql_expr_part())
        join_parts.append(column.join_sql_part())
    inner = _native.grouping_id_column([column._inner for column in columns])
    name = f"grouping_id({', '.join(parts)})"
    sql = f"grouping_id({', '.join(sql_parts)})"
    join_sql = f"grouping_id({', '.join(join_parts)})"
    if columns:
        first = columns[0]
        return Column(
            inner,
            agg_name=name,
            sql_expr=sql,
            join_sql_expr=join_sql,
            spark_display=name,
            projection_name=name,
            partition_transform=first._partition_transform,
            **_thread_origin(first),
        )
    return Column(
        inner,
        agg_name=name,
        sql_expr=sql,
        join_sql_expr=join_sql,
        spark_display=name,
        projection_name=name,
    )


def count_min_sketch(
    col: Column | str,
    eps: Column | float,
    confidence: Column | float,
    seed: Column | int | None = None,
) -> Column:
    """Count-min sketch of a group as bytes (PySpark ``functions.count_min_sketch``)."""
    if isinstance(eps, bool) or not isinstance(eps, (int, float, Column)):
        raise PySparkTypeError(
            f"count_min_sketch eps must be a number or a Column, got {type(eps).__name__}"
        )
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float, Column)):
        raise PySparkTypeError(
            "count_min_sketch confidence must be a number or a Column, "
            f"got {type(confidence).__name__}"
        )
    if seed is None:
        seed = random.randint(0, 2147483646)
    if isinstance(seed, bool) or not isinstance(seed, (int, Column)):
        raise PySparkTypeError(
            f"count_min_sketch seed must be an int, a Column or None, got {type(seed).__name__}"
        )
    column, part = _aggregate_argument(col)
    eps_column = eps if isinstance(eps, Column) else lit(eps)
    conf_column = confidence if isinstance(confidence, Column) else lit(confidence)
    seed_column = seed if isinstance(seed, Column) else lit(seed)
    eps_part = eps.spark_display_part() if isinstance(eps, Column) else repr(eps)
    conf_part = (
        confidence.spark_display_part() if isinstance(confidence, Column) else repr(confidence)
    )
    seed_part = seed.spark_display_part() if isinstance(seed, Column) else repr(seed)
    agg_name = f"count_min_sketch({part}, {eps_part}, {conf_part}, {seed_part})"
    return Column(
        column._inner.aggregate_binary(
            "count_min_sketch", [eps_column._inner, conf_column._inner, seed_column._inner]
        ),
        agg_name=agg_name,
        sql_expr=(
            f"count_min_sketch({column.sql_expr_part()}, {eps_column.sql_expr_part()}, "
            f"{conf_column.sql_expr_part()}, {seed_column.sql_expr_part()})"
        ),
        join_sql_expr=(
            f"count_min_sketch({column.join_sql_part()}, {eps_column.join_sql_part()}, "
            f"{conf_column.join_sql_part()}, {seed_column.join_sql_part()})"
        ),
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def product(col: Column | str) -> Column:
    """Product of a group as ``DoubleType`` (PySpark ``functions.product``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"product({part})"
    return Column(
        column._inner.aggregate("product", False),
        agg_name=agg_name,
        sql_expr=f"__repark_product({column.sql_expr_part()})",
        join_sql_expr=f"__repark_product({column.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def _mode_with_flag(col: Column | str, deterministic: bool | Column) -> Column:
    """Shared ``mode`` builder: the flag always issues a two-argument aggregate."""
    flag = lit(deterministic) if isinstance(deterministic, bool) else deterministic
    column, part = _aggregate_argument(col)
    if deterministic is True:
        agg_name = f"mode() WITHIN GROUP (ORDER BY {part} DESC)"
    else:
        agg_name = f"mode({part})"
    return Column(
        column._inner.aggregate_binary("mode", [flag._inner]),
        agg_name=agg_name,
        sql_expr=f"mode({column.sql_expr_part()}, {flag.sql_expr_part()})",
        join_sql_expr=f"mode({column.join_sql_part()}, {flag.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )
