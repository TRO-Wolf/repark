"""FNP-AGG-1 slice (d) aggregate: grouping_id."""

from __future__ import annotations

from typing import Any

from repark.spark.column import Column
from repark.spark.functions import _aggregate_argument, _thread_origin

FNPAGG1_EXPORTS: tuple[str, ...] = (
    "grouping_id",
)


def install_into(namespace: dict[str, Any], all: list[str]) -> None:
    """Expose the slice-(d) aggregate name on ``repark.spark.functions``."""
    from repark.spark import functions_agg_1 as module

    for name in FNPAGG1_EXPORTS:
        namespace[name] = getattr(module, name)
    all.extend(FNPAGG1_EXPORTS)


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
