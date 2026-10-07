from __future__ import annotations

from typing import TYPE_CHECKING

from repark.spark import column_fields as _column_fields

if TYPE_CHECKING:
    from repark.spark.column import Column


def _with_sort_order(column: Column, *, ascending: bool, nulls_first: bool) -> Column:
    from repark.spark.column import Column

    return Column(
        column._inner,
        sort_ascending=ascending,
        sort_nulls_first=nulls_first,
        spark_display=column._spark_display,
        projection_name=column._projection_name,
        stable_name=column._stable_name,
        agg_name=column._agg_name,
        is_aggregate=column._is_aggregate,
        is_foldable=column._is_foldable,
        has_free_attribute=column._has_free_attribute,
        has_ungroupable=column._has_ungroupable,
        is_aggregate_function=column._is_aggregate_function,
        partition_transform=column._partition_transform,
        sql_expr=column._sql_expr,
        generator=column._generator,
        generator_cast=column._generator_cast,
        when_pairs=column._when_pairs,
        attr_id=column._attr_id,
        birth_frame=column._birth_frame,
        qualifiers=column._qualifiers,
        join_sql_expr=column._join_sql_expr,
        **_column_fields.carried_select_attrs(column),
    )


def asc(column: Column) -> Column:
    """Mark this column for ascending order (PySpark ``Column.asc``; nulls first)."""
    return _with_sort_order(column, ascending=True, nulls_first=True)


def asc_nulls_first(column: Column) -> Column:
    """Ascending order, nulls first (PySpark ``Column.asc_nulls_first``; same as ``asc``)."""
    return _with_sort_order(column, ascending=True, nulls_first=True)


def asc_nulls_last(column: Column) -> Column:
    """Ascending order, nulls LAST (PySpark ``Column.asc_nulls_last``)."""
    return _with_sort_order(column, ascending=True, nulls_first=False)


def desc(column: Column) -> Column:
    """Mark this column for descending order (PySpark ``Column.desc``; nulls last)."""
    return _with_sort_order(column, ascending=False, nulls_first=False)


def desc_nulls_first(column: Column) -> Column:
    """Descending order, nulls FIRST (PySpark ``Column.desc_nulls_first``)."""
    return _with_sort_order(column, ascending=False, nulls_first=True)


def desc_nulls_last(column: Column) -> Column:
    """Descending order, nulls last (PySpark ``Column.desc_nulls_last``; same as ``desc``)."""
    return _with_sort_order(column, ascending=False, nulls_first=False)
