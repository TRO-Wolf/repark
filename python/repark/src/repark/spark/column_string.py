from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark import _native
from repark.errors import PySparkTypeError

if TYPE_CHECKING:
    from repark.spark.column import Column, Scalar


def _string_predicate(
    column: Column,
    call_name: str,
    other: Column | Scalar,
    *,
    display_name: str | None = None,
) -> Column:
    from repark.spark.column import Column

    shown = display_name or call_name
    column._reject_nested_generator(shown)
    right = Column._to_column(other)
    right._reject_nested_generator(shown)
    parts = _native.PyColumnParts.string_predicate(
        column._inner,
        right._inner,
        call_name,
        shown,
        (column.spark_wrap_display_part(), right.spark_wrap_display_part()),
        (column.sql_expr_part(), right.sql_expr_part()),
    )
    is_aggregate = column._is_aggregate or right._is_aggregate
    is_foldable = column._is_foldable and right._is_foldable and not is_aggregate
    has_free_attribute = column._has_free_attribute or right._has_free_attribute
    has_ungroupable = column._has_ungroupable or right._has_ungroupable
    return Column(
        parts[0],
        spark_display=parts[1],
        sql_expr=parts[2],
        stable_name=False,
        is_aggregate=is_aggregate,
        is_foldable=is_foldable,
        has_free_attribute=has_free_attribute,
        has_ungroupable=has_ungroupable,
        partition_transform=column._partition_transform or right._partition_transform,
    )


def contains(column: Column, other: Column | Scalar) -> Column:
    """Substring containment (PySpark ``Column.contains``)."""
    return _string_predicate(column, "contains", other)


def substr(column: Column, startPos: Column | int, length: Column | int) -> Column:  # noqa: N803
    """Substring slice (PySpark ``Column.substr``).

    Spark 1-based positions; ``startPos=0`` is treated as 1 (owned substring UDF).
    ``startPos`` and ``length`` must share a type (both int or both Column) — same
    checks as classic ``Column.__getitem__`` slice path.
    """
    from repark.spark.column import Column
    from repark.spark.functions import lit

    column._reject_nested_generator("substr")
    start = startPos
    stop = length
    if type(start) is not type(stop):
        raise PySparkTypeError(
            errorClass="NOT_SAME_TYPE",
            messageParameters={
                "arg_name1": "startPos",
                "arg_name2": "length",
                "arg_type1": type(start).__name__,
                "arg_type2": type(stop).__name__,
            },
        )
    if isinstance(start, int):
        start_col = lit(int(start))
        length_col = lit(int(stop))
        start_display: Any = start
        length_display: Any = stop
    elif isinstance(start, Column):
        start_col = start
        length_col = stop  # type: ignore[assignment]
        start_display = start.spark_wrap_display_part()
        length_display = length_col.spark_wrap_display_part()
    else:
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_INT",
            messageParameters={
                "arg_name": "startPos",
                "arg_type": type(start).__name__,
            },
        )
    parts = _native.PyColumnParts.substr(
        column._inner,
        start_col._inner,
        length_col._inner,
        (column.spark_wrap_display_part(), str(start_display), str(length_display)),
        (column.sql_expr_part(), start_col.sql_expr_part(), length_col.sql_expr_part()),
    )
    return Column(
        parts[0],
        spark_display=parts[1],
        sql_expr=parts[2],
        has_free_attribute=column._has_free_attribute,
        is_foldable=column._is_foldable and not column._is_aggregate,
        is_aggregate=column._is_aggregate,
        has_ungroupable=column._has_ungroupable,
    )


def startswith(column: Column, other: Column | Scalar) -> Column:
    """Prefix test (PySpark ``Column.startswith``)."""
    return _string_predicate(column, "starts_with", other, display_name="startswith")


def endswith(column: Column, other: Column | Scalar) -> Column:
    """Suffix test (PySpark ``Column.endswith``)."""
    return _string_predicate(column, "ends_with", other, display_name="endswith")


def like(column: Column, other: Column | Scalar) -> Column:
    """SQL ``LIKE`` (PySpark ``Column.like``)."""
    return _string_predicate(column, "like", other)


def ilike(column: Column, other: Column | Scalar) -> Column:
    """Case-insensitive ``LIKE`` (PySpark ``Column.ilike``)."""
    return _string_predicate(column, "ilike", other)


def rlike(column: Column, other: Column | Scalar) -> Column:
    """Regex match (PySpark ``Column.rlike``)."""
    return _string_predicate(column, "rlike", other)
