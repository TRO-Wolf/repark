"""Method bodies for the COLUMN-PARITY-1 ``Column`` surface, bound on the class.

pins: column-parity-1/C-001, C-002, C-003, C-004, C-005, C-008
"""

from __future__ import annotations

from typing import Any

from repark import _native
from repark.errors import (
    PySparkRuntimeError,
    PySparkTypeError,
    UnsupportedOperationException,
)


def carried_select_attrs(column: Any) -> dict[str, Any]:
    """The select-boundary attributes every ``Column`` re-wrap must preserve."""
    return {
        "alias_metadata": column._alias_metadata,
        "outer": column._outer,
    }


def select_field_metadata(projected: list[Any]) -> dict[str, dict[str, Any]] | None:
    """Alias ``metadata=`` payloads keyed by projection name for ``DataFrame.schema``."""
    metadata = {
        column._projection_name: column._alias_metadata
        for column in projected
        if column._alias_metadata and column._projection_name is not None
    }
    return metadata or None


def apply_field_metadata(
    fields: list[Any], metadata: dict[str, dict[str, Any]] | None
) -> list[Any]:
    """Overlay carried alias metadata onto analyzed ``StructField``s."""
    if not metadata:
        return fields
    from repark.spark.types import StructField

    return [
        StructField(
            field.name,
            field.dataType,
            field.nullable,
            {**field.metadata, **metadata.get(field.name, {})},
        )
        for field in fields
    ]


def dataframe_dtypes(frame: Any) -> list[tuple[str, str]]:
    """Column name + simple type string pairs (PySpark ``DataFrame.dtypes``)."""
    return [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]


def dataframe_str(frame: Any) -> str:
    """``DataFrame[name: type, …]`` (PySpark ``DataFrame.__str__``)."""
    frame._ensure_alive()
    parts = [f"{name}: {type_name}" for name, type_name in frame.dtypes]
    return f"DataFrame[{', '.join(parts)}]"


def column_window_spec(column: Any) -> Any | None:
    """Return the window specification retained by a column, if any."""
    return getattr(column, "_window_spec", None)


def between(column: Any, lowerBound: Any, upperBound: Any) -> Any:  # noqa: N803 — PySpark args
    """``lowerBound <= column <= upperBound`` (PySpark ``Column.between``)."""
    column._reject_nested_generator("between")
    lower = column._to_column(lowerBound)
    upper = column._to_column(upperBound)
    lower._reject_nested_generator("between")
    upper._reject_nested_generator("between")
    return (column >= lower) & (column <= upper)


def eq_null_safe(column: Any, other: Any) -> Any:
    """Null-safe equality (``IS NOT DISTINCT FROM``; PySpark ``Column.eqNullSafe``)."""
    from repark.spark.column import Column

    column._reject_nested_generator("eqNullSafe")
    right = column._to_column(other)
    right._reject_nested_generator("eqNullSafe")
    parts = _native.PyColumnParts.eq_null_safe(
        column._inner,
        right._inner,
        (column.spark_wrap_display_part(), column.sql_expr_part(), column.join_sql_part()),
        (right.spark_wrap_display_part(), right.sql_expr_part(), right.join_sql_part()),
    )
    is_aggregate = column._is_aggregate or right._is_aggregate
    is_foldable = column._is_foldable and right._is_foldable and not is_aggregate
    return Column(
        parts[0],
        spark_display=parts[1],
        sql_expr=parts[2],
        join_sql_expr=parts[3],
        stable_name=False,
        is_aggregate=is_aggregate,
        is_foldable=is_foldable,
        has_free_attribute=column._has_free_attribute or right._has_free_attribute,
        has_ungroupable=column._has_ungroupable or right._has_ungroupable,
        partition_transform=column._partition_transform or right._partition_transform,
    )


def isin(column: Any, *cols: Any) -> Any:
    """SQL ``IN`` membership over one native list (PySpark ``Column.isin``)."""
    from repark.spark.column import Column

    values: list[Any] = list(cols)
    if len(values) == 1 and isinstance(values[0], (list, set)):
        values = list(values[0])
    for value in values:
        if isinstance(value, tuple):
            rendered = "[" + ", ".join(str(item) for item in value) + "]"
            raise PySparkRuntimeError(
                f"The feature is not supported: Literal for '{rendered}' "
                "of class java.util.ArrayList.",
                errorClass="UNSUPPORTED_FEATURE.LITERAL_TYPE",
                messageParameters={"type": "class java.util.ArrayList", "value": rendered},
            )
    value_columns = [Column._to_column(value) for value in values]
    if not value_columns:
        false_column = Column._to_column(False)
        inner = false_column._inner
        sql_expr: str | None = false_column._sql_expr
        join_expr: str | None = false_column._join_sql_expr
        is_aggregate = false_column._is_aggregate
        is_foldable = false_column._is_foldable
        free = false_column._has_free_attribute
        ungroupable = false_column._has_ungroupable
    else:
        column._reject_nested_generator("isin")
        for value_column in value_columns:
            value_column._reject_nested_generator("isin")
        rights = [
            (
                value_column.spark_wrap_display_part(),
                value_column.sql_expr_part(),
                value_column.join_sql_part(),
            )
            for value_column in value_columns
        ]
        rendered = _native.PyColumnParts.in_list(
            column._inner,
            [value_column._inner for value_column in value_columns],
            (
                column.spark_wrap_display_part(),
                column.sql_expr_part(),
                column.join_sql_part(),
            ),
            rights,
        )
        inner, _, sql_expr, join_expr = rendered
        is_aggregate = bool(
            column._is_aggregate or any(value._is_aggregate for value in value_columns)
        )
        is_foldable = (
            bool(column._is_foldable and all(value._is_foldable for value in value_columns))
            and not is_aggregate
        )
        free = bool(
            column._has_free_attribute or any(value._has_free_attribute for value in value_columns)
        )
        ungroupable = bool(
            column._has_ungroupable or any(value._has_ungroupable for value in value_columns)
        )
    display = (
        f"({column.spark_wrap_display_part()} IN "
        f"({', '.join(v.spark_wrap_display_part() for v in value_columns)}))"
    )
    return Column(
        inner,
        spark_display=display,
        projection_name=display,
        stable_name=False,
        is_aggregate=is_aggregate,
        is_foldable=is_foldable,
        has_free_attribute=free,
        has_ungroupable=ungroupable,
        sql_expr=sql_expr,
        join_sql_expr=join_expr,
    )


def is_nan(column: Any) -> Any:
    """NaN test with engine-side type dispatch (PySpark ``Column.isNaN``)."""
    from repark.spark.column import Column

    column._reject_nested_generator("isNaN")
    parts = _native.PyColumnParts.repark_isnan(
        column._inner,
        (column.spark_wrap_display_part(), column.sql_expr_part(), column.join_sql_part()),
    )
    display = f"isnan({column.spark_wrap_display_part()})"
    is_aggregate = column._is_aggregate
    return Column(
        parts[0],
        spark_display=display,
        projection_name=display,
        stable_name=False,
        is_aggregate=is_aggregate,
        is_foldable=bool(column._is_foldable) and not is_aggregate,
        has_free_attribute=column._has_free_attribute,
        has_ungroupable=column._has_ungroupable,
        sql_expr=parts[2],
        join_sql_expr=parts[3],
        partition_transform=column._partition_transform,
    )


def astype(column: Any, dataType: Any) -> Any:  # noqa: N803 — PySpark arg name
    """Cast to ``dataType`` (PySpark ``Column.astype`` — same grammar as ``cast``)."""
    casted = column.cast(dataType)
    if column._stable_name and column._projection_name is not None:
        return casted.alias(column._projection_name)
    return casted


def name(column: Any, *alias: str, **kwargs: Any) -> Any:
    """Rename the column (PySpark ``Column.name`` — ``alias`` spelling)."""
    return column.alias(*alias, **kwargs)


def outer(column: Any) -> Any:
    """Mark an outer reference for a correlated subquery (PySpark ``Column.outer``)."""
    from repark.spark.column import Column

    return Column(
        column._inner,
        sort_ascending=column._sort_ascending,
        sort_nulls_first=column._sort_nulls_first,
        when_pairs=column._when_pairs,
        agg_name=column._agg_name,
        is_aggregate=column._is_aggregate,
        is_foldable=column._is_foldable,
        has_free_attribute=column._has_free_attribute,
        has_ungroupable=column._has_ungroupable,
        is_aggregate_function=column._is_aggregate_function,
        generator=column._generator,
        generator_cast=column._generator_cast,
        spark_display=f"lazy({column.spark_wrap_display_part()})",
        projection_name=column._projection_name,
        stable_name=column._stable_name,
        partition_transform=column._partition_transform,
        sql_expr=column._sql_expr,
        origin_plan_id=column._origin_plan_id,
        origin_field=column._origin_field,
        join_sql_expr=column._join_sql_expr,
        g2_range_order_names=column._g2_range_order_names,
        window_spec=column._window_spec,
        alias_metadata=column._alias_metadata,
        outer=True,
    )


def with_field(column: Any, fieldName: Any, col: Any) -> Any:  # noqa: N803 — PySpark arg names
    """Add or replace a struct field (PySpark ``Column.withField``)."""
    from repark.spark.column import Column

    if not isinstance(fieldName, str):
        raise PySparkTypeError(
            errorClass="NOT_STR",
            messageParameters={"arg_name": "fieldName", "arg_type": type(fieldName).__name__},
        )
    if not isinstance(col, Column):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN",
            messageParameters={"arg_name": "col", "arg_type": type(col).__name__},
        )
    parts = _native.PyColumnParts.update_fields(
        column._inner,
        ["with"],
        [fieldName],
        [col._inner],
        (column.spark_wrap_display_part(), column.sql_expr_part(), column.join_sql_part()),
        [(col.spark_wrap_display_part(), col.sql_expr_part(), col.join_sql_part())],
    )
    return _update_fields_result(column, col, parts)


def drop_fields(column: Any, *fieldNames: Any) -> Any:  # noqa: N803 — PySpark arg name
    """Drop struct fields by name, dotted paths allowed (PySpark ``Column.dropFields``)."""
    from repark.spark.column import Column

    if not fieldNames:
        raise UnsupportedOperationException("tail of empty list")
    for field_name in fieldNames:
        if not isinstance(field_name, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={
                    "arg_name": "fieldNames",
                    "arg_type": type(field_name).__name__,
                },
            )
    parts = _native.PyColumnParts.update_fields(
        column._inner,
        ["drop"] * len(fieldNames),
        list(fieldNames),
        [],
        (column.spark_wrap_display_part(), column.sql_expr_part(), column.join_sql_part()),
        [],
    )
    return Column(
        parts[0],
        spark_display=parts[1],
        projection_name=parts[1],
        stable_name=False,
        is_aggregate=column._is_aggregate,
        is_foldable=column._is_foldable and not column._is_aggregate,
        has_free_attribute=column._has_free_attribute,
        has_ungroupable=column._has_ungroupable,
        sql_expr=parts[2],
        join_sql_expr=parts[3],
        partition_transform=column._partition_transform,
        outer=column._outer,
    )


def _update_fields_result(column: Any, value: Any, parts: Any) -> Any:
    """Wrap one native ``update_fields`` call with OR-propagated expression flags."""
    from repark.spark.column import Column

    is_aggregate = column._is_aggregate or value._is_aggregate
    return Column(
        parts[0],
        spark_display=parts[1],
        projection_name=parts[1],
        stable_name=False,
        is_aggregate=is_aggregate,
        is_foldable=column._is_foldable and value._is_foldable and not is_aggregate,
        has_free_attribute=column._has_free_attribute or value._has_free_attribute,
        has_ungroupable=column._has_ungroupable or value._has_ungroupable,
        sql_expr=parts[2],
        join_sql_expr=parts[3],
        partition_transform=column._partition_transform or value._partition_transform,
        outer=column._outer,
    )
