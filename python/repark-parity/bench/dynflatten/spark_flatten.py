"""PySpark 4.1.2 explode + struct expansion matching repark ``dynamicFlatten``.

Sequential Cartesian expansion: lists explode one column at a time in schema
order. Multi-column Unnest zip/pad is not used.
"""

from __future__ import annotations

from typing import Any

MAX_DEPTH_DEFAULT = 100


def _is_struct(data_type: Any) -> bool:
    """True when Spark type is a struct."""
    return type(data_type).__name__ == "StructType"


def _is_array(data_type: Any) -> bool:
    """True when Spark type is an array."""
    return type(data_type).__name__ == "ArrayType"


def _is_null_type(data_type: Any) -> bool:
    """True when Spark type is NullType."""
    return type(data_type).__name__ == "NullType"


def spark_dynamic_flatten(
    frame: Any,
    *,
    separator: str = "_",
    explode_lists: bool = True,
    drop_null_lists: bool = True,
    empty_as_null: bool = True,
    max_depth: int = MAX_DEPTH_DEFAULT,
) -> Any:
    """Flatten ``frame`` the way repark ``dynamicFlatten`` rewrites a plan.

    Args:
        frame: a PySpark DataFrame.
        separator: parent-path prefix separator.
        explode_lists: explode arrays after structs are gone.
        drop_null_lists: drop ``array<void>`` columns.
        empty_as_null: rewrite empty arrays to a singleton-null array first.
        max_depth: rewrite-pass bound.

    Returns:
        The flattened PySpark DataFrame.

    Raises:
        ValueError: nesting remains after ``max_depth`` passes.
    """
    from pyspark.sql import functions as spark_functions
    from pyspark.sql.types import NullType

    current = frame
    for _pass in range(max_depth):
        fields = list(current.schema.fields)
        struct_fields = [field for field in fields if _is_struct(field.dataType)]
        if struct_fields:
            expressions = []
            for field in fields:
                if _is_struct(field.dataType):
                    for nested in field.dataType.fields:
                        prefixed = f"{field.name}{separator}{nested.name}"
                        expressions.append(
                            spark_functions.col(f"`{field.name}`.`{nested.name}`").alias(prefixed)
                        )
                else:
                    expressions.append(spark_functions.col(f"`{field.name}`"))
            if not expressions:
                msg = "dynamicFlatten: schema is only empty struct column(s)"
                raise ValueError(msg)
            current = current.select(*expressions)
            continue
        if not explode_lists:
            return current
        array_fields = [field for field in fields if _is_array(field.dataType)]
        if not array_fields:
            return current
        for field in array_fields:
            element = field.dataType.elementType
            if drop_null_lists and (_is_null_type(element) or isinstance(element, NullType)):
                current = current.drop(field.name)
                continue
            column = spark_functions.col(f"`{field.name}`")
            if empty_as_null:
                singleton = spark_functions.array(spark_functions.lit(None).cast(element))
                current = current.withColumn(
                    field.name,
                    spark_functions.when(spark_functions.size(column) == 0, singleton).otherwise(
                        column
                    ),
                )
            exploded = f"{field.name}__e"
            live = spark_functions.col(f"`{field.name}`")
            current = (
                current.select("*", spark_functions.explode_outer(live).alias(exploded))
                .drop(field.name)
                .withColumnRenamed(exploded, field.name)
            )
    msg = f"dynamicFlatten exceeded max_depth={max_depth} with nested columns remaining"
    raise ValueError(msg)
