"""Method bodies for the COLUMN-PARITY-1 ``Column`` surface, bound on the class.

pins: column-parity-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

from typing import Any

from repark import _native
from repark.errors import (
    AnalysisException,
    PySparkRuntimeError,
    PySparkTypeError,
    UnsupportedOperationException,
)

_UNRESOLVED_STRUCT_EDIT_FIELD = "__repark_unresolved_struct_edit__"


def carried_select_attrs(column: Any) -> dict[str, Any]:
    """The select-boundary attributes every ``Column`` re-wrap must preserve."""
    return {
        "alias_metadata": column._alias_metadata,
        "outer": column._outer,
        "struct_edit_source": column._struct_edit_source,
        "struct_edits": column._struct_edits,
        "struct_edit_display": column._struct_edit_display,
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
    """SQL ``IN`` membership (PySpark ``Column.isin``)."""
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
        combined = Column._to_column(False)
    else:
        combined = column == value_columns[0]
        for value_column in value_columns[1:]:
            combined = combined | (column == value_column)
    display = (
        f"({column.spark_wrap_display_part()} IN "
        f"({', '.join(v.spark_wrap_display_part() for v in value_columns)}))"
    )
    return Column(
        combined._inner,
        spark_display=display,
        projection_name=display,
        stable_name=False,
        is_aggregate=combined._is_aggregate,
        is_foldable=combined._is_foldable,
        has_free_attribute=combined._has_free_attribute,
        has_ungroupable=combined._has_ungroupable,
        sql_expr=combined._sql_expr,
        join_sql_expr=combined._join_sql_expr,
    )


def is_nan(column: Any) -> Any:
    """NaN test coerced through DOUBLE (PySpark ``Column.isNaN``)."""
    from repark.spark.column import Column
    from repark.spark.functions_expr import isnan as _isnan

    result = _isnan(column.cast("double"))
    display = f"isnan({column.spark_wrap_display_part()})"
    return Column(
        result._inner,
        spark_display=display,
        projection_name=display,
        stable_name=False,
        is_aggregate=result._is_aggregate,
        is_foldable=result._is_foldable,
        has_free_attribute=result._has_free_attribute,
        has_ungroupable=result._has_ungroupable,
        sql_expr=result._sql_expr,
        join_sql_expr=result._join_sql_expr,
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
    path = tuple(fieldName.split("."))
    edit = ("with", path, col)
    display_piece = f"WithField({col.spark_wrap_display_part()})"
    return _extend_struct_edit(column, [edit], [display_piece])


def drop_fields(column: Any, *fieldNames: Any) -> Any:  # noqa: N803 — PySpark arg name
    """Drop struct fields by name, dotted paths allowed (PySpark ``Column.dropFields``)."""
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
    edits = [("drop", tuple(field_name.split(".")), None) for field_name in fieldNames]
    pieces = ["dropfield()"] * len(edits)
    return _extend_struct_edit(column, edits, pieces)


def extend_struct_item(column: Any, key: Any) -> Any:
    """Append a post-resolution item/field access to a pending struct-edit column."""
    return _extend_struct_edit(column, [("item", key, None)], None)


def _extend_struct_edit(
    column: Any,
    edits: list[tuple[str, Any, Any]],
    display_pieces: list[str] | None,
) -> Any:
    """Return a pending column whose struct edits resolve at the select boundary."""
    from repark.spark.column import Column

    source = column._struct_edit_source if column._struct_edit_source is not None else column
    prior_edits = column._struct_edits or []
    all_edits = [*prior_edits, *edits]
    prior_display = column._struct_edit_display
    if display_pieces is None:
        display = prior_display
    elif prior_display is not None:
        display = f"update_fields({prior_display}, {', '.join(display_pieces)})"
    else:
        display = f"update_fields({column.spark_wrap_display_part()}, {', '.join(display_pieces)})"
    return Column(
        _native.PyColumn.column(_UNRESOLVED_STRUCT_EDIT_FIELD),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        has_free_attribute=column._has_free_attribute,
        join_sql_expr=column._join_sql_expr,
        struct_edit_source=source,
        struct_edits=all_edits,
        struct_edit_display=display,
        alias_metadata=column._alias_metadata,
        outer=column._outer,
    )


def resolve_struct_edit_column(frame: Any, column: Any) -> Any:
    """Rebuild a pending struct-edit column against the frame's analyzed schema."""
    from repark.spark.column import Column
    from repark.spark.types import DataType, StructType

    if column._struct_edits is None:
        return column
    edits = list(column._struct_edits)
    struct_edits = [edit for edit in edits if edit[0] in ("with", "drop")]
    item_keys = [edit[1] for edit in edits if edit[0] == "item"]
    source = frame._rebind_origin_column(column._struct_edit_source)
    fields = frame._inner.select([source._inner]).logical_schema_fields()
    _field_name, type_key, _nullable = fields[0]
    data_type = DataType.fromDDL(type_key)
    sql_expr = column._struct_edit_display or "update_fields"
    if not isinstance(data_type, StructType):
        source_display = source.spark_wrap_display_part()
        raise AnalysisException(
            f'[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "{sql_expr}" due to '
            f'data type mismatch: The first parameter requires the "STRUCT" type, however '
            f'"{source_display}" has the type "{_spark_sql_type_name(type_key)}".'
        )
    rebound_edits = [
        (kind, path, frame._rebind_origin_column(value) if kind == "with" else value)
        for kind, path, value in struct_edits
    ]
    source_column = Column(source._inner, has_free_attribute=True)
    rebuilt = _rebuild_struct(source_column, data_type, rebound_edits, sql_expr)
    if rebuilt is None:
        rebuilt = source_column
    for key in item_keys:
        rebuilt = rebuilt[key]
    resolved_inner = rebuilt._inner
    if column._projection_name is not None:
        resolved_inner = resolved_inner.alias(column._projection_name)
    return Column(
        resolved_inner,
        spark_display=column._spark_display,
        projection_name=column._projection_name,
        stable_name=column._stable_name,
        has_free_attribute=True,
        alias_metadata=column._alias_metadata,
        outer=column._outer,
    )


def _rebuild_struct(
    source: Any,
    struct_type: Any,
    edits: list[tuple[str, tuple[str, ...], Any]],
    sql_expr: str,
) -> Any:
    """Return the rewritten struct expression, or ``None`` when nothing changed."""
    from repark.spark.column import Column
    from repark.spark.types import StructType

    out_fields: list[tuple[str, Any]] = []
    consumed: set[int] = set()
    changed = False
    for field in struct_type.fields:
        matching = [
            (index, edit)
            for index, edit in enumerate(edits)
            if edit[1] and edit[1][0].lower() == field.name.lower()
        ]
        for index, _edit in matching:
            consumed.add(index)
        sub_edits = [
            (kind, path[1:], value) for _i, (kind, path, value) in matching if len(path) > 1
        ]
        direct = [edit for _i, edit in matching if len(edit[1]) == 1]
        current: tuple[str, Any] | None = (field.name, source.getField(field.name))
        if sub_edits:
            if not isinstance(field.dataType, StructType):
                names = ", ".join(f"`{item.name}`" for item in struct_type.fields)
                raise AnalysisException(
                    f"[FIELD_NOT_FOUND] No such struct field `{sub_edits[0][1][0]}` in {names}."
                )
            inner = _rebuild_struct(
                source.getField(field.name), field.dataType, sub_edits, sql_expr
            )
            if inner is not None:
                current = (field.name, inner)
                changed = True
        for kind, path, value in direct:
            current = None if kind == "drop" else (path[0], value)
            changed = True
        if current is not None:
            out_fields.append(current)
    for index, (kind, path, value) in enumerate(edits):
        if index in consumed:
            continue
        if kind == "with":
            if len(path) == 1:
                out_fields.append((path[0], value))
                changed = True
            else:
                names = ", ".join(f"`{item.name}`" for item in struct_type.fields)
                raise AnalysisException(
                    f"[FIELD_NOT_FOUND] No such struct field `{path[0]}` in {names}."
                )
    if not out_fields:
        raise AnalysisException(
            f'[DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS] Cannot resolve "{sql_expr}" due to '
            "data type mismatch: Cannot drop all fields in struct."
        )
    if not changed:
        return None
    natives = [
        value._inner.alias(field_name or f"col{index}")
        for index, (field_name, value) in enumerate(out_fields)
    ]
    built = Column(
        _native.PyColumn.make_struct(natives),
        spark_display=sql_expr,
        has_free_attribute=True,
    )
    return _when_not_null(source, built)


def _when_not_null(source: Any, value: Any) -> Any:
    """``value`` only while ``source`` is non-NULL; NULL rows stay NULL."""
    from repark.spark.functions_expr import when as _when

    return _when(source.is_not_null(), value)


def _spark_sql_type_name(type_key: str) -> str:
    """Render a ``logical_schema_fields`` type key as Spark's SQL type name."""
    return type_key.upper()
