"""Method bodies for the COLUMN-PARITY-1 ``Column`` surface, bound on the class.

pins: column-parity-1/C-001, C-002, C-003, C-004, C-005, C-008
"""

from __future__ import annotations

import functools
import re
from typing import Any

from repark import _native
from repark.errors import (
    AnalysisException,
    PySparkRuntimeError,
    PySparkTypeError,
    UnsupportedOperationException,
)
from repark.spark.column_errors import (
    _raise_folded_ambiguous,
    _raise_unresolved_name,
)
from repark.spark.filter_quote import (
    _FILTER_TOKEN_PATTERN,
    _bind_filter_token,
    _FoldedLambdaFallbackError,
    _frame_qualifiers_for_bind,
    _group_candidates,
    _known_qualifiers,
    _lambda_quoted_span,
    _lambda_scopes,
    _main_path_filter_sql,
    _refuse_ambiguous_free_names,
    _scopes_have_folded_collision,
    _unqualified_candidates,
)
from repark.spark.qualified_names import (
    _frame_id_snapshot,
    _rebind_qualified_refs,
    _refuse_using_key_name,
    _refuse_using_key_text,
    _resolve_sort_qualified_name,
    _rewrap_rebound_column,
    _route_sort_key_through_input,
    _stamped_frame_id_snapshot,
    _using_mark,
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


def is_bare_star(column: Any) -> bool:
    """Return whether ``column`` is an unaliased ``F.col("*")``."""
    return (getattr(column, "_sql_expr", None), getattr(column, "_projection_name", None)) == (
        "`*`",
        "*",
    )


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
        _native.column_outer(column._inner),
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
        attr_id=column._attr_id,
        birth_frame=column._birth_frame,
        qualifiers=column._qualifiers,
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


def _stamped_ids_and_engines(frame: Any) -> tuple[list[str | None], list[str]]:
    _, held, engines = _stamped_frame_id_snapshot(frame)
    return (held, engines)


def _bound_attr_id(frame: Any, engine_field: str) -> str | None:
    native, held, native_names = _stamped_frame_id_snapshot(frame)
    if engine_field not in native_names:
        raise RuntimeError(f"internal error: engine field {engine_field!r} left the native schema")
    attr_id: str | None = held[native_names.index(engine_field)]
    if attr_id is None and _native.frame_is_relation(native):
        raise RuntimeError(f"internal error: stamped field {engine_field!r} has no attribute id")
    return attr_id


def is_duplicated(column: Any) -> Any:
    """True where this value occurs more than once in the frame (polars ``is_duplicated``).

    Groups null with null, NaN with NaN, and -0.0 with 0.0. This is a RePark
    extension; PySpark has no such method.
    """
    from repark.spark.column import Column

    column._reject_nested_generator("is_duplicated")
    parts = _native.PyColumnParts.is_duplicated(
        column._inner,
        column.spark_wrap_display_part(),
        column.sql_expr_part(),
        column.join_sql_part(),
    )
    return Column(
        parts[0],
        spark_display=parts[1],
        sql_expr=parts[2],
        join_sql_expr=parts[3],
        stable_name=False,
        has_ungroupable=True,
    )


def column_or_str_error(item: Any) -> PySparkTypeError:
    """The ``select``/``_column_of`` rejection — ``TableArg`` gets Spark's conditioned error."""
    if type(item).__name__ == "TableArg" or "table_arg" in (type(item).__module__ or "").lower():
        return PySparkTypeError(
            "[NOT_COLUMN_OR_STR] Argument `col` should be a Column or str, got TableArg.",
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "col", "arg_type": "TableArg"},
        )
    return PySparkTypeError(f"expected a column name (str) or Column, got {type(item).__name__}")


def _live_rule_hits(frame: Any, written: str, displays: list[str]) -> list[int]:
    exact_hits = [index for index, display in enumerate(displays) if display == written]
    if _native.session_case_sensitive(frame._session):
        return exact_hits
    return exact_hits + _native.java_fold_hits(written, displays, "b")


@functools.lru_cache(maxsize=2048)
def _split_written_name(written: str) -> tuple[list[str] | None, str] | None:
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    index = 0
    while index < len(written):
        char = written[index]
        if char == "`":
            if quoted and index + 1 < len(written) and written[index + 1] == "`":
                current.append("`")
                index += 2
                continue
            quoted = not quoted
            index += 1
            continue
        if char == "." and not quoted:
            parts.append("".join(current))
            current = []
            index += 1
            continue
        current.append(char)
        index += 1
    if quoted:
        return None
    parts.append("".join(current))
    if any(not part for part in parts):
        return None
    if len(parts) == 1:
        return (None, parts[0])
    if any("." in part or "`" in part for part in parts):
        return None
    return (parts[:-1], parts[-1])


def _bind_resolved_name(frame: Any, written: str) -> Any:
    from repark.spark._idents import quote_ident as _quote_ident
    from repark.spark.column import Column

    split = _split_written_name(written)
    if split is None:
        return frame._bind_schema_column(written)
    qualifier_parts, name = split
    if name == "*":
        return frame._bind_schema_column(written)
    native, held, engine_names = _stamped_frame_id_snapshot(frame)
    displays = frame.columns
    if len(displays) != len(engine_names):
        return frame._bind_schema_column(written)
    if qualifier_parts is None:
        exact_hits, folded_hits = _unqualified_candidates(name, displays)
        if len(exact_hits) == 1 and not folded_hits:
            status, hits = "bound", exact_hits
        else:
            exact = _native.session_case_sensitive(frame._session)
            candidates = exact_hits if exact else exact_hits + folded_hits
            status, hits = _group_candidates(candidates, held)
            if status == "ambiguous" and name in displays:
                if written != name:
                    return frame._bind_schema_column(written)
                if len(exact_hits) != 1:
                    could_be = ", ".join(f"`{displays[index]}`" for index in exact_hits)
                    raise AnalysisException(
                        f"[AMBIGUOUS_REFERENCE] Reference `{name}` is ambiguous, "
                        f"could be: [{could_be}]."
                    )
                twin_engine = engine_names[exact_hits[0]]
                twin_quoted = _quote_ident(twin_engine)
                twin_born = Column(
                    _native.PyColumn.column(twin_quoted).alias(name),
                    spark_display=name,
                    projection_name=name,
                    stable_name=True,
                    has_free_attribute=True,
                    birth_frame=frame,
                )
                twin_born._sql_expr = twin_quoted
                return twin_born
    else:
        _refuse_using_key_name(frame, held, qualifier_parts, name)
        if not _native.frame_is_relation(native):
            return frame._bind_schema_column(written)
        exact = _native.session_case_sensitive(frame._session)
        qualifier = ".".join(qualifier_parts)
        status, hits, plan_quals = _native.resolve_display_name(
            native,
            name,
            qualifier,
            displays,
            exact,
            _frame_qualifiers_for_bind(frame),
        )
    if status == "missing":
        _raise_unresolved_name(qualifier_parts, name, displays)
    if status == "ambiguous":
        _raise_folded_ambiguous(qualifier_parts, name, hits, displays)
    position = hits[0]
    engine_field = engine_names[position]
    quoted = _quote_ident(engine_field)
    if qualifier_parts is not None:
        held_parts = plan_quals[0] if plan_quals else None
        if held_parts:
            quoted = ".".join([*(_quote_ident(part) for part in held_parts), quoted])
    attr_id = held[position]
    if attr_id is None and _native.frame_is_relation(native):
        raise RuntimeError(f"internal error: stamped field {engine_field!r} has no attribute id")
    repr_display = None
    if qualifier_parts is not None:
        shown = ".".join(qualifier_parts)
        if len(qualifier_parts) == 1:
            held_quals = (frame._frame_qualifiers or {}).get(attr_id) or ()
            match = [qual for qual in held_quals if qual == shown] or [
                qual for qual in held_quals if qual.lower() == shown.lower()
            ]
            if len(match) == 1:
                shown = match[0]
        repr_display = f"{shown}.{name}"
    bound = Column(
        _native.PyColumn.column(quoted).alias(name),
        spark_display=name,
        projection_name=name,
        stable_name=True,
        has_free_attribute=True,
        attr_id=attr_id,
        birth_frame=frame,
        qualifiers=frozenset((frame._frame_qualifiers or {}).get(attr_id) or ()),
    )
    bound._sql_expr = quoted
    bound._repr_display = repr_display
    return bound


def _rewrap_with_markers(column: Any, bound: Any) -> Any:
    from repark.spark.column import Column

    if column._sort_ascending is None and column._sort_nulls_first is None:
        return bound
    return Column(
        bound._inner,
        sort_ascending=column._sort_ascending,
        sort_nulls_first=column._sort_nulls_first,
        spark_display=bound._spark_display,
        projection_name=bound._projection_name,
        stable_name=bound._stable_name,
        agg_name=column._agg_name,
        is_aggregate=column._is_aggregate,
        is_foldable=column._is_foldable,
        has_free_attribute=bound._has_free_attribute or column._has_free_attribute,
        has_ungroupable=bound._has_ungroupable or column._has_ungroupable,
        is_aggregate_function=column._is_aggregate_function,
        partition_transform=column._partition_transform,
        sql_expr=bound._sql_expr if bound._sql_expr is not None else column._sql_expr,
        generator=column._generator,
        generator_cast=column._generator_cast,
        when_pairs=column._when_pairs,
        attr_id=bound._attr_id or column._attr_id,
        birth_frame=(bound._birth_frame if bound._birth_frame is not None else column._birth_frame),
        qualifiers=bound._qualifiers or column._qualifiers,
        join_sql_expr=bound._join_sql_expr or column._join_sql_expr,
    )


def _exact_rebind_position(
    frame: Any,
    column: Any,
    native_names: list[str],
    held: list[str | None],
    position: int,
) -> Any:
    from repark.spark._idents import quote_ident as _quote_ident
    from repark.spark.column import Column

    engine_field = native_names[position]
    if native_names.count(engine_field) != 1:
        return column
    quoted = _quote_ident(engine_field)
    shown = column._projection_name if column._projection_name is not None else engine_field
    rebound = Column(
        _native.attribute_column(engine_field).alias(shown),
        spark_display=column._spark_display,
        projection_name=column._projection_name,
        stable_name=True,
        has_free_attribute=True,
        attr_id=column._attr_id,
        birth_frame=frame,
        qualifiers=frozenset((frame._frame_qualifiers or {}).get(held[position]) or ()),
        join_sql_expr=column._join_sql_expr,
        **carried_select_attrs(column),
    )
    rebound._sql_expr = quoted
    return _rewrap_with_markers(column, rebound)


def _qualified_narrow_position(
    frame: Any, column: Any, held: list[str | None], displays: list[str]
) -> int | None:
    qualifiers = column._qualifiers
    frame_quals = frame._frame_qualifiers or {}
    if not qualifiers or not frame_quals:
        return None
    name = column._projection_name or column._spark_display
    if name is None:
        return None
    exact = bool(_native.session_case_sensitive(frame._session))
    hits = []
    for position, held_id in enumerate(held):
        if held_id is None or position >= len(displays):
            continue
        if not (qualifiers & (frame_quals.get(held_id) or set())):
            continue
        display = displays[position]
        if (display == name) if exact else (display.casefold() == name.casefold()):
            hits.append(position)
    if len(hits) == 1:
        return hits[0]
    return None


def _column_frame_id(column: Any) -> int | None:
    """Return the frame-node id a column was bound against, else ``None``."""
    birth = column._birth_frame
    if birth is None:
        return None
    return birth._frame_node.id


def _bind_stable_id_column(frame: Any, column: Any) -> Any | None:
    birth = column._birth_frame
    if birth is not None and birth is frame:
        return column
    attr_id = column._attr_id
    if attr_id is None:
        return None
    native, held, native_names = _frame_id_snapshot(frame)
    if attr_id in held:
        narrowed = _qualified_narrow_position(frame, column, held, list(frame.columns))
        if narrowed is not None:
            return _exact_rebind_position(frame, column, native_names, held, narrowed)
        hits = [position for position, held_id in enumerate(held) if held_id == attr_id]
        return _exact_rebind_position(frame, column, native_names, held, hits[0])
    name = column._projection_name or column._spark_display
    if name is None:
        return column
    displays = list(frame.columns)
    if len(displays) != len(native_names):
        return column
    exact = bool(_native.session_case_sensitive(frame._session))
    sources = _native.projection_source_ids(native)
    for position, source in enumerate(sources):
        if source != attr_id or position >= len(native_names):
            continue
        output = displays[position]
        if (output == name) if exact else (output.casefold() == name.casefold()):
            return _exact_rebind_position(frame, column, native_names, held, position)
    return column


def _rebind_stable_name_column(frame: Any, column: Any) -> Any:
    rebound = _bind_stable_id_column(frame, column)
    if rebound is not None:
        return rebound
    if not column._stable_name:
        _refuse_tokenless_free_names(frame, column)
        return column
    name = column._projection_name
    if name is None or name == "" or name == "*":
        return column
    if column._spark_display != name:
        _refuse_tokenless_free_names(frame, column)
        return column
    try:
        bound = _bind_resolved_name(frame, name)
    except AnalysisException as error:
        if error.getCondition() == "AMBIGUOUS_REFERENCE":
            split = _split_written_name(name)
            if split is not None and split[0] is None:
                raise
        _refuse_tokenless_free_names(frame, column)
        return column
    return _rewrap_with_markers(column, bound)


def _refuse_tokenless_free_names(frame: Any, column: Any) -> None:
    """Refuse free names in columns holding no parent-born token."""
    if "__REPARK_ATTR_" in column.join_sql_part():
        return
    spec = column_window_spec(column)
    if spec is not None and _window_keys_bound(spec):
        return
    _refuse_ambiguous_free_names(frame, column=column)


def _window_keys_bound(spec: Any) -> bool:
    """Whether every window partition and order key carries a bind marker."""
    keys = [*spec._partition_columns, *spec._order_columns]
    return bool(keys) and all(
        key._attr_id is not None or "__REPARK_ATTR_" in key.join_sql_part() for key in keys
    )


def _column_of(frame: Any, item: Any) -> Any:
    from repark.spark.column import Column

    if isinstance(item, Column):
        if _is_ambiguous_qualified_ref(frame, item):
            refused = frame._refuse_unemitted_ids(item)
            return _rebind_qualified_refs(frame, refused, False)
        rebound = _rebind_stable_name_column(frame, item)
        if rebound is item:
            return _rebind_qualified_refs(frame, frame._refuse_unemitted_ids(rebound), False)
        return frame._refuse_unemitted_ids(rebound)
    if isinstance(item, str):
        return _bind_resolved_name(frame, item)
    raise column_or_str_error(item)


def _is_ambiguous_qualified_ref(frame: Any, item: Any) -> bool:
    if not item._qualifiers or item._attr_id is None:
        return False
    held: list[str | None] = list(_native.attribute_ids(frame._plan()))
    return sum(1 for held_id in held if held_id == item._attr_id) > 1


def _ascii_folded(text: str) -> str:
    return "".join(chr(ord(char) + 32) if "A" <= char <= "Z" else char for char in text)


def _frame_has_unicode_folded_rivals(displays: list[str], held: list[str | None]) -> bool:
    groups: dict[str, list[int]] = {}
    for position, display in enumerate(displays):
        groups.setdefault(display.casefold(), []).append(position)
    for positions in groups.values():
        ids = {held[position] for position in positions if held[position] is not None}
        rivals = {_ascii_folded(displays[position]) for position in positions}
        if len(ids) > 1 and len(rivals) > 1:
            return True
    return False


def _rebind_free_names(frame: Any, column: Any, for_sort: bool) -> Any:
    if not for_sort and "__REPARK_ATTR_" not in column.join_sql_part():
        _refuse_ambiguous_free_names(frame, column=column, qualified_only=True)
    native, held, _ = _stamped_frame_id_snapshot(frame)
    exact = bool(_native.session_case_sensitive(frame._session))
    displays = list(frame.columns)
    if not exact and _frame_has_unicode_folded_rivals(displays, held):
        return column
    rebound = _native.bind_free_names(native, column._inner, displays, exact, for_sort)
    return _rewrap_rebound_column(column, rebound)


def _build_sort_bound_column(
    frame: Any, engine: str, display: str, shown: str, attr: str | None
) -> Any:
    from repark.spark._idents import quote_ident as _quote_ident
    from repark.spark.column import Column

    quoted = _quote_ident(engine)
    bound = Column(
        _native.PyColumn.column(quoted).alias(shown),
        spark_display=shown,
        projection_name=shown,
        stable_name=True,
        has_free_attribute=True,
        attr_id=attr,
        birth_frame=frame,
        qualifiers=frozenset((frame._frame_qualifiers or {}).get(attr) or ()),
    )
    bound._sql_expr = quoted
    return bound


def _checked_sort_position(
    frame: Any, native: Any, engine_names: list[str], held: list[str | None], position: int
) -> str:
    engine_field = engine_names[position]
    if held[position] is None and _native.frame_is_relation(native):
        detail = f"internal error: stamped field {engine_field!r} has no attribute id"
        raise RuntimeError(detail)
    return engine_field


def _resolve_sort_name(frame: Any, written: str, is_column_key: bool = False) -> Any:
    from repark.spark.column import Column

    split = _split_written_name(written)
    if split is None:
        return frame._bind_schema_column(written)
    qualifier_parts, name = split
    if name == "*":
        return frame._bind_schema_column(written)
    if qualifier_parts is not None:
        return _resolve_sort_qualified_name(frame, written, qualifier_parts, name)
    native, held, engine_names = _frame_id_snapshot(frame)
    displays = list(frame.columns)
    if len(displays) != len(engine_names):
        return frame._bind_schema_column(written)
    if None in held:
        native, held, engine_names = _stamped_frame_id_snapshot(frame)
    exact_hits, folded_hits = _unqualified_candidates(name, displays)
    if len(exact_hits) == 1 and not folded_hits:
        position = exact_hits[0]
        engine = _checked_sort_position(frame, native, engine_names, held, position)
        return _build_sort_bound_column(frame, engine, displays[position], name, held[position])
    exact = bool(_native.session_case_sensitive(frame._session))
    candidates = exact_hits if exact else exact_hits + folded_hits
    status, hits = _group_candidates(candidates, held)
    if status == "bound":
        position = hits[0]
        engine = _checked_sort_position(frame, native, engine_names, held, position)
        return _build_sort_bound_column(frame, engine, displays[position], name, held[position])
    if status == "missing":
        if _native.grandchild_key_status(native, name, exact) == "ambiguous":
            _raise_unresolved_name(None, name, displays)
        return Column(
            _native.PyColumn.column(name),
            spark_display=name,
            projection_name=name,
            stable_name=True,
        )
    if written != name:
        return frame._bind_schema_column(written)
    if _native.sort_child_shape(native) == "project":
        if _native.sort_hits_meet_at_join(native, hits):
            _raise_unresolved_name(None, name, displays)
        engine = _native.sort_sourced_twin_engine(native, hits, name, exact)
        if engine is not None:
            position = engine_names.index(engine)
            bound = _checked_sort_position(frame, native, engine_names, held, position)
            return _build_sort_bound_column(frame, bound, name, name, held[position])
        return _route_sort_key_through_input(native, name, hits, exact, engine_names, is_column_key)
    _raise_unresolved_name(None, name, displays)


def _bind_sort_key(frame: Any, item: Any) -> Any:
    from repark.spark.column import Column

    if isinstance(item, str):
        return _resolve_sort_name(frame, item)
    if isinstance(item, Column):
        rebound = _bind_stable_id_column(frame, item)
        if rebound is not None and rebound is not item:
            return rebound
        if rebound is item and item._sort_ascending is None and item._sort_nulls_first is None:
            return rebound
        if item._stable_name and item._attr_id is None:
            if _born_ambiguous(item):
                return item
            name = item._projection_name
            if name is not None and name != "" and name != "*" and item._spark_display == name:
                bound = _resolve_sort_name(frame, name, is_column_key=True)
                return _rewrap_with_markers(item, bound)
        if item._is_aggregate and _native.sort_child_shape(frame._plan()) == "aggregate":
            display = item._projection_name
            if display is None:
                display = item._spark_display
            if display is not None and display != "":
                try:
                    bound = _resolve_sort_name(frame, display, is_column_key=True)
                except AnalysisException:
                    pass
                else:
                    return _rewrap_with_markers(item, bound)
        if item._attr_id is not None and not item._qualifiers:
            _, held, engine_names = _frame_id_snapshot(frame)
            if item._attr_id not in held:
                name = item._projection_name or item._spark_display
                if name is not None and name != "" and name != "*":
                    displays = list(frame.columns)
                    if len(displays) == len(engine_names):
                        if None in held:
                            _, held, engine_names = _stamped_frame_id_snapshot(frame)
                        if len(displays) == len(engine_names):
                            exact_hits, folded_hits = _unqualified_candidates(name, displays)
                            exact = bool(_native.session_case_sensitive(frame._session))
                            candidates = exact_hits if exact else exact_hits + folded_hits
                            status, _ = _group_candidates(candidates, held)
                            if status == "ambiguous":
                                return item
        return _rebind_qualified_refs(frame, _rebind_free_names(frame, item, True), True)
    raise column_or_str_error(item)


def _born_ambiguous(column: Any) -> bool:
    """Whether ``column`` was born where its name already had a folded rival."""
    birth = column._birth_frame
    name = column._projection_name
    if birth is None or name is None:
        return False
    folded = name.casefold()
    return any(display != name and display.casefold() == folded for display in birth.columns)


def _quote_filter_sql_identifiers(frame: Any, sql: str) -> str:
    _refuse_ambiguous_free_names(frame, sql=sql)
    native, held, engine_names = _frame_id_snapshot(frame)
    displays = list(frame.columns)
    if not displays or len(displays) != len(engine_names):
        return sql
    if None in held:
        native, held, engine_names = _stamped_frame_id_snapshot(frame)
    exact = bool(_native.session_case_sensitive(frame._session))
    _refuse_using_key_text(_using_mark(frame), held, sql, exact, False)
    qualifiers, frame_bind_quals = _known_qualifiers(frame, native, exact)
    fold_map: dict[str, list[int]] = {}
    for position, display in enumerate(displays):
        fold_map.setdefault(display.casefold(), []).append(position)
    scopes: list[tuple[int, int, list[str]]] = []
    decls: dict[tuple[int, int], str] = {}
    if "->" in sql:
        scopes, decls = _lambda_scopes(sql)
    binder = functools.partial(
        _bind_filter_token,
        native=native,
        displays=displays,
        engine_names=engine_names,
        held=held,
        exact=exact,
        qualifiers=qualifiers,
        fold_map=fold_map,
        scopes=scopes,
        decls=decls,
        collision=_scopes_have_folded_collision(scopes, exact),
        frame_qualifiers=frame_bind_quals,
    )
    pieces = re.split(r"('(?:[^']|'')*')", sql)
    rebuilt: list[str] = []
    offset = 0
    try:
        for piece in pieces:
            if piece.startswith("'"):
                rebuilt.append(piece)
                offset += len(piece)
                continue
            subpieces = re.split(r'("(?:[^"]|"")*"|`(?:[^`]|``)*`)', piece)
            for subpiece in subpieces:
                if subpiece.startswith('"'):
                    rebuilt.append(subpiece)
                elif subpiece.startswith("`"):
                    rebuilt.append(_lambda_quoted_span(subpiece, scopes, decls, offset, exact))
                else:
                    scoped = functools.partial(binder, base=offset)
                    rebuilt.append(_FILTER_TOKEN_PATTERN.sub(scoped, subpiece))
                offset += len(subpiece)
    except _FoldedLambdaFallbackError:
        return _main_path_filter_sql(sql, displays)
    return "".join(rebuilt)
