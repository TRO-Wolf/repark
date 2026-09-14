"""The ``DataFrame.replace`` body — PySpark-shaped validation and one searched CASE per column."""

from __future__ import annotations

import uuid
import warnings
from typing import TYPE_CHECKING, Any

from repark.errors import (
    IllegalArgumentException,
    PySparkTypeError,
    PySparkValueError,
)
from repark.spark.column import Column

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame


def _validate_replace_arguments(to_replace: Any, value: Any, subset: Any) -> None:
    """Eager argument checks, in PySpark ``DataFrame.replace`` order."""
    if not isinstance(to_replace, (bool, float, int, str, list, tuple, dict)):
        raise PySparkTypeError(
            errorClass="NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE",
            messageParameters={
                "arg_name": "to_replace",
                "arg_type": type(to_replace).__name__,
            },
        )
    if (
        not isinstance(value, (bool, float, int, str, list, tuple))
        and value is not None
        and not isinstance(to_replace, dict)
    ):
        raise PySparkTypeError(
            errorClass="NOT_BOOL_OR_FLOAT_OR_INT_OR_LIST_OR_NONE_OR_STR_OR_TUPLE",
            messageParameters={
                "arg_name": "value",
                "arg_type": type(value).__name__,
            },
        )
    if (
        isinstance(to_replace, (list, tuple))
        and isinstance(value, (list, tuple))
        and len(to_replace) != len(value)
    ):
        raise PySparkValueError(
            errorClass="LENGTH_SHOULD_BE_THE_SAME",
            messageParameters={
                "arg1": "to_replace",
                "arg2": "value",
                "arg1_length": str(len(to_replace)),
                "arg2_length": str(len(value)),
            },
        )
    if not (subset is None or isinstance(subset, (list, tuple, str))):
        raise PySparkTypeError(
            errorClass="NOT_LIST_OR_STR_OR_TUPLE",
            messageParameters={"arg_name": "subset", "arg_type": type(subset).__name__},
        )


def _replacement_dict(to_replace: Any, value: Any) -> dict[Any, Any]:
    """Reshape scalar/list/tuple arguments into the replacement mapping PySpark builds."""
    if isinstance(to_replace, (float, int, str)):
        to_replace = [to_replace]
    if isinstance(to_replace, dict):
        if value is not None:
            warnings.warn(
                "to_replace is a dict and value is not None. value will be ignored.",
                UserWarning,
                stacklevel=4,
            )
        return dict(to_replace)
    if isinstance(value, (float, int, str)) or value is None:
        value = [value] * len(to_replace)
    return dict(zip(to_replace, value, strict=False))


def _check_replacement_groups(rep_dict: dict[Any, Any]) -> None:
    """PySpark's same-type-group rule: keys and non-None values share one group."""
    keys = list(rep_dict)
    values = [item for item in rep_dict.values() if item is not None]
    if not any(
        all(isinstance(key, group) for key in keys)
        and all(isinstance(item, group) for item in values)
        for group in (bool, str, (float, int))
    ):
        raise PySparkValueError(errorClass="MIXED_TYPE_REPLACEMENT", messageParameters={})


def _resolve_subset_targets(frame: DataFrame, subset: list[str] | None) -> set[str] | None:
    """Resolve subset names eagerly like Spark's ``df.resolve`` — exact spelling only matches.

    A case-variant name resolves but its ``contains`` check never matches (silent no-op);
    an absent name raises ``AnalysisException``.
    """
    if subset is None:
        return None
    targets: set[str] = set()
    for name in subset:
        if not isinstance(name, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "subset", "arg_type": type(name).__name__},
            )
        canonical = frame._resolve_getitem_column_name(name)
        if name == canonical:
            targets.add(name)
    return targets


def _target_group(rep_dict: dict[Any, Any]) -> str:
    """The JVM's target column family, keyed by the first replacement key's type."""
    head = next(iter(rep_dict))
    if isinstance(head, bool):
        return "bool"
    if isinstance(head, str):
        return "str"
    return "numeric"


def _to_replacement_double(value: Any) -> float:
    """The JVM ``convertToDouble`` — numerics pass, anything else refuses."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    java_name = "java.lang.Boolean" if isinstance(value, bool) else type(value).__name__
    java_str = ("true" if value else "false") if isinstance(value, bool) else str(value)
    raise IllegalArgumentException(f"Unsupported value type {java_name} ({java_str}).")


def _converted_pairs(rep_dict: dict[Any, Any]) -> list[tuple[Any, Any]]:
    """The JVM pair rewrite: str/bool values and str/bool keys with null values keep their
    type; every other arm converts both sides to double."""
    pairs: list[tuple[Any, Any]] = []
    for key, value in rep_dict.items():
        if isinstance(value, (str, bool)) or (isinstance(key, (str, bool)) and value is None):
            pairs.append((key, value))
        elif value is None:
            pairs.append((_to_replacement_double(key), None))
        else:
            pairs.append((_to_replacement_double(key), _to_replacement_double(value)))
    return pairs


def _arrow_type_family(arrow_type: Any) -> str:
    """The JVM's target column family, keyed by the column's physical Arrow type.

    ``logical_schema_fields`` collapses ``binary`` to ``string`` and narrows widths; the
    physical type is what keeps a ``binary`` column out of a string-key replacement.
    """
    import pyarrow as pa

    if (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_string_view(arrow_type)
    ):
        return "str"
    if pa.types.is_boolean(arrow_type):
        return "bool"
    if (
        pa.types.is_integer(arrow_type)
        or pa.types.is_floating(arrow_type)
        or pa.types.is_decimal(arrow_type)
    ):
        return "numeric"
    return "other"


def _bind_qualified_column(frame: DataFrame, display: str, qualifier: str) -> Column:
    """Bind one field through its relation qualifier (join multi-name output)."""
    from repark import _native
    from repark.spark._idents import quote_ident as _quote_ident

    quoted = f"{_quote_ident(qualifier)}.{_quote_ident(display)}"
    native = _native.PyColumn.column(quoted)
    return Column(
        native.alias(display),
        spark_display=display,
        projection_name=display,
        stable_name=True,
        has_free_attribute=True,
        sql_expr=quoted,
        origin_plan_id=frame._plan_id,
        origin_field=display,
    )


def _iter_replace_bound_columns(frame: DataFrame) -> list[Column]:
    """Bound columns for replace — duplicate-name equi-join output binds by qualifier."""
    qualifiers = frame._join_qualifiers
    if (
        frame._display_names is None
        and qualifiers is not None
        and len(qualifiers) == len(frame.columns)
    ):
        return [
            _bind_qualified_column(frame, name, qualifier)
            for name, qualifier in zip(frame.columns, qualifiers, strict=True)
        ]
    return frame._iter_bound_columns()


def _aliased_join_sides(
    left: DataFrame, right: DataFrame
) -> tuple[DataFrame, DataFrame, tuple[str, str]]:
    """Alias both join inputs under generated `_repark_jl_*`/`_repark_jr_*` names."""
    left_name = f"_repark_jl_{uuid.uuid4().hex[:12]}"
    right_name = f"_repark_jr_{uuid.uuid4().hex[:12]}"
    return left.alias(left_name), right.alias(right_name), (left_name, right_name)


def _assign_join_qualifiers(
    child: DataFrame, left_column_count: int, side_names: tuple[str, str] | None
) -> None:
    """Record which generated side alias qualifies each field of multi-name join output."""
    if side_names is not None:
        child._join_qualifiers = [side_names[0]] * left_column_count + [side_names[1]] * (
            len(child.columns) - left_column_count
        )


def _inherit_plan_metadata(parent: DataFrame, child: DataFrame) -> DataFrame:
    """Carry the display/engine overlay, origin map, and join qualifiers to a same-schema child."""
    if parent._display_names is not None:
        child._display_names = list(parent._display_names)
        child._engine_names = (
            list(parent._engine_names) if parent._engine_names is not None else None
        )
        child._origin_map = dict(parent._origin_map) if parent._origin_map is not None else None
    if parent._join_qualifiers is not None:
        child._join_qualifiers = list(parent._join_qualifiers)
    return child


def _replace_case(bound: Column, pairs: list[tuple[Any, Any]], type_key: str) -> Column:
    """One searched CASE ``WHEN col = k THEN CAST(v AS coltype) … ELSE col END``."""
    from repark.spark.functions import lit

    arms: list[tuple[Column, Column]] = []
    for key, value in pairs:
        replacement = lit(value) if value is None else lit(value).cast(type_key)
        arms.append((bound == lit(key), replacement))
    return Column._from_when_pairs(arms, otherwise=bound)


def _replace(
    frame: DataFrame,
    to_replace: Any,
    value: Any,
    subset: str | list[str] | tuple[str, ...] | None,
) -> DataFrame:
    """Replace value(s) across columns (PySpark ``DataFrame.replace``)."""
    frame._ensure_alive()
    _validate_replace_arguments(to_replace, value, subset)
    rep_dict = _replacement_dict(to_replace, value)
    subset_names = [subset] if isinstance(subset, str) else subset
    _check_replacement_groups(rep_dict)
    targets = _resolve_subset_targets(
        frame, list(subset_names) if subset_names is not None else None
    )
    if not rep_dict:
        return frame._identity_child()
    group = _target_group(rep_dict)
    pairs = _converted_pairs(rep_dict)
    bound_columns = _iter_replace_bound_columns(frame)
    type_fields = frame._inner.logical_schema_fields()
    arrow_fields = frame._analyzed_arrow_schema()
    projected: list[Column] = []
    for bound, (_engine_name, type_key, _nullable), arrow_field in zip(
        bound_columns, type_fields, arrow_fields, strict=True
    ):
        display = bound._projection_name or bound.spark_display_part()
        if targets is not None and display not in targets:
            projected.append(bound)
            continue
        if _arrow_type_family(arrow_field.type) != group:
            projected.append(bound)
            continue
        expression = _replace_case(bound, pairs, type_key)
        if bound._origin_plan_id is not None and bound._origin_field is not None:
            projected.append(
                Column(
                    expression._inner.alias(display),
                    spark_display=display,
                    projection_name=display,
                    stable_name=True,
                    has_free_attribute=True,
                    origin_plan_id=bound._origin_plan_id,
                    origin_field=bound._origin_field,
                    join_sql_expr=expression.join_sql_part(),
                    sql_expr=expression._sql_expr,
                )
            )
        else:
            projected.append(expression.alias(display))
    return frame.select(*projected)
