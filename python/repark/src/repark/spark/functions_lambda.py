"""Spark higher-order function wrappers.

The facade builds lambda expression trees from placeholder Columns. Parameter display names
match Spark; plan names include nesting depth so nested scopes cannot capture each other.
"""

from __future__ import annotations

import contextvars
import inspect
from collections.abc import Callable
from typing import Any

from repark import _native
from repark.errors import PySparkValueError
from repark.spark.column import Column
from repark.spark.functions import _as_column_arg

# PySpark's own parameter names, in order (`builtin.py` `_get_lambda_parameters`). These are the
# DISPLAY names; the plan name adds a depth suffix — see `_parameter_names`.
_LAMBDA_PARAMETER_NAMES = ("x", "y", "z")

# How many lambdas enclose the one being built. A `ContextVar` rather than a plain global so two
# threads building lambdas at once cannot interleave each other's depth; each thread runs in its
# own context and so keeps its own count.
_LAMBDA_DEPTH: contextvars.ContextVar[int] = contextvars.ContextVar(
    "repark_lambda_depth", default=0
)


def _parameter_names(arity: int, depth: int) -> tuple[list[str], list[str]]:
    """``(plan names, display names)`` — depth-tagged in the plan, x/y/z on screen."""
    display = list(_LAMBDA_PARAMETER_NAMES[:arity])
    return [f"{name}_{depth}" for name in display], display


# The parameter kinds a higher-order lambda may use. Spark's own check reads "should use only
# POSITIONAL or POSITIONAL OR KEYWORD arguments", and `lambda x, /: x > 2` does work there
# Positional-only is allowed; the other parameter kinds are refused.
_LAMBDA_PARAMETER_KINDS = (
    inspect.Parameter.POSITIONAL_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
)


def _lambda_arity(function: Callable[..., Column]) -> int:
    """How many parameters the callable takes; the analyzer judges whether Spark accepts it."""
    parameters = inspect.signature(function).parameters
    if any(parameter.kind not in _LAMBDA_PARAMETER_KINDS for parameter in parameters.values()):
        raise PySparkValueError(
            f"[UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION] Function "
            f"`{getattr(function, '__name__', type(function).__name__)}` should use only "
            "POSITIONAL or POSITIONAL OR KEYWORD arguments."
        )
    arity = len(parameters)
    if arity < 1 or arity > 3:
        name = getattr(function, "__name__", type(function).__name__)
        raise PySparkValueError(
            f"[WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION] Function `{name}` should take "
            f"between 1 and 3 arguments, but the provided function takes {arity}.",
            errorClass="WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION",
            messageParameters={"func_name": name, "num_args": str(arity)},
        )
    return arity


def _build_lambda(
    function: Callable[..., Column], arity: int
) -> tuple[list[str], list[str], Column]:
    """Mint placeholders, call the user's callable, return ``(plan names, display names, body)``.

    The plan names carry this lambda's nesting depth, so a lambda built inside the callable cannot
    capture an enclosing one's binding; the display names stay ``x``/``y``/``z`` so the projection
    reads like PySpark's. The depth is raised for the duration of the callable — that call is
    exactly the window in which a nested lambda can be built — and restored afterwards, so two
    builds of the same expression mint the same names.
    """
    depth = _LAMBDA_DEPTH.get()
    plan_names, display_names = _parameter_names(arity, depth)
    placeholders = [
        Column(_native.PyColumn.lambda_variable(plan), spark_display=shown)
        for plan, shown in zip(plan_names, display_names, strict=True)
    ]
    token = _LAMBDA_DEPTH.set(depth + 1)
    try:
        body = function(*placeholders)
    finally:
        _LAMBDA_DEPTH.reset(token)
    if not isinstance(body, Column):
        raise PySparkValueError(
            f"[HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN] Function `{function.__name__}` "
            f"should return Column, got {type(body).__name__}.",
            errorClass="HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN",
            messageParameters={
                "func_name": function.__name__,
                "return_type": type(body).__name__,
            },
        )
    return plan_names, display_names, body


def _higher_order(
    name: str,
    values: list[Column | str],
    functions: list[Callable[..., Column]],
) -> Column:
    """Build a higher-order call: value arguments first, then one lambda per callable.

    Every Spark higher-order function has that shape, so the split is the signature rather than a
    convention invented here.
    """
    value_columns = [_as_column_arg(value, as_lit=False) for value in values]
    built = [_build_lambda(function, _lambda_arity(function)) for function in functions]

    display_lambdas = [
        f"{', '.join(display)} -> {body.spark_wrap_display_part()}"
        for _plan, display, body in built
    ]
    display_parts = [column.spark_wrap_display_part() for column in value_columns]
    sql_parts = [column.sql_expr_part() for column in value_columns]
    shown = f"{name}({', '.join(display_parts + display_lambdas)})"
    sql = f"{name}({', '.join(sql_parts + display_lambdas)})"

    return Column(
        _native.PyColumn.call_higher_order(
            name,
            [column._inner for column in value_columns],
            [(plan, body._inner) for plan, _display, body in built],
        ),
        spark_display=shown,
        projection_name=shown,
        sql_expr=sql,
        join_sql_expr=sql,
        stable_name=False,
        is_aggregate=any(column._is_aggregate for column in value_columns),
        is_foldable=False,
        has_free_attribute=any(column._has_free_attribute for column in value_columns),
        has_ungroupable=any(column._has_ungroupable for column in value_columns),
    )


def exists(col: Column | str, f: Callable[[Column], Column]) -> Column:
    """True when any element satisfies ``f`` (PySpark ``functions.exists``).

    Three-valued: an element that makes ``f`` NULL neither confirms nor denies, so a NULL among
    otherwise-false elements yields NULL rather than false.
    """
    return _higher_order("exists", [col], [f])


def transform(
    col: Column | str,
    f: Callable[..., Column],
) -> Column:
    """Apply ``f`` to every array element (PySpark ``functions.transform``).

    ``f`` is unary ``(x)`` or binary ``(x, i)`` with a 0-based index.
    """
    return _higher_order("transform", [col], [f])


def filter(
    col: Column | str,
    f: Callable[..., Column],
) -> Column:
    """Keep array elements for which ``f`` is true (PySpark ``functions.filter``).

    ``f`` is unary ``(x)`` or binary ``(x, i)`` with a 0-based index. A null
    predicate drops the element.
    """
    return _higher_order("filter", [col], [f])


def forall(col: Column | str, f: Callable[[Column], Column]) -> Column:
    """True when every element satisfies ``f`` (PySpark ``functions.forall``).

    Empty array is true. A null predicate among otherwise-true elements yields
    null. Any false element yields false.
    """
    return _higher_order("forall", [col], [f])


def aggregate(
    col: Column | str,
    initialValue: Column | str,  # noqa: N803 — PySpark parameter name
    merge: Callable[[Column, Column], Column],
    finish: Callable[[Column], Column] | None = None,
) -> Column:
    """Fold an array from ``initialValue`` with ``merge`` (PySpark ``functions.aggregate``).

    ``finish`` is optional. An empty array yields ``initialValue`` then ``finish``.
    """
    if finish is not None:
        return _higher_order(
            "aggregate",
            [col, initialValue],
            [merge, finish],
        )
    return _higher_order("aggregate", [col, initialValue], [merge])


def reduce(
    col: Column | str,
    initialValue: Column | str,  # noqa: N803 — PySpark parameter name
    merge: Callable[[Column, Column], Column],
    finish: Callable[[Column], Column] | None = None,
) -> Column:
    """Alias of :func:`aggregate` (PySpark ``functions.reduce``)."""
    if finish is not None:
        return _higher_order(
            "reduce",
            [col, initialValue],
            [merge, finish],
        )
    return _higher_order("reduce", [col, initialValue], [merge])


def zip_with(
    left: Column | str,
    right: Column | str,
    f: Callable[[Column, Column], Column],
) -> Column:
    """Pair two arrays with ``f``, null-padding the shorter (PySpark ``functions.zip_with``)."""
    return _higher_order("zip_with", [left, right], [f])


def transform_keys(col: Column | str, f: Callable[[Column, Column], Column]) -> Column:
    """Rewrite map keys with ``(k, v) -> new_key`` (PySpark ``functions.transform_keys``)."""
    return _higher_order("transform_keys", [col], [f])


def transform_values(col: Column | str, f: Callable[[Column, Column], Column]) -> Column:
    """Rewrite map values with ``(k, v) -> new_value`` (PySpark ``functions.transform_values``)."""
    return _higher_order("transform_values", [col], [f])


def map_filter(col: Column | str, f: Callable[[Column, Column], Column]) -> Column:
    """Keep map entries whose ``(k, v)`` predicate is true (PySpark ``functions.map_filter``)."""
    return _higher_order("map_filter", [col], [f])


def map_zip_with(
    col1: Column | str,
    col2: Column | str,
    f: Callable[[Column, Column, Column], Column],
) -> Column:
    """Merge two maps with ``(k, v1, v2)`` (PySpark ``functions.map_zip_with``).

    Key order is map1 keys, then map2-only keys. A missing side is null.
    """
    return _higher_order("map_zip_with", [col1, col2], [f])


HIGHER_ORDER_EXPORTS: tuple[str, ...] = (
    "aggregate",
    "filter",
    "forall",
    "map_filter",
    "map_zip_with",
    "reduce",
    "transform",
    "transform_keys",
    "transform_values",
    "zip_with",
)


def install_into(namespace: dict[str, Any], exported: list[str]) -> None:
    """Copy the FNP-4c higher-order names onto the canonical functions module."""
    for name in HIGHER_ORDER_EXPORTS:
        namespace[name] = globals()[name]
        if name not in exported:
            exported.append(name)
