"""Spark higher-order function wrappers.

The facade builds lambda expression trees from placeholder Columns. Parameter display names
match Spark; plan names include nesting depth so nested scopes cannot capture each other.
"""

from __future__ import annotations

import contextvars
import inspect
from collections.abc import Callable

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


def _lambda_arity(function: Callable[..., Column], *, allowed: tuple[int, ...]) -> int:
    """How many parameters the callable takes, refused loudly if Spark does not accept that many."""
    parameters = inspect.signature(function).parameters
    if any(parameter.kind not in _LAMBDA_PARAMETER_KINDS for parameter in parameters.values()):
        raise PySparkValueError(
            f"[UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION] Function "
            f"`{getattr(function, '__name__', type(function).__name__)}` should use only "
            "POSITIONAL or POSITIONAL OR KEYWORD arguments."
        )
    arity = len(parameters)
    if arity not in allowed:
        expected = " or ".join(str(count) for count in allowed)
        raise PySparkValueError(
            f"lambda takes {arity} parameters, but this function expects {expected}"
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
            f"a higher-order function lambda must return a Column, got {type(body).__name__}"
        )
    return plan_names, display_names, body


def _higher_order(
    name: str,
    values: list[Column | str],
    functions: list[tuple[Callable[..., Column], tuple[int, ...]]],
) -> Column:
    """Build a higher-order call: value arguments first, then one lambda per callable.

    Every Spark higher-order function has that shape, so the split is the signature rather than a
    convention invented here.
    """
    value_columns = [_as_column_arg(value, as_lit=False) for value in values]
    built = [
        _build_lambda(function, _lambda_arity(function, allowed=allowed))
        for function, allowed in functions
    ]

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
    return _higher_order("exists", [col], [(f, (1,))])
