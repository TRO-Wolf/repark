"""Spark higher-order (lambda) functions.

Public names are re-exported from ``functions.py``.

**How a Python lambda becomes an engine expression.** A ``Column`` is standalone — it has no frame
and no schema — so the facade cannot evaluate the user's callable against data. It does not need
to. It mints one placeholder ``Column`` per lambda parameter, calls the callable with them, and
takes whatever ``Column`` comes back as the lambda *body*; the binding then assembles
``HigherOrderFunction(func, [values…, Lambda(params, body)])``. The placeholders carry no type
until ``DataFrame`` resolves them against its schema at plan-build time.

Python therefore decides nothing semantic here. It binds names to positions — which is the
callable's own signature — and hands the tree to Rust.

**Parameter names are ours, not the user's.** PySpark names lambda parameters ``x``/``y``/``z``
regardless of what the caller wrote, because the names travel into the plan and a user-chosen name
could collide with a column. This module does the same.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable

from repark import _native
from repark.errors import PySparkValueError
from repark.spark.column import Column
from repark.spark.functions import _as_column_arg

# PySpark's own parameter names, in order (`builtin.py` `_get_lambda_parameters`).
_LAMBDA_PARAMETER_NAMES = ("x", "y", "z")


def _lambda_arity(function: Callable[..., Column], *, allowed: tuple[int, ...]) -> int:
    """How many parameters the callable takes, refused loudly if Spark does not accept that many."""
    parameters = inspect.signature(function).parameters
    if any(
        parameter.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        for parameter in parameters.values()
    ):
        raise PySparkValueError(
            "higher-order function lambdas take a fixed number of positional parameters; "
            "*args / **kwargs are not supported"
        )
    arity = len(parameters)
    if arity not in allowed:
        expected = " or ".join(str(count) for count in allowed)
        raise PySparkValueError(
            f"lambda takes {arity} parameters, but this function expects {expected}"
        )
    return arity


def _build_lambda(function: Callable[..., Column], arity: int) -> tuple[list[str], Column]:
    """Mint placeholder columns, call the user's callable, and return ``(param names, body)``."""
    names = list(_LAMBDA_PARAMETER_NAMES[:arity])
    placeholders = [
        Column(_native.PyColumn.lambda_variable(name), spark_display=name) for name in names
    ]
    body = function(*placeholders)
    if not isinstance(body, Column):
        raise PySparkValueError(
            f"a higher-order function lambda must return a Column, got {type(body).__name__}"
        )
    return names, body


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
        f"{', '.join(names)} -> {body.spark_wrap_display_part()}" for names, body in built
    ]
    display_parts = [column.spark_wrap_display_part() for column in value_columns]
    sql_parts = [column.sql_expr_part() for column in value_columns]
    shown = f"{name}({', '.join(display_parts + display_lambdas)})"
    sql = f"{name}({', '.join(sql_parts + display_lambdas)})"

    return Column(
        _native.PyColumn.call_higher_order(
            name,
            [column._inner for column in value_columns],
            [(names, body._inner) for names, body in built],
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
