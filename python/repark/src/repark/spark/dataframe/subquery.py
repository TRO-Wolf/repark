"""Subquery surface for ``DataFrame`` — ``scalar`` / ``exists`` / ``lateralJoin`` / ``asTable``.

Bound onto the facade ``DataFrame`` through ``DECLARED_MEMBERS`` (``core.py`` is at
its file-size baseline, so these PySpark 4.0 subquery methods live here). The engine
work — scope resolution, single-row enforcement, ``EXISTS`` rewrite, and lateral
projection hoisting — happens in ``repark_core::session::df_guards::subquery``; this
module is a thin PySpark-signature adapter over ``repark._native``.

pins: df-subquery-1
"""

from __future__ import annotations

from types import MethodType
from typing import Any, NoReturn

from repark.errors import AnalysisException, PySparkTypeError
from repark.spark._integral import (
    _attached_error_class,
    _attached_message_parameters,
    _attached_sql_state,
)

_LATERAL_SUPPORTED = "'inner', 'leftouter', 'left', 'left_outer', 'cross'"


def _raise_analysis(
    message: str,
    error_class: str,
    message_parameters: dict[str, str],
    sql_state: str,
) -> NoReturn:
    """Raise ``AnalysisException`` carrying Spark's errorClass, params, and SQLSTATE."""
    error = AnalysisException(message)
    error._spark_error_class = error_class
    error._spark_message_parameters = message_parameters
    error._spark_sql_state = sql_state
    error.getErrorClass = MethodType(_attached_error_class, error)
    error.getCondition = MethodType(_attached_error_class, error)
    error.getMessageParameters = MethodType(_attached_message_parameters, error)
    error.getSqlState = MethodType(_attached_sql_state, error)
    raise error


def _subquery_field_list(frame: Any) -> str:
    """Render ``[name: type, ...]`` for the subquery Column display part."""
    return ", ".join(
        f"{field.name}: {field.dataType.simpleString()}" for field in frame.schema.fields
    )


def scalar(frame: Any) -> Any:
    """Return a ``Column`` for a SCALAR subquery (PySpark ``DataFrame.scalar``)."""
    from repark import _native
    from repark.spark.column import Column

    frame._ensure_alive()
    width = len(frame.schema.fields)
    if width != 1:
        _raise_analysis(
            "[INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN] "
            f"Invalid subquery: Scalar subquery must return only one column, but got {width}. "
            "SQLSTATE: 42823",
            "INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN",
            {"number": str(width)},
            "42823",
        )
    display = f"([{_subquery_field_list(frame)}])"
    return Column(
        _native.scalar_subquery(frame._plan()),
        spark_display=display,
        projection_name="scalarsubquery()",
    )


def exists(frame: Any) -> Any:
    """Return a ``Column`` for an EXISTS subquery (PySpark ``DataFrame.exists``)."""
    from repark import _native
    from repark.spark.column import Column

    frame._ensure_alive()
    display = f"EXISTS ([{_subquery_field_list(frame)}])"
    return Column(
        _native.exists_subquery(frame._plan()),
        spark_display=display,
        projection_name="exists()",
    )


def lateralJoin(  # noqa: N802 — PySpark method name
    frame: Any,
    other: Any,
    on: Any = None,
    how: str | None = None,
) -> Any:
    """Lateral join with ``other`` (PySpark ``DataFrame.lateralJoin``).

    ``how`` accepts only ``inner``/``cross``/``left``/``leftouter``/``left_outer``;
    anything else is Spark's ``UNSUPPORTED_JOIN_TYPE`` analysis error. ``on`` is a
    join-condition ``Column`` resolved against both sides' scopes.
    """
    from repark import _native
    from repark.spark.column import Column
    from repark.spark.dataframe.core import DataFrame

    frame._ensure_alive()
    if not isinstance(other, DataFrame):
        raise PySparkTypeError(
            errorClass="NOT_DATAFRAME",
            messageParameters={"arg_name": "other", "arg_type": type(other).__name__},
        )
    other._ensure_alive()
    on_column = on
    on_names: list[str] | None = None
    if isinstance(on, str):
        on_column, on_names = None, [on]
    elif isinstance(on, (list, tuple)):
        if not all(isinstance(name, str) for name in on):
            raise PySparkTypeError(
                errorClass="NOT_LIST_OF_STR",
                messageParameters={"arg_name": "on", "arg_type": type(on).__name__},
            )
        on_column, on_names = None, list(on)
    elif on is not None and not isinstance(on, Column):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN",
            messageParameters={"arg_name": "on", "arg_type": type(on).__name__},
        )
    if how is not None and not isinstance(how, str):
        raise PySparkTypeError(
            errorClass="NOT_STR",
            messageParameters={"arg_name": "how", "arg_type": type(how).__name__},
        )
    requested = (how or "inner").strip().lower()
    normalized = requested.replace("_", "").replace(" ", "")
    if normalized in {"left", "leftouter"}:
        engine_how = "left"
    elif normalized in {"inner", "cross"}:
        engine_how = "inner"
    else:
        _raise_analysis(
            f"[UNSUPPORTED_JOIN_TYPE] Unsupported join type '{requested}'. "
            f"Supported join types include: {_LATERAL_SUPPORTED}. SQLSTATE: 0A000",
            "UNSUPPORTED_JOIN_TYPE",
            {"typ": requested, "supported": _LATERAL_SUPPORTED},
            "0A000",
        )
    joined = _native.lateral_join(
        frame._plan(),
        other._plan(),
        engine_how,
        on_column._inner if on_column is not None else None,
        on_names,
    )
    return frame._spawn(joined)


def asTable(frame: Any) -> Any:  # noqa: N802 — PySpark method name
    """Convert this frame into a ``TableArg`` for TVF/UDTF calls (PySpark ``DataFrame.asTable``)."""
    from repark.spark.table_arg import TableArg

    frame._ensure_alive()
    return TableArg(frame)


DECLARED_MEMBERS = (scalar, exists, lateralJoin, asTable)


__all__ = ["asTable", "exists", "lateralJoin", "scalar"]
