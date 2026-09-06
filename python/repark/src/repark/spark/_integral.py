"""Spark INTEGRAL-type coercion for facade integer knobs.

PySpark `functions.percentile_approx` takes an INTEGRAL accuracy: Python ints and
`__index__` types run. bool, float and str fail analysis with Spark 4.1.2's
`AnalysisException` / `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` (params sqlExpr,
paramIndex, inputSql, inputType, requiredType). Raw `numpy.int64` through Spark's
`lit` fails with Spark `INTERNAL_ERROR` on an aliased CAST; repark accepts `__index__`
as INTEGRAL.
"""

from __future__ import annotations

import operator
from types import MethodType
from typing import NoReturn

from repark.errors import AnalysisException

_ERROR_CLASS = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
_SQLSTATE = "42K09"


def _attached_error_class(error: object) -> str | None:
    """Return the Spark error class attached on a native AnalysisException."""
    value = getattr(error, "_spark_error_class", None)
    return value if isinstance(value, str) else None


def _attached_message_parameters(error: object) -> dict[str, str] | None:
    """Return the Spark message parameters attached on a native AnalysisException."""
    value = getattr(error, "_spark_message_parameters", None)
    if not isinstance(value, dict):
        return None
    return {str(key): str(item) for key, item in value.items()}


def _attached_sql_state(error: object) -> str | None:
    """Return the Spark SQLSTATE attached on a native AnalysisException."""
    value = getattr(error, "_spark_sql_state", None)
    return value if isinstance(value, str) else None


def _column_sql(column: object) -> str:
    """SQL rendering of a column argument, matching Spark's unquoted name."""
    if isinstance(column, str):
        return column
    sql_expr_part = getattr(column, "sql_expr_part", None)
    if callable(sql_expr_part):
        return str(sql_expr_part())
    return str(column)


def _percentage_sql(percentage: object) -> str:
    """SQL rendering of a percentage argument, matching Spark's lit/array form."""
    if isinstance(percentage, (list, tuple)):
        joined = ", ".join(str(item) for item in percentage)
        return f"array({joined})"
    return str(percentage)


def _accuracy_sql_and_type(value: object) -> tuple[str, str]:
    """Spark toSQLExpr / toSQLType pair for a rejected accuracy Python value."""
    if isinstance(value, bool):
        return ("true" if value else "false", "BOOLEAN")
    if isinstance(value, float):
        return (str(value), "DOUBLE")
    if isinstance(value, str):
        return (value, "STRING")
    return (str(value), type(value).__name__.upper())


def _raise_unexpected_input_type(sql_expr: str, input_sql: str, input_type: str) -> NoReturn:
    """Raise AnalysisException with Spark 4.1.2's UNEXPECTED_INPUT_TYPE payload."""
    parameters = {
        "sqlExpr": f'"{sql_expr}"',
        "paramIndex": "third",
        "inputSql": f'"{input_sql}"',
        "inputType": f'"{input_type}"',
        "requiredType": '"INTEGRAL"',
    }
    message = (
        f'[{_ERROR_CLASS}] Cannot resolve "{sql_expr}" due to data type mismatch: '
        f'The third parameter requires the "INTEGRAL" type, however "{input_sql}" '
        f'has the type "{input_type}". SQLSTATE: {_SQLSTATE};'
    )
    error = AnalysisException(message)
    error._spark_error_class = _ERROR_CLASS
    error._spark_message_parameters = parameters
    error._spark_sql_state = _SQLSTATE
    error.getErrorClass = MethodType(_attached_error_class, error)
    error.getCondition = MethodType(_attached_error_class, error)
    error.getMessageParameters = MethodType(_attached_message_parameters, error)
    error.getSqlState = MethodType(_attached_sql_state, error)
    raise error


def checked_integral(
    argument: str,
    value: object,
    column: object = None,
    percentage: object = None,
) -> int | None:
    """Coerce an INTEGRAL knob to int under Spark's type contract.

    Args:
        argument: The knob name, used only when column/percentage are omitted.
        value: The caller-supplied knob, None for the function default.
        column: The value column, for Spark's sqlExpr on a type mismatch.
        percentage: The percentage argument, for Spark's sqlExpr on a type mismatch.

    Returns:
        The knob as a plain int, or None for the default.

    Raises:
        AnalysisException: Spark `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` when the
            knob is a bool or has no integer index.
    """
    if value is None or type(value) is int:
        return None if value is None else int(value)
    if not isinstance(value, bool):
        try:
            return operator.index(value)
        except TypeError:
            pass
    input_sql, input_type = _accuracy_sql_and_type(value)
    column_sql = argument if column is None else _column_sql(column)
    percentage_sql = "percentage" if percentage is None else _percentage_sql(percentage)
    sql_expr = f"percentile_approx({column_sql}, {percentage_sql}, {input_sql})"
    _raise_unexpected_input_type(sql_expr, input_sql, input_type)
