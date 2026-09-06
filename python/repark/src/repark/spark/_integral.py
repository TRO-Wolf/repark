"""Spark INTEGRAL-type coercion for facade integer knobs.

PySpark `functions.percentile_approx` takes an INTEGRAL accuracy: Python ints and
`__index__` types (numpy integers pass Py4J through) run, while bool, float and str
fail analysis with DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE (live 4.1.2).
"""

from __future__ import annotations

import operator

from repark.errors import PySparkTypeError


def checked_integral(argument: str, value: int | None) -> int | None:
    """Coerce an INTEGRAL knob to int under Spark's type contract.

    Args:
        argument: The knob name for the error parameters.
        value: The caller-supplied knob, None for the function default.

    Returns:
        The knob as a plain int, or None for the default.

    Raises:
        PySparkTypeError: With `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` when the
            knob is a bool or has no integer index.
    """
    if value is None or type(value) is int:
        return value
    if isinstance(value, bool):
        raise PySparkTypeError(
            errorClass="DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
            messageParameters={"arg_name": argument, "arg_type": type(value).__name__},
        )
    try:
        return operator.index(value)
    except TypeError:
        raise PySparkTypeError(
            errorClass="DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
            messageParameters={"arg_name": argument, "arg_type": type(value).__name__},
        ) from None
