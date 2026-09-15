"""The streaming-named PySpark DataFrame members answered on every batch frame."""

from __future__ import annotations

import re
from types import MethodType
from typing import TYPE_CHECKING, NoReturn

from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkNotImplementedError,
    PySparkTypeError,
)
from repark.spark._integral import (
    _attached_error_class,
    _attached_message_parameters,
    _attached_sql_state,
)

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame

_INTERVAL_UNITS = (
    "nanoseconds?|microseconds?|milliseconds?|seconds?|minutes?|hours?|days?|weeks?|months?|years?"
)

_INTERVAL_STRING_RE = re.compile(
    rf"^\s*(?:interval\s+)?((?:[+-]?\s*\d+(?:\.\d+)?\s+(?:{_INTERVAL_UNITS})\s*)+)$",
    re.IGNORECASE,
)

_INTERVAL_GROUP_RE = re.compile(
    rf"([+-]?)\s*\d+(?:\.\d+)?\s+(?:{_INTERVAL_UNITS})",
    re.IGNORECASE,
)


def _raise_analysis(
    message: str,
    error_class: str,
    message_parameters: dict[str, str] | None = None,
    sql_state: str | None = None,
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


def refuse_write_stream(frame: DataFrame) -> NoReturn:
    """Refuse ``DataFrame.writeStream`` on a batch frame. pins: df-stream-batch-1/C-001"""
    _raise_analysis(
        "[WRITE_STREAM_NOT_ALLOWED] `writeStream` can be called only on streaming "
        "Dataset/DataFrame. SQLSTATE: 42601",
        "WRITE_STREAM_NOT_ALLOWED",
        message_parameters={},
        sql_state="42601",
    )


def refuse_rdd(frame: DataFrame) -> NoReturn:
    """Refuse ``DataFrame.rdd`` — repark has no RDD layer. pins: df-stream-batch-1/C-004"""
    raise PySparkNotImplementedError(
        "[NOT_IMPLEMENTED] rdd is not implemented.",
        errorClass="NOT_IMPLEMENTED",
        messageParameters={"feature": "rdd"},
    )


def refuse_plot(frame: DataFrame) -> NoReturn:
    """Refuse ``DataFrame.plot`` — no plotting backend. pins: df-stream-batch-1/C-004"""
    raise PySparkNotImplementedError(
        "[NOT_IMPLEMENTED] plot is not implemented.",
        errorClass="NOT_IMPLEMENTED",
        messageParameters={"feature": "plot"},
    )


def pandas_api(frame: DataFrame, index_col: object = None) -> NoReturn:
    """Refuse ``DataFrame.pandas_api`` — no pandas-on-Spark. pins: df-stream-batch-1/C-004"""
    raise PySparkNotImplementedError(
        "[NOT_IMPLEMENTED] pandas_api is not implemented.",
        errorClass="NOT_IMPLEMENTED",
        messageParameters={"feature": "pandas_api"},
    )


def with_watermark(
    frame: DataFrame,
    eventTime: object,  # noqa: N803 — PySpark kwarg name
    delayThreshold: object,  # noqa: N803 — PySpark kwarg name
) -> DataFrame:
    """Validate the batch watermark contract, then return self. pins: df-stream-batch-1/C-002"""
    if not isinstance(eventTime, str):
        raise PySparkTypeError(
            f"[NOT_STR] Argument `eventTime` should be a str, got {type(eventTime).__name__}.",
            errorClass="NOT_STR",
            messageParameters={
                "arg_name": "eventTime",
                "arg_type": type(eventTime).__name__,
            },
        )
    if not isinstance(delayThreshold, str):
        raise PySparkTypeError(
            f"[NOT_STR] Argument `delayThreshold` should be a str, got "
            f"{type(delayThreshold).__name__}.",
            errorClass="NOT_STR",
            messageParameters={
                "arg_name": "delayThreshold",
                "arg_type": type(delayThreshold).__name__,
            },
        )
    frame._ensure_alive()
    match = _INTERVAL_STRING_RE.match(delayThreshold)
    if match is None:
        _raise_analysis(
            f"[CANNOT_PARSE_INTERVAL] Unable to parse '{delayThreshold}'. Please ensure "
            "that the value provided is in a valid format for defining an interval. You "
            "can reference the documentation for the correct format. If the issue "
            "persists, please double check that the input value is not null or empty "
            "and try again. SQLSTATE: 22006",
            "CANNOT_PARSE_INTERVAL",
            message_parameters={"intervalString": f"'{delayThreshold}'"},
            sql_state="22006",
        )
    if any(group.group(1) == "-" for group in _INTERVAL_GROUP_RE.finditer(match.group(1))):
        raise IllegalArgumentException(
            f"requirement failed: delay threshold ({delayThreshold}) should not be negative."
        )
    return frame


def drop_duplicates_within_watermark(frame: DataFrame, subset: object = None) -> NoReturn:
    """Refuse watermark dedup on batch after Spark's own checks. pins: df-stream-batch-1/C-003"""
    names: list[str] | None = None
    if subset is not None:
        if not isinstance(subset, (list, tuple)):
            raise PySparkTypeError(
                f"[NOT_LIST_OR_TUPLE] Argument `subset` should be a list or tuple, got "
                f"{type(subset).__name__}.",
                errorClass="NOT_LIST_OR_TUPLE",
                messageParameters={
                    "arg_name": "subset",
                    "arg_type": type(subset).__name__,
                },
            )
        names = []
        for item in subset:
            if not isinstance(item, str):
                raise PySparkTypeError(
                    f"[NOT_STR] Argument `subset` should be a str, got {type(item).__name__}.",
                    errorClass="NOT_STR",
                    messageParameters={
                        "arg_name": "subset",
                        "arg_type": type(item).__name__,
                    },
                )
            names.append(item)
    frame._ensure_alive()
    if names:
        available = frame.columns
        resolved = {name.casefold() for name in available}
        for name in names:
            if name.casefold() not in resolved:
                _raise_analysis(
                    f'Cannot resolve column name "{name}" among ({", ".join(available)}).',
                    "_LEGACY_ERROR_TEMP_1201",
                    message_parameters={
                        "colName": name,
                        "fieldNames": ", ".join(available),
                    },
                )
    _raise_analysis(
        "dropDuplicatesWithinWatermark is not supported with batch DataFrames/DataSets;",
        "_LEGACY_ERROR_TEMP_3102",
        message_parameters={
            "msg": "dropDuplicatesWithinWatermark is not supported with batch DataFrames/DataSets"
        },
    )


DECLARED_MEMBERS = (
    property(refuse_rdd),
    property(refuse_plot),
    property(refuse_write_stream),
    pandas_api,
)
