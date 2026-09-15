"""DataFrame foreach / foreachPartition / observe bodies."""

from __future__ import annotations

from types import MethodType
from typing import TYPE_CHECKING, NoReturn

from repark.errors import (
    AnalysisException,
    PySparkAssertionError,
    PySparkTypeError,
    PySparkValueError,
)
from repark.spark._integral import (
    _attached_error_class,
    _attached_message_parameters,
    _attached_sql_state,
)
from repark.spark.column import Column
from repark.spark.observation import Observation

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame

_FILL_DEPTH = 0


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


def _require_callable(value: object) -> None:
    """Refuse a non-callable ``f`` with Spark's ``NOT_CALLABLE`` shape."""
    if not callable(value):
        raise PySparkTypeError(
            f"[NOT_CALLABLE] Argument `f` should be a callable, got {type(value).__name__}.",
            errorClass="NOT_CALLABLE",
            messageParameters={"arg_name": "f", "arg_type": type(value).__name__},
        )


def _refuse_non_aggregate(exprs: tuple[object, ...]) -> None:
    """Raise Spark's observed-metrics analysis error for a non-aggregate expr."""
    for expr in exprs:
        if isinstance(expr, Column) and expr._is_aggregate:
            continue
        display = expr.spark_display_part() if isinstance(expr, Column) else str(expr)
        _raise_analysis(
            "[INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_ATTRIBUTE] Invalid "
            f'observed metrics. Attribute "{display}" can only be used as an argument '
            "to an aggregate function. SQLSTATE: 42K0E;",
            "INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_ATTRIBUTE",
            message_parameters={"expr": f'"{display}"'},
            sql_state="42K0E",
        )


def foreach(frame: DataFrame, f: object) -> None:
    """Call ``f`` once per row through ``toLocalIterator``. pins: df-surface-b-1/C-001"""
    _require_callable(f)
    frame._ensure_alive()
    for row in frame.toLocalIterator():
        f(row)


def foreachPartition(frame: DataFrame, f: object) -> None:  # noqa: N802
    """Call ``f`` once per record batch with a Row iterator. pins: df-surface-b-1/C-002"""
    _require_callable(f)
    frame._ensure_alive()
    for batch in frame.to_arrow_batches():
        f(iter(frame._iter_rows_from_record_batch(batch)))


def observe(frame: DataFrame, observation: object, *exprs: object) -> DataFrame:
    """Attach observed metrics to a child of ``frame``. pins: df-surface-b-1/C-003"""
    if not exprs:
        raise PySparkValueError(
            "[CANNOT_BE_EMPTY] At least one exprs must be specified.",
            errorClass="CANNOT_BE_EMPTY",
            messageParameters={"item": "exprs"},
        )
    if isinstance(observation, Observation):
        if observation._used:
            raise PySparkAssertionError(
                "[REUSE_OBSERVATION] An Observation can be used with a DataFrame only once.",
                errorClass="REUSE_OBSERVATION",
                messageParameters={},
            )
        observation._used = True
        observation._attached = True
        attachment: tuple[Observation | None, tuple[object, ...]] = (observation, exprs)
    elif isinstance(observation, str):
        attachment = (None, exprs)
    else:
        raise PySparkTypeError(
            f"[NOT_LIST_OF_COLUMN] Argument `observation` should be a list of Column, "
            f"got {type(observation).__name__}.",
            errorClass="NOT_LIST_OF_COLUMN",
            messageParameters={
                "arg_name": "observation",
                "arg_type": type(observation).__name__,
            },
        )
    frame._ensure_alive()
    child = frame._identity_child()
    child._observations = (*frame._observations, attachment)
    return child


def fill_on_action(frame: DataFrame) -> None:
    """Evaluate attached metric exprs once on the first action. pins: df-surface-b-1/C-004"""
    global _FILL_DEPTH
    attachments = getattr(frame, "_observations", ())
    if not attachments or _FILL_DEPTH:
        return
    _FILL_DEPTH += 1
    try:
        for observation, exprs in attachments:
            _refuse_non_aggregate(exprs)
            if observation is None or observation._metrics is not None:
                continue
            row = frame.agg(*exprs).collect()[0]
            observation._metrics = dict(row.asDict())
    finally:
        _FILL_DEPTH -= 1
