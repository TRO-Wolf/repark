"""DataFrame foreach / foreachPartition / observe bodies."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from types import MethodType
from typing import TYPE_CHECKING, Any, NoReturn

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


class _ObservedMetricsAttachment:
    """One ``observe`` attachment shared by the observed frame and its descendants."""

    __slots__ = ("_filling", "_lock", "exprs", "observation", "observed_frame")

    def __init__(
        self,
        observation: Observation | None,
        exprs: tuple[object, ...],
        observed_frame: DataFrame,
    ) -> None:
        """Record the metric exprs and the frame they aggregate over."""
        self.observation = observation
        self.exprs = exprs
        self.observed_frame = observed_frame
        self._lock = threading.Lock()
        self._filling = False


def _refuse_non_aggregate(exprs: tuple[object, ...]) -> None:
    """Raise Spark's observed-metrics analysis error for a non-aggregate expr."""
    for expr in exprs:
        if (
            isinstance(expr, Column)
            and not expr._has_free_attribute
            and (expr._is_aggregate or expr._is_foldable)
        ):
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
    if not all(isinstance(expr, Column) for expr in exprs):
        raise PySparkTypeError(
            "[NOT_LIST_OF_COLUMN] Argument `exprs` should be a list[Column].",
            errorClass="NOT_LIST_OF_COLUMN",
            messageParameters={"arg_name": "exprs"},
        )
    handle: Observation | None
    if isinstance(observation, Observation):
        if observation._used:
            raise PySparkAssertionError(
                "[REUSE_OBSERVATION] An Observation can be used with a DataFrame only once.",
                errorClass="REUSE_OBSERVATION",
                messageParameters={},
            )
        observation._used = True
        observation._attached = True
        handle = observation
    elif isinstance(observation, str):
        handle = None
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
    attachment = _ObservedMetricsAttachment(handle, exprs, child)
    child._observations = (*frame._observations, attachment)
    return child


_FILL_SUPPRESSED = threading.local()


def fill_on_action(frame: DataFrame) -> None:
    """Fill each attached Observation once via an extra agg over its observed frame."""
    if getattr(_FILL_SUPPRESSED, "depth", 0):
        return
    for attachment in getattr(frame, "_observations", ()):
        _fill_attachment(attachment)


def _fill_attachment(attachment: _ObservedMetricsAttachment) -> None:
    """Run the one extra ``observed_frame.agg(*exprs)`` that fills the Observation."""
    observation = attachment.observation
    if observation is not None and observation._metrics is not None:
        return
    with attachment._lock:
        if attachment._filling:
            return
        attachment._filling = True
    try:
        _refuse_non_aggregate(attachment.exprs)
        if observation is None or observation._metrics is not None:
            return
        exprs: list[object] = [
            expr.alias(expr.spark_display_part())
            if isinstance(expr, Column)
            and expr._is_foldable
            and not expr._is_aggregate
            and not expr._stable_name
            else expr
            for expr in attachment.exprs
        ]
        sentinel: str | None = None
        if not any(isinstance(expr, Column) and expr._is_aggregate for expr in exprs):
            sentinel = f"__repark_observed_metrics_{uuid.uuid4().hex}"
            from repark import functions as functions_module

            exprs.append(functions_module.count(functions_module.lit(1)).alias(sentinel))
        row = attachment.observed_frame.agg(*exprs).collect()[0]
        metrics = dict(row.asDict())
        if sentinel is not None:
            del metrics[sentinel]
        observation._metrics = metrics
    finally:
        with attachment._lock:
            attachment._filling = False


@contextmanager
def _fill_suppressed() -> Iterator[None]:
    """Suppress Observation fills for plan-only work on this thread."""
    _FILL_SUPPRESSED.depth = getattr(_FILL_SUPPRESSED, "depth", 0) + 1
    try:
        yield
    finally:
        _FILL_SUPPRESSED.depth -= 1


def register_view_without_fill(frame: DataFrame, name: str) -> None:
    """Register ``frame`` as a temp view without filling its Observation."""
    from repark.spark.catalog_surface import _register_temp_view

    with _fill_suppressed():
        _register_temp_view(frame, name)


def rows_without_fill(frame: DataFrame) -> list[Any]:
    """Iterate a plan-only frame (EXPLAIN) without filling an Observation."""
    with _fill_suppressed():
        return list(frame.toLocalIterator())


def empty_rows_after_fill(frame: DataFrame) -> list[Any]:
    """Answer ``tail(0)`` as an empty list after the first-action fill."""
    fill_on_action(frame)
    return []
