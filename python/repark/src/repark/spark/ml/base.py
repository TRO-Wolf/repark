"""Estimator and transformer contracts for the Spark ML facade.

Fits use session queries or Rust Arrow streams. Models store parameters and fitted
metadata, while transforms remain lazy plan expressions. Foreign frames are refused.
"""

from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from repark.errors import AnalysisException, IllegalArgumentException, PySparkTypeError
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark._temp_views import scratch_view_name
from repark.spark.ml.param import HasInputCol, HasOutputCol, Param, Params

if TYPE_CHECKING:
    from repark.spark.dataframe import DataFrame

M = TypeVar("M", bound="Model")


def _require_repark_dataframe(dataset: Any, *, verb: str) -> DataFrame:
    """Require a DataFrame from a ReparkSession and name foreign inputs in the error."""
    from repark.spark.dataframe import DataFrame as ReparkDataFrame

    if isinstance(dataset, ReparkDataFrame):
        return dataset
    type_name = type(dataset).__name__
    module_name = type(dataset).__module__
    raise PySparkTypeError(
        f"{verb} accepts only repark.dataframe.DataFrame (ReparkSession); "
        f"got {module_name}.{type_name}"
    )


def _refuse_output_collision(frame: Any, output_col: str, *, stage: str) -> None:
    """Refuse an output column collision instead of silently overwriting input data."""
    names = list(frame.columns) if hasattr(frame, "columns") else []
    if not names:
        try:
            names = [field.name for field in frame.schema.fields]
        except Exception:
            names = []
    if output_col in names:
        raise AnalysisException(
            f"{stage}: outputCol {output_col!r} already exists in the input schema "
            f"(repark.ml refuses silent overwrite)"
        )


def _require_dense_feature_width(
    frame: Any,
    features_col: str,
    num_features: int,
    *,
    verb: str,
) -> None:
    """Validate dense feature nulls and width with a plan aggregate before transforms.

    Out-of-range ``array_element`` calls otherwise produce NULL without an error.
    """
    quoted = _quote_ident(features_col)
    view = scratch_view_name(frame._session, "__repark_fw_")
    frame.createOrReplaceTempView(view)
    try:
        sql = (
            f"SELECT COUNT(*) AS n FROM {view} WHERE {quoted} IS NULL "
            f"OR array_length({quoted}) <> {int(num_features)}"
        )
        rows = list(frame._spawn(frame._session.sql(sql)).collect())
        if not rows:
            return
        values = list(rows[0].asDict().values()) if hasattr(rows[0], "asDict") else list(rows[0])
        bad = int(values[0] or 0)
        if bad > 0:
            raise IllegalArgumentException(
                f"{verb}: {bad} row(s) have null features or width != {num_features} "
                f"(dense features must match fitted num_features; array_element would "
                f"silently NULL out-of-range indices)"
            )
    finally:
        with contextlib.suppress(Exception):
            frame._session.drop_temp_view(view)


class Estimator(Params, Generic[M], ABC):
    """Fit a model on a dataset."""

    @abstractmethod
    def _fit(self, dataset: DataFrame) -> M:
        """Fit through the session and return a model."""

    def fit(self, dataset: Any, params: dict[Param[Any], Any] | None = None) -> M:
        """Fit on ``dataset`` with an optional one-shot parameter override."""
        frame = _require_repark_dataframe(dataset, verb="Estimator.fit")
        if params is None:
            return self._fit(frame)
        if isinstance(params, dict):
            return self.copy(extra=params)._fit(frame)
        raise PySparkTypeError(
            f"fit params must be a Param map dict or None, got {type(params).__name__}"
        )


class Transformer(Params, ABC):
    """Transform a dataset into another dataset."""

    @abstractmethod
    def _transform(self, dataset: DataFrame) -> DataFrame:
        """Return a lazy plan-built DataFrame."""

    def transform(self, dataset: Any, params: dict[Param[Any], Any] | None = None) -> DataFrame:
        """Transform ``dataset`` with an optional one-shot parameter override."""
        frame = _require_repark_dataframe(dataset, verb="Transformer.transform")
        if params is None:
            return self._transform(frame)
        if isinstance(params, dict):
            return self.copy(extra=params)._transform(frame)
        raise PySparkTypeError(
            f"transform params must be a Param map dict or None, got {type(params).__name__}"
        )


class Model(Transformer, ABC):
    """A fitted transformer produced by an estimator."""


class UnaryTransformer(HasInputCol, HasOutputCol, Transformer, ABC):
    """Transformer with one input and one output column."""

    def __init__(self) -> None:
        """Initialize input and output parameters."""
        super().__init__()

    @abstractmethod
    def createTransformFunc(self) -> Any:
        """Return the Spark-shaped transform callable or plan factory."""

    def _transform(self, dataset: DataFrame) -> DataFrame:
        """Reject the default path because transforms must be plan-built."""
        raise IllegalArgumentException(
            f"{type(self).__name__} must implement _transform as a plan-built transform "
            "(repark does not apply Python row callables)"
        )


__all__ = [
    "Estimator",
    "Model",
    "Transformer",
    "UnaryTransformer",
    "_refuse_output_collision",
    "_require_dense_feature_width",
    "_require_repark_dataframe",
]
