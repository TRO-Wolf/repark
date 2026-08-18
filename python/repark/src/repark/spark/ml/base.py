"""Estimator / Transformer / Model contracts (PySpark ``ml.base``).

**Design principle (campaign-wide):**

* Feature ``fit(dataset)`` runs aggregate/distinct **queries** planned through the session.
* Estimator ``fit`` (M3+) may multi-pass stream Arrow batches in Rust via the session;
  models hold **params only** (never training rows). Python never trains.
* ``transform(dataset)`` returns a DataFrame whose plan is expressions only
  (CASE / join / arithmetic / array ops).

Python never touches training rows. Stages hold Param maps + fitted metadata + plan
fragments — never cached Arrow batches. ``fit`` / ``transform`` accept only
:class:`~repark.dataframe.DataFrame` from a :class:`~repark.session.ReparkSession`;
foreign objects (real PySpark DataFrames, pandas frames) are refused loud.
"""

from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from repark.errors import AnalysisException, IllegalArgumentException, PySparkTypeError

# === r23 QI1: idents ===
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark._temp_views import scratch_view_name
from repark.spark.ml.param import HasInputCol, HasOutputCol, Param, Params

if TYPE_CHECKING:
    from repark.spark.dataframe import DataFrame

M = TypeVar("M", bound="Model")


def _require_repark_dataframe(dataset: Any, *, verb: str) -> DataFrame:
    """Refuse foreign frame types loud, naming what was passed (greylight Q10)."""
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
    """Refuse if output column already exists (no silent overwrite on transform)."""
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
    """Refuse null / wrong-width dense feature rows before plan-built transform (octo C3).

    Uses a plan aggregate (not a Python training-row loop). ``array_element`` would
    otherwise silently yield NULL for out-of-range indices.
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
    """Fits a :class:`Model` on a dataset (Spark ``Estimator``)."""

    @abstractmethod
    def _fit(self, dataset: DataFrame) -> M:
        """Subclass implement: fit via session queries only."""

    def fit(self, dataset: Any, params: dict[Param[Any], Any] | None = None) -> M:
        """Fit on ``dataset``; optional one-shot param override (Spark ``fit``)."""
        frame = _require_repark_dataframe(dataset, verb="Estimator.fit")
        if params is None:
            return self._fit(frame)
        if isinstance(params, dict):
            return self.copy(extra=params)._fit(frame)
        raise PySparkTypeError(
            f"fit params must be a Param map dict or None, got {type(params).__name__}"
        )


class Transformer(Params, ABC):
    """Transforms a dataset into another (Spark ``Transformer``)."""

    @abstractmethod
    def _transform(self, dataset: DataFrame) -> DataFrame:
        """Subclass implement: return a plan-built DataFrame (no Python row loops)."""

    def transform(self, dataset: Any, params: dict[Param[Any], Any] | None = None) -> DataFrame:
        """Transform ``dataset``; optional one-shot param override."""
        frame = _require_repark_dataframe(dataset, verb="Transformer.transform")
        if params is None:
            return self._transform(frame)
        if isinstance(params, dict):
            return self.copy(extra=params)._transform(frame)
        raise PySparkTypeError(
            f"transform params must be a Param map dict or None, got {type(params).__name__}"
        )


class Model(Transformer, ABC):
    """A fitted :class:`Transformer` produced by an :class:`Estimator`."""


class UnaryTransformer(HasInputCol, HasOutputCol, Transformer, ABC):
    """Transformer with single input/output columns (Spark ``UnaryTransformer``)."""

    def __init__(self) -> None:
        """Init input/output mixins (MRO cooperative)."""
        super().__init__()

    @abstractmethod
    def createTransformFunc(self) -> Any:
        """Return a callable or plan factory (Spark API shape; repark uses plan paths)."""

    def _transform(self, dataset: DataFrame) -> DataFrame:
        """Default unary path: subclasses should override with plan SQL when possible."""
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
