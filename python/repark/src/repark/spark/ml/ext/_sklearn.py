"""Delegated scikit-learn RandomForest estimators.

scikit-learn offers no supported non-pickle restore path here. RandomForest
models refuse save and load with the exact pickle-safety reason.
"""

from __future__ import annotations

from typing import Any

from repark.errors import IllegalArgumentException, UnsupportedOperationException
from repark.spark.ml.base import (
    Estimator,
    Model,
    _refuse_output_collision,
    _require_repark_dataframe,
)
from repark.spark.ml.ext._arrow_util import (
    features_matrix_from_arrow,
    label_vector_from_arrow,
    reenter_with_prediction,
)
from repark.spark.ml.ext._deps import require_sklearn
from repark.spark.ml.ext._persist import PICKLE_FORBIDDEN_REASON, SKLEARN_SAVE_UNSUPPORTED
from repark.spark.ml.param import (
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Param,
    TypeConverters,
)

_ = PICKLE_FORBIDDEN_REASON


def _ensure_sklearn_loaded() -> Any:
    """Force sklearn import at class-touch time."""
    return require_sklearn()


class RandomForestRegressor(
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Estimator["RandomForestRegressorModel"],
):
    """Delegated sklearn RandomForestRegressor (optional ``repark[ml-ext]``)."""

    def __init__(
        self,
        *,
        featuresCol: str | None = None,  # noqa: N803
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        numTrees: int | None = None,  # noqa: N803
        maxDepth: int | None = None,  # noqa: N803
        seed: int | None = None,
    ) -> None:
        """Optional kwargs; requires ``repark[ml-ext]``."""
        _ensure_sklearn_loaded()
        super().__init__()
        self.numTrees: Param[int] = Param(
            self, "numTrees", "number of trees in the forest", TypeConverters.toInt
        )
        self.maxDepth: Param[int] = Param(
            self, "maxDepth", "maximum tree depth", TypeConverters.toInt
        )
        self.seed: Param[int] = Param(self, "seed", "random seed", TypeConverters.toInt)
        self._setDefault(numTrees=20, maxDepth=5, seed=0)
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if labelCol is not None:
            self.setLabelCol(labelCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)
        if numTrees is not None:
            self._set(numTrees=numTrees)
        if maxDepth is not None:
            self._set(maxDepth=maxDepth)
        if seed is not None:
            self._set(seed=seed)

    def _fit(self, dataset: Any) -> RandomForestRegressorModel:
        """Fit sklearn RandomForestRegressor."""
        frame = _require_repark_dataframe(dataset, verb="RandomForestRegressor.fit")
        sklearn = require_sklearn()
        from sklearn.ensemble import RandomForestRegressor as SKRandomForestRegressor

        _ = sklearn
        table = frame.to_arrow()
        features_col = self.getFeaturesCol()
        label_col = self.getLabelCol()
        feature_matrix = features_matrix_from_arrow(table, features_col)
        labels = label_vector_from_arrow(table, label_col)
        kwargs = {
            "n_estimators": int(self.getOrDefault(self.numTrees)),
            "max_depth": int(self.getOrDefault(self.maxDepth)),
            "random_state": int(self.getOrDefault(self.seed)),
            "n_jobs": 1,
        }
        estimator = SKRandomForestRegressor(**kwargs)
        estimator.fit(feature_matrix, labels)
        num_rows = int(feature_matrix.shape[0])
        num_features = int(feature_matrix.shape[1])
        del feature_matrix, labels, table
        model = RandomForestRegressorModel(
            booster=estimator,
            featuresCol=features_col,
            predictionCol=self.getPredictionCol(),
            num_features=num_features,
            num_rows=num_rows,
            fit_params=dict(kwargs),
        )
        model.uid = self.uid
        return model


class RandomForestRegressorModel(HasFeaturesCol, HasPredictionCol, Model):
    """Fitted sklearn RF regressor."""

    def __init__(
        self,
        *,
        booster: Any = None,
        featuresCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        num_features: int = 0,
        num_rows: int = 0,
        fit_params: dict[str, Any] | None = None,
    ) -> None:
        """Store estimator handle."""
        _ensure_sklearn_loaded()
        super().__init__()
        self._booster = booster
        self.num_features = int(num_features)
        self.num_rows = int(num_rows)
        self.fit_params: dict[str, Any] = dict(fit_params or {})
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)

    def _transform(self, dataset: Any) -> Any:
        """Batch predict → Arrow re-entry."""
        frame = _require_repark_dataframe(dataset, verb="RandomForestRegressorModel.transform")
        if self._booster is None:
            raise IllegalArgumentException("RandomForestRegressorModel has no fitted estimator")
        _refuse_output_collision(
            frame, self.getPredictionCol(), stage="RandomForestRegressorModel.transform"
        )
        table = frame.to_arrow()
        feature_matrix = features_matrix_from_arrow(table, self.getFeaturesCol())
        predictions = self._booster.predict(feature_matrix)
        return reenter_with_prediction(frame, table, predictions, self.getPredictionCol())

    def save(self, path: str) -> None:
        """Refuse persistence because pickle loading is unsafe."""
        raise UnsupportedOperationException(SKLEARN_SAVE_UNSUPPORTED)

    def write(self) -> Any:
        """Refuse persistence because pickle loading is unsafe."""
        raise UnsupportedOperationException(SKLEARN_SAVE_UNSUPPORTED)

    @classmethod
    def load(cls, path: str) -> RandomForestRegressorModel:
        """Refuse loading because pickle loading is unsafe."""
        raise UnsupportedOperationException(SKLEARN_SAVE_UNSUPPORTED)

    @classmethod
    def read(cls) -> Any:
        """Pin-refuse reader (pickle forbidden)."""
        raise UnsupportedOperationException(SKLEARN_SAVE_UNSUPPORTED)


class RandomForestClassifier(
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Estimator["RandomForestClassifierModel"],
):
    """Delegated sklearn RandomForestClassifier."""

    def __init__(
        self,
        *,
        featuresCol: str | None = None,  # noqa: N803
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        numTrees: int | None = None,  # noqa: N803
        maxDepth: int | None = None,  # noqa: N803
        seed: int | None = None,
    ) -> None:
        """Configure optional parameters."""
        _ensure_sklearn_loaded()
        super().__init__()
        self.numTrees: Param[int] = Param(self, "numTrees", "num trees", TypeConverters.toInt)
        self.maxDepth: Param[int] = Param(self, "maxDepth", "max depth", TypeConverters.toInt)
        self.seed: Param[int] = Param(self, "seed", "seed", TypeConverters.toInt)
        self._setDefault(numTrees=20, maxDepth=5, seed=0)
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if labelCol is not None:
            self.setLabelCol(labelCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)
        if numTrees is not None:
            self._set(numTrees=numTrees)
        if maxDepth is not None:
            self._set(maxDepth=maxDepth)
        if seed is not None:
            self._set(seed=seed)

    def _fit(self, dataset: Any) -> RandomForestClassifierModel:
        """Fit sklearn RandomForestClassifier."""
        frame = _require_repark_dataframe(dataset, verb="RandomForestClassifier.fit")
        from sklearn.ensemble import RandomForestClassifier as SKRandomForestClassifier

        table = frame.to_arrow()
        features_col = self.getFeaturesCol()
        label_col = self.getLabelCol()
        feature_matrix = features_matrix_from_arrow(table, features_col)
        labels = label_vector_from_arrow(table, label_col)
        kwargs = {
            "n_estimators": int(self.getOrDefault(self.numTrees)),
            "max_depth": int(self.getOrDefault(self.maxDepth)),
            "random_state": int(self.getOrDefault(self.seed)),
            "n_jobs": 1,
        }
        estimator = SKRandomForestClassifier(**kwargs)
        estimator.fit(feature_matrix, labels)
        num_rows = int(feature_matrix.shape[0])
        num_features = int(feature_matrix.shape[1])
        del feature_matrix, labels, table
        model = RandomForestClassifierModel(
            booster=estimator,
            featuresCol=features_col,
            predictionCol=self.getPredictionCol(),
            num_features=num_features,
            num_rows=num_rows,
            fit_params=dict(kwargs),
        )
        model.uid = self.uid
        return model


class RandomForestClassifierModel(HasFeaturesCol, HasPredictionCol, Model):
    """Fitted sklearn RF classifier."""

    def __init__(
        self,
        *,
        booster: Any = None,
        featuresCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        num_features: int = 0,
        num_rows: int = 0,
        fit_params: dict[str, Any] | None = None,
    ) -> None:
        """Store estimator."""
        _ensure_sklearn_loaded()
        super().__init__()
        self._booster = booster
        self.num_features = int(num_features)
        self.num_rows = int(num_rows)
        self.fit_params: dict[str, Any] = dict(fit_params or {})
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)

    def _transform(self, dataset: Any) -> Any:
        """Class predictions."""
        frame = _require_repark_dataframe(dataset, verb="RandomForestClassifierModel.transform")
        if self._booster is None:
            raise IllegalArgumentException("RandomForestClassifierModel has no fitted estimator")
        _refuse_output_collision(
            frame, self.getPredictionCol(), stage="RandomForestClassifierModel.transform"
        )
        table = frame.to_arrow()
        feature_matrix = features_matrix_from_arrow(table, self.getFeaturesCol())
        predictions = self._booster.predict(feature_matrix)
        return reenter_with_prediction(frame, table, predictions, self.getPredictionCol())

    def save(self, path: str) -> None:
        """Refuse persistence because pickle loading is unsafe."""
        raise UnsupportedOperationException(SKLEARN_SAVE_UNSUPPORTED)

    def write(self) -> Any:
        """Refuse persistence because pickle loading is unsafe."""
        raise UnsupportedOperationException(SKLEARN_SAVE_UNSUPPORTED)

    @classmethod
    def load(cls, path: str) -> RandomForestClassifierModel:
        """Refuse loading because pickle loading is unsafe."""
        raise UnsupportedOperationException(SKLEARN_SAVE_UNSUPPORTED)

    @classmethod
    def read(cls) -> Any:
        """Pin-refuse reader (pickle forbidden)."""
        raise UnsupportedOperationException(SKLEARN_SAVE_UNSUPPORTED)


__all__ = [
    "PICKLE_FORBIDDEN_REASON",
    "SKLEARN_SAVE_UNSUPPORTED",
    "RandomForestClassifier",
    "RandomForestClassifierModel",
    "RandomForestRegressor",
    "RandomForestRegressorModel",
]
