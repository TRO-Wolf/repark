"""Delegated LightGBM estimators.

Fitted models save and load with LightGBM's native text format and a library-major
version guard. Persistence is atomic and never uses pickle.
"""

from __future__ import annotations

from typing import Any

from repark.errors import IllegalArgumentException
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
from repark.spark.ml.ext._deps import require_lightgbm, require_numpy
from repark.spark.ml.ext._persist import load_ext_model_envelope, write_ext_model_tree
from repark.spark.ml.param import (
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Param,
    TypeConverters,
)
from repark.spark.ml.util import MLReadable, MLReader, MLWritable, MLWriter

_LGB_BOOSTER_BLOB_NAME = "booster.txt"
_LGB_LIBRARY_NAME = "lightgbm"


def _ensure_lightgbm_loaded() -> Any:
    """Force lightgbm import at class-touch time."""
    return require_lightgbm()


def _lightgbm_version() -> str:
    """Installed lightgbm version string."""
    lgb = require_lightgbm()
    return str(getattr(lgb, "__version__", "unknown"))


class _LgbmPredictShell:
    """Predict through a native ``lightgbm.Booster`` restored from text."""

    def __init__(
        self,
        booster: Any,
        *,
        classifier: bool,
        classes: list[Any] | None = None,
    ) -> None:
        """Store booster handle + task kind."""
        self._booster = booster
        self._classifier = classifier
        self._classes = list(classes) if classes is not None else None

    def predict(self, feature_matrix: Any) -> Any:
        """Match sklearn LGBMRegressor/Classifier.predict on the same matrix."""
        np = require_numpy()
        raw = np.asarray(self._booster.predict(feature_matrix))
        if not self._classifier:
            return raw.reshape(-1)
        if raw.ndim == 1:
            indices = (raw > 0.5).astype(np.int64)
        else:
            indices = raw.argmax(axis=1).astype(np.int64)
        if self._classes is None:
            return indices.astype(np.float64)
        classes = np.asarray(self._classes)
        return classes[indices].astype(np.float64)


def _model_to_string_bytes(sklearn_estimator: Any) -> bytes:
    """Library-native text model → UTF-8 bytes (never pickle)."""
    booster = getattr(sklearn_estimator, "booster_", None)
    if booster is None:
        raise IllegalArgumentException(
            "LightGBM save: fitted estimator has no booster_ (not fitted?)"
        )
    text = booster.model_to_string()
    if not isinstance(text, str) or not text:
        raise IllegalArgumentException("LightGBM save: model_to_string returned empty")
    return text.encode("utf-8")


def _classes_list(sklearn_estimator: Any) -> list[Any] | None:
    """Best-effort class labels from a fitted LGBMClassifier."""
    classes = getattr(sklearn_estimator, "classes_", None)
    if classes is None:
        return None
    return [item.item() if hasattr(item, "item") else item for item in list(classes)]


def _shell_from_bytes(
    raw: bytes,
    *,
    classifier: bool,
    classes: list[Any] | None,
) -> _LgbmPredictShell:
    """Rebuild predict shell from model_to_string UTF-8 bytes."""
    lgb = require_lightgbm()
    text = raw.decode("utf-8")
    booster = lgb.Booster(model_str=text)
    return _LgbmPredictShell(booster, classifier=classifier, classes=classes)


class LightGBMRegressor(
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Estimator["LightGBMRegressorModel"],
):
    """Delegated LightGBM regressor (optional ``repark[ml-ext]``)."""

    def __init__(
        self,
        *,
        featuresCol: str | None = None,  # noqa: N803
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        maxDepth: int | None = None,  # noqa: N803
        nEstimators: int | None = None,  # noqa: N803
        learningRate: float | None = None,  # noqa: N803
        seed: int | None = None,
    ) -> None:
        """Optional kwargs; requires ``repark[ml-ext]``."""
        _ensure_lightgbm_loaded()
        super().__init__()
        self.maxDepth: Param[int] = Param(
            self, "maxDepth", "maximum tree depth (-1 = no limit in LGBM)", TypeConverters.toInt
        )
        self.nEstimators: Param[int] = Param(
            self, "nEstimators", "number of boosting rounds", TypeConverters.toInt
        )
        self.learningRate: Param[float] = Param(
            self, "learningRate", "boosting learning rate", TypeConverters.toFloat
        )
        self.seed: Param[int] = Param(self, "seed", "random seed", TypeConverters.toInt)
        self._setDefault(maxDepth=6, nEstimators=50, learningRate=0.1, seed=0)
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if labelCol is not None:
            self.setLabelCol(labelCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)
        if maxDepth is not None:
            self._set(maxDepth=maxDepth)
        if nEstimators is not None:
            self._set(nEstimators=nEstimators)
        if learningRate is not None:
            self._set(learningRate=learningRate)
        if seed is not None:
            self._set(seed=seed)

    def _fit(self, dataset: Any) -> LightGBMRegressorModel:
        """Fit via lightgbm.LGBMRegressor on Arrow-derived dense matrix."""
        frame = _require_repark_dataframe(dataset, verb="LightGBMRegressor.fit")
        lgb = require_lightgbm()
        table = frame.to_arrow()
        features_col = self.getFeaturesCol()
        label_col = self.getLabelCol()
        feature_matrix = features_matrix_from_arrow(table, features_col)
        labels = label_vector_from_arrow(table, label_col)
        num_rows = int(feature_matrix.shape[0])
        num_features = int(feature_matrix.shape[1])
        kwargs = {
            "max_depth": int(self.getOrDefault(self.maxDepth)),
            "n_estimators": int(self.getOrDefault(self.nEstimators)),
            "learning_rate": float(self.getOrDefault(self.learningRate)),
            "random_state": int(self.getOrDefault(self.seed)),
            "verbosity": -1,
            "n_jobs": 1,
        }
        booster = lgb.LGBMRegressor(**kwargs)
        booster.fit(feature_matrix, labels)
        del feature_matrix, labels, table
        model = LightGBMRegressorModel(
            booster=booster,
            featuresCol=features_col,
            predictionCol=self.getPredictionCol(),
            num_features=num_features,
            num_rows=num_rows,
            fit_params=dict(kwargs),
        )
        model.uid = self.uid
        return model


class LightGBMRegressorModel(HasFeaturesCol, HasPredictionCol, Model, MLWritable, MLReadable):
    """Fitted LightGBM regressor with native text persistence."""

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
        """Store booster handle."""
        _ensure_lightgbm_loaded()
        super().__init__()
        self._booster = booster
        self.num_features = int(num_features)
        self.num_rows = int(num_rows)
        self.fit_params: dict[str, Any] = dict(fit_params or {})
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)

    @property
    def booster(self) -> Any:
        """Underlying LGBMRegressor or post-load predict shell."""
        return self._booster

    def _transform(self, dataset: Any) -> Any:
        """Batch predict → Arrow re-entry."""
        frame = _require_repark_dataframe(dataset, verb="LightGBMRegressorModel.transform")
        if self._booster is None:
            raise IllegalArgumentException("LightGBMRegressorModel has no fitted booster")
        _refuse_output_collision(
            frame, self.getPredictionCol(), stage="LightGBMRegressorModel.transform"
        )
        table = frame.to_arrow()
        feature_matrix = features_matrix_from_arrow(table, self.getFeaturesCol())
        if self.num_features and feature_matrix.shape[1] != self.num_features:
            raise IllegalArgumentException(
                f"LightGBMRegressorModel.transform: feature width {feature_matrix.shape[1]} "
                f"!= fitted num_features {self.num_features}"
            )
        predictions = self._booster.predict(feature_matrix)
        return reenter_with_prediction(frame, table, predictions, self.getPredictionCol())

    def write(self) -> MLWriter:
        """Return a native text writer."""
        return _LightGBMModelWriter(self, kind="LightGBMRegressorModel", classifier=False)

    def save(self, path: str) -> None:
        """Save via :meth:`write`."""
        self.write().save(path)

    @classmethod
    def read(cls) -> MLReader:
        """Return model_to_string reader."""
        return _LightGBMModelReader(cls, kind="LightGBMRegressorModel", classifier=False)

    @classmethod
    def load(cls, path: str) -> LightGBMRegressorModel:
        """Load a model saved by :meth:`save`."""
        return cls.read().load(path)

    def copy(self, extra: dict[Any, Any] | None = None) -> LightGBMRegressorModel:
        """Shallow copy; apply ``extra`` Param overrides."""
        that = LightGBMRegressorModel(
            booster=self._booster,
            featuresCol=self.getFeaturesCol(),
            predictionCol=self.getPredictionCol(),
            num_features=self.num_features,
            num_rows=self.num_rows,
            fit_params=dict(self.fit_params),
        )
        that.uid = self.uid
        if extra:
            for param, value in extra.items():
                name = param.name if hasattr(param, "name") else str(param)
                that._set(**{name: value})
        return that


class LightGBMClassifier(
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Estimator["LightGBMClassifierModel"],
):
    """Delegated LightGBM classifier."""

    def __init__(
        self,
        *,
        featuresCol: str | None = None,  # noqa: N803
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        maxDepth: int | None = None,  # noqa: N803
        nEstimators: int | None = None,  # noqa: N803
        learningRate: float | None = None,  # noqa: N803
        seed: int | None = None,
    ) -> None:
        """Configure optional parameters."""
        _ensure_lightgbm_loaded()
        super().__init__()
        self.maxDepth: Param[int] = Param(self, "maxDepth", "max depth", TypeConverters.toInt)
        self.nEstimators: Param[int] = Param(
            self, "nEstimators", "boosting rounds", TypeConverters.toInt
        )
        self.learningRate: Param[float] = Param(
            self, "learningRate", "learning rate", TypeConverters.toFloat
        )
        self.seed: Param[int] = Param(self, "seed", "seed", TypeConverters.toInt)
        self._setDefault(maxDepth=6, nEstimators=50, learningRate=0.1, seed=0)
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if labelCol is not None:
            self.setLabelCol(labelCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)
        if maxDepth is not None:
            self._set(maxDepth=maxDepth)
        if nEstimators is not None:
            self._set(nEstimators=nEstimators)
        if learningRate is not None:
            self._set(learningRate=learningRate)
        if seed is not None:
            self._set(seed=seed)

    def _fit(self, dataset: Any) -> LightGBMClassifierModel:
        """Fit LGBMClassifier."""
        frame = _require_repark_dataframe(dataset, verb="LightGBMClassifier.fit")
        lgb = require_lightgbm()
        table = frame.to_arrow()
        features_col = self.getFeaturesCol()
        label_col = self.getLabelCol()
        feature_matrix = features_matrix_from_arrow(table, features_col)
        labels = label_vector_from_arrow(table, label_col)
        kwargs = {
            "max_depth": int(self.getOrDefault(self.maxDepth)),
            "n_estimators": int(self.getOrDefault(self.nEstimators)),
            "learning_rate": float(self.getOrDefault(self.learningRate)),
            "random_state": int(self.getOrDefault(self.seed)),
            "verbosity": -1,
            "n_jobs": 1,
        }
        booster = lgb.LGBMClassifier(**kwargs)
        booster.fit(feature_matrix, labels)
        num_rows = int(feature_matrix.shape[0])
        num_features = int(feature_matrix.shape[1])
        classes = _classes_list(booster)
        del feature_matrix, labels, table
        model = LightGBMClassifierModel(
            booster=booster,
            featuresCol=features_col,
            predictionCol=self.getPredictionCol(),
            num_features=num_features,
            num_rows=num_rows,
            fit_params=dict(kwargs),
            classes=classes,
        )
        model.uid = self.uid
        return model


class LightGBMClassifierModel(HasFeaturesCol, HasPredictionCol, Model, MLWritable, MLReadable):
    """Fitted LightGBM classifier with native text persistence."""

    def __init__(
        self,
        *,
        booster: Any = None,
        featuresCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        num_features: int = 0,
        num_rows: int = 0,
        fit_params: dict[str, Any] | None = None,
        classes: list[Any] | None = None,
    ) -> None:
        """Store booster + optional class labels."""
        _ensure_lightgbm_loaded()
        super().__init__()
        self._booster = booster
        self.num_features = int(num_features)
        self.num_rows = int(num_rows)
        self.fit_params: dict[str, Any] = dict(fit_params or {})
        self.classes: list[Any] | None = list(classes) if classes is not None else None
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)

    @property
    def booster(self) -> Any:
        """Underlying LGBMClassifier or post-load predict shell."""
        return self._booster

    def _transform(self, dataset: Any) -> Any:
        """Class predictions via predict."""
        frame = _require_repark_dataframe(dataset, verb="LightGBMClassifierModel.transform")
        if self._booster is None:
            raise IllegalArgumentException("LightGBMClassifierModel has no fitted booster")
        _refuse_output_collision(
            frame, self.getPredictionCol(), stage="LightGBMClassifierModel.transform"
        )
        table = frame.to_arrow()
        feature_matrix = features_matrix_from_arrow(table, self.getFeaturesCol())
        if self.num_features and feature_matrix.shape[1] != self.num_features:
            raise IllegalArgumentException(
                f"LightGBMClassifierModel.transform: feature width {feature_matrix.shape[1]} "
                f"!= fitted num_features {self.num_features}"
            )
        predictions = self._booster.predict(feature_matrix)
        return reenter_with_prediction(frame, table, predictions, self.getPredictionCol())

    def write(self) -> MLWriter:
        """Return a native text writer."""
        return _LightGBMModelWriter(self, kind="LightGBMClassifierModel", classifier=True)

    def save(self, path: str) -> None:
        """Save via :meth:`write`."""
        self.write().save(path)

    @classmethod
    def read(cls) -> MLReader:
        """Return model_to_string reader."""
        return _LightGBMModelReader(cls, kind="LightGBMClassifierModel", classifier=True)

    @classmethod
    def load(cls, path: str) -> LightGBMClassifierModel:
        """Load a model saved by :meth:`save`."""
        return cls.read().load(path)

    def copy(self, extra: dict[Any, Any] | None = None) -> LightGBMClassifierModel:
        """Shallow copy; apply ``extra`` Param overrides."""
        that = LightGBMClassifierModel(
            booster=self._booster,
            featuresCol=self.getFeaturesCol(),
            predictionCol=self.getPredictionCol(),
            num_features=self.num_features,
            num_rows=self.num_rows,
            fit_params=dict(self.fit_params),
            classes=list(self.classes) if self.classes is not None else None,
        )
        that.uid = self.uid
        if extra:
            for param, value in extra.items():
                name = param.name if hasattr(param, "name") else str(param)
                that._set(**{name: value})
        return that


class _LightGBMModelWriter(MLWriter):
    """Write a LightGBM model with native text and an atomic envelope."""

    def __init__(
        self,
        instance: LightGBMRegressorModel | LightGBMClassifierModel,
        *,
        kind: str,
        classifier: bool,
    ) -> None:
        """Bind model + kind."""
        super().__init__(instance)
        self._kind = kind
        self._classifier = classifier

    def saveImpl(self, path: str) -> None:
        """Write the envelope and native booster text without training rows or pickle."""
        model = self.instance
        if model._booster is None:
            raise IllegalArgumentException(f"{self._kind}.save: no fitted booster")
        if isinstance(model._booster, _LgbmPredictShell):
            text = model._booster._booster.model_to_string()
            if not isinstance(text, str) or not text:
                raise IllegalArgumentException(f"{self._kind}.save: model_to_string returned empty")
            raw = text.encode("utf-8")
            classes = model._booster._classes
        else:
            raw = _model_to_string_bytes(model._booster)
            classes = None
            if self._classifier:
                classes = getattr(model, "classes", None)
                if classes is None:
                    classes = _classes_list(model._booster)
        fitted_payload: dict[str, Any] = {
            "num_features": model.num_features,
            "num_rows": model.num_rows,
            "featuresCol": model.getFeaturesCol(),
            "predictionCol": model.getPredictionCol(),
            "fit_params": dict(model.fit_params),
            "booster_blob": _LGB_BOOSTER_BLOB_NAME,
            "booster_format": "model_to_string",
            "classifier": self._classifier,
        }
        if self._classifier and classes is not None:
            fitted_payload["classes"] = list(classes)
        write_ext_model_tree(
            path,
            overwrite=self.should_overwrite,
            kind=self._kind,
            model_class=type(model),
            uid=model.uid,
            params={
                "featuresCol": model.getFeaturesCol(),
                "predictionCol": model.getPredictionCol(),
            },
            fitted_payload=fitted_payload,
            booster_blob_name=_LGB_BOOSTER_BLOB_NAME,
            booster_bytes=raw,
            library_name=_LGB_LIBRARY_NAME,
            library_version=_lightgbm_version(),
            extra_metadata={"booster_format": "model_to_string"},
        )


class _LightGBMModelReader(MLReader):
    """Load LightGBM*Model from model_to_string envelope."""

    def __init__(
        self,
        cls: type[LightGBMRegressorModel] | type[LightGBMClassifierModel],
        *,
        kind: str,
        classifier: bool,
    ) -> None:
        """Bind the model class + kind."""
        self._cls = cls
        self._kind = kind
        self._classifier = classifier

    def load(self, path: str) -> LightGBMRegressorModel | LightGBMClassifierModel:
        """Read envelope + rebuild predict shell."""
        metadata, fitted, raw = load_ext_model_envelope(
            path,
            expected_kind=self._kind,
            library_name=_LGB_LIBRARY_NAME,
            current_library_version=_lightgbm_version(),
            default_blob_name=_LGB_BOOSTER_BLOB_NAME,
            expected_classifier=self._classifier,
        )
        classes = fitted.get("classes")
        if classes is not None and not isinstance(classes, list):
            classes = list(classes)
        shell = _shell_from_bytes(raw, classifier=self._classifier, classes=classes)
        kwargs: dict[str, Any] = {
            "booster": shell,
            "featuresCol": fitted.get("featuresCol")
            or (metadata.get("params") or {}).get("featuresCol"),
            "predictionCol": fitted.get("predictionCol")
            or (metadata.get("params") or {}).get("predictionCol"),
            "num_features": int(fitted.get("num_features") or 0),
            "num_rows": int(fitted.get("num_rows") or 0),
            "fit_params": dict(fitted.get("fit_params") or {}),
        }
        if self._classifier:
            kwargs["classes"] = classes
        model = self._cls(**kwargs)
        if metadata.get("uid"):
            model.uid = str(metadata["uid"])
        return model


__all__ = [
    "LightGBMClassifier",
    "LightGBMClassifierModel",
    "LightGBMRegressor",
    "LightGBMRegressorModel",
]
