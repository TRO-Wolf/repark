"""XGBoost delegated estimators — ``pyspark.ml``-shaped Params/fit/transform (M4/M5/M8).

Fit pulls training via ``to_arrow()``; the model holds the external booster + params
only (**no training-row re-hold** after fit). Transform: batch predict → Arrow →
``createDataFrame`` re-entry.

**M8 persistence:** ``XGBoostRegressorModel`` and ``XGBoostClassifierModel`` save/load
via the repark-ml v1 envelope (``metadata.json`` + fitted params parquet +
``booster.raw`` from ``save_raw()``), atomic write (M7 staging), and library-major
version guard on load. Never pickle.
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
from repark.spark.ml.ext._deps import require_numpy, require_xgboost
from repark.spark.ml.ext._persist import (
    EXT_SAVE_UNSUPPORTED,
    load_ext_model_envelope,
    write_ext_model_tree,
)
from repark.spark.ml.param import (
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Param,
    TypeConverters,
)
from repark.spark.ml.util import MLReadable, MLReader, MLWritable, MLWriter

# Booster-bytes layout (M1 envelope + blob).
_XGB_BOOSTER_BLOB_NAME = "booster.raw"
_XGB_BOOSTER_FORMAT = "ubj"
_XGB_LIBRARY_NAME = "xgboost"


def _ensure_xgboost_loaded() -> Any:
    """Force xgboost import at class-touch time (ImportError names the extra)."""
    return require_xgboost()


def _xgboost_version() -> str:
    """Installed xgboost version string."""
    xgb = require_xgboost()
    return str(getattr(xgb, "__version__", "unknown"))


def _load_xgb_estimator_from_raw(
    raw: bytes,
    *,
    fit_params: dict[str, Any],
    classifier: bool,
) -> Any:
    """Rebuild an XGBRegressor/XGBClassifier shell and load booster bytes."""
    xgb = require_xgboost()
    kwargs = dict(fit_params)
    if classifier:
        booster = xgb.XGBClassifier(**kwargs) if kwargs else xgb.XGBClassifier()
    else:
        booster = xgb.XGBRegressor(**kwargs) if kwargs else xgb.XGBRegressor()
    booster.load_model(bytearray(raw))
    return booster


def _booster_raw_bytes(booster_handle: Any) -> bytes:
    """Serialize via library-native ``save_raw`` (never pickle)."""
    raw = booster_handle.get_booster().save_raw(_XGB_BOOSTER_FORMAT)
    return bytes(raw)


class XGBoostRegressor(
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Estimator["XGBoostRegressorModel"],
):
    """Delegated XGBoost regressor (optional ``repark[ml-ext]``).

    Params mirror a practical ``pyspark.ml`` / xgboost-spark subset. Training uses
    ``xgboost.XGBRegressor`` on a dense feature matrix derived from ``to_arrow()``.
    """

    def __init__(
        self,
        *,
        featuresCol: str | None = None,  # noqa: N803
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        maxDepth: int | None = None,  # noqa: N803
        nEstimators: int | None = None,  # noqa: N803
        learningRate: float | None = None,  # noqa: N803
        subsample: float | None = None,
        colsampleBytree: float | None = None,  # noqa: N803
        regLambda: float | None = None,  # noqa: N803
        regAlpha: float | None = None,  # noqa: N803
        minChildWeight: float | None = None,  # noqa: N803
        gamma: float | None = None,
        seed: int | None = None,
        objective: str | None = None,
    ) -> None:
        """Optional kwargs; importing this class requires ``repark[ml-ext]``."""
        _ensure_xgboost_loaded()
        super().__init__()
        self.maxDepth: Param[int] = Param(
            self, "maxDepth", "maximum tree depth", TypeConverters.toInt
        )
        self.nEstimators: Param[int] = Param(
            self, "nEstimators", "number of boosting rounds", TypeConverters.toInt
        )
        self.learningRate: Param[float] = Param(
            self, "learningRate", "boosting learning rate (eta)", TypeConverters.toFloat
        )
        self.subsample: Param[float] = Param(
            self, "subsample", "subsample ratio of training rows", TypeConverters.toFloat
        )
        self.colsampleBytree: Param[float] = Param(
            self,
            "colsampleBytree",
            "subsample ratio of columns when constructing each tree",
            TypeConverters.toFloat,
        )
        self.regLambda: Param[float] = Param(
            self, "regLambda", "L2 regularization", TypeConverters.toFloat
        )
        self.regAlpha: Param[float] = Param(
            self, "regAlpha", "L1 regularization", TypeConverters.toFloat
        )
        self.minChildWeight: Param[float] = Param(
            self,
            "minChildWeight",
            "minimum sum of instance weight in a child",
            TypeConverters.toFloat,
        )
        self.gamma: Param[float] = Param(
            self, "gamma", "minimum loss reduction to make a split", TypeConverters.toFloat
        )
        self.seed: Param[int] = Param(
            self, "seed", "random seed for booster determinism", TypeConverters.toInt
        )
        self.objective: Param[str] = Param(
            self, "objective", "xgboost objective string", TypeConverters.toString
        )
        self._setDefault(
            maxDepth=6,
            nEstimators=50,
            learningRate=0.3,
            subsample=1.0,
            colsampleBytree=1.0,
            regLambda=1.0,
            regAlpha=0.0,
            minChildWeight=1.0,
            gamma=0.0,
            seed=0,
            objective="reg:squarederror",
        )
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
        if subsample is not None:
            self._set(subsample=subsample)
        if colsampleBytree is not None:
            self._set(colsampleBytree=colsampleBytree)
        if regLambda is not None:
            self._set(regLambda=regLambda)
        if regAlpha is not None:
            self._set(regAlpha=regAlpha)
        if minChildWeight is not None:
            self._set(minChildWeight=minChildWeight)
        if gamma is not None:
            self._set(gamma=gamma)
        if seed is not None:
            self._set(seed=seed)
        if objective is not None:
            self._set(objective=objective)

    def setMaxDepth(self, value: int) -> XGBoostRegressor:
        """Set maxDepth."""
        return self._set(maxDepth=value)

    def getMaxDepth(self) -> int:
        """Get maxDepth."""
        return int(self.getOrDefault(self.maxDepth))

    def setNEstimators(self, value: int) -> XGBoostRegressor:
        """Set nEstimators (boosting rounds)."""
        return self._set(nEstimators=value)

    def getNEstimators(self) -> int:
        """Get nEstimators."""
        return int(self.getOrDefault(self.nEstimators))

    def setLearningRate(self, value: float) -> XGBoostRegressor:
        """Set learningRate."""
        return self._set(learningRate=value)

    def getLearningRate(self) -> float:
        """Get learningRate."""
        return float(self.getOrDefault(self.learningRate))

    def setSeed(self, value: int) -> XGBoostRegressor:
        """Set seed."""
        return self._set(seed=value)

    def getSeed(self) -> int:
        """Get seed."""
        return int(self.getOrDefault(self.seed))

    def _booster_kwargs(self) -> dict[str, Any]:
        """Map Params → ``XGBRegressor`` constructor kwargs."""
        return {
            "max_depth": int(self.getOrDefault(self.maxDepth)),
            "n_estimators": int(self.getOrDefault(self.nEstimators)),
            "learning_rate": float(self.getOrDefault(self.learningRate)),
            "subsample": float(self.getOrDefault(self.subsample)),
            "colsample_bytree": float(self.getOrDefault(self.colsampleBytree)),
            "reg_lambda": float(self.getOrDefault(self.regLambda)),
            "reg_alpha": float(self.getOrDefault(self.regAlpha)),
            "min_child_weight": float(self.getOrDefault(self.minChildWeight)),
            "gamma": float(self.getOrDefault(self.gamma)),
            "random_state": int(self.getOrDefault(self.seed)),
            "objective": str(self.getOrDefault(self.objective)),
            "n_jobs": 1,
            "verbosity": 0,
        }

    def _fit(self, dataset: Any) -> XGBoostRegressorModel:
        """Fit via to_arrow → dense matrix → xgboost.XGBRegressor; drop training rows."""
        frame = _require_repark_dataframe(dataset, verb="XGBoostRegressor.fit")
        xgb = require_xgboost()
        table = frame.to_arrow()
        features_col = self.getFeaturesCol()
        label_col = self.getLabelCol()
        feature_matrix = features_matrix_from_arrow(table, features_col)
        labels = label_vector_from_arrow(table, label_col)
        num_rows = int(feature_matrix.shape[0])
        num_features = int(feature_matrix.shape[1])
        kwargs = self._booster_kwargs()
        booster = xgb.XGBRegressor(**kwargs)
        booster.fit(feature_matrix, labels)
        # Deliberately drop feature_matrix / labels / table references after fit.
        del feature_matrix, labels, table
        model = XGBoostRegressorModel(
            booster=booster,
            featuresCol=features_col,
            predictionCol=self.getPredictionCol(),
            num_features=num_features,
            num_rows=num_rows,
            fit_params=dict(kwargs),
        )
        model.uid = self.uid
        return model


class XGBoostRegressorModel(HasFeaturesCol, HasPredictionCol, Model, MLWritable, MLReadable):
    """Fitted XGBoost regressor — holds external booster + params only.

    **M8:** :meth:`save` / :meth:`load` use the repark-ml v1 envelope plus a
    ``booster.raw`` blob from ``get_booster().save_raw()``, atomic M7 publish, and
    library-major version guard. Load restores predict-parity with the pre-save model.
    """

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
        """Store booster handle (no training rows)."""
        _ensure_xgboost_loaded()
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
        """Underlying ``xgboost.XGBRegressor`` (params-only after fit)."""
        return self._booster

    def _transform(self, dataset: Any) -> Any:
        """Batch predict → Arrow append → createDataFrame re-entry."""
        frame = _require_repark_dataframe(dataset, verb="XGBoostRegressorModel.transform")
        if self._booster is None:
            raise IllegalArgumentException("XGBoostRegressorModel has no fitted booster")
        _refuse_output_collision(
            frame, self.getPredictionCol(), stage="XGBoostRegressorModel.transform"
        )
        table = frame.to_arrow()
        feature_matrix = features_matrix_from_arrow(table, self.getFeaturesCol())
        if self.num_features and feature_matrix.shape[1] != self.num_features:
            raise IllegalArgumentException(
                f"XGBoostRegressorModel.transform: feature width {feature_matrix.shape[1]} "
                f"!= fitted num_features {self.num_features}"
            )
        predictions = self._booster.predict(feature_matrix)
        return reenter_with_prediction(frame, table, predictions, self.getPredictionCol())

    def write(self) -> MLWriter:
        """Return a booster-bytes writer (M8 / repark-ml v1 envelope + atomic)."""
        return _XGBoostModelWriter(self, kind="XGBoostRegressorModel", classifier=False)

    def save(self, path: str) -> None:
        """Save via :meth:`write` (booster-bytes + M1 envelope)."""
        self.write().save(path)

    @classmethod
    def read(cls) -> MLReader:
        """Return a booster-bytes reader."""
        return _XGBoostModelReader(cls, kind="XGBoostRegressorModel", classifier=False)

    @classmethod
    def load(cls, path: str) -> XGBoostRegressorModel:
        """Load a model saved by :meth:`save`."""
        return cls.read().load(path)

    def copy(self, extra: dict[Any, Any] | None = None) -> XGBoostRegressorModel:
        """Shallow copy of model shell; apply ``extra`` for ``transform(df, params)`` (C4-L-002)."""
        that = XGBoostRegressorModel(
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


class XGBoostClassifier(
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Estimator["XGBoostClassifierModel"],
):
    """Delegated XGBoost classifier (optional ``repark[ml-ext]``)."""

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
        objective: str | None = None,
    ) -> None:
        """Optional kwargs; requires ``repark[ml-ext]``."""
        _ensure_xgboost_loaded()
        super().__init__()
        self.maxDepth: Param[int] = Param(
            self, "maxDepth", "maximum tree depth", TypeConverters.toInt
        )
        self.nEstimators: Param[int] = Param(
            self, "nEstimators", "number of boosting rounds", TypeConverters.toInt
        )
        self.learningRate: Param[float] = Param(
            self, "learningRate", "boosting learning rate", TypeConverters.toFloat
        )
        self.seed: Param[int] = Param(self, "seed", "random seed", TypeConverters.toInt)
        self.objective: Param[str] = Param(
            self, "objective", "xgboost objective", TypeConverters.toString
        )
        self._setDefault(
            maxDepth=6,
            nEstimators=50,
            learningRate=0.3,
            seed=0,
            objective="binary:logistic",
        )
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
        if objective is not None:
            self._set(objective=objective)

    def _fit(self, dataset: Any) -> XGBoostClassifierModel:
        """Fit binary/multi classifier via xgboost.XGBClassifier."""
        frame = _require_repark_dataframe(dataset, verb="XGBoostClassifier.fit")
        xgb = require_xgboost()
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
            "objective": str(self.getOrDefault(self.objective)),
            "n_jobs": 1,
            "verbosity": 0,
        }
        booster = xgb.XGBClassifier(**kwargs)
        booster.fit(feature_matrix, labels)
        del feature_matrix, labels, table
        model = XGBoostClassifierModel(
            booster=booster,
            featuresCol=features_col,
            predictionCol=self.getPredictionCol(),
            num_features=num_features,
            num_rows=num_rows,
            fit_params=dict(kwargs),
        )
        model.uid = self.uid
        return model


class XGBoostClassifierModel(HasFeaturesCol, HasPredictionCol, Model, MLWritable, MLReadable):
    """Fitted XGBoost classifier — booster + params only (M8 booster-bytes save/load)."""

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
        _ensure_xgboost_loaded()
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
        """Underlying ``xgboost.XGBClassifier``."""
        return self._booster

    def _transform(self, dataset: Any) -> Any:
        """Hard-threshold class prediction (argmax / binary threshold via predict)."""
        frame = _require_repark_dataframe(dataset, verb="XGBoostClassifierModel.transform")
        if self._booster is None:
            raise IllegalArgumentException("XGBoostClassifierModel has no fitted booster")
        _refuse_output_collision(
            frame, self.getPredictionCol(), stage="XGBoostClassifierModel.transform"
        )
        table = frame.to_arrow()
        feature_matrix = features_matrix_from_arrow(table, self.getFeaturesCol())
        if self.num_features and feature_matrix.shape[1] != self.num_features:
            raise IllegalArgumentException(
                f"XGBoostClassifierModel.transform: feature width {feature_matrix.shape[1]} "
                f"!= fitted num_features {self.num_features}"
            )
        raw = self._booster.predict(feature_matrix)
        np = require_numpy()
        predictions = np.asarray(raw, dtype=np.float64).reshape(-1)
        return reenter_with_prediction(frame, table, predictions, self.getPredictionCol())

    def write(self) -> MLWriter:
        """Return a booster-bytes writer (M8)."""
        return _XGBoostModelWriter(self, kind="XGBoostClassifierModel", classifier=True)

    def save(self, path: str) -> None:
        """Save via :meth:`write`."""
        self.write().save(path)

    @classmethod
    def read(cls) -> MLReader:
        """Return a booster-bytes reader."""
        return _XGBoostModelReader(cls, kind="XGBoostClassifierModel", classifier=True)

    @classmethod
    def load(cls, path: str) -> XGBoostClassifierModel:
        """Load a model saved by :meth:`save`."""
        return cls.read().load(path)

    def copy(self, extra: dict[Any, Any] | None = None) -> XGBoostClassifierModel:
        """Shallow copy; apply ``extra`` Param overrides."""
        that = XGBoostClassifierModel(
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


class _XGBoostModelWriter(MLWriter):
    """Write XGBoost*Model: metadata.json + fitted/params + booster.raw (atomic)."""

    def __init__(
        self,
        instance: XGBoostRegressorModel | XGBoostClassifierModel,
        *,
        kind: str,
        classifier: bool,
    ) -> None:
        """Bind model + kind."""
        super().__init__(instance)
        self._kind = kind
        self._classifier = classifier

    def saveImpl(self, path: str) -> None:
        """M1 envelope + booster-bytes blob via atomic M7 publish (never training rows)."""
        model = self.instance
        if model._booster is None:
            raise IllegalArgumentException(f"{self._kind}.save: no fitted booster")
        raw = _booster_raw_bytes(model._booster)
        fitted_payload = {
            "num_features": model.num_features,
            "num_rows": model.num_rows,
            "featuresCol": model.getFeaturesCol(),
            "predictionCol": model.getPredictionCol(),
            "fit_params": dict(model.fit_params),
            "booster_format": _XGB_BOOSTER_FORMAT,
            "booster_blob": _XGB_BOOSTER_BLOB_NAME,
            "classifier": self._classifier,
        }
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
            booster_blob_name=_XGB_BOOSTER_BLOB_NAME,
            booster_bytes=raw,
            library_name=_XGB_LIBRARY_NAME,
            library_version=_xgboost_version(),
            extra_metadata={"booster_format": _XGB_BOOSTER_FORMAT},
        )


class _XGBoostModelReader(MLReader):
    """Load XGBoost*Model from booster-bytes envelope."""

    def __init__(
        self,
        cls: type[XGBoostRegressorModel] | type[XGBoostClassifierModel],
        *,
        kind: str,
        classifier: bool,
    ) -> None:
        """Bind the model class + kind."""
        self._cls = cls
        self._kind = kind
        self._classifier = classifier

    def load(self, path: str) -> XGBoostRegressorModel | XGBoostClassifierModel:
        """Read metadata + fitted params + booster.raw; restore predict-capable model."""
        metadata, fitted, raw = load_ext_model_envelope(
            path,
            expected_kind=self._kind,
            library_name=_XGB_LIBRARY_NAME,
            current_library_version=_xgboost_version(),
            default_blob_name=_XGB_BOOSTER_BLOB_NAME,
            expected_classifier=self._classifier,
        )
        fit_params = dict(fitted.get("fit_params") or {})
        booster = _load_xgb_estimator_from_raw(
            raw, fit_params=fit_params, classifier=self._classifier
        )
        model = self._cls(
            booster=booster,
            featuresCol=fitted.get("featuresCol")
            or (metadata.get("params") or {}).get("featuresCol"),
            predictionCol=fitted.get("predictionCol")
            or (metadata.get("params") or {}).get("predictionCol"),
            num_features=int(fitted.get("num_features") or 0),
            num_rows=int(fitted.get("num_rows") or 0),
            fit_params=fit_params,
        )
        if metadata.get("uid"):
            model.uid = str(metadata["uid"])
        return model


__all__ = [
    "EXT_SAVE_UNSUPPORTED",
    "XGBoostClassifier",
    "XGBoostClassifierModel",
    "XGBoostRegressor",
    "XGBoostRegressorModel",
]
