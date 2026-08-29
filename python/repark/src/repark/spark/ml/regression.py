"""Linear regression backed by native streaming ordinary least squares."""

from __future__ import annotations

import contextlib
from typing import Any

from repark import _native
from repark.errors import IllegalArgumentException, UnsupportedOperationException
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark._temp_views import scratch_view_name
from repark.spark.ml.base import (
    Estimator,
    Model,
    _refuse_output_collision,
    _require_dense_feature_width,
    _require_repark_dataframe,
)
from repark.spark.ml.param import (
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Param,
    TypeConverters,
)

SOLVER_DIVERGENCE = (
    "repark LinearRegression uses streaming normal equations + Cholesky; Spark MLlib uses "
    "normal equations / L-BFGS / OWL-QN depending on params. Singular / ill-conditioned designs "
    "fail loud here (no pinv / silent ridge)."
)
ELASTIC_NET_SEED = "elasticNetParam != 0 → M4 (coordinate descent / elastic net not in M3)"
STANDARDIZATION_NOTE = (
    "standardization=True unsupported; raw features only. Fit StandardScaler upstream."
)


def _sql_float(value: float) -> str:
    """Render a finite or special float as a SQL literal."""
    if value != value:
        return "CAST('NaN' AS DOUBLE)"
    if value == float("inf"):
        return "CAST('Infinity' AS DOUBLE)"
    if value == float("-inf"):
        return "CAST('-Infinity' AS DOUBLE)"
    return repr(float(value))


class LinearRegression(
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Estimator["LinearRegressionModel"],
):
    """Fit ordinary least squares with a native streaming solver.

    Defaults are ``fitIntercept=True``, ``regParam=0``, ``elasticNetParam=0``, and
    ``standardization=False``. Fit refuses nonzero regularization or elastic net and
    refuses ``standardization=True``.
    """

    def __init__(
        self,
        *,
        featuresCol: str | None = None,  # noqa: N803
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        fitIntercept: bool | None = None,  # noqa: N803
        elasticNetParam: float | None = None,  # noqa: N803
        regParam: float | None = None,  # noqa: N803
        standardization: bool | None = None,
        maxIter: int | None = None,  # noqa: N803
        tol: float | None = None,
    ) -> None:
        """Initialize parameters with Spark-shaped defaults and supported-fit guards."""
        super().__init__()
        self.fitIntercept: Param[bool] = Param(
            self,
            "fitIntercept",
            "whether to fit an intercept term.",
            TypeConverters.toBoolean,
        )
        self.elasticNetParam: Param[float] = Param(
            self,
            "elasticNetParam",
            "elastic net mixing (0 = ridge/L2 path only; nonzero unsupported in M3).",
            TypeConverters.toFloat,
        )
        self.regParam: Param[float] = Param(
            self,
            "regParam",
            "regularization parameter (must be 0 in M3 pure OLS).",
            TypeConverters.toFloat,
        )
        self.standardization: Param[bool] = Param(
            self,
            "standardization",
            "whether to standardize features before fit (unsupported if True).",
            TypeConverters.toBoolean,
        )
        self.maxIter: Param[int] = Param(
            self,
            "maxIter",
            "max iterations (unused for closed-form OLS; kept for API shape).",
            TypeConverters.toInt,
        )
        self.tol: Param[float] = Param(
            self,
            "tol",
            "convergence tolerance (unused for closed-form OLS; kept for API shape).",
            TypeConverters.toFloat,
        )
        self._setDefault(
            fitIntercept=True,
            elasticNetParam=0.0,
            regParam=0.0,
            standardization=False,
            maxIter=100,
            tol=1e-6,
        )
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if labelCol is not None:
            self.setLabelCol(labelCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)
        if fitIntercept is not None:
            self._set(fitIntercept=fitIntercept)
        if elasticNetParam is not None:
            self._set(elasticNetParam=elasticNetParam)
        if regParam is not None:
            self._set(regParam=regParam)
        if standardization is not None:
            self._set(standardization=standardization)
        if maxIter is not None:
            self._set(maxIter=maxIter)
        if tol is not None:
            self._set(tol=tol)

    def setFitIntercept(self, value: bool) -> LinearRegression:
        """Set whether to fit an intercept."""
        return self._set(fitIntercept=value)

    def getFitIntercept(self) -> bool:
        """Return whether to fit an intercept."""
        return bool(self.getOrDefault(self.fitIntercept))

    def setElasticNetParam(self, value: float) -> LinearRegression:
        """Set the elastic-net mixing parameter."""
        return self._set(elasticNetParam=value)

    def getElasticNetParam(self) -> float:
        """Return the elastic-net mixing parameter."""
        return float(self.getOrDefault(self.elasticNetParam))

    def setRegParam(self, value: float) -> LinearRegression:
        """Set the regularization parameter."""
        return self._set(regParam=value)

    def getRegParam(self) -> float:
        """Return the regularization parameter."""
        return float(self.getOrDefault(self.regParam))

    def setStandardization(self, value: bool) -> LinearRegression:
        """Set whether to standardize features."""
        return self._set(standardization=value)

    def getStandardization(self) -> bool:
        """Return whether features are standardized."""
        return bool(self.getOrDefault(self.standardization))

    def _fit(self, dataset: Any) -> LinearRegressionModel:
        """Fit native streaming OLS. Refuse unsupported regularization and standardization."""
        frame = _require_repark_dataframe(dataset, verb="LinearRegression.fit")
        reg = float(self.getOrDefault(self.regParam))
        if abs(reg) > 1e-15:
            raise UnsupportedOperationException(
                f"repark.ml LinearRegression: regParam={reg} unsupported in M3 "
                f"(pure OLS only; regParam must be 0). {ELASTIC_NET_SEED}"
            )
        elastic = float(self.getOrDefault(self.elasticNetParam))
        standardization = bool(self.getOrDefault(self.standardization))
        fit_intercept = bool(self.getOrDefault(self.fitIntercept))
        features_col = self.getFeaturesCol()
        label_col = self.getLabelCol()
        result = _native.fit_linear_regression(
            frame._plan(),
            features_col,
            label_col,
            fit_intercept,
            elastic,
            standardization,
        )
        model = LinearRegressionModel(
            coefficients=list(result["coefficients"]),
            intercept=float(result["intercept"]),
            featuresCol=features_col,
            predictionCol=self.getPredictionCol(),
            fit_intercept=bool(result["fit_intercept"]),
            num_features=int(result["num_features"]),
            num_rows=int(result["num_rows"]),
        )
        model.uid = self.uid
        return model


class LinearRegressionModel(HasFeaturesCol, HasPredictionCol, Model):
    """Fitted OLS model that adds a plan-built prediction."""

    def __init__(
        self,
        *,
        coefficients: list[float] | None = None,
        intercept: float = 0.0,
        featuresCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        fit_intercept: bool = True,
        num_features: int = 0,
        num_rows: int = 0,
    ) -> None:
        """Initialize fitted coefficients, intercept, and metadata."""
        super().__init__()
        self.coefficients = [float(value) for value in (coefficients or [])]
        self.intercept = float(intercept)
        self.fit_intercept = fit_intercept
        self.num_features = int(num_features or len(self.coefficients))
        self.num_rows = int(num_rows)
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)

    def _transform(self, dataset: Any) -> Any:
        """Add a prediction after validating feature width.

        Coefficients map to zero-based ``array_element`` feature indices.
        """
        frame = _require_repark_dataframe(dataset, verb="LinearRegressionModel.transform")
        _refuse_output_collision(
            frame, self.getPredictionCol(), stage="LinearRegressionModel.transform"
        )
        width = len(self.coefficients)
        if self.num_features != width:
            raise IllegalArgumentException(
                f"LinearRegressionModel.transform: num_features={self.num_features} "
                f"desynced from coefficients length {width}"
            )
        _require_dense_feature_width(
            frame,
            self.getFeaturesCol(),
            width,
            verb="LinearRegressionModel.transform",
        )
        features = _quote_ident(self.getFeaturesCol())
        prediction = _quote_ident(self.getPredictionCol())
        terms = [_sql_float(self.intercept)]
        for index, coef in enumerate(self.coefficients):
            terms.append(f"({_sql_float(coef)} * array_element({features}, {index}))")
        expr = " + ".join(terms) if terms else "CAST(0.0 AS DOUBLE)"
        view = scratch_view_name(frame._session, "__repark_lr_")
        frame.createOrReplaceTempView(view)
        sql = f"SELECT {view}.*, ({expr}) AS {prediction} FROM {view}"
        try:
            return frame._spawn(frame._session.sql(sql))
        finally:
            with contextlib.suppress(Exception):
                frame._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Return fitted parameters without training rows."""
        return {
            "coefficients": list(self.coefficients),
            "intercept": self.intercept,
            "fit_intercept": self.fit_intercept,
            "num_features": self.num_features,
            "num_rows": self.num_rows,
            "featuresCol": self.getFeaturesCol(),
            "predictionCol": self.getPredictionCol(),
        }

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> LinearRegressionModel:
        """Rebuild a model from persisted parameters and fitted state."""
        payload = {**params, **(fitted_state or {})}
        return cls(
            coefficients=list(payload.get("coefficients") or []),
            intercept=float(payload.get("intercept", 0.0)),
            featuresCol=payload.get("featuresCol"),
            predictionCol=payload.get("predictionCol"),
            fit_intercept=bool(payload.get("fit_intercept", True)),
            num_features=int(payload.get("num_features") or 0),
            num_rows=int(payload.get("num_rows") or 0),
        )

    def copy(self, extra: dict[Any, Any] | None = None) -> LinearRegressionModel:
        """Deep-copy model parameters and apply optional overrides."""
        that = LinearRegressionModel(
            coefficients=list(self.coefficients),
            intercept=self.intercept,
            featuresCol=self.getFeaturesCol(),
            predictionCol=self.getPredictionCol(),
            fit_intercept=self.fit_intercept,
            num_features=self.num_features,
            num_rows=self.num_rows,
        )
        that.uid = self.uid
        if extra:
            for param, value in extra.items():
                name = param.name if hasattr(param, "name") else str(param)
                that._set(**{name: value})
        return that


class LinearRegressionSummary:
    """Summary proxy that refuses metrics not computed by the model."""

    def __init__(self, model: LinearRegressionModel) -> None:
        """Bind a fitted model without materializing residuals."""
        self.model = model

    def __getattr__(self, name: str) -> Any:
        """Reject summary fields that are not computed."""
        raise UnsupportedOperationException(
            f"LinearRegressionSummary.{name} is not computed in M3 "
            f"(minimal summaries only; use repark.ml.evaluation.RegressionEvaluator)"
        )


__all__ = [
    "ELASTIC_NET_SEED",
    "SOLVER_DIVERGENCE",
    "STANDARDIZATION_NOTE",
    "LinearRegression",
    "LinearRegressionModel",
    "LinearRegressionSummary",
]
