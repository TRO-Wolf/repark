"""Regression estimators — native Rust OLS (M3).

``LinearRegression.fit`` streams Arrow batches inside Rust (``repark-ml`` Cholesky OLS).
Python never trains; the model holds coefficients / intercept only. ``transform`` is
plan-built (dot product + intercept).
"""

from __future__ import annotations

import contextlib
import uuid
from typing import Any

from repark import _native

# === r23 QI1: idents ===
from repark._idents import quote_ident as _quote_ident
from repark.errors import IllegalArgumentException, UnsupportedOperationException
from repark.ml.base import (
    Estimator,
    Model,
    _refuse_output_collision,
    _require_dense_feature_width,
    _require_repark_dataframe,
)
from repark.ml.param import (
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Param,
    TypeConverters,
)

# Divergence pins (oracle strings; this module is the home)
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
    """Render a float for SQL (Infinity-safe)."""
    if value != value:  # NaN
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
    """Ordinary least squares (native Rust stream + Cholesky).

    Params mirror Spark shape. Defaults: ``fitIntercept=True``, ``elasticNetParam=0``,
    ``standardization=False`` (raw features). ``regParam`` is accepted only as 0.
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
        """Optional kwargs mirror Spark constructor names."""
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
        """Set fitIntercept."""
        return self._set(fitIntercept=value)

    def getFitIntercept(self) -> bool:
        """Get fitIntercept."""
        return bool(self.getOrDefault(self.fitIntercept))

    def setElasticNetParam(self, value: float) -> LinearRegression:
        """Set elasticNetParam."""
        return self._set(elasticNetParam=value)

    def getElasticNetParam(self) -> float:
        """Get elasticNetParam."""
        return float(self.getOrDefault(self.elasticNetParam))

    def setRegParam(self, value: float) -> LinearRegression:
        """Set regParam."""
        return self._set(regParam=value)

    def getRegParam(self) -> float:
        """Get regParam."""
        return float(self.getOrDefault(self.regParam))

    def setStandardization(self, value: bool) -> LinearRegression:
        """Set standardization."""
        return self._set(standardization=value)

    def getStandardization(self) -> bool:
        """Get standardization."""
        return bool(self.getOrDefault(self.standardization))

    def _fit(self, dataset: Any) -> LinearRegressionModel:
        """Native Rust streaming OLS; Python never iterates training rows."""
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
        # Native stream fit — no numpy, no Python row loops.
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
    """Fitted OLS model — params only; plan-built transform."""

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
        """Store coefficients / intercept (never training rows)."""
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
        """Plan: prediction = intercept + sum_i coef_i * array_element(features, i)."""
        frame = _require_repark_dataframe(dataset, verb="LinearRegressionModel.transform")
        _refuse_output_collision(
            frame, self.getPredictionCol(), stage="LinearRegressionModel.transform"
        )
        # Coefficients are the source of truth for width; refuse desynced num_features.
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
            # DataFusion array_element is 0-based (M2 ledger).
            terms.append(f"({_sql_float(coef)} * array_element({features}, {index}))")
        expr = " + ".join(terms) if terms else "CAST(0.0 AS DOUBLE)"
        view = f"__repark_lr_{uuid.uuid4().hex[:12]}"
        frame.createOrReplaceTempView(view)
        sql = f"SELECT {view}.*, ({expr}) AS {prediction} FROM {view}"
        try:
            return frame._spawn(frame._session.sql(sql))
        finally:
            with contextlib.suppress(Exception):
                frame._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Params only — never training rows."""
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
        """Rebuild from repark-ml persistence."""
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
        """Copy model params; apply ``extra`` so ``transform(df, params)`` works (C4-L-002)."""
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
    """Minimal summary placeholder — absent metrics are loud-disclosed."""

    def __init__(self, model: LinearRegressionModel) -> None:
        """Bind a fitted model (no residual materialization)."""
        self.model = model

    def __getattr__(self, name: str) -> Any:
        """Loud-disclose Spark summary fields we do not compute in M3."""
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
