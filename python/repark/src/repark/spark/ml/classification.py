"""Classification estimators — IRLS logistic (M3).

``LogisticRegression.fit`` multi-pass streams Arrow batches inside Rust (IRLS + Cholesky).
Python never trains; the model holds coefficients / intercept only.
"""

from __future__ import annotations

import contextlib
from typing import Any

from repark import _native
from repark.errors import IllegalArgumentException, UnsupportedOperationException

# === r23 QI1: idents ===
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


def _sql_float(value: float) -> str:
    """Render a float for SQL."""
    if value != value:
        return "CAST('NaN' AS DOUBLE)"
    if value == float("inf"):
        return "CAST('Infinity' AS DOUBLE)"
    if value == float("-inf"):
        return "CAST('-Infinity' AS DOUBLE)"
    return repr(float(value))


class LogisticRegression(
    HasFeaturesCol,
    HasLabelCol,
    HasPredictionCol,
    Estimator["LogisticRegressionModel"],
):
    """Binary logistic regression via native IRLS (multi-pass Rust stream)."""

    def __init__(
        self,
        *,
        featuresCol: str | None = None,  # noqa: N803
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        fitIntercept: bool | None = None,  # noqa: N803
        maxIter: int | None = None,  # noqa: N803
        tol: float | None = None,
        family: str | None = None,
    ) -> None:
        """Optional kwargs mirror Spark constructor names."""
        super().__init__()
        self.fitIntercept: Param[bool] = Param(
            self,
            "fitIntercept",
            "whether to fit an intercept term.",
            TypeConverters.toBoolean,
        )
        self.maxIter: Param[int] = Param(
            self,
            "maxIter",
            "maximum IRLS iterations.",
            TypeConverters.toInt,
        )
        self.tol: Param[float] = Param(
            self,
            "tol",
            "convergence tolerance on max |Δβ|.",
            TypeConverters.toFloat,
        )
        self.family: Param[str] = Param(
            self,
            "family",
            "family (only 'binomial' in M3).",
            TypeConverters.toString,
        )
        self._setDefault(fitIntercept=True, maxIter=100, tol=1e-6, family="binomial")
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if labelCol is not None:
            self.setLabelCol(labelCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)
        if fitIntercept is not None:
            self._set(fitIntercept=fitIntercept)
        if maxIter is not None:
            self._set(maxIter=maxIter)
        if tol is not None:
            self._set(tol=tol)
        if family is not None:
            self._set(family=family)

    def _fit(self, dataset: Any) -> LogisticRegressionModel:
        """Native multi-pass IRLS; Python never iterates training rows."""
        frame = _require_repark_dataframe(dataset, verb="LogisticRegression.fit")
        family = str(self.getOrDefault(self.family))
        if family not in {"binomial", "auto"}:
            raise UnsupportedOperationException(
                f"repark.ml LogisticRegression: family={family!r} unsupported "
                f"(only binomial / auto in M3)"
            )
        result = _native.fit_logistic_regression(
            frame._plan(),
            self.getFeaturesCol(),
            self.getLabelCol(),
            bool(self.getOrDefault(self.fitIntercept)),
            int(self.getOrDefault(self.maxIter)),
            float(self.getOrDefault(self.tol)),
        )
        model = LogisticRegressionModel(
            coefficients=list(result["coefficients"]),
            intercept=float(result["intercept"]),
            featuresCol=self.getFeaturesCol(),
            predictionCol=self.getPredictionCol(),
            fit_intercept=bool(result["fit_intercept"]),
            num_features=int(result["num_features"]),
            num_rows=int(result["num_rows"]),
            iterations=int(result["iterations"]),
            converged=bool(result["converged"]),
        )
        model.uid = self.uid
        return model


class LogisticRegressionModel(HasFeaturesCol, HasPredictionCol, Model):
    """Fitted logistic model — params only; plan-built hard 0/1 prediction."""

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
        iterations: int = 0,
        converged: bool = False,
    ) -> None:
        """Store coefficients / intercept."""
        super().__init__()
        self.coefficients = [float(value) for value in (coefficients or [])]
        self.intercept = float(intercept)
        self.fit_intercept = fit_intercept
        self.num_features = int(num_features or len(self.coefficients))
        self.num_rows = int(num_rows)
        self.iterations = int(iterations)
        self.converged = bool(converged)
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)

    def _transform(self, dataset: Any) -> Any:
        """Plan: prediction = CASE WHEN sigmoid(η) >= 0.5 THEN 1.0 ELSE 0.0 END."""
        frame = _require_repark_dataframe(dataset, verb="LogisticRegressionModel.transform")
        _refuse_output_collision(
            frame, self.getPredictionCol(), stage="LogisticRegressionModel.transform"
        )
        width = len(self.coefficients)
        if self.num_features != width:
            raise IllegalArgumentException(
                f"LogisticRegressionModel.transform: num_features={self.num_features} "
                f"desynced from coefficients length {width}"
            )
        _require_dense_feature_width(
            frame,
            self.getFeaturesCol(),
            width,
            verb="LogisticRegressionModel.transform",
        )
        features = _quote_ident(self.getFeaturesCol())
        prediction = _quote_ident(self.getPredictionCol())
        terms = [_sql_float(self.intercept)]
        for index, coef in enumerate(self.coefficients):
            terms.append(f"({_sql_float(coef)} * array_element({features}, {index}))")
        eta = " + ".join(terms)
        # Stable-ish sigmoid via 1/(1+exp(-eta)); threshold at 0.5 ≡ eta >= 0.
        expr = f"CASE WHEN ({eta}) >= 0.0 THEN 1.0 ELSE 0.0 END"
        view = scratch_view_name(frame._session, "__repark_logit_")
        frame.createOrReplaceTempView(view)
        sql = f"SELECT {view}.*, ({expr}) AS {prediction} FROM {view}"
        try:
            return frame._spawn(frame._session.sql(sql))
        finally:
            with contextlib.suppress(Exception):
                frame._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Params only."""
        return {
            "coefficients": list(self.coefficients),
            "intercept": self.intercept,
            "fit_intercept": self.fit_intercept,
            "num_features": self.num_features,
            "num_rows": self.num_rows,
            "iterations": self.iterations,
            "converged": self.converged,
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
    ) -> LogisticRegressionModel:
        """Rebuild from save."""
        payload = {**params, **(fitted_state or {})}
        return cls(
            coefficients=list(payload.get("coefficients") or []),
            intercept=float(payload.get("intercept", 0.0)),
            featuresCol=payload.get("featuresCol"),
            predictionCol=payload.get("predictionCol"),
            fit_intercept=bool(payload.get("fit_intercept", True)),
            num_features=int(payload.get("num_features") or 0),
            num_rows=int(payload.get("num_rows") or 0),
            iterations=int(payload.get("iterations") or 0),
            converged=bool(payload.get("converged", False)),
        )

    def copy(self, extra: dict[Any, Any] | None = None) -> LogisticRegressionModel:
        """Copy model params; apply ``extra`` so ``transform(df, params)`` works (C4-L-002)."""
        that = LogisticRegressionModel(
            coefficients=list(self.coefficients),
            intercept=self.intercept,
            featuresCol=self.getFeaturesCol(),
            predictionCol=self.getPredictionCol(),
            fit_intercept=self.fit_intercept,
            num_features=self.num_features,
            num_rows=self.num_rows,
            iterations=self.iterations,
            converged=self.converged,
        )
        that.uid = self.uid
        if extra:
            for param, value in extra.items():
                name = param.name if hasattr(param, "name") else str(param)
                that._set(**{name: value})
        return that


__all__ = [
    "LogisticRegression",
    "LogisticRegressionModel",
]
