"""Clustering estimators — Lloyd k-means (M3).

Default Spark ``initMode`` is k-means|| — we **refuse** it loud and require
``initMode="random"`` (no fake k-means||).
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
from repark.spark.ml.param import HasFeaturesCol, HasPredictionCol, Param, TypeConverters


def _sql_float(value: float) -> str:
    """Render a float for SQL."""
    if value != value:
        return "CAST('NaN' AS DOUBLE)"
    if value == float("inf"):
        return "CAST('Infinity' AS DOUBLE)"
    if value == float("-inf"):
        return "CAST('-Infinity' AS DOUBLE)"
    return repr(float(value))


class KMeans(HasFeaturesCol, HasPredictionCol, Estimator["KMeansModel"]):
    """Lloyd k-means over streamed batches (initMode=random only)."""

    def __init__(
        self,
        *,
        featuresCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        k: int | None = None,
        maxIter: int | None = None,  # noqa: N803
        seed: int | None = None,
        initMode: str | None = None,  # noqa: N803
    ) -> None:
        """Optional kwargs. Default initMode matches Spark (k-means||) and **errors on fit**."""
        super().__init__()
        self.k: Param[int] = Param(self, "k", "number of clusters.", TypeConverters.toInt)
        self.maxIter: Param[int] = Param(
            self, "maxIter", "maximum Lloyd iterations.", TypeConverters.toInt
        )
        self.seed: Param[int] = Param(self, "seed", "random seed.", TypeConverters.toInt)
        self.initMode: Param[str] = Param(
            self,
            "initMode",
            'initialization mode: set "random" explicitly (default k-means|| is refused).',
            TypeConverters.toString,
        )
        # Match Spark default initMode so accidental use fails loud with guidance.
        self._setDefault(k=2, maxIter=20, seed=42, initMode="k-means||")
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)
        if k is not None:
            self._set(k=k)
        if maxIter is not None:
            self._set(maxIter=maxIter)
        if seed is not None:
            self._set(seed=seed)
        if initMode is not None:
            self._set(initMode=initMode)

    def setInitMode(self, value: str) -> KMeans:
        """Set initMode (must be ``random`` for M3)."""
        return self._set(initMode=value)

    def getInitMode(self) -> str:
        """Get initMode."""
        return str(self.getOrDefault(self.initMode))

    def setK(self, value: int) -> KMeans:
        """Set k."""
        return self._set(k=value)

    def getK(self) -> int:
        """Get k."""
        return int(self.getOrDefault(self.k))

    def _fit(self, dataset: Any) -> KMeansModel:
        """Native Lloyd; requires initMode=random."""
        frame = _require_repark_dataframe(dataset, verb="KMeans.fit")
        init_mode = str(self.getOrDefault(self.initMode))
        result = _native.fit_kmeans(
            frame._plan(),
            self.getFeaturesCol(),
            int(self.getOrDefault(self.k)),
            int(self.getOrDefault(self.maxIter)),
            int(self.getOrDefault(self.seed)),
            init_mode,
        )
        model = KMeansModel(
            centers=[list(map(float, center)) for center in result["centers"]],
            featuresCol=self.getFeaturesCol(),
            predictionCol=self.getPredictionCol(),
            k=int(result["k"]),
            num_features=int(result["num_features"]),
            num_rows=int(result["num_rows"]),
            iterations=int(result["iterations"]),
        )
        model.uid = self.uid
        return model


class KMeansModel(HasFeaturesCol, HasPredictionCol, Model):
    """Fitted k-means — centers only; nearest-center plan transform."""

    def __init__(
        self,
        *,
        centers: list[list[float]] | None = None,
        featuresCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        k: int = 0,
        num_features: int = 0,
        num_rows: int = 0,
        iterations: int = 0,
    ) -> None:
        """Store centers (params only)."""
        super().__init__()
        self.centers = [[float(v) for v in center] for center in (centers or [])]
        self.k = int(k or len(self.centers))
        self.num_features = int(num_features)
        self.num_rows = int(num_rows)
        self.iterations = int(iterations)
        if featuresCol is not None:
            self.setFeaturesCol(featuresCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)

    def clusterCenters(self) -> list[list[float]]:
        """Spark-shaped accessor for centers."""
        return [list(center) for center in self.centers]

    def _transform(self, dataset: Any) -> Any:
        """Plan: argmin_k sum_i (array_element(features,i) - center_k[i])^2."""
        frame = _require_repark_dataframe(dataset, verb="KMeansModel.transform")
        if not self.centers:
            raise UnsupportedOperationException("KMeansModel has no centers")
        _refuse_output_collision(frame, self.getPredictionCol(), stage="KMeansModel.transform")
        width = len(self.centers[0])
        if any(len(center) != width for center in self.centers):
            raise IllegalArgumentException(
                "KMeansModel.transform: centers have inconsistent feature widths"
            )
        if self.num_features != 0 and self.num_features != width:
            raise IllegalArgumentException(
                f"KMeansModel.transform: num_features={self.num_features} "
                f"desynced from center width {width}"
            )
        _require_dense_feature_width(
            frame,
            self.getFeaturesCol(),
            width,
            verb="KMeansModel.transform",
        )
        features = _quote_ident(self.getFeaturesCol())
        prediction = _quote_ident(self.getPredictionCol())
        # Build CASE over pairwise distance comparisons (lowest index wins ties).
        dist_exprs: list[str] = []
        for center in self.centers:
            parts = []
            for index, value in enumerate(center):
                parts.append(f"power(array_element({features}, {index}) - {_sql_float(value)}, 2)")
            dist_exprs.append("(" + " + ".join(parts) + ")" if parts else "0.0")
        # Nested CASE: for each cluster index, check if it has the min distance.
        case_arms: list[str] = []
        for index, dist in enumerate(dist_exprs):
            others_ge = " AND ".join(
                f"({dist}) <= ({other})" for j, other in enumerate(dist_exprs) if j != index
            )
            if not others_ge:
                others_ge = "TRUE"
            # Prefer lower index on ties: also require strictly < previous clusters.
            stricter = " AND ".join(
                f"({dist}) < ({other})" for j, other in enumerate(dist_exprs) if j < index
            )
            cond = others_ge if not stricter else f"({others_ge}) AND ({stricter})"
            case_arms.append(f"WHEN {cond} THEN CAST({index} AS DOUBLE)")
        case_sql = "CASE " + " ".join(case_arms) + " ELSE CAST(0 AS DOUBLE) END"
        view = scratch_view_name(frame._session, "__repark_km_")
        frame.createOrReplaceTempView(view)
        sql = f"SELECT {view}.*, ({case_sql}) AS {prediction} FROM {view}"
        try:
            return frame._spawn(frame._session.sql(sql))
        finally:
            with contextlib.suppress(Exception):
                frame._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Centers only — never training rows."""
        return {
            "centers": [list(center) for center in self.centers],
            "k": self.k,
            "num_features": self.num_features,
            "num_rows": self.num_rows,
            "iterations": self.iterations,
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
    ) -> KMeansModel:
        """Rebuild from save."""
        payload = {**params, **(fitted_state or {})}
        return cls(
            centers=list(payload.get("centers") or []),
            featuresCol=payload.get("featuresCol"),
            predictionCol=payload.get("predictionCol"),
            k=int(payload.get("k") or 0),
            num_features=int(payload.get("num_features") or 0),
            num_rows=int(payload.get("num_rows") or 0),
            iterations=int(payload.get("iterations") or 0),
        )

    def copy(self, extra: dict[Any, Any] | None = None) -> KMeansModel:
        """Copy model params (deep-copy centers — no aliasing)."""
        that = KMeansModel(
            centers=[list(center) for center in self.centers],
            featuresCol=self.getFeaturesCol(),
            predictionCol=self.getPredictionCol(),
            k=self.k,
            num_features=self.num_features,
            num_rows=self.num_rows,
            iterations=self.iterations,
        )
        that.uid = self.uid
        return that


__all__ = [
    "KMeans",
    "KMeansModel",
]
