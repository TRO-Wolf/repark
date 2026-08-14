"""``repark.ml`` — near-drop-in PySpark ML surface (pipeline + feature + estimators).

Import paths mirror ``pyspark.ml`` under the ``repark.ml`` namespace only — there is
**no** ``pyspark.ml`` alias shim tonight (C1 patch-map informs that decision later)::

    from repark.spark.ml import Pipeline, PipelineModel, Estimator, Transformer, Model
    from repark.spark.ml.linalg import Vectors, DenseVector, SparseVector, VectorUDT
    from repark.spark.ml.feature import VectorAssembler  # M2
    from repark.spark.ml.regression import LinearRegression  # M3

Design decisions live in the package modules themselves: vector Arrow layout in
:mod:`repark.ml.linalg`, the fit/transform Rust rule in :mod:`repark.ml.base`, and
estimator divergences next to their pins (e.g. :mod:`repark.ml.regression`).
"""

from __future__ import annotations

from repark.spark.ml.base import Estimator, Model, Transformer, UnaryTransformer
from repark.spark.ml.linalg import DenseVector, SparseVector, Vector, Vectors, VectorUDT
from repark.spark.ml.param import (
    HasFeaturesCol,
    HasHandleInvalid,
    HasInputCol,
    HasInputCols,
    HasLabelCol,
    HasOutputCol,
    HasOutputCols,
    HasPredictionCol,
    Param,
    Params,
    TypeConverters,
)
from repark.spark.ml.pipeline import Pipeline, PipelineModel
from repark.spark.ml.tuning import CrossValidator, CrossValidatorModel, ParamGridBuilder
from repark.spark.ml.util import Identifiable, MLReadable, MLWritable

__all__ = [
    "CrossValidator",
    "CrossValidatorModel",
    "DenseVector",
    "Estimator",
    "HasFeaturesCol",
    "HasHandleInvalid",
    "HasInputCol",
    "HasInputCols",
    "HasLabelCol",
    "HasOutputCol",
    "HasOutputCols",
    "HasPredictionCol",
    "Identifiable",
    "MLReadable",
    "MLWritable",
    "Model",
    "Param",
    "ParamGridBuilder",
    "Params",
    "Pipeline",
    "PipelineModel",
    "SparseVector",
    "Transformer",
    "TypeConverters",
    "UnaryTransformer",
    "Vector",
    "VectorUDT",
    "Vectors",
]
