"""``repark.ml`` — near-drop-in PySpark ML surface (pipeline + feature + estimators).

Import paths mirror ``pyspark.ml`` under the ``repark.ml`` namespace only — there is
**no** ``pyspark.ml`` alias shim tonight (C1 patch-map informs that decision later)::

    from repark.ml import Pipeline, PipelineModel, Estimator, Transformer, Model
    from repark.ml.linalg import Vectors, DenseVector, SparseVector, VectorUDT
    from repark.ml.feature import VectorAssembler  # M2
    from repark.ml.regression import LinearRegression  # M3

Design decisions (vector types + parity bar + fit Rust rule) live in ``docs/ml-design.md``.
"""

from __future__ import annotations

from repark.ml.base import Estimator, Model, Transformer, UnaryTransformer
from repark.ml.linalg import DenseVector, SparseVector, Vector, Vectors, VectorUDT
from repark.ml.param import (
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
from repark.ml.pipeline import Pipeline, PipelineModel
from repark.ml.tuning import CrossValidator, CrossValidatorModel, ParamGridBuilder
from repark.ml.util import Identifiable, MLReadable, MLWritable

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
