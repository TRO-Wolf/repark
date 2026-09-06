# map — docs/examples/ml/

## Purpose

Worked examples for the `repark.spark.ml` module surface (`pyspark.ml` camelCase:
vectors, params/mixins, pipeline, tuning, persistence). Examples construct the
session as `repark = ReparkSession.builder…`; see [../map.md](../map.md). Every
asserted value was measured on live PySpark 4.1.2 first (ANSI on, UTC). Mixin
classes are taught only through Tokenizer / VectorAssembler / OneHotEncoder /
LinearRegression. UnaryTransformer is taught as a plan-built `_transform`.
Diverged arms stay in §7 as EX-ML-1..9; the names stay covered by the
Spark-equal arms.

## Contents

- [vectors.py](vectors.py) — DenseVector, SparseVector, Vector, VectorUDT,
  Vectors.
- [params.py](params.py) — Param, Params, TypeConverters, and the
  HasInputCol / HasOutputCol / HasInputCols / HasOutputCols / HasHandleInvalid /
  HasFeaturesCol / HasLabelCol / HasPredictionCol mixins, read off concrete
  stages.
- [pipeline.py](pipeline.py) — Pipeline, PipelineModel, Estimator, Model,
  Transformer, a plan-built UnaryTransformer, Identifiable.
- [tuning.py](tuning.py) — ParamGridBuilder, CrossValidator,
  CrossValidatorModel.
- [persistence.py](persistence.py) — MLReadable, MLWritable (repark-ml round-trip).

## Pointers

- Up: [../map.md](../map.md)
- Registry: [../../spark-sql-iceberg-parity.md](../../spark-sql-iceberg-parity.md) §7
  EX-ML-1..9
- Pins: [../../../python/repark/tests/test_examples_ml.py](../../../python/repark/tests/test_examples_ml.py)

pins: ex-27-ml/C-001, C-002, C-003, C-004, C-005, C-006, C-007
