"""CrossValidator fold-degeneracy fallback pins."""

from __future__ import annotations

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException
from repark.spark.ml.evaluation import RegressionEvaluator
from repark.spark.ml.feature import VectorAssembler
from repark.spark.ml.regression import LinearRegression
from repark.spark.ml.tuning import CrossValidator, ParamGridBuilder


def _session() -> ReparkSession:
    return ReparkSession.builder.appName("ml-cv-folds-test").getOrCreate()


def test_cross_validator_degenerate_hash_falls_back_to_row_number() -> None:
    """A degenerate hash assignment must fall back to row_number folds (WO-8).

    The 4-row/2-fold/seed-0 fixture hashes to 3/1 folds, which would train on one row;
    the guard must answer the balanced row_number assignment and fit green.
    """
    spark = _session()
    try:
        rows = [(1.0, 5.0), (2.0, 8.0), (3.0, 11.0), (4.0, 14.0)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        lr = LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction")
        grid = ParamGridBuilder().addGrid(lr.fitIntercept, [True, False]).build()
        evaluator = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="rmse"
        )
        cv = CrossValidator(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=2,
            seed=0,
        )
        folded, fold_col, _mat_view = cv._with_fold_column(assembled, 2, 0)
        by_x = {row["x"]: int(row[fold_col]) for row in folded.collect()}
        assert [by_x[x] for x in (1.0, 2.0, 3.0, 4.0)] == [0, 1, 0, 1]
        model = cv.fit(assembled)
        assert model.bestModel is not None
        assert len(model.avgMetrics) == len(grid)
        assert bool(model.bestModel.fit_intercept) is True
    finally:
        spark.stop()


def test_cross_validator_singular_data_still_refuses_cholesky() -> None:
    """Genuinely singular data must still raise the Cholesky refusal (WO-8 near miss).

    The fold-degeneracy guard must not swallow the native solver refusal: a constant
    feature stays singular under every fold assignment.
    """
    spark = _session()
    try:
        rows = [(1.0, float(label)) for label in (2.0, 3.0, 4.0, 5.0)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        lr = LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction")
        grid = ParamGridBuilder().addGrid(lr.fitIntercept, [True]).build()
        evaluator = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="rmse"
        )
        cv = CrossValidator(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=2,
            seed=0,
        )
        with pytest.raises(IllegalArgumentException, match="singular or ill-conditioned"):
            cv.fit(assembled)
    finally:
        spark.stop()


def test_cross_validator_nondegenerate_hash_keeps_hash_path() -> None:
    """Non-degenerate hash folds must keep the hash assignment bit-for-bit (WO-8 near miss).

    Direct hash equality pins the exact Spark-equal assignment; seed-sensitivity alone
    would also pass under a seed-honoring non-hash path. The fixture differs from the
    row_number assignment, so the pin tells the arms apart.
    """
    spark = _session()
    try:
        rows = [(float(x), 1.0 + 2.0 * float(x) + (0.01 * (x % 3))) for x in range(24)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        cv = CrossValidator(numFolds=3, seed=42)
        folded, fold_col, _mat_view = cv._with_fold_column(assembled, 3, 42)
        actual = {row["x"]: int(row[fold_col]) for row in folded.collect()}
        assembled.createOrReplaceTempView("wo8_hash_path_src")
        hashed = {
            row["x"]: int(row["fold"])
            for row in spark.sql(
                "SELECT *, MOD(abs(hash(concat(CAST(ROW_NUMBER() OVER (ORDER BY 1) "
                "AS VARCHAR), '_', CAST(42 AS VARCHAR)))), 3) AS fold "
                "FROM wo8_hash_path_src"
            ).collect()
        }
        fallback = {
            row["x"]: int(row["fold"])
            for row in spark.sql(
                "SELECT *, MOD(ROW_NUMBER() OVER (ORDER BY 1) - 1, 3) AS fold "
                "FROM wo8_hash_path_src"
            ).collect()
        }
        assert hashed != fallback
        assert actual == hashed
    finally:
        spark.stop()
