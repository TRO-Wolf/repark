"""Param grids and cross-validation over a tiny deterministic OLS fixture.

pins: ex-27-ml/C-005
"""

from __future__ import annotations

from repark.spark import ReparkSession, ml
from repark.spark.ml.evaluation import RegressionEvaluator
from repark.spark.ml.feature import VectorAssembler
from repark.spark.ml.regression import LinearRegression

COVERS: list[str] = [
    "ml.CrossValidator",
    "ml.CrossValidatorModel",
    "ml.ParamGridBuilder",
]

ROWS = [(1.0, 5.0), (2.0, 8.0), (3.0, 11.0), (4.0, 14.0)]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def expect_close(label: str, got: float, wanted: float) -> None:
    scale = max(1.0, abs(wanted))
    if abs(got - wanted) > 1e-6 * scale:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Build a two-point intercept grid and select the Spark-equal OLS fit."""
    toy = LinearRegression()
    grid = ml.ParamGridBuilder().addGrid(toy.fitIntercept, [True, False]).build()
    expect("ParamGridBuilder.len", len(grid), 2)
    expect(
        "ParamGridBuilder.values",
        sorted(bool(param_map[toy.fitIntercept]) for param_map in grid),
        [False, True],
    )
    base = ml.ParamGridBuilder().baseOn({toy.maxIter: 20}).build()
    expect("ParamGridBuilder.baseOn.dict", base[0][toy.maxIter], 20)
    expect("ParamGridBuilder.empty.len", len(ml.ParamGridBuilder().build()), 1)

    validator = ml.CrossValidator()
    expect("CrossValidator.isinstance.Estimator", isinstance(validator, ml.Estimator), True)
    expect("CrossValidator.getNumFolds.default", validator.getNumFolds(), 3)
    expect("CrossValidator.getParallelism.default", validator.getParallelism(), 1)

    repark = ReparkSession.builder.appName("ex-ml-tuning").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(ROWS, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(frame)
        estimator = LinearRegression(featuresCol="features", labelCol="label")
        search = ml.ParamGridBuilder().addGrid(estimator.fitIntercept, [True, False]).build()
        evaluator = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="rmse",
        )
        cross = ml.CrossValidator(
            estimator=estimator,
            estimatorParamMaps=search,
            evaluator=evaluator,
            numFolds=2,
            seed=0,
            parallelism=1,
        )
        expect("CrossValidator.getNumFolds.set", cross.getNumFolds(), 2)
        fitted = cross.fit(assembled)
        expect("CrossValidatorModel.class", type(fitted).__name__, "CrossValidatorModel")
        expect(
            "CrossValidatorModel.isinstance.CrossValidatorModel",
            isinstance(fitted, ml.CrossValidatorModel),
            True,
        )
        expect("CrossValidatorModel.isinstance.Model", isinstance(fitted, ml.Model), True)
        expect("CrossValidatorModel.avgMetrics.len", len(fitted.avgMetrics), 2)
        best = fitted.bestModel
        expect_close("best.coefficients[0]", float(best.coefficients[0]), 3.0)
        expect_close("best.intercept", float(best.intercept), 2.0)
        predicted = sorted(
            (float(row["x"]), float(row["label"]), float(row["prediction"]))
            for row in fitted.transform(assembled).collect()
        )
        expect("CrossValidatorModel.transform.len", len(predicted), 4)
        for x_value, label, prediction in predicted:
            expect_close(f"cv.prediction.x={x_value}", prediction, label)
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
