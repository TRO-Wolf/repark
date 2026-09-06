"""Pipeline stage order, fitted OLS outputs, and a UnaryTransformer subclass.

pins: ex-27-ml/C-004
"""

from __future__ import annotations

from typing import Any

from repark.spark import ReparkSession, ml
from repark.spark.functions import col
from repark.spark.ml.feature import VectorAssembler
from repark.spark.ml.regression import LinearRegression

COVERS: list[str] = [
    "ml.Estimator",
    "ml.Identifiable",
    "ml.Model",
    "ml.Pipeline",
    "ml.PipelineModel",
    "ml.Transformer",
    "ml.UnaryTransformer",
]

ROWS = [(1.0, 5.0), (2.0, 8.0), (3.0, 11.0), (4.0, 14.0)]


class ShiftOne(ml.UnaryTransformer):
    def createTransformFunc(self) -> Any:  # noqa: N802
        return lambda value: float(value) + 1.0

    def _transform(self, dataset: Any) -> Any:
        return dataset.withColumn(self.getOutputCol(), col(self.getInputCol()) + 1.0)


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def expect_close(label: str, got: float, wanted: float) -> None:
    scale = max(1.0, abs(wanted))
    if abs(got - wanted) > 1e-6 * scale:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Fit a two-stage pipeline on y = 2 + 3x and shift a column by one."""
    repark = ReparkSession.builder.appName("ex-ml-pipeline").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(ROWS, ["x", "label"])
        assembler = VectorAssembler(inputCols=["x"], outputCol="features")
        regression = LinearRegression(featuresCol="features", labelCol="label")
        expect("Assembler.isinstance.Transformer", isinstance(assembler, ml.Transformer), True)
        expect("LinearRegression.isinstance.Estimator", isinstance(regression, ml.Estimator), True)

        empty = ml.Pipeline()
        expect("Pipeline.isinstance.Estimator", isinstance(empty, ml.Estimator), True)
        expect("Pipeline.isinstance.Identifiable", isinstance(empty, ml.Identifiable), True)
        expect("Pipeline.uid.prefix", empty.uid.startswith("Pipeline_"), True)
        expect("Pipeline.getStages.default", empty.getStages(), [])
        expect("Identifiable.repr.eq.uid", repr(empty) == empty.uid, True)

        pipe = ml.Pipeline(stages=[assembler, regression])
        expect(
            "Pipeline.getStages.types",
            [type(stage).__name__ for stage in pipe.getStages()],
            ["VectorAssembler", "LinearRegression"],
        )
        model = pipe.fit(frame)
        expect("PipelineModel.class", type(model).__name__, "PipelineModel")
        expect("PipelineModel.isinstance.PipelineModel", isinstance(model, ml.PipelineModel), True)
        expect("PipelineModel.isinstance.Model", isinstance(model, ml.Model), True)
        expect("PipelineModel.isinstance.Transformer", isinstance(model, ml.Transformer), True)
        expect(
            "PipelineModel.stages.types",
            [type(stage).__name__ for stage in model.stages],
            ["VectorAssembler", "LinearRegressionModel"],
        )
        fitted = model.stages[1]
        expect("fitted.coefficients.len", len(fitted.coefficients), 1)
        expect_close("fitted.coefficients[0]", float(fitted.coefficients[0]), 3.0)
        expect_close("fitted.intercept", float(fitted.intercept), 2.0)
        predicted = sorted(
            (float(row["x"]), float(row["label"]), float(row["prediction"]))
            for row in model.transform(frame).collect()
        )
        expect("PipelineModel.transform.len", len(predicted), 4)
        for x_value, label, prediction in predicted:
            expect_close(f"prediction.x={x_value}", prediction, label)

        shifter = ShiftOne()
        expect(
            "ShiftOne.isinstance.UnaryTransformer", isinstance(shifter, ml.UnaryTransformer), True
        )
        expect("ShiftOne.isinstance.Transformer", isinstance(shifter, ml.Transformer), True)
        expect("ShiftOne.isinstance.HasInputCol", isinstance(shifter, ml.HasInputCol), True)
        shifter.setInputCol("x").setOutputCol("x1")
        expect("ShiftOne.getInputCol", shifter.getInputCol(), "x")
        expect("ShiftOne.getOutputCol", shifter.getOutputCol(), "x1")
        shifted = sorted(
            (float(row["x"]), float(row["x1"])) for row in shifter.transform(frame).collect()
        )
        expect("ShiftOne.transform", shifted, [(1.0, 2.0), (2.0, 3.0), (3.0, 4.0), (4.0, 5.0)])
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
