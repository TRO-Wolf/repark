"""Pipeline and PipelineModel read/write round-trips through a temp dir.

pins: ex-27-ml/C-006
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from repark.spark import ReparkSession, ml
from repark.spark.ml.feature import VectorAssembler
from repark.spark.ml.regression import LinearRegression

COVERS: list[str] = [
    "ml.MLReadable",
    "ml.MLWritable",
]

ROWS = [(1.0, 5.0), (2.0, 8.0), (3.0, 11.0), (4.0, 14.0)]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def expect_close(label: str, got: float, wanted: float) -> None:
    scale = max(1.0, abs(wanted))
    if abs(got - wanted) > 1e-6 * scale:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def collect_predictions(model: ml.Transformer, frame: object) -> list[tuple[float, float, float]]:
    return sorted(
        (float(row["x"]), float(row["label"]), float(row["prediction"]))
        for row in model.transform(frame).collect()
    )


def main() -> None:
    """Save and load a fitted OLS pipeline; transform answers stay Spark-equal."""
    expect("Pipeline.issubclass.MLWritable", issubclass(ml.Pipeline, ml.MLWritable), True)
    expect("Pipeline.issubclass.MLReadable", issubclass(ml.Pipeline, ml.MLReadable), True)
    expect("PipelineModel.issubclass.MLWritable", issubclass(ml.PipelineModel, ml.MLWritable), True)
    expect("PipelineModel.issubclass.MLReadable", issubclass(ml.PipelineModel, ml.MLReadable), True)

    repark = ReparkSession.builder.appName("ex-ml-persistence").master("local[1]").getOrCreate()
    scratch = tempfile.TemporaryDirectory(prefix="ex-ml-persist-")
    try:
        frame = repark.createDataFrame(ROWS, ["x", "label"])
        pipe = ml.Pipeline(
            stages=[
                VectorAssembler(inputCols=["x"], outputCol="features"),
                LinearRegression(featuresCol="features", labelCol="label"),
            ]
        )
        expect("Pipeline.isinstance.MLWritable", isinstance(pipe, ml.MLWritable), True)
        model = pipe.fit(frame)
        expect("PipelineModel.isinstance.MLWritable", isinstance(model, ml.MLWritable), True)
        original = collect_predictions(model, frame)
        expect("original.len", len(original), 4)
        for x_value, label, prediction in original:
            expect_close(f"original.x={x_value}", prediction, label)

        model_path = str(Path(scratch.name) / "model")
        model.write().overwrite().save(model_path)
        loaded_model = ml.PipelineModel.load(model_path)
        expect("loaded.isinstance.PipelineModel", isinstance(loaded_model, ml.PipelineModel), True)
        restored = collect_predictions(loaded_model, frame)
        expect("PipelineModel.roundtrip", restored, original)

        assembler_pipe = ml.Pipeline(
            stages=[VectorAssembler(inputCols=["x"], outputCol="features")]
        )
        pipe_path = str(Path(scratch.name) / "pipe")
        assembler_pipe.write().overwrite().save(pipe_path)
        loaded_pipe = ml.Pipeline.load(pipe_path)
        expect("loaded.pipe.isinstance.Pipeline", isinstance(loaded_pipe, ml.Pipeline), True)
        expect(
            "loaded.pipe.stages",
            [type(stage).__name__ for stage in loaded_pipe.getStages()],
            ["VectorAssembler"],
        )
        loaded_assembler = loaded_pipe.getStages()[0]
        expect("loaded.assembler.inputCols", loaded_assembler.getInputCols(), ["x"])
        expect("loaded.assembler.outputCol", loaded_assembler.getOutputCol(), "features")
    finally:
        scratch.cleanup()
        repark.stop()


if __name__ == "__main__":
    main()
