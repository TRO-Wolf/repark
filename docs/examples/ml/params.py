"""Params, converters, and the shared column mixins read the Spark-docs way.

pins: ex-27-ml/C-003
"""

from __future__ import annotations

from repark.spark import ml
from repark.spark.ml.feature import Tokenizer, VectorAssembler
from repark.spark.ml.regression import LinearRegression

COVERS: list[str] = [
    "ml.HasFeaturesCol",
    "ml.HasHandleInvalid",
    "ml.HasInputCol",
    "ml.HasInputCols",
    "ml.HasLabelCol",
    "ml.HasOutputCol",
    "ml.HasOutputCols",
    "ml.HasPredictionCol",
    "ml.Param",
    "ml.Params",
    "ml.TypeConverters",
]


class Toy(ml.Params):
    def __init__(self) -> None:
        super().__init__()
        self.maxIter = ml.Param(
            self,
            "maxIter",
            "max iterations.",
            typeConverter=ml.TypeConverters.toInt,
        )
        self._setDefault(maxIter=10)


class InputStage(ml.HasInputCol, ml.Params):
    def __init__(self) -> None:
        super().__init__()


class OutputStage(ml.HasOutputCol, ml.Params):
    def __init__(self) -> None:
        super().__init__()


class InputColsStage(ml.HasInputCols, ml.Params):
    def __init__(self) -> None:
        super().__init__()


class OutputColsStage(ml.HasOutputCols, ml.Params):
    def __init__(self) -> None:
        super().__init__()


class HandleStage(ml.HasHandleInvalid, ml.Params):
    def __init__(self) -> None:
        super().__init__()


class FeaturesStage(ml.HasFeaturesCol, ml.Params):
    def __init__(self) -> None:
        super().__init__()


class LabelStage(ml.HasLabelCol, ml.Params):
    def __init__(self) -> None:
        super().__init__()


class PredictionStage(ml.HasPredictionCol, ml.Params):
    def __init__(self) -> None:
        super().__init__()


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the Spark-measured param defaults, converters, and mixin get/set answers."""
    toy = Toy()
    expect("Param.name", toy.maxIter.name, "maxIter")
    expect("Param.doc", toy.maxIter.doc, "max iterations.")
    expect("Param.parent.eq.uid", toy.maxIter.parent == toy.uid, True)
    expect("Param.str.suffix", str(toy.maxIter).endswith("__maxIter"), True)
    expect("Params.getOrDefault.default", toy.getOrDefault(toy.maxIter), 10)
    expect("Params.hasDefault", toy.hasDefault(toy.maxIter), True)
    expect("Params.isSet.before", toy.isSet(toy.maxIter), False)
    expect("Params.isDefined.default", toy.isDefined(toy.maxIter), True)
    toy._set(maxIter=3)
    expect("Params.getOrDefault.set", toy.getOrDefault("maxIter"), 3)
    expect("Params.isSet.after", toy.isSet(toy.maxIter), True)
    expect(
        "Params.explainParam",
        toy.explainParam("maxIter"),
        "maxIter: max iterations. (default: 10, current: 3)",
    )
    copied = toy.copy()
    expect("Params.copy.uid.eq", copied.uid == toy.uid, True)
    expect("Params.copy.value", copied.getOrDefault("maxIter"), 3)
    copied_extra = toy.copy(extra={toy.maxIter: 9})
    expect("Params.copy.extra", copied_extra.getOrDefault("maxIter"), 9)

    expect("TypeConverters.toList", ml.TypeConverters.toList((1, 2)), [1, 2])
    expect("TypeConverters.toListFloat", ml.TypeConverters.toListFloat([1, 2]), [1.0, 2.0])
    expect("TypeConverters.toListInt", ml.TypeConverters.toListInt([1.0, 2.0]), [1, 2])
    expect("TypeConverters.toListString", ml.TypeConverters.toListString(["a", "b"]), ["a", "b"])
    expect("TypeConverters.toFloat", ml.TypeConverters.toFloat(3), 3.0)
    expect("TypeConverters.toInt", ml.TypeConverters.toInt(3.0), 3)
    expect("TypeConverters.toString", ml.TypeConverters.toString("x"), "x")
    expect("TypeConverters.toBoolean", ml.TypeConverters.toBoolean(True), True)
    expect("TypeConverters.identity", ml.TypeConverters.identity(7), 7)

    tokenizer = Tokenizer()
    expect("Tokenizer.isinstance.HasInputCol", isinstance(tokenizer, ml.HasInputCol), True)
    expect("Tokenizer.isinstance.HasOutputCol", isinstance(tokenizer, ml.HasOutputCol), True)
    expect(
        "Tokenizer.getInputCol.eq.uid", tokenizer.getInputCol() == tokenizer.uid + "__input", True
    )
    tokenizer.setInputCol("text").setOutputCol("words")
    expect("Tokenizer.setInputCol", tokenizer.getInputCol(), "text")
    expect("Tokenizer.setOutputCol", tokenizer.getOutputCol(), "words")

    inp = InputStage()
    expect("HasInputCol.default.eq.uid", inp.getInputCol() == inp.uid + "__input", True)
    inp.setInputCol("tokens")
    expect("HasInputCol.set", inp.getInputCol(), "tokens")
    out = OutputStage()
    expect("HasOutputCol.default.eq.uid", out.getOutputCol() == out.uid + "__output", True)
    out.setOutputCol("tokens_out")
    expect("HasOutputCol.set", out.getOutputCol(), "tokens_out")

    input_cols = InputColsStage()
    input_cols.setInputCols(["a", "b"])
    expect("HasInputCols.set", input_cols.getInputCols(), ["a", "b"])
    output_cols = OutputColsStage()
    output_cols.setOutputCols(["c", "d"])
    expect("HasOutputCols.set", output_cols.getOutputCols(), ["c", "d"])

    handle = HandleStage()
    expect("HasHandleInvalid.default", handle.getHandleInvalid(), "error")
    handle.setHandleInvalid("skip")
    expect("HasHandleInvalid.set", handle.getHandleInvalid(), "skip")
    expect("HasFeaturesCol.default", FeaturesStage().getFeaturesCol(), "features")
    expect("HasLabelCol.default", LabelStage().getLabelCol(), "label")
    expect("HasPredictionCol.default", PredictionStage().getPredictionCol(), "prediction")

    assembler = VectorAssembler()
    expect("VectorAssembler.isinstance.HasInputCols", isinstance(assembler, ml.HasInputCols), True)
    expect(
        "VectorAssembler.isinstance.HasHandleInvalid",
        isinstance(assembler, ml.HasHandleInvalid),
        True,
    )
    expect("VectorAssembler.getHandleInvalid", assembler.getHandleInvalid(), "error")
    assembler.setInputCols(["x", "y"]).setOutputCol("features")
    expect("VectorAssembler.setInputCols", assembler.getInputCols(), ["x", "y"])

    regression = LinearRegression()
    expect(
        "LinearRegression.isinstance.HasFeaturesCol",
        isinstance(regression, ml.HasFeaturesCol),
        True,
    )
    expect("LinearRegression.isinstance.HasLabelCol", isinstance(regression, ml.HasLabelCol), True)
    expect(
        "LinearRegression.isinstance.HasPredictionCol",
        isinstance(regression, ml.HasPredictionCol),
        True,
    )
    expect("LinearRegression.getFeaturesCol", regression.getFeaturesCol(), "features")
    expect("LinearRegression.getLabelCol", regression.getLabelCol(), "label")
    expect("LinearRegression.getPredictionCol", regression.getPredictionCol(), "prediction")
    regression.setFeaturesCol("x").setLabelCol("y").setPredictionCol("hat")
    expect("HasFeaturesCol.set", regression.getFeaturesCol(), "x")
    expect("HasLabelCol.set", regression.getLabelCol(), "y")
    expect("HasPredictionCol.set", regression.getPredictionCol(), "hat")


if __name__ == "__main__":
    main()
