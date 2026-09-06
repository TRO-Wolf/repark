"""Divergence pins for the EX-27 ml example batch.

Registry §7 rows EX-ML-1..9.

pins: ex-27-ml/C-007
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark.errors import IllegalArgumentException, PySparkTypeError
from repark.spark import ReparkSession, ml
from repark.spark.ml.feature import Tokenizer, VectorAssembler
from repark.spark.ml.regression import LinearRegression
from repark.spark.types import DoubleType


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-ex27-ml").getOrCreate()
    yield session
    session.stop()


class _SparkShaped(ml.UnaryTransformer):
    """UnaryTransformer that only implements Spark's Python-callable hooks."""

    def createTransformFunc(self) -> Any:  # noqa: N802
        """Return a Python row callable Spark would apply."""
        return lambda value: float(value) + 1.0

    def outputDataType(self) -> Any:  # noqa: N802
        """Return the output type Spark's UnaryTransformer requires."""
        return DoubleType()

    def validateInputType(self, input_type: Any) -> None:  # noqa: N802
        """Accept any input type."""
        return None


def test_vector_size_is_a_method() -> None:
    """DenseVector.size is a method; Spark exposes a property (EX-ML-1)."""
    dense = ml.Vectors.dense(1.0, 0.0, 3.0)
    assert callable(dense.size)
    assert dense.size() == 3
    sparse = ml.Vectors.sparse(5, [1, 3], [1.0, 2.0])
    assert callable(sparse.size)
    assert sparse.size() == 5


def test_vector_udt_typename_and_sql_type() -> None:
    """VectorUDT.typeName is vector and sqlType uses int; Spark uses vectorudt/tinyint (EX-ML-2)."""
    udt = ml.VectorUDT()
    assert udt.typeName() == "vector"
    assert udt.simpleString() == "vector"
    fields = [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in udt.sqlType().fields
    ]
    assert fields == [
        ("type", "int", False),
        ("size", "int", True),
        ("indices", "array<int>", True),
        ("values", "array<double>", True),
    ]
    assert udt.jsonValue() == {"type": "vector", "class": "repark.spark.ml.linalg.VectorUDT"}
    assert hasattr(udt, "serialize") is False
    assert hasattr(udt, "deserialize") is False


def test_has_input_col_is_not_params() -> None:
    """HasInputCol is not Params; Tokenizer defaults inputCol to uid__input (EX-ML-3)."""
    assert issubclass(ml.HasInputCol, ml.Params) is False
    with pytest.raises(PySparkTypeError, match="Identifiable"):
        ml.HasInputCol()
    tokenizer = Tokenizer()
    assert tokenizer.getInputCol() == tokenizer.uid + "__input"
    assert "(undefined)" not in tokenizer.explainParams()
    assert tokenizer.uid + "__input" in tokenizer.explainParams()


def test_param_grid_base_on_pairs() -> None:
    """baseOn(param, value) works; Spark wants a dict or (param, value) tuples (EX-ML-4)."""
    estimator = LinearRegression()
    built = ml.ParamGridBuilder().baseOn(estimator.maxIter, 20).build()
    assert len(built) == 1
    assert built[0][estimator.maxIter] == 20
    with pytest.raises(
        IllegalArgumentException,
        match="expects a dict or even-length Param/value pairs",
    ):
        ml.ParamGridBuilder().baseOn((estimator.maxIter, 20))


def test_mixin_setters_present() -> None:
    """Shared mixins carry setters; Spark's mixins do not (EX-ML-5)."""
    assert hasattr(ml.HasInputCol, "setInputCol") is True
    assert hasattr(ml.HasOutputCol, "setOutputCol") is True
    assert hasattr(ml.HasInputCols, "setInputCols") is True
    assert hasattr(ml.HasOutputCols, "setOutputCols") is True
    assert hasattr(ml.HasHandleInvalid, "setHandleInvalid") is True
    assembler = VectorAssembler()
    assert assembler.getHandleInvalid() == "error"


def test_empty_pipeline_get_stages_defaults_to_empty_list() -> None:
    """Pipeline().getStages() answers []; Spark KeyErrors on the unset param (EX-ML-6)."""
    assert ml.Pipeline().getStages() == []


def test_spark_shaped_unary_transformer_refuses(spark: ReparkSession) -> None:
    """A Spark-shaped UnaryTransformer raises; Spark applies the Python callable (EX-ML-7)."""
    frame = spark.createDataFrame([(1.0,)], ["x"])
    stage = _SparkShaped()
    stage.setInputCol("x").setOutputCol("x1")
    with pytest.raises(
        IllegalArgumentException,
        match="must implement _transform as a plan-built transform",
    ):
        stage.transform(frame)


def test_persistence_format_is_repark_ml(spark: ReparkSession, tmp_path: Path) -> None:
    """Saved pipelines write metadata.json format repark-ml; Spark writes metadata/ (EX-ML-8)."""
    pipe = ml.Pipeline(stages=[VectorAssembler(inputCols=["x"], outputCol="features")])
    saved = tmp_path / "pipe"
    pipe.write().overwrite().save(str(saved))
    meta = json.loads((saved / "metadata.json").read_text(encoding="utf-8"))
    assert meta["format"] == "repark-ml"
    spark_shaped = tmp_path / "spark_shaped"
    (spark_shaped / "metadata").mkdir(parents=True)
    with pytest.raises(IllegalArgumentException, match=r"missing metadata\.json"):
        ml.PipelineModel.load(str(spark_shaped))


def test_dense_vector_lacks_dot_and_squared_distance() -> None:
    """DenseVector has no dot or squared_distance; Spark answers 4.0 and 10.0 (EX-ML-9)."""
    dense = ml.Vectors.dense(1.0, 0.0, 3.0)
    assert hasattr(dense, "dot") is False
    assert hasattr(dense, "squared_distance") is False
