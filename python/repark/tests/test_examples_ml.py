"""Divergence pins for the EX-27 ml example batch.

Registry §7 rows EX-ML-1..4.

pins: ex-27-ml/C-007
"""

from __future__ import annotations

import pytest

from repark.errors import PySparkTypeError
from repark.spark import ml
from repark.spark.ml.regression import LinearRegression


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


def test_has_input_col_is_not_params() -> None:
    """HasInputCol is not Params; Spark's mixin extends Params (EX-ML-3)."""
    assert issubclass(ml.HasInputCol, ml.Params) is False
    with pytest.raises(PySparkTypeError, match="Identifiable"):
        ml.HasInputCol()


def test_param_grid_base_on_pairs() -> None:
    """baseOn(param, value) works; Spark 4.1.2 wants a dict or (param, value) tuples (EX-ML-4)."""
    estimator = LinearRegression()
    built = ml.ParamGridBuilder().baseOn(estimator.maxIter, 20).build()
    assert len(built) == 1
    assert built[0][estimator.maxIter] == 20
