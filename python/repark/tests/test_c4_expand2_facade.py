"""C4 expand2 — facade pins for repartition* validation + fillna errorClass + AssertionError.

Apache cohort: ``test_repartition`` / ``test_stat`` fillna EC tails; ``PySparkAssertionError``
identity for ``check_error`` after the errors overlay (hour-0 FAIL-ERROR-CLASS x5 in
``test_utils``). Single-node no-op bodies stay disclosed — multi-partition routing is a seed.
"""

from __future__ import annotations

import pytest

from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import (
    PySparkAssertionError,
    PySparkException,
    PySparkTypeError,
    PySparkValueError,
)
from repark.session import ReparkSession, _reset_active_session_for_tests


@pytest.fixture()
def spark() -> ReparkSession:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-c4-expand2").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


# ==================================================================================================
# PySparkAssertionError (check_error / assert*equal overlay)
# ==================================================================================================


def test_pyspark_assertion_error_is_pyspark_exception() -> None:
    """Apache check_error requires isinstance(..., PySparkException) after overlay."""
    import repark.errors as repark_errors

    # Overlay __all__ surface (parity harness cannot import repark — pin here).
    assert "PySparkAssertionError" in repark_errors.__all__
    assert issubclass(repark_errors.PySparkAssertionError, repark_errors.PySparkException)
    assert issubclass(repark_errors.PySparkAssertionError, AssertionError)

    error = PySparkAssertionError(
        errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
        messageParameters={
            "expected_type": "DataFrame",
            "arg_name": "actual",
            "actual_type": "NoneType",
        },
    )
    assert isinstance(error, PySparkException)
    assert isinstance(error, AssertionError)
    assert error.getErrorClass() == "INVALID_TYPE_DF_EQUALITY_ARG"
    assert error.getCondition() == "INVALID_TYPE_DF_EQUALITY_ARG"


def test_pyspark_assertion_error_preserves_none_message_parameter() -> None:
    """Apache assertDataFrameEqual uses actual_type=None for a missing arg."""
    error = PySparkAssertionError(
        errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
        messageParameters={
            "expected_type": "Union[DataFrame, ps.DataFrame, List[Row]]",
            "arg_name": "actual",
            "actual_type": None,  # type: ignore[dict-item]
        },
    )
    params = error.getMessageParameters()
    assert params is not None
    assert params["actual_type"] is None
    assert params["arg_name"] == "actual"


# ==================================================================================================
# repartition / repartitionByRange / repartitionById
# ==================================================================================================


def test_repartition_list_num_partitions_error_class(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(14, "Tom")], ["age", "name"])
    with pytest.raises(PySparkTypeError) as raised:
        frame.repartition([10], "name", "age")
    assert raised.value.getErrorClass() == "NOT_COLUMN_OR_STR"
    assert raised.value.getMessageParameters() == {
        "arg_name": "numPartitions",
        "arg_type": "list",
    }


def test_repartition_list_sole_arg_error_class(spark: ReparkSession) -> None:
    """Sole-arg list must raise (octo C4 C1-S1-002) — not a silent single-node no-op."""
    frame = spark.createDataFrame([(14, "Tom")], ["age", "name"])
    with pytest.raises(PySparkTypeError) as raised:
        frame.repartition([10])
    assert raised.value.getErrorClass() == "NOT_COLUMN_OR_STR"
    assert raised.value.getMessageParameters() == {
        "arg_name": "numPartitions",
        "arg_type": "list",
    }
    with pytest.raises(PySparkTypeError) as raised_bool:
        frame.repartition(True)  # type: ignore[arg-type]
    assert raised_bool.value.getErrorClass() == "NOT_COLUMN_OR_STR"
    assert raised_bool.value.getMessageParameters() == {
        "arg_name": "numPartitions",
        "arg_type": "bool",
    }


def test_repartition_by_range_list_error_class(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(14, "Tom")], ["age", "name"])
    with pytest.raises(PySparkTypeError) as raised:
        frame.repartitionByRange([10], "name", "age")
    assert raised.value.getErrorClass() == "NOT_COLUMN_OR_INT_OR_STR"
    assert raised.value.getMessageParameters() == {
        "arg_name": "numPartitions",
        "arg_type": "list",
    }
    # Sole-arg list (symmetric to repartition sole-arg pin — octo C4 C6).
    with pytest.raises(PySparkTypeError) as raised_sole:
        frame.repartitionByRange([10])
    assert raised_sole.value.getErrorClass() == "NOT_COLUMN_OR_INT_OR_STR"
    assert raised_sole.value.getMessageParameters() == {
        "arg_name": "numPartitions",
        "arg_type": "list",
    }


def test_repartition_by_range_noop_returns_frame(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(14, "Tom"), (23, "Alice")], ["age", "name"])
    out = frame.repartitionByRange(2, "name", "age")
    assert out.count() == 2


def test_repartition_by_id_invalid_num_partitions(spark: ReparkSession) -> None:
    frame = spark.range(5)
    with pytest.raises(PySparkTypeError) as raised:
        frame.repartitionById("5", F.col("id"))
    assert raised.value.getErrorClass() == "NOT_INT"
    assert raised.value.getMessageParameters() == {
        "arg_name": "numPartitions",
        "arg_type": "str",
    }
    with pytest.raises(PySparkValueError) as raised_zero:
        frame.repartitionById(0, F.col("id"))
    assert raised_zero.value.getErrorClass() == "VALUE_NOT_POSITIVE"
    assert raised_zero.value.getMessageParameters() == {
        "arg_name": "numPartitions",
        "arg_value": "0",
    }
    with pytest.raises(PySparkValueError) as raised_neg:
        frame.repartitionById(-1, F.col("id"))
    assert raised_neg.value.getErrorClass() == "VALUE_NOT_POSITIVE"
    assert raised_neg.value.getMessageParameters() == {
        "arg_name": "numPartitions",
        "arg_value": "-1",
    }


def test_repartition_by_id_noop_collect(spark: ReparkSession) -> None:
    """Apache ``test_repartition_by_id_out_of_range`` only requires row count (RDD optional)."""
    frame = spark.range(20)
    out = frame.repartitionById(10, F.col("id"))
    assert out.count() == 20


def test_repartition_by_id_non_int_column_raises(spark: ReparkSession) -> None:
    """Apache ``test_repartition_by_id_error_non_int_type`` — analysis refuse on string col."""
    from repark.errors import AnalysisException

    frame = spark.range(5).withColumn("s", F.lit("a"))
    with pytest.raises(AnalysisException, match="integer partition"):
        frame.repartitionById(5, F.col("s")).collect()


# ==================================================================================================
# fillna errorClass (Apache test_fillna tails)
# ==================================================================================================


def test_fillna_list_value_error_class(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(None,), (True,)], ["a"])
    with pytest.raises(PySparkTypeError) as raised:
        frame.fillna(["a", True])
    assert raised.value.getErrorClass() == "NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR"
    assert raised.value.getMessageParameters() == {
        "arg_name": "value",
        "arg_type": "list",
    }


def test_fillna_subset_int_error_class(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(None,), (True,)], ["a"])
    with pytest.raises(PySparkTypeError) as raised:
        frame.fillna(50, subset=10)  # type: ignore[arg-type]
    assert raised.value.getErrorClass() == "NOT_LIST_OR_TUPLE"
    assert raised.value.getMessageParameters() == {
        "arg_name": "subset",
        "arg_type": "int",
    }


def test_fillna_none_and_tuple_value_error_class(spark: ReparkSession) -> None:
    """None / tuple values use the same Spark list EC class (octo C4 C6 pin)."""
    frame = spark.createDataFrame([(None,), (True,)], ["a"])
    with pytest.raises(PySparkTypeError) as raised_none:
        frame.fillna(None)  # type: ignore[arg-type]
    assert raised_none.value.getErrorClass() == "NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR"
    assert raised_none.value.getMessageParameters() == {
        "arg_name": "value",
        "arg_type": "NoneType",
    }
    with pytest.raises(PySparkTypeError) as raised_tuple:
        frame.fillna(("a", True))  # type: ignore[arg-type]
    assert raised_tuple.value.getErrorClass() == "NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR"
    assert raised_tuple.value.getMessageParameters() == {
        "arg_name": "value",
        "arg_type": "tuple",
    }
