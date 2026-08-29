"""F1 true-EC residual pins — class + parameter-key equality only.

Covers array.array unsupported typecodes, calendar-interval collect refuse,
``_merge_type`` / ``_make_type_verifier`` (Apache test_types private helpers).
"""

from __future__ import annotations

import array

import pytest

from repark.errors import PySparkNotImplementedError, PySparkTypeError, PySparkValueError
from repark.spark.row import Row
from repark.spark.session import ReparkSession, _reset_active_session_for_tests
from repark.spark.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    StringType,
    StructField,
    StructType,
    _make_type_verifier,
    _merge_type,
)


@pytest.fixture()
def spark() -> ReparkSession:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-f1-errorclass").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


# test_array_types residual — unsupported array.array typecodes


def test_array_array_supported_int_collects(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([Row(myarray=array.array("i", [1, 2, 3]))])
    assert frame.first()["myarray"] == [1, 2, 3]


def test_array_array_unsupported_typecode_cannot_infer_field(spark: ReparkSession) -> None:
    """Unsupported typecodes raise CANNOT_INFER_TYPE_FOR_FIELD with field_name (Apache)."""
    # On typical 64-bit Linux, 'q'/'Q'/'L' are unsupported (JVM has no matching slot).
    unsupported: list[str] = []
    for typecode in ("q", "Q", "L"):
        try:
            array.array(typecode)
        except (ValueError, TypeError):
            continue
        unsupported.append(typecode)
    if not unsupported:
        pytest.skip("no unsupported array.array typecodes on this platform")
    for typecode in unsupported:
        with pytest.raises(PySparkTypeError) as caught:
            spark.createDataFrame([Row(myarray=array.array(typecode))]).collect()
        assert caught.value.getErrorClass() == "CANNOT_INFER_TYPE_FOR_FIELD"
        assert caught.value.getMessageParameters() == {"field_name": "myarray"}


# test_cal_interval_in_collect residual


def test_cal_interval_make_interval_collect_not_implemented(spark: ReparkSession) -> None:
    with pytest.raises(PySparkNotImplementedError) as caught:
        spark.sql("SELECT make_interval(100, 11, 1, 1, 12, 30, 01.001001)").first()
    assert isinstance(caught.value, PySparkNotImplementedError)
    assert caught.value.getErrorClass() == "NOT_IMPLEMENTED"


# _merge_type / _make_type_verifier (Apache private helpers)


def test_merge_type_null_identity_and_same() -> None:
    assert isinstance(_merge_type(LongType(), NullType()), LongType)
    assert isinstance(_merge_type(NullType(), LongType()), LongType)
    assert isinstance(_merge_type(LongType(), LongType()), LongType)
    merged = _merge_type(ArrayType(LongType()), ArrayType(LongType()))
    assert isinstance(merged, ArrayType)
    assert isinstance(merged.elementType, LongType)


def test_merge_type_array_conflict_cannot_merge() -> None:
    with pytest.raises(PySparkTypeError) as caught:
        _merge_type(ArrayType(LongType()), ArrayType(DoubleType()))
    assert caught.value.getErrorClass() == "CANNOT_MERGE_TYPE"
    assert caught.value.getMessageParameters() == {
        "data_type1": "LongType",
        "data_type2": "DoubleType",
    }


def test_merge_type_map_and_struct_conflict() -> None:
    with pytest.raises(PySparkTypeError) as caught:
        _merge_type(
            MapType(StringType(), LongType()),
            MapType(StringType(), DoubleType()),
        )
    assert caught.value.getErrorClass() == "CANNOT_MERGE_TYPE"
    assert caught.value.getMessageParameters() == {
        "data_type1": "LongType",
        "data_type2": "DoubleType",
    }
    with pytest.raises(PySparkTypeError) as caught_struct:
        _merge_type(
            StructType([StructField("f1", LongType()), StructField("f2", StringType())]),
            StructType([StructField("f1", DoubleType()), StructField("f2", StringType())]),
        )
    assert caught_struct.value.getErrorClass() == "CANNOT_MERGE_TYPE"


def test_make_type_verifier_not_nullable_with_name() -> None:
    with pytest.raises(PySparkValueError) as caught:
        _make_type_verifier(StringType(), nullable=False, name="test_name")(None)
    assert caught.value.getErrorClass() == "FIELD_NOT_NULLABLE_WITH_NAME"
    assert caught.value.getMessageParameters() == {"field_name": "test_name"}


def test_make_type_verifier_nested_unacceptable_with_name() -> None:
    schema = StructType([StructField("a", StructType([StructField("b", IntegerType())]))])
    with pytest.raises(PySparkTypeError) as caught:
        _make_type_verifier(schema)([["data"]])
    assert caught.value.getErrorClass() == "FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME"
    params = caught.value.getMessageParameters()
    assert params is not None
    assert params["field_name"] == "field b in field a"
    assert params["data_type"] == "IntegerType()"
    assert params["obj"] == "'data'"
    assert params["obj_type"] == "<class 'str'>"


def test_make_type_verifier_integer_rejects_bool_and_float() -> None:
    """octo C3-Q-002: IntegerType must not soft-accept bool/float."""
    verifier = _make_type_verifier(IntegerType(), name="n")
    for bad in (True, False, 1.5, "1"):
        with pytest.raises(PySparkTypeError) as caught:
            verifier(bad)
        assert caught.value.getErrorClass() == "FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME"
        assert caught.value.getMessageParameters() is not None
        assert caught.value.getMessageParameters()["field_name"] == "n"
    verifier(42)


def test_merge_type_map_key_string_atomic_soft_merge() -> None:
    """Spark merges map key String+Double → StringType (AtomicType + StringType branch)."""
    merged = _merge_type(
        MapType(StringType(), LongType()),
        MapType(DoubleType(), LongType()),
    )
    assert isinstance(merged, MapType)
    assert isinstance(merged.keyType, StringType)
    assert isinstance(merged.valueType, LongType)
