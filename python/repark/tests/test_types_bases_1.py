"""TYPES-BASES-1: abstract type bases, spatial types, ``UserDefinedType``, ``types.Row``.

pins: types-bases-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import repark.spark.types as spark_types
from repark import ReparkSession
from repark.errors import (
    IllegalArgumentException,
    PySparkNotImplementedError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark.row import Row

_CELLS = json.loads(
    (Path(__file__).with_name("facade_types_oracle.json")).read_text(encoding="utf-8")
)["cells"]

_BASE_NAMES = [
    "DataType",
    "AtomicType",
    "NumericType",
    "IntegralType",
    "FractionalType",
    "DatetimeType",
    "AnyTimeType",
    "AnsiIntervalType",
    "SpatialType",
    "UserDefinedType",
]


def _result(cell_id: str) -> dict:
    """The ``result`` payload of an oracle cell."""
    return _CELLS[cell_id]["result"]


def _error(cell_id: str) -> dict:
    """The ``error`` payload of an oracle cell."""
    return _CELLS[cell_id]["error"]


def _value(item: dict):
    """Unwrap one oracle value node (scalar, list, or dict)."""
    if "value" in item:
        return item["value"]
    if item["kind"] == "list":
        return [_value(child) for child in item["items"]]
    return _items(item)


def _items(payload: dict) -> dict:
    """Unwrap a ``kind: dict`` oracle payload into name → value."""
    return {name: _value(item) for name, item in payload["items"].items()}


def _instances() -> dict:
    """The probe's INSTANCES table rebuilt against repark types."""
    types = spark_types
    return {
        "NullType": types.NullType(),
        "StringType": types.StringType(),
        "CharType": types.CharType(3),
        "VarcharType": types.VarcharType(3),
        "BinaryType": types.BinaryType(),
        "BooleanType": types.BooleanType(),
        "DateType": types.DateType(),
        "TimeType": types.TimeType(),
        "TimestampType": types.TimestampType(),
        "TimestampNTZType": types.TimestampNTZType(),
        "DecimalType": types.DecimalType(),
        "DoubleType": types.DoubleType(),
        "FloatType": types.FloatType(),
        "ByteType": types.ByteType(),
        "IntegerType": types.IntegerType(),
        "LongType": types.LongType(),
        "ShortType": types.ShortType(),
        "CalendarIntervalType": types.CalendarIntervalType(),
        "DayTimeIntervalType": types.DayTimeIntervalType(),
        "YearMonthIntervalType": types.YearMonthIntervalType(),
        "VariantType": types.VariantType(),
        "ArrayType": types.ArrayType(types.IntegerType()),
        "MapType": types.MapType(types.StringType(), types.IntegerType()),
        "StructField": types.StructField("a", types.IntegerType()),
        "StructType": types.StructType([]),
        "GeographyType": types.GeographyType(4326),
        "GeometryType": types.GeometryType(0),
    }


_MATRIX = _items(_result("isinstance_matrix"))
_MRO = _items(_result("mro"))
_INSTANTIATE_CELLS = sorted(name for name in _CELLS if name.startswith("instantiate_"))


@pytest.mark.parametrize("type_name", sorted(_MATRIX))
def test_isinstance_matrix_both_ways(type_name: str) -> None:
    """Cell ``isinstance_matrix``: every (type, base) pair both ways. pins: types-bases-1/C-001"""
    instance = _instances()[type_name]
    expected = set(_MATRIX[type_name])
    for base_name in _BASE_NAMES:
        base = getattr(spark_types, base_name)
        assert isinstance(instance, base) == (base_name in expected), (type_name, base_name)


@pytest.mark.parametrize("type_name", sorted(_MRO))
def test_mro_matches_oracle(type_name: str) -> None:
    """Cell ``mro``: the recorded resolution order of each class. pins: types-bases-1/C-001"""
    cls = getattr(spark_types, type_name)
    assert [c.__name__ for c in cls.__mro__] == _MRO[type_name]


@pytest.mark.parametrize("cell_id", _INSTANTIATE_CELLS)
def test_base_instantiation_cells(cell_id: str) -> None:
    """Cells ``instantiate_*``: bases instantiate and answer Spark. pins: types-bases-1/C-002"""
    cls = getattr(spark_types, cell_id.removeprefix("instantiate_"))
    expected = _items(_result(cell_id))
    instance = cls()
    assert repr(instance) == expected["repr"]
    assert instance.typeName() == expected["typeName"]
    assert cls.typeName() == expected["typeName"]
    assert instance.simpleString() == expected["simpleString"]
    assert instance.json() == expected["json"]
    assert (instance == cls()) is expected["eq_new"]
    assert (hash(instance) == hash(cls())) is expected["hash_eq"]


def test_spatial_type_instantiates() -> None:
    """Cell ``spatial_instantiate``: ``SpatialType()`` instantiates. pins: types-bases-1/C-002"""
    assert repr(spark_types.SpatialType()) == _result("spatial_instantiate")["value"]


def test_time_type_cells() -> None:
    """Cells ``time_default_repr`` / ``time_isinstance``. pins: types-bases-1/C-001"""
    assert repr(spark_types.TimeType()) == _result("time_default_repr")["value"]
    bases = [spark_types.AtomicType, spark_types.DatetimeType, spark_types.AnyTimeType]
    expected = [item["value"] for item in _result("time_isinstance")["items"]]
    assert [isinstance(spark_types.TimeType(), base) for base in bases] == expected


def test_geography_attrs() -> None:
    """Cell ``geography_attrs``: ``GeographyType(4326)`` surface. pins: types-bases-1/C-003"""
    expected = _items(_result("geography_attrs"))
    geo = spark_types.GeographyType(4326)
    assert repr(geo) == expected["repr"]
    assert geo.simpleString() == expected["simple"]
    assert geo.json() == expected["json"]
    assert geo.srid == expected["srid"]
    assert geo.typeName() == expected["typeName"]
    assert spark_types.GeographyType.typeName() == expected["typeName"]


def test_geography_any() -> None:
    """Cell ``geography_any``: the mixed-SRID ``GeographyType("ANY")``. pins: types-bases-1/C-003"""
    geo = spark_types.GeographyType("ANY")
    assert repr(geo) == _result("geography_any")["value"]
    assert geo.simpleString() == "geography(any)"
    assert geo.srid == spark_types.SpatialType.MIXED_SRID


def test_geography_bad_srid() -> None:
    """Cell ``geography_bad_srid``: non-geographic SRID refuses. pins: types-bases-1/C-003"""
    expected = _error("geography_bad_srid")
    with pytest.raises(IllegalArgumentException) as excinfo:
        spark_types.GeographyType(3857)
    error = excinfo.value
    assert str(error) == expected["message"]
    assert error.getCondition() == expected["condition"]
    assert error.getErrorClass() == expected["condition"]
    assert error.getMessageParameters() == expected["params"]


def test_geography_requires_srid() -> None:
    """Cell ``geography_default``: ``srid`` is required. pins: types-bases-1/C-003"""
    expected = _error("geography_default")
    with pytest.raises(TypeError) as excinfo:
        spark_types.GeographyType()
    assert str(excinfo.value) == expected["message"]


def test_geometry_attrs() -> None:
    """Cell ``geometry_attrs``: ``GeometryType(0)`` surface. pins: types-bases-1/C-003"""
    expected = _items(_result("geometry_attrs"))
    geo = spark_types.GeometryType(0)
    assert repr(geo) == expected["repr"]
    assert geo.simpleString() == expected["simple"]
    assert geo.json() == expected["json"]
    assert geo.srid == expected["srid"]
    assert geo.typeName() == expected["typeName"]
    assert spark_types.GeometryType.typeName() == expected["typeName"]


def test_geometry_any() -> None:
    """Cell ``geometry_any``: the mixed-SRID ``GeometryType("ANY")``. pins: types-bases-1/C-003"""
    geo = spark_types.GeometryType("ANY")
    assert repr(geo) == _result("geometry_any")["value"]
    assert geo.simpleString() == "geometry(any)"
    assert geo.srid == spark_types.SpatialType.MIXED_SRID


def test_geometry_accepts_supported_srid() -> None:
    """The Cartesian SRID table accepts 3857 where geography refused. pins: types-bases-1/C-003"""
    geo = spark_types.GeometryType(3857)
    assert geo.simpleString() == "geometry(3857)"
    assert geo.json() == '"geometry(EPSG:3857)"'


def test_geometry_requires_srid() -> None:
    """Cell ``geometry_default``: ``srid`` is required. pins: types-bases-1/C-003"""
    expected = _error("geometry_default")
    with pytest.raises(TypeError) as excinfo:
        spark_types.GeometryType()
    assert str(excinfo.value) == expected["message"]


def test_geometry_ddl_parses() -> None:
    """Cell ``geo_ddl``: ``g geometry(4326)`` answers the struct. pins: types-bases-1/C-003"""
    parsed = spark_types.DataType.fromDDL("g geometry(4326)")
    assert repr(parsed) == _result("geo_ddl")["value"]


def test_geo_fromjson_refuses() -> None:
    """Cell ``geo_fromjson``: SRID-form geometry in JSON refuses. pins: types-bases-1/C-003"""
    expected = _error("geo_fromjson")
    payload = {
        "type": "struct",
        "fields": [{"name": "g", "type": "geometry(4326)", "nullable": True, "metadata": {}}],
    }
    with pytest.raises(PySparkValueError) as excinfo:
        spark_types.StructType.fromJson(payload)
    error = excinfo.value
    assert str(error) == expected["message"]
    assert error.getCondition() == expected["condition"]
    assert error.getMessageParameters() == expected["params"]


def test_spatial_column_use_refuses_naming_the_type() -> None:
    """Spatial column types refuse per V3-GEO-1. pins: types-bases-1/C-003"""
    spark = ReparkSession.builder.appName("types-bases-1-spatial").getOrCreate()
    try:
        frame = spark.createDataFrame([(1,)], ["a"])
        for spatial in (spark_types.GeographyType(4326), spark_types.GeometryType(0)):
            schema = spark_types.StructType([spark_types.StructField("g", spatial, True)])
            with pytest.raises(UnsupportedOperationException) as excinfo:
                spark.createDataFrame([], schema)
            assert spatial.simpleString() in str(excinfo.value)
            with pytest.raises(UnsupportedOperationException) as excinfo:
                frame["a"].cast(spatial)
            assert spatial.simpleString() in str(excinfo.value)
    finally:
        spark.stop()


def test_udt_cells() -> None:
    """Cells ``udt_instantiate`` / ``udt_typeName`` / ``udt_sqlType``. pins: types-bases-1/C-004"""
    assert repr(spark_types.UserDefinedType()) == _result("udt_instantiate")["value"]
    assert spark_types.UserDefinedType.typeName() == _result("udt_typeName")["value"]
    expected = _error("udt_sqlType")
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        spark_types.UserDefinedType.sqlType()
    error = excinfo.value
    assert str(error) == expected["message"]
    assert error.getCondition() == expected["condition"]
    assert error.getMessageParameters() == expected["params"]


def test_udt_column_use_refuses() -> None:
    """R-3: a UDT subclass as column type refuses ``NOT_IMPLEMENTED``. pins: types-bases-1/C-004"""

    class _PointUdt(spark_types.UserDefinedType):
        """User UDT subclass for the refusal probes."""

    spark = ReparkSession.builder.appName("types-bases-1-udt").getOrCreate()
    try:
        schema = spark_types.StructType([spark_types.StructField("p", _PointUdt(), True)])
        with pytest.raises(PySparkNotImplementedError) as excinfo:
            spark.createDataFrame([], schema)
        assert excinfo.value.getCondition() == "NOT_IMPLEMENTED"
        assert excinfo.value.getMessageParameters() == {"feature": "UserDefinedType"}
        frame = spark.createDataFrame([(1,)], ["a"])
        with pytest.raises(PySparkNotImplementedError) as excinfo:
            frame["a"].cast(_PointUdt())
        assert excinfo.value.getCondition() == "NOT_IMPLEMENTED"
        assert excinfo.value.getMessageParameters() == {"feature": "UserDefinedType"}
    finally:
        spark.stop()


def test_types_row_is_sql_row() -> None:
    """Cell ``types_row_is_sql_row`` (probe cells.py:64): one Row. pins: types-bases-1/C-005"""
    assert spark_types.Row is Row
    assert "Row" in spark_types.__all__


def test_singleton_identity_not_reproduced() -> None:
    """R-2: ``DataTypeSingleton`` not reproduced (declared divergence). pins: types-bases-1/C-002"""
    assert _result("singleton_integral")["value"] is True
    assert _result("numeric_singleton")["value"] is False
    assert (spark_types.IntegralType() is spark_types.IntegralType()) is False
    assert (spark_types.NumericType() is spark_types.NumericType()) is False
