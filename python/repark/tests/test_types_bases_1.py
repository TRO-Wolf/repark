"""TYPES-BASES-1: abstract type bases, spatial types, ``UserDefinedType``, ``types.Row``.

pins: types-bases-1/C-001, C-002, C-003, C-004, C-005, C-006
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

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


def test_geometry_ddl_door_blocked() -> None:
    """Cell ``geo_ddl``: the DDL door is the Rust type table, whose parser has no spatial arm.

    Today's ``ValueError`` refusal is pinned so the door stays honest until the Rust spatial
    step lands; the oracle's struct answer resumes on that arm. pins: types-bases-1/C-003
    """
    with pytest.raises(ValueError, match="cannot parse datatype"):
        spark_types.DataType.fromDDL("g geometry(4326)")


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
    assert (spark_types.IntegralType() is spark_types.IntegralType()) is False
    assert (spark_types.NumericType() is spark_types.NumericType()) is False


class PointUdt(spark_types.UserDefinedType):
    """Spark-shaped UDT subclass for the template-method pins (critic PointUDT table)."""

    @classmethod
    def sqlType(cls) -> spark_types.DataType:  # noqa: N802
        """Two-double struct storage type."""
        return spark_types.StructType(
            [
                spark_types.StructField("x", spark_types.DoubleType()),
                spark_types.StructField("y", spark_types.DoubleType()),
            ]
        )

    @classmethod
    def module(cls) -> str:
        """Probe module name."""
        return "probe"

    def serialize(self, obj: tuple[float, float]) -> list[float]:
        """Point pair to a SQL list."""
        return [obj[0], obj[1]]

    def deserialize(self, datum: Any) -> tuple[float, ...]:
        """SQL datum back to a point tuple."""
        return tuple(datum)


def test_merge_type_mixed_srid_spatial() -> None:
    """L-001: SRID mismatches merge to ``ANY`` (Spark). pins: types-bases-1/C-006"""
    merged = spark_types._merge_type(spark_types.GeometryType(4326), spark_types.GeometryType(0))
    assert repr(merged) == "GeometryType(ANY)"
    assert merged == spark_types.GeometryType("ANY")
    merged = spark_types._merge_type(
        spark_types.GeographyType(4326), spark_types.GeographyType("ANY")
    )
    assert repr(merged) == "GeographyType(ANY)"
    assert merged == spark_types.GeographyType("ANY")
    same = spark_types._merge_type(spark_types.GeometryType(4326), spark_types.GeometryType(4326))
    assert same == spark_types.GeometryType(4326)


def test_merge_type_mixed_srid_nested() -> None:
    """L-001: nested struct/array merges reach the spatial SRID arms. pins: types-bases-1/C-006"""
    left = spark_types.StructType(
        [spark_types.StructField("g", spark_types.GeometryType(4326), True)]
    )
    right = spark_types.StructType(
        [spark_types.StructField("g", spark_types.GeometryType(0), True)]
    )
    merged = spark_types._merge_type(left, right)
    assert merged.fields[0].dataType == spark_types.GeometryType("ANY")
    merged_array = spark_types._merge_type(
        spark_types.ArrayType(spark_types.GeographyType(4326)),
        spark_types.ArrayType(spark_types.GeographyType("ANY")),
    )
    assert merged_array.elementType == spark_types.GeographyType("ANY")


def test_merge_type_spatial_string() -> None:
    """L-001: spatial + StringType → StringType (Spark). pins: types-bases-1/C-006"""
    assert (
        spark_types._merge_type(spark_types.GeometryType(0), spark_types.StringType())
        == spark_types.StringType()
    )
    assert (
        spark_types._merge_type(spark_types.StringType(), spark_types.GeographyType(4326))
        == spark_types.StringType()
    )


def test_udt_subclass_template_methods() -> None:
    """L-002: a Spark-shaped UDT subclass converts and emits the UDT JSON dict.

    pins: types-bases-1/C-006
    """
    udt = PointUdt()
    assert udt.toInternal((1.0, 2.0)) == (1.0, 2.0)
    assert udt.fromInternal([1.0, 2.0]) == (1.0, 2.0)
    assert udt.toInternal(None) is None
    assert udt.fromInternal(None) is None
    assert udt.needConversion() is True
    value = udt.jsonValue()
    assert value["type"] == "udt"
    assert value["pyClass"] == "probe.PointUdt"
    assert "serializedClass" in value
    assert value["sqlType"]["type"] == "struct"


def test_udt_base_template_refusals() -> None:
    """L-002: base template refusals name Spark's features. pins: types-bases-1/C-006"""
    base = spark_types.UserDefinedType()
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        base.serialize(1)
    assert excinfo.value.getMessageParameters() == {"feature": "toInternal()"}
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        base.deserialize(1)
    assert excinfo.value.getMessageParameters() == {"feature": "fromInternal()"}
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        base.jsonValue()
    assert excinfo.value.getMessageParameters() == {"feature": "module()"}


def test_udt_eq_compares_class() -> None:
    """L-002: UDT equality is ``type(self) == type(other)`` and unhashable (Spark).

    pins: types-bases-1/C-006
    """

    class _PointUdtB(PointUdt):
        """A second UDT subclass."""

    first = PointUdt()
    first.marker = 1
    second = PointUdt()
    second.marker = 2
    assert (first == second) is True
    assert (first == _PointUdtB()) is False
    with pytest.raises(TypeError):
        hash(first)


def test_udt_fromjson_dict_refuses() -> None:
    """``{"type": "udt"}`` JSON stays the TYPES-UDT-1 refusal. pins: types-bases-1/C-006"""
    payload = {
        "type": "struct",
        "fields": [
            {
                "name": "p",
                "type": {"type": "udt", "pyClass": "probe.PointUdt"},
                "nullable": True,
                "metadata": {},
            }
        ],
    }
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        spark_types.StructType.fromJson(payload)
    assert excinfo.value.getCondition() == "NOT_IMPLEMENTED"
    assert excinfo.value.getMessageParameters() == {"feature": "UserDefinedType"}


def test_spatial_srid_edge_cells() -> None:
    """L-003: bool / float / string SRID edges answer Spark. pins: types-bases-1/C-006"""
    false_geometry = spark_types.GeometryType(False)
    assert repr(false_geometry) == "GeometryType(False)"
    assert false_geometry.simpleString() == "geometry(False)"
    assert false_geometry.srid is False
    assert (false_geometry == spark_types.GeometryType(0)) is True
    assert hash(false_geometry) != hash(spark_types.GeometryType(0))
    constructors = (
        lambda: spark_types.GeometryType(True),
        lambda: spark_types.GeographyType(True),
        lambda: spark_types.GeometryType(4326.0),
        lambda: spark_types.GeographyType("4326"),
        lambda: spark_types.GeographyType("any"),
        lambda: spark_types.GeometryType("any"),
    )
    for construct in constructors:
        with pytest.raises(IllegalArgumentException) as excinfo:
            construct()
        assert excinfo.value.getCondition() == "ST_INVALID_SRID_VALUE"


def test_spatial_json_edge_cells() -> None:
    """L-003: SRID-form geography hits the algorithm refusal; CRS forms round trip.

    pins: types-bases-1/C-006
    """
    payload = {
        "type": "struct",
        "fields": [{"name": "g", "type": "geography(4326)", "nullable": True, "metadata": {}}],
    }
    with pytest.raises(IllegalArgumentException) as excinfo:
        spark_types.StructType.fromJson(payload)
    assert excinfo.value.getCondition() == "ST_INVALID_ALGORITHM_VALUE"
    assert excinfo.value.getMessageParameters() == {"alg": "4326"}
    for spatial in (
        spark_types.GeographyType(4326),
        spark_types.GeometryType(0),
        spark_types.GeometryType(4326),
        spark_types.GeometryType(3857),
        spark_types.GeometryType("ANY"),
        spark_types.GeographyType("ANY"),
    ):
        round_trip = (
            spark_types.StructType.fromJson(
                {
                    "type": "struct",
                    "fields": [
                        {"name": "g", "type": spatial.jsonValue(), "nullable": True, "metadata": {}}
                    ],
                }
            )
            .fields[0]
            .dataType
        )
        assert round_trip == spatial


def test_spatial_ddl_door_blocked() -> None:
    """L-003/R-4: spatial DDL tokens refuse through the Rust type table (UNMEASURED door).

    pins: types-bases-1/C-006
    """
    for ddl in (
        "geometry",
        "geography",
        "geometry(4326)",
        "geography(4326)",
        "geometry(any)",
    ):
        with pytest.raises(ValueError, match="cannot parse datatype"):
            spark_types.DataType.fromDDL(ddl)


def test_spatial_reader_schema_refuses_naming_the_type(tmp_path: Path) -> None:
    """L-004: a user ``StructType`` in the reader schema door refuses per V3-GEO-1.

    Repark exposes no ``catalog.createTable`` / user-schema ``writeTo`` door; the reader
    schema is the door where a ``StructField`` carrying ``GeographyType(4326)`` reaches a
    column cast. pins: types-bases-1/C-006
    """
    csv_file = tmp_path / "geo.csv"
    csv_file.write_text("g\nPOINT(1 2)\n", encoding="utf-8")
    spark = ReparkSession.builder.appName("types-bases-1-geo-schema").getOrCreate()
    try:
        schema = spark_types.StructType(
            [spark_types.StructField("g", spark_types.GeographyType(4326), True)]
        )
        with pytest.raises(UnsupportedOperationException) as excinfo:
            spark.read.schema(schema).csv(str(csv_file), header=True).collect()
        assert "geography(4326)" in str(excinfo.value)
    finally:
        spark.stop()
