"""R-PARITY-NITS / X2 — DataType.simpleString / typeName / json / fromDDL / StructType.add.

Oracle shapes from Spark 4.x ``pyspark.sql.types`` (typeName = class-name-without-Type
lowercased; IntegerType.simpleString is the short ``int``; DecimalType jsonValue is the
simpleString form; StringType accepts collation).
"""

from __future__ import annotations

import datetime

from repark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    VarcharType,
)


def test_integer_type_simple_string_and_type_name() -> None:
    integer = IntegerType()
    assert integer.simpleString() == "int"
    assert integer.typeName() == "integer"
    assert integer.jsonValue() == "integer"
    assert integer.json() == '"integer"'


def test_atomic_type_names() -> None:
    assert StringType().simpleString() == "string"
    assert StringType().typeName() == "string"
    assert DoubleType().simpleString() == "double"
    assert BooleanType().simpleString() == "boolean"
    assert DateType().simpleString() == "date"
    assert TimestampType().simpleString() == "timestamp"
    assert LongType().simpleString() == "bigint"
    assert LongType().typeName() == "long"
    assert ShortType().simpleString() == "smallint"
    assert ByteType().simpleString() == "tinyint"
    assert FloatType().simpleString() == "float"
    assert BinaryType().simpleString() == "binary"
    assert NullType().typeName() == "void"


def test_string_type_collation() -> None:
    """Spark 4 StringType(collation) simpleString + repr (Apache test_string_type_simple_string)."""
    assert StringType().simpleString() == "string"
    assert StringType("UTF8_BINARY").simpleString() == "string"
    assert StringType("UTF8_LCASE").simpleString() == "string collate UTF8_LCASE"
    assert StringType("UNICODE").simpleString() == "string collate UNICODE"
    assert repr(StringType("UNICODE")) == "StringType('UNICODE')"
    assert StringType("UNICODE") == StringType("UNICODE")
    assert StringType("UNICODE") != StringType()


def test_decimal_simple_string() -> None:
    decimal = DecimalType(10, 2)
    assert decimal.simpleString() == "decimal(10,2)"
    assert decimal.typeName() == "decimal"
    assert decimal.jsonValue() == "decimal(10,2)"


def test_struct_type_simple_string() -> None:
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
        ]
    )
    assert schema.simpleString() == "struct<id:int,name:string>"
    assert schema.typeName() == "struct"
    assert schema.jsonValue()["type"] == "struct"
    assert len(schema.jsonValue()["fields"]) == 2  # type: ignore[arg-type]


def test_struct_type_add_and_access() -> None:
    """StructType.add chaining + fieldNames / index / slice (Apache test_struct_type)."""
    struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
    struct2 = StructType(
        [StructField("f1", StringType(), True), StructField("f2", StringType(), True, None)]
    )
    assert struct1.fieldNames() == struct2.names
    assert struct1 == struct2
    assert len(struct1) == 2
    assert struct1["f1"] is struct1.fields[0]
    assert struct1[0] is struct1.fields[0]
    assert struct1[0:1] == StructType(struct1.fields[0:1])
    try:
        _ = struct1["f9"]
        raise AssertionError("expected KeyError")
    except KeyError:
        pass


def test_struct_field_metadata_and_type_name() -> None:
    field = StructField("a", IntegerType(), True, {"k": 1})
    assert field.metadata == {"k": 1}
    try:
        field.typeName()
        raise AssertionError("expected TypeError")
    except TypeError:
        pass


def test_array_map_types() -> None:
    array_type = ArrayType(IntegerType())
    assert array_type.simpleString() == "array<int>"
    assert array_type == ArrayType(IntegerType(), True)
    assert ArrayType.fromJson(
        {"type": "array", "elementType": "string", "containsNull": True}
    ) == ArrayType(StringType(), True)
    map_type = MapType(StringType(), IntegerType())
    assert map_type.simpleString() == "map<string,int>"
    assert MapType.fromJson(
        {
            "type": "map",
            "keyType": "string",
            "valueType": "string",
            "valueContainsNull": True,
        }
    ) == MapType(StringType(), StringType(), True)


def test_from_ddl_and_to_ddl() -> None:
    assert DataType.fromDDL("long") == LongType()
    assert DataType.fromDDL("a: int, b: string") == StructType(
        [StructField("a", IntegerType()), StructField("b", StringType())]
    )
    assert DataType.fromDDL("a int, b string") == StructType(
        [StructField("a", IntegerType()), StructField("b", StringType())]
    )
    schema = StructType().add("a", IntegerType()).add("b", StringType())
    assert schema.toDDL() == "a INT,b STRING"
    schema_null = StructType().add("a", FloatType()).add("b", LongType(), False)
    assert schema_null.toDDL() == "a FLOAT,b BIGINT NOT NULL"
    # Nested DDL + bare varchar alias (octo X2 C1).
    nested = DataType.fromDDL("a array<int>, b map<string,string>")
    assert nested == StructType(
        [
            StructField("a", ArrayType(IntegerType())),
            StructField("b", MapType(StringType(), StringType())),
        ]
    )
    assert DataType.fromDDL("struct<a:varchar,b:int>") == StructType(
        [StructField("a", StringType()), StructField("b", IntegerType())]
    )


def test_date_timestamp_internal() -> None:
    assert DateType().fromInternal(0) == datetime.date(1970, 1, 1)
    # Microsecond component of datetime.max must survive toInternal (SPARK-17035).
    assert TimestampType().toInternal(datetime.datetime.max) % 1_000_000 == 999_999


def test_repr_roundtrip_common() -> None:
    for instance in (
        NullType(),
        StringType(),
        StringType("UTF8_LCASE"),
        BinaryType(),
        BooleanType(),
        DateType(),
        TimestampType(),
        DecimalType(),
        DoubleType(),
        FloatType(),
        ByteType(),
        IntegerType(),
        LongType(),
        ShortType(),
        ArrayType(StringType()),
        MapType(StringType(), IntegerType()),
        StructField("f1", StringType(), True),
        StructType([StructField("f1", StringType(), True)]),
        VarcharType(10),
    ):
        assert eval(repr(instance)) == instance
