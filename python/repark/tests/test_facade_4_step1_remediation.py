"""FACADE-4 step-1 remediation pins: byte-identity repros for the round-2 findings."""

from __future__ import annotations

from typing import Any

import pytest

from repark.spark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    CalendarIntervalType,
    CharType,
    DataType,
    DateType,
    DayTimeIntervalType,
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
    TimestampNTZType,
    TimestampType,
    TimeType,
    VarcharType,
    VariantType,
    YearMonthIntervalType,
    _arrow_type_to_repark,
    repark_type_to_arrow,
    struct_type_from_arrow,
)

_BASES = [
    DataType,
    NullType,
    StringType,
    CharType,
    VarcharType,
    BinaryType,
    BooleanType,
    DateType,
    TimestampType,
    TimestampNTZType,
    TimeType,
    DecimalType,
    DoubleType,
    FloatType,
    ByteType,
    IntegerType,
    LongType,
    ShortType,
    CalendarIntervalType,
    DayTimeIntervalType,
    YearMonthIntervalType,
    VariantType,
    ArrayType,
    MapType,
    StructType,
    StructField,
]

_SUBCLASS_ARGS = {
    "CharType": (5,),
    "VarcharType": (10,),
    "TimeType": (6,),
    "DecimalType": (10, 2),
    "DayTimeIntervalType": (0, 3),
    "YearMonthIntervalType": (0, 1),
    "ArrayType": (IntegerType(),),
    "MapType": (StringType(), IntegerType()),
    "StructType": ([StructField("a", IntegerType())],),
    "StructField": ("f", IntegerType()),
}

_SUBCLASS_GOLDEN: dict[str, dict[str, Any]] = {
    "DataType": {
        "simpleString": "mydata",
        "typeName": "mydata",
        "_engine_type": "mydata",
        "jsonValue": "mydata",
        "toddl_leaf": "f MYDATA",
    },
    "NullType": {
        "simpleString": "void",
        "typeName": "void",
        "_engine_type": "void",
        "jsonValue": "void",
        "toddl_leaf": "f VOID",
    },
    "StringType": {
        "simpleString": "string",
        "typeName": "mystring",
        "_engine_type": "string",
        "jsonValue": "string",
        "toddl_leaf": "f STRING",
    },
    "CharType": {
        "simpleString": "char(5)",
        "typeName": "mychar",
        "_engine_type": "string",
        "jsonValue": "char(5)",
        "toddl_leaf": "f CHAR(5)",
    },
    "VarcharType": {
        "simpleString": "varchar(10)",
        "typeName": "myvarchar",
        "_engine_type": "string",
        "jsonValue": "varchar(10)",
        "toddl_leaf": "f VARCHAR(10)",
    },
    "BinaryType": {
        "simpleString": "mybinary",
        "typeName": "mybinary",
        "_engine_type": "binary",
        "jsonValue": "mybinary",
        "toddl_leaf": "f BINARY",
    },
    "BooleanType": {
        "simpleString": "myboolean",
        "typeName": "myboolean",
        "_engine_type": "boolean",
        "jsonValue": "myboolean",
        "toddl_leaf": "f BOOLEAN",
    },
    "DateType": {
        "simpleString": "mydate",
        "typeName": "mydate",
        "_engine_type": "date",
        "jsonValue": "mydate",
        "toddl_leaf": "f DATE",
    },
    "TimestampType": {
        "simpleString": "mytimestamp",
        "typeName": "mytimestamp",
        "_engine_type": "timestamp",
        "jsonValue": "mytimestamp",
        "toddl_leaf": "f TIMESTAMP",
    },
    "TimestampNTZType": {
        "simpleString": "timestamp_ntz",
        "typeName": "timestamp_ntz",
        "_engine_type": "timestamp_ntz",
        "jsonValue": "timestamp_ntz",
        "toddl_leaf": "f TIMESTAMP_NTZ",
    },
    "TimeType": {
        "simpleString": "time(6)",
        "typeName": "mytime",
        "_engine_type": "time(6)",
        "jsonValue": "time(6)",
        "toddl_leaf": "f TIME(6)",
    },
    "DecimalType": {
        "simpleString": "decimal(10,2)",
        "typeName": "mydecimal",
        "_engine_type": "decimal(10,2)",
        "jsonValue": "decimal(10,2)",
        "toddl_leaf": "f DECIMAL(10,2)",
    },
    "DoubleType": {
        "simpleString": "mydouble",
        "typeName": "mydouble",
        "_engine_type": "double",
        "jsonValue": "mydouble",
        "toddl_leaf": "f DOUBLE",
    },
    "FloatType": {
        "simpleString": "float",
        "typeName": "myfloat",
        "_engine_type": "float",
        "jsonValue": "myfloat",
        "toddl_leaf": "f FLOAT",
    },
    "ByteType": {
        "simpleString": "tinyint",
        "typeName": "mybyte",
        "_engine_type": "byte",
        "jsonValue": "mybyte",
        "toddl_leaf": "f TINYINT",
    },
    "IntegerType": {
        "simpleString": "int",
        "typeName": "myinteger",
        "_engine_type": "int",
        "jsonValue": "myinteger",
        "toddl_leaf": "f INT",
    },
    "LongType": {
        "simpleString": "bigint",
        "typeName": "mylong",
        "_engine_type": "long",
        "jsonValue": "mylong",
        "toddl_leaf": "f BIGINT",
    },
    "ShortType": {
        "simpleString": "smallint",
        "typeName": "myshort",
        "_engine_type": "short",
        "jsonValue": "myshort",
        "toddl_leaf": "f SMALLINT",
    },
    "CalendarIntervalType": {
        "simpleString": "interval",
        "typeName": "interval",
        "_engine_type": "interval",
        "jsonValue": "interval",
        "toddl_leaf": "f INTERVAL",
    },
    "DayTimeIntervalType": {
        "simpleString": "interval day to second",
        "typeName": "mydaytimeinterval",
        "_engine_type": "interval day to second",
        "jsonValue": "interval day to second",
        "toddl_leaf": "f INTERVAL DAY TO SECOND",
    },
    "YearMonthIntervalType": {
        "simpleString": "interval year to month",
        "typeName": "myyearmonthinterval",
        "_engine_type": "interval year to month",
        "jsonValue": "interval year to month",
        "toddl_leaf": "f INTERVAL YEAR TO MONTH",
    },
    "VariantType": {
        "simpleString": "variant",
        "typeName": "myvariant",
        "_engine_type": "variant",
        "jsonValue": "myvariant",
        "toddl_leaf": "f VARIANT",
    },
    "ArrayType": {
        "simpleString": "array<int>",
        "typeName": "myarray",
        "_engine_type": "array<int>",
        "jsonValue": {"type": "myarray", "elementType": "integer", "containsNull": True},
        "toddl_leaf": "f ARRAY<INT>",
    },
    "MapType": {
        "simpleString": "map<string,int>",
        "typeName": "mymap",
        "_engine_type": "map<string,int>",
        "jsonValue": {
            "type": "mymap",
            "keyType": "string",
            "valueType": "integer",
            "valueContainsNull": True,
        },
        "toddl_leaf": "f MAP<STRING,INT>",
    },
    "StructType": {
        "simpleString": "struct<a:int>",
        "typeName": "mystruct",
        "_engine_type": "struct<a:int>",
        "jsonValue": {
            "type": "mystruct",
            "fields": [{"name": "a", "type": "integer", "nullable": True, "metadata": {}}],
        },
        "toddl_leaf": "f STRUCT<a:INT>",
    },
    "StructField": {
        "simpleString": "f:int",
        "typeName": (
            "TypeError",
            "StructField.typeName() missing 1 required positional argument: 'self'",
        ),
        "_engine_type": "f:int",
        "jsonValue": {"name": "f", "type": "integer", "nullable": True, "metadata": {}},
        "toddl_leaf": "f F:INT",
    },
}


def _surface_answer(thunk: Any) -> Any:
    """Answer value or an ``("TypeError", message)`` refusal pair."""
    try:
        return thunk()
    except Exception as error:
        return (type(error).__name__, str(error))


@pytest.mark.parametrize("base", _BASES, ids=[base.__name__ for base in _BASES])
def test_subclass_tokens_match_base_golden(base: type) -> None:
    """Pass-through subclass answers equal base's recorded golden for every surface.

    pins: facade-4/C-021, C-025
    """
    name = base.__name__
    label = name[:-4] if name.endswith("Type") else name
    sub = type(f"My{label}", (base,), {})
    inst = sub(*_SUBCLASS_ARGS.get(name, ()))
    golden = _SUBCLASS_GOLDEN[name]
    measured = {
        "simpleString": _surface_answer(lambda: inst.simpleString()),
        "typeName": _surface_answer(lambda: type(inst).typeName()),
        "_engine_type": _surface_answer(lambda: inst._engine_type()),
        "jsonValue": _surface_answer(lambda: inst.jsonValue()),
        "toddl_leaf": _surface_answer(lambda: StructType([StructField("f", inst)]).toDDL()),
    }
    for surface, expected in golden.items():
        assert measured[surface] == expected, f"{name}.{surface}: {measured[surface]!r}"


_L001_INPUTS = [
    ("decimal(9223372036854775808,0)", "decimal(9223372036854775808,0)"),
    ("char(" + "9" * 40 + ")", "char(" + "9" * 40 + ")"),
    ("decimal(10," + "9" * 40 + ")", "decimal(10," + "9" * 40 + ")"),
    ("varchar(9223372036854775808)", "varchar(9223372036854775808)"),
    ("time(9223372036854775808)", "time(9223372036854775808)"),
    ("decimal(9223372036854775807,0)", "decimal(9223372036854775807,0)"),
    (
        "a: decimal(9223372036854775808,0)",
        "struct<a:decimal(9223372036854775808,0)>",
    ),
    (
        "b decimal(9223372036854775808,0)",
        "struct<b:decimal(9223372036854775808,0)>",
    ),
    (
        "struct<c:decimal(9223372036854775808,0)>",
        "struct<c:decimal(9223372036854775808,0)>",
    ),
]


@pytest.mark.parametrize("text,expected", _L001_INPUTS)
def test_fromddl_integer_params_beyond_i64(text: str, expected: str) -> None:
    """Integer parameters beyond i64 take base's Python parse — unbounded values.

    pins: facade-4/C-020
    """
    assert DataType.fromDDL(text).simpleString() == expected


def _subclass(base: type, label: str) -> type:
    return type(label, (base,), {})


_L003_INPUTS = [
    (_subclass(IntegerType, "MyInt")(), "INT"),
    (_subclass(LongType, "MyLong")(), "BIGINT"),
    (_subclass(ByteType, "MyByte")(), "TINYINT"),
    (_subclass(NullType, "MyNull")(), "VOID"),
    (_subclass(DecimalType, "MyDec")(10, 2), "DECIMAL(10,2)"),
    (ArrayType(_subclass(IntegerType, "MyInt")(), True), "ARRAY<INT>"),
    (
        StructType([StructField("a", _subclass(IntegerType, "MyInt")(), True)]),
        "STRUCT<a:INT>",
    ),
]


@pytest.mark.parametrize("data_type,expected", _L003_INPUTS)
def test_sql_type_leaf_markers_for_subclasses(data_type: Any, expected: str) -> None:
    """Foreign subclasses take base's isinstance SQL marker, not the engine token.

    pins: facade-4/C-022
    """
    from repark.spark.session.create_dataframe_values import _data_type_to_sql_type

    assert _data_type_to_sql_type(data_type) == expected


_L004_INPUTS = [
    ("\x00", "cannot parse datatype: '\\x00'"),
    ("\x01", "cannot parse datatype: '\\x01'"),
    ("\x07", "cannot parse datatype: '\\x07'"),
    ("\x08", "cannot parse datatype: '\\x08'"),
    ("\x0e", "cannot parse datatype: '\\x0e'"),
    ("\x1b", "cannot parse datatype: '\\x1b'"),
    ("x\x1cy", "cannot parse datatype: 'x\\x1cy'"),
    ("\x7f", "cannot parse datatype: '\\x7f'"),
    ("\x80", "cannot parse datatype: '\\x80'"),
    ("\x9f", "cannot parse datatype: '\\x9f'"),
    ("​", "cannot parse datatype: '\\u200b'"),
    ("it's", 'cannot parse datatype: "it\'s"'),
    ('say "hi"', "cannot parse datatype: 'say \"hi\"'"),
    ("both'and\"", "cannot parse datatype: 'both\\'and\"'"),
]


@pytest.mark.parametrize("text,message", _L004_INPUTS)
def test_fromddl_refusal_repr_bytes(text: str, message: str) -> None:
    """``fromDDL`` refusal messages carry Python ``repr`` bytes for every code point.

    pins: facade-4/C-023
    """
    with pytest.raises(ValueError, match="cannot parse datatype") as caught:
        DataType.fromDDL(text)
    assert str(caught.value) == message


def _nested_array(data_type: Any, depth: int) -> Any:
    for _ in range(depth):
        data_type = ArrayType(data_type)
    return data_type


def _nested_pa_list(depth: int) -> Any:
    import pyarrow as pa

    arrow_type = pa.int32()
    for _ in range(depth):
        arrow_type = pa.list_(arrow_type)
    return arrow_type


@pytest.mark.parametrize("depth", [63, 64, 70])
def test_arrow_nesting_depth_ceiling_fallback(depth: int) -> None:
    """Arrow C-Data recursion ceilings fall back to the Python converters.

    pins: facade-4/C-024
    """
    import pyarrow as pa

    arrow_type = repark_type_to_arrow(_nested_array(IntegerType(), depth))
    assert str(arrow_type).count("list<item:") == depth
    inbound = _arrow_type_to_repark(_nested_pa_list(depth))
    assert inbound.simpleString().count("array<") == depth
    schema = pa.schema([pa.field("c", _nested_pa_list(depth))])
    struct = struct_type_from_arrow(schema)
    assert struct.simpleString().count("array<") == depth


def test_wide_decimal_conversions_take_python_fallback() -> None:
    """Decimals outside the FFI envelope keep base's answers through the fallback.

    pins: facade-4/C-028
    """
    import pyarrow as pa

    outbound = repark_type_to_arrow(DecimalType(10, 300))
    assert str(outbound) == "decimal128(10, 300)"
    with pytest.raises(ValueError, match="precision should be between 1 and 38"):
        repark_type_to_arrow(DecimalType(76, 10))
    inbound = _arrow_type_to_repark(pa.decimal256(76, 200))
    assert isinstance(inbound, DecimalType)
    assert (inbound.precision, inbound.scale) == (76, 200)


def test_csv_rung_cast_tokens_agree_with_rust_table() -> None:
    """Rung engine/SQL cast tokens equal the ``rung_to_spark_type`` chain's answers.

    pins: facade-4/C-027
    """
    from repark import _native
    from repark.spark._csv_smart import (
        ColumnResolution,
        rung_to_engine_cast,
        rung_to_spark_type,
        rung_to_sql_cast,
    )

    resolutions = [
        ColumnResolution(rung=rung, fallback_count=0, null_count=0, sample_count=1)
        for rung in ("bool", "int32", "int64", "float64", "date", "timestamp", "unknown")
    ]
    resolutions.append(
        ColumnResolution(
            rung="decimal128",
            fallback_count=0,
            null_count=0,
            sample_count=1,
            decimal_precision=12,
            decimal_scale=3,
        )
    )
    resolutions.append(
        ColumnResolution(
            rung="decimal128",
            fallback_count=0,
            null_count=0,
            sample_count=1,
        )
    )
    for resolution in resolutions:
        engine = rung_to_engine_cast(resolution)
        assert engine == rung_to_spark_type(resolution)._engine_type(), resolution.rung
        assert rung_to_sql_cast(resolution) == _native.csv_sql_cast_token(
            rung_to_spark_type(resolution)._engine_type()
        ), resolution.rung
