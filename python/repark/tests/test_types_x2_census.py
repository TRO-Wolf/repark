"""X2 / R-CENSUS-TYPES — Row repr factory + createDataFrame nested / LongType schema.

Pins Apache ``test_types`` unblocks: Row empty/unnamed repr, factory arity, LongType in
StructType schema, nested list/dict/Row via createDataFrame (no Python row compute — Arrow
path only).
"""

from __future__ import annotations

import pytest

from repark.spark.row import Row
from repark.spark.session import ReparkSession
from repark.spark.types import IntegerType, LongType, StringType, StructField, StructType

# Row factory / repr (Apache DataTypeTests)


def test_row_without_column_name_repr() -> None:
    """Unnamed positional value rows use angle-bracket repr (SPARK-23299)."""
    assert repr(Row("Alice", 11)) == "<Row('Alice', 11)>"
    assert repr(Row("数", "量")) == "<Row('数', '量')>"


def test_row_repr_with_empty_row() -> None:
    """Empty factory / empty value row nested repr (SPARK-44643)."""
    assert repr(Row(a=Row())) == "Row(a=<Row()>)"
    assert repr(Row(Row())) == "<Row(<Row()>)>"
    empty_row = Row()
    assert repr(Row(a=empty_row())) == "Row(a=Row())"
    assert repr(Row(empty_row())) == "<Row(Row())>"


def test_row_factory_arity_raises_value_error() -> None:
    """Factory with wrong value count raises ValueError (Apache test_invalid_create_row)."""
    row_class = Row("c1", "c2")
    with pytest.raises(ValueError):
        row_class(1, 2, 3)


def test_empty_row_len() -> None:
    """``Row()`` factory has length 0 (Apache test_empty_row)."""
    assert len(Row()) == 0


# createDataFrame LongType + nested


def test_create_dataframe_long_type_schema() -> None:
    """LongType schema field is supported; values coerce list→str for StringType column."""
    spark = ReparkSession.builder.master("local[1]").appName("x2-long").getOrCreate()
    try:
        data = [[[123], 120]]
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("income", LongType(), True),
            ]
        )
        frame = spark.createDataFrame(data, schema)
        assert frame.schema.fields[1].dataType == LongType()
        assert frame.count() == 1
        head = frame.head()
        assert head.income == 120
        # Spark converts non-string cell to string for StringType column.
        assert head.name == "[123]"
    finally:
        spark.stop()


def test_create_dataframe_nested_list_struct_map() -> None:
    """Nested list / Row struct / dict map materialize via createDataFrame (X2 unblock)."""
    spark = ReparkSession.builder.master("local[1]").appName("x2-nested").getOrCreate()
    try:
        frame = spark.createDataFrame([Row(l=[1], r=Row(a=1, b="b"), d={"k": "v"})])
        assert frame.count() == 1
        row = frame.collect()[0]
        assert list(row.l) == [1]
        # Struct may collect as dict (Arrow) or Row — both carry field a=1.
        nested = row.r
        if isinstance(nested, dict):
            assert nested["a"] == 1 and nested["b"] == "b"
        else:
            assert nested.a == 1 and nested.b == "b"
        map_cell = row.d
        if isinstance(map_cell, dict):
            assert map_cell["k"] == "v"
        else:
            assert dict(map_cell)["k"] == "v"
    finally:
        spark.stop()


def test_create_dataframe_list_of_ints_not_fixed_size_ml_vector() -> None:
    """Plain int lists are variable arrays — empty+nonempty must not hit ML fixed-width error."""
    spark = ReparkSession.builder.master("local[1]").appName("x2-arr").getOrCreate()
    try:
        frame = spark.createDataFrame([Row(f1=[]), Row(f1=[1])])
        assert frame.count() == 2
        values = [list(row.f1) if row.f1 is not None else [] for row in frame.collect()]
        assert values == [[], [1]]
    finally:
        spark.stop()


def test_struct_field_metadata_create_dataframe() -> None:
    """StructField metadata (incl. None) accepted by constructor (Apache test_metadata_null)."""
    spark = ReparkSession.builder.master("local[1]").appName("x2-meta").getOrCreate()
    try:
        schema = StructType(
            [
                StructField("f1", StringType(), True, None),
                StructField("f2", StringType(), True, {"a": None}),
            ]
        )
        frame = spark.createDataFrame([["a", "b"], ["c", "d"]], schema)
        assert frame.count() == 2
    finally:
        spark.stop()


def test_from_ddl_array_struct() -> None:
    from repark.spark.types import ArrayType, DataType, DoubleType

    assert DataType.fromDDL("array<int>") == ArrayType(IntegerType())
    assert DataType.fromDDL("struct<a:string,b:array<long>>") == StructType(
        [
            StructField("a", StringType()),
            StructField("b", ArrayType(LongType())),
        ]
    )
    # Bare VARCHAR is string (nested engine markers).
    assert DataType.fromDDL("varchar") == StringType()
    assert DataType.fromDDL("array<varchar>") == ArrayType(StringType())
    _ = DoubleType  # silence if unused in future edits


def test_create_dataframe_explicit_nested_struct_with_string() -> None:
    """StructType nested fields with StringType must stay struct — not stringify."""
    from repark.spark.types import ArrayType

    spark = ReparkSession.builder.master("local[1]").appName("x2-nested-schema").getOrCreate()
    try:
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField(
                    "items",
                    ArrayType(
                        StructType(
                            [
                                StructField("a", IntegerType()),
                                StructField("b", StringType()),
                            ]
                        )
                    ),
                ),
            ]
        )
        frame = spark.createDataFrame(
            [(1, [Row(a=1, b="x"), Row(a=2, b="y")])],
            schema,
        )
        arrow = frame.to_arrow()
        assert str(arrow.schema.field("items").type).startswith("list")
        assert "string" not in str(arrow.schema.field("items").type).split("item:")[0]
        values = arrow.to_pydict()
        assert values["id"] == [1]
        assert values["items"] == [[{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]]
        # Field-order remap: kwargs order b then a still binds by name.
        frame2 = spark.createDataFrame([Row(s=Row(b="z", a=9))], "s STRUCT<a:INT,b:STRING>")
        assert frame2.to_arrow().to_pydict()["s"] == [{"a": 9, "b": "z"}]
    finally:
        spark.stop()


def test_create_dataframe_explicit_map_and_array_string() -> None:
    """MapType / ArrayType(StringType) explicit schema must not collapse to string."""
    from repark.spark.types import ArrayType, MapType

    spark = ReparkSession.builder.master("local[1]").appName("x2-map-arr").getOrCreate()
    try:
        map_schema = StructType([StructField("m", MapType(StringType(), IntegerType()))])
        map_frame = spark.createDataFrame([({"k": 1, "z": 2},)], map_schema)
        map_arrow = map_frame.to_arrow()
        assert "map" in str(map_arrow.schema.field("m").type)
        map_values = map_arrow.to_pydict()["m"][0]
        assert dict(map_values) == {"k": 1, "z": 2}

        array_schema = StructType([StructField("a", ArrayType(StringType()))])
        # Non-string elements coerce element-wise (Spark to_str), not whole-array str().
        array_frame = spark.createDataFrame([([1, 2, 3],)], array_schema)
        assert array_frame.to_arrow().to_pydict()["a"] == [["1", "2", "3"]]
    finally:
        spark.stop()


def test_create_dataframe_ddl_nested_array() -> None:
    """DDL schema field list accepts nested array/map/struct types."""
    spark = ReparkSession.builder.master("local[1]").appName("x2-ddl-nested").getOrCreate()
    try:
        frame = spark.createDataFrame([([1, 2],)], "a ARRAY<INT>")
        assert frame.to_arrow().to_pydict()["a"] == [[1, 2]]
        frame_colon = spark.createDataFrame([([3],)], "a: array<int>")
        assert frame_colon.to_arrow().to_pydict()["a"] == [[3]]
    finally:
        spark.stop()


def test_create_dataframe_map_int_keys_inferred() -> None:
    """Inferred map keys follow sample key type (not always string)."""
    spark = (
        ReparkSession.builder.master("local[1]")
        .appName("x2-map-keys")
        # FA-4: repark defaults inferNestedDictAsStruct to true; this pin is the MAP path.
        .config("spark.sql.pyspark.inferNestedDictAsStruct.enabled", "false")
        .getOrCreate()
    )
    try:
        frame = spark.createDataFrame([Row(m={1: "a", 2: "b"})])
        arrow = frame.to_arrow()
        assert "int" in str(arrow.schema.field("m").type).lower()
        pairs = dict(arrow.to_pydict()["m"][0])
        assert pairs == {1: "a", 2: "b"}
    finally:
        spark.stop()


def test_create_dataframe_tuple_as_struct_positional() -> None:
    """Tuple cells bind to StructType fields positionally."""
    spark = ReparkSession.builder.master("local[1]").appName("x2-tuple-struct").getOrCreate()
    try:
        schema = StructType(
            [
                StructField(
                    "s",
                    StructType(
                        [
                            StructField("a", IntegerType()),
                            StructField("b", StringType()),
                        ]
                    ),
                )
            ]
        )
        frame = spark.createDataFrame([((1, "hi"),)], schema)
        assert frame.to_arrow().to_pydict()["s"] == [{"a": 1, "b": "hi"}]
        # List-of-dict rows (not a 1-tuple wrapping a row-dict).
        dict_frame = spark.createDataFrame([{"s": {"a": 2, "b": "yo"}}], schema)
        assert dict_frame.to_arrow().to_pydict()["s"] == [{"a": 2, "b": "yo"}]
        # Nested dict cell under a tuple row.
        nested_dict = spark.createDataFrame([({"a": 3, "b": "z"},)], schema)
        assert nested_dict.to_arrow().to_pydict()["s"] == [{"a": 3, "b": "z"}]
    finally:
        spark.stop()


def test_create_dataframe_sparse_vector_dict_exact_keys() -> None:
    """Sparse ML dict needs exact key set; extra keys stay map."""
    spark = (
        ReparkSession.builder.master("local[1]")
        .appName("x2-sparse")
        # FA-4: repark defaults inferNestedDictAsStruct to true; the boundary (extra
        # key falls back to MAP, not sparse struct) is a PySpark-default-path pin.
        .config("spark.sql.pyspark.inferNestedDictAsStruct.enabled", "false")
        .getOrCreate()
    )
    try:
        sparse = spark.createDataFrame(
            [Row(v={"size": 3, "indices": [0, 2], "values": [1.0, 2.0]})]
        )
        assert "struct" in str(sparse.to_arrow().schema.field("v").type)
        # Extra key → plain map, not sparse struct (homogeneous string values).
        mapped = spark.createDataFrame(
            [Row(v={"size": "1", "indices": "[]", "values": "[]", "note": "x"})]
        )
        assert "map" in str(mapped.to_arrow().schema.field("v").type)
        # Wrong value shapes for sparse keys → map, not silent struct.
        not_sparse = spark.createDataFrame([Row(v={"size": "3", "indices": "x", "values": "y"})])
        assert "map" in str(not_sparse.to_arrow().schema.field("v").type)
    finally:
        spark.stop()
