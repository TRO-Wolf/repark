"""R-PARITY3 — createDataFrame(schema=StructType/DDL) + show(vertical=True).

Oracle: live PySpark 4.1.2 (zulu-17), 2026-07-29.
"""

from __future__ import annotations

import io
from contextlib import redirect_stdout

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark.types import IntegerType, StringType, StructField, StructType


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-parity3").getOrCreate()


def test_create_dataframe_struct_type_preserves_int32(spark: ReparkSession) -> None:
    schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
    frame = spark.createDataFrame([(1, "x"), (2, "y")], schema=schema)
    table = frame.to_arrow()
    assert table.schema.field("a").type == pa.int32()
    assert table.column("a").to_pylist() == [1, 2]
    assert table.column("b").to_pylist() == ["x", "y"]


def test_create_dataframe_ddl_string_schema(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1, "x")], schema="a INT, b STRING")
    table = frame.to_arrow()
    assert table.schema.field("a").type == pa.int32()
    assert table.to_pylist() == [{"a": 1, "b": "x"}]


def test_show_vertical_true_record_layout(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1, "x"), (2, "y")], ["a", "b"])
    buf = io.StringIO()
    with redirect_stdout(buf):
        frame.show(vertical=True)
    text = buf.getvalue()
    assert "-RECORD 0" in text
    assert "-RECORD 1" in text
    assert "a" in text and "1" in text
    assert "b" in text and "x" in text
    assert "only showing" not in text


def test_show_vertical_only_showing_top_n(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1, "x"), (2, "y")], ["a", "b"])
    buf = io.StringIO()
    with redirect_stdout(buf):
        frame.show(1, vertical=True)
    text = buf.getvalue()
    assert "-RECORD 0" in text
    assert "-RECORD 1" not in text
    assert "only showing top 1 row" in text
