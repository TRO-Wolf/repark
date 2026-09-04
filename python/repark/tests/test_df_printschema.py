"""DF-PRINTSCHEMA-1 — printSchema stdout is byte-identical to Spark's."""

from __future__ import annotations

import io
from contextlib import redirect_stdout

import pytest

from repark import ReparkSession


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-df-printschema").getOrCreate()
    yield session
    session.stop()


def _shapes(spark: ReparkSession) -> dict[str, object]:
    return {
        "flat": spark.createDataFrame([("a", 1, 10.0)], ["g", "k", "v"]),
        "nested": spark.createDataFrame([(1, (2, "x"))], ["a", "b"]),
        "array": spark.createDataFrame([([1, 2],)], ["a"]),
    }


def test_flat_schema_stdout_ends_with_blank_line(spark: ReparkSession) -> None:
    """Flat tree plus Spark's blank line. pins: df-printschema-1-trailing-newline/C-001, C-002"""
    frame = _shapes(spark)["flat"]
    buf = io.StringIO()
    with redirect_stdout(buf):
        frame.printSchema()
    assert buf.getvalue() == (
        "root\n"
        " |-- g: string (nullable = true)\n"
        " |-- k: long (nullable = true)\n"
        " |-- v: double (nullable = true)\n"
        "\n"
    )


def test_nested_struct_stdout_ends_with_blank_line(spark: ReparkSession) -> None:
    """Nested struct tree plus Spark's blank line. pins: df-printschema-1-trailing-newline/C-001, C-002"""
    frame = _shapes(spark)["nested"]
    buf = io.StringIO()
    with redirect_stdout(buf):
        frame.printSchema()
    assert buf.getvalue() == (
        "root\n"
        " |-- a: long (nullable = true)\n"
        " |-- b: struct (nullable = true)\n"
        " |    |-- _1: long (nullable = true)\n"
        " |    |-- _2: string (nullable = true)\n"
        "\n"
    )


def test_array_column_stdout_ends_with_blank_line(spark: ReparkSession) -> None:
    """Array tree plus Spark's blank line. pins: df-printschema-1-trailing-newline/C-001, C-002"""
    frame = _shapes(spark)["array"]
    buf = io.StringIO()
    with redirect_stdout(buf):
        frame.printSchema()
    assert buf.getvalue() == (
        "root\n"
        " |-- a: array (nullable = true)\n"
        " |    |-- element: long (containsNull = true)\n"
        "\n"
    )


def test_level_1_stdout_ends_with_blank_line(spark: ReparkSession) -> None:
    """Level-1 truncated tree plus Spark's blank line. pins: df-printschema-1-trailing-newline/C-001, C-002"""
    frame = _shapes(spark)["nested"]
    buf = io.StringIO()
    with redirect_stdout(buf):
        frame.printSchema(1)
    assert buf.getvalue() == (
        "root\n"
        " |-- a: long (nullable = true)\n"
        " |-- b: struct (nullable = true)\n"
        "\n"
    )
