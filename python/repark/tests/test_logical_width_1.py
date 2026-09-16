"""LOGICAL-WIDTH-1 — Spark's logical widths on the facade, both doors.

Every pin is driven from a named cell of the live PySpark 4.1.2 recording in
``facade_logical_width_oracle.json`` (run 17b, 2026-09-15). Nullability is out of
scope (W-7): pins assert names, type labels and values, never ``nullable``.
"""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.sql import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.types import (
    BinaryType,
    ByteType,
    FloatType,
    ShortType,
    StructField,
    StructType,
)

_ORACLE = json.loads((Path(__file__).parent / "facade_logical_width_oracle.json").read_text())

_DDL = "sh smallint, ti tinyint, f float, d double, b binary, s string, i int, l bigint"
_ROWS = [
    (3, 1, 1.5, 2.5, b"ab", "x", 7, 8),
    (None, None, None, None, None, None, None, None),
]


def _cell(name: str) -> dict:
    """Return the recorded PySpark 4.1.2 oracle cell."""
    return _ORACLE[name]


@pytest.fixture
def spark() -> ReparkSession:
    """Fresh session per test."""
    session = ReparkSession.builder.appName("pytest-logical-width-1").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def wide(spark: ReparkSession) -> object:
    """Two-row frame carrying all four narrow widths plus the wide controls."""
    return spark.createDataFrame(_ROWS, _DDL)


def _print_schema(frame: object) -> str:
    """Capture ``printSchema`` stdout."""
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        frame.printSchema()
    return buffer.getvalue()


def test_ddl_string_reports_narrow_widths_logical_width_1(
    wide: object,
) -> None:
    """pins: logical-width-1/C-001 — cell ddl_schema."""
    result = _cell("ddl_schema")["result"]
    assert wide.dtypes == [tuple(pair) for pair in result["dtypes"]]
    assert wide.schema.simpleString() == result["simpleString"]
    assert [field.dataType.typeName() for field in wide.schema.fields] == result["json_types"]
    assert _print_schema(wide) == result["printSchema"]


def test_schema_json_reports_narrow_type_names_logical_width_1(wide: object) -> None:
    """pins: logical-width-1/C-001 — cell schema_json."""
    assert wide.schema.json() == _cell("schema_json")["result"]


def test_struct_type_reports_narrow_widths_logical_width_1(spark: ReparkSession) -> None:
    """pins: logical-width-1/C-001 — cell struct_schema."""
    frame = spark.createDataFrame(
        [(3, 1, 1.5, b"ab")],
        StructType(
            [
                StructField("sh", ShortType()),
                StructField("ti", ByteType()),
                StructField("f", FloatType()),
                StructField("b", BinaryType()),
            ]
        ),
    )
    result = _cell("struct_schema")["result"]
    assert frame.dtypes == [tuple(pair) for pair in result["dtypes"]]
    assert frame.schema.simpleString() == result["simpleString"]
    assert _print_schema(frame) == result["printSchema"]


def test_inference_reports_bytes_as_binary_logical_width_1(spark: ReparkSession) -> None:
    """pins: logical-width-1/C-005 — cell infer_schema."""
    frame = spark.createDataFrame([(1, 1.5, b"ab", True, "s")], ["i", "f", "b", "t", "s"])
    result = _cell("infer_schema")["result"]
    assert frame.dtypes == [tuple(pair) for pair in result["dtypes"]]
    assert frame.schema.simpleString() == result["simpleString"]
    assert _print_schema(frame) == result["printSchema"]


def test_nested_widths_stay_narrow_logical_width_1(spark: ReparkSession) -> None:
    """pins: logical-width-1/C-005 — cell nested_schema (green before, guard)."""
    frame = spark.createDataFrame(
        [((3, 1.5, b"ab"), [1], {"k": 1.5})],
        "s struct<sh:smallint,f:float,b:binary>, arr array<smallint>, m map<string,float>",
    )
    result = _cell("nested_schema")["result"]
    assert frame.dtypes == [tuple(pair) for pair in result["dtypes"]]
    assert frame.schema.simpleString() == result["simpleString"]
    assert _print_schema(frame) == result["printSchema"]


def test_collect_value_types_and_values_logical_width_1(wide: object) -> None:
    """pins: logical-width-1/C-005 — cells ddl_collect_types, ddl_collect_values (guards)."""
    assert [[type(value).__name__ for value in row] for row in wide.collect()] == _cell(
        "ddl_collect_types"
    )["result"]
    assert [[repr(value) for value in row] for row in wide.collect()] == _cell(
        "ddl_collect_values"
    )["result"]


def test_sql_door_reports_narrow_widths_logical_width_1(spark: ReparkSession) -> None:
    """pins: logical-width-1/C-002 — cell sql_cast."""
    frame = spark.sql(
        "SELECT CAST(1 AS SMALLINT) a, CAST(1 AS TINYINT) b, CAST(1.5 AS FLOAT) c, X'6162' d"
    )
    result = _cell("sql_cast")["result"]
    assert frame.dtypes == [tuple(pair) for pair in result["dtypes"]]
    assert frame.schema.simpleString() == result["simpleString"]
    assert [field.dataType.typeName() for field in frame.schema.fields] == result["json_types"]
    assert _print_schema(frame) == result["printSchema"]


def test_sql_door_collects_narrow_values_logical_width_1(spark: ReparkSession) -> None:
    """pins: logical-width-1/C-002 — cell sql_describe."""
    rows = spark.sql("SELECT CAST(1 AS SMALLINT) a, CAST(1.5 AS FLOAT) c, X'6162' d").collect()
    assert [
        [value if not isinstance(value, bytes) else repr(value) for value in row] for row in rows
    ] == _cell("sql_describe")["result"]


def test_python_cast_keeps_narrow_widths_logical_width_1(spark: ReparkSession) -> None:
    """pins: logical-width-1/C-003 — cells sql_cast, ddl_schema (CAST spelling rule)."""
    frame = spark.range(1).select(
        F.col("id").cast("smallint").alias("sh"),
        F.col("id").cast("tinyint").alias("ti"),
        F.col("id").cast("float").alias("f"),
    )
    assert frame.dtypes == [("sh", "smallint"), ("ti", "tinyint"), ("f", "float")]
    assert frame.schema.simpleString() == "struct<sh:smallint,ti:tinyint,f:float>"


def test_arith_keeps_spark_widths_logical_width_1(wide: object) -> None:
    """pins: logical-width-1/C-003 — cell arith_width (st, fd)."""
    frame = wide.select(
        (F.col("sh") + F.col("ti")).alias("st"),
        (F.col("f") + F.col("d")).alias("fd"),
    )
    assert frame.dtypes == [("st", "smallint"), ("fd", "double")]
    assert frame.schema.simpleString() == "struct<st:smallint,fd:double>"


def test_float_times_int_literal_divergence_arith_float_int_1(wide: object) -> None:
    """pins: ARITH-FLOAT-INT-1 (BACKLOG) — Spark cell arith_width records f2 double.

    The engine answers float32 for ``float * int``; Spark widens float-by-int to
    double. This pin holds the tree answer and reds when the coercion lands.
    """
    frame = wide.select((F.col("f") * 2).alias("f2"))
    assert frame.dtypes == [("f2", "float")]
    assert _cell("arith_width")["result"]["dtypes"][1] == ["f2", "double"]


def test_agg_keeps_spark_widths_logical_width_1(wide: object) -> None:
    """pins: logical-width-1/C-003 — cell agg_width."""
    frame = wide.select(
        F.sum("sh").alias("s"),
        F.avg("f").alias("a"),
        F.max("ti").alias("m"),
        F.min("f").alias("n"),
    )
    result = _cell("agg_width")["result"]
    assert frame.dtypes == [tuple(pair) for pair in result["dtypes"]]
    assert frame.schema.simpleString() == result["simpleString"]


def test_union_keeps_narrow_widths_logical_width_1(wide: object) -> None:
    """pins: logical-width-1/C-003 — cell union_width."""
    frame = wide.union(wide)
    result = _cell("union_width")["result"]
    assert frame.dtypes == [tuple(pair) for pair in result["dtypes"]]
    assert frame.schema.simpleString() == result["simpleString"]


def test_fillna_keeps_narrow_widths_logical_width_1(wide: object) -> None:
    """pins: logical-width-1/C-004 — cells fillna_width, fillna_values."""
    filled = wide.fillna(0)
    result = _cell("fillna_width")["result"]
    assert filled.dtypes == [tuple(pair) for pair in result["dtypes"]]
    assert filled.schema.simpleString() == result["simpleString"]
    assert [[repr(value) for value in row] for row in filled.collect()] == _cell("fillna_values")[
        "result"
    ]


def test_fillna_float_value_keeps_column_widths_logical_width_1(wide: object) -> None:
    """pins: logical-width-1/C-004 — tree-measured invariant (no oracle cell).

    Spark casts the fill literal to the column type, so a fill never changes the
    schema; only the fillna(0) shape carries a recorded cell.
    """
    assert wide.fillna(1.5).dtypes == wide.dtypes


def test_parquet_roundtrip_keeps_narrow_widths_logical_width_1(
    wide: object, spark: ReparkSession, tmp_path: Path
) -> None:
    """pins: logical-width-1/C-006 — cell write_read_parquet."""
    path = str(tmp_path / "wide.parquet")
    wide.write.mode("overwrite").parquet(path)
    reread = spark.read.parquet(path)
    result = _cell("write_read_parquet")["result"]
    assert reread.dtypes == [tuple(pair) for pair in result["dtypes"]]
    assert reread.schema.simpleString() == result["simpleString"]


def test_iceberg_roundtrip_matches_spark_boundary_logical_width_1(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """pins: logical-width-1/C-007 — Spark cells iceberg_schema, iceberg_rows, iceberg_desc."""
    spark.register_memory_catalog("hc", tmp_path)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hc.ns")
    spark.sql("DROP TABLE IF EXISTS hc.ns.w2")
    spark.sql(
        "CREATE TABLE hc.ns.w2 (id BIGINT, sh SMALLINT, ti TINYINT, price FLOAT, b BINARY,"
        " dprice DOUBLE) USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(
        "INSERT INTO hc.ns.w2 VALUES (1, CAST(3 AS SMALLINT), CAST(1 AS TINYINT),"
        " CAST(1.5 AS FLOAT), X'6162', 2.5), (2, NULL, NULL, NULL, NULL, NULL)"
    )
    table = spark.read.table("hc.ns.w2")
    assert table.dtypes == [
        ("id", "bigint"),
        ("sh", "int"),
        ("ti", "int"),
        ("price", "float"),
        ("b", "binary"),
        ("dprice", "double"),
    ]
    assert table.schema.simpleString() == (
        "struct<id:bigint,sh:int,ti:int,price:float,b:binary,dprice:double>"
    )
    spark_schema = _cell("iceberg_schema")["result"]
    spark_by_name = {pair[0]: pair[1] for pair in spark_schema["dtypes"]}
    for name, dtype in table.dtypes:
        assert spark_by_name[name] == dtype
    narrow = table.orderBy("id").select("id", "sh", "ti", "price", "b")
    assert [[repr(value) for value in row] for row in narrow.collect()] == _cell("iceberg_rows")[
        "result"
    ]
    spark_desc = {row[0]: row[1] for row in _cell("iceberg_desc")["result"]}
    for row in spark.sql("DESCRIBE TABLE hc.ns.w2").collect():
        assert spark_desc[row[0]] == row[1]


def test_lit_bytes_refuses_df_lit_binary_1(spark: ReparkSession) -> None:
    """pins: logical-width-1/C-008 — BACKLOG guard (Spark target: cell lit_width result)."""
    from repark.errors import PySparkTypeError

    with pytest.raises(PySparkTypeError) as caught:
        spark.range(1).select(F.lit(1.5).alias("f"), F.lit(b"ab").alias("b"))
    assert str(caught.value) == (
        "lit() supports None, bool, int, float, str, date, datetime, time, list, tuple, "
        "ndarray, Decimal, or Enum; got bytes"
    )
    assert _cell("lit_width")["result"]["dtypes"] == [["f", "double"], ["b", "binary"]]


def test_cast_bigint_to_binary_refuses_under_ansi_logical_width_1(
    spark: ReparkSession,
) -> None:
    """pins: logical-width-1/C-009 — cell cast_schema (BL-11 regression guard)."""
    with pytest.raises(AnalysisException) as caught:
        spark.range(1).select(
            F.col("id").cast("smallint").alias("sh"),
            F.col("id").cast("tinyint").alias("ti"),
            F.col("id").cast("float").alias("f"),
            F.col("id").cast("binary").alias("b"),
        ).collect()
    assert _cell("cast_schema")["error"]["condition"] in str(caught.value)
    assert 'cannot cast "BIGINT" to "BINARY" with ANSI mode on' in str(caught.value)
    assert "\"spark.sql.ansi.enabled\" as 'false'" in str(caught.value)


def test_typename_spellings_logical_width_1() -> None:
    """pins: logical-width-1/C-001 — cell typename_strings."""
    from repark.spark.types import (
        BinaryType,
        ByteType,
        DoubleType,
        FloatType,
        IntegerType,
        ShortType,
    )

    result = _cell("typename_strings")["result"]
    assert ShortType().simpleString() == result["ShortType"]
    assert ByteType().simpleString() == result["ByteType"]
    assert FloatType().simpleString() == result["FloatType"]
    assert BinaryType().simpleString() == result["BinaryType"]
    assert DoubleType().simpleString() == result["DoubleType"]
    assert IntegerType().simpleString() == result["IntegerType"]


_R3_ROWS = [
    (1, 3, 1, 1.5, 2.5, b"x"),
    (1, 4, 2, 2.25, 0.5, b"y"),
]
_R3_DDL = "g int, sh smallint, ti tinyint, f float, d double, b binary"


@pytest.fixture
def narrow_mixed(spark: ReparkSession) -> object:
    """Two-row frame with smallint/tinyint/float measures plus wide controls."""
    return spark.createDataFrame(_R3_ROWS, _R3_DDL)


def _assert_cell_frame(frame: object, name: str) -> None:
    """Assert columns, dtypes and collected rows against the named Spark cell."""
    result = _cell(name)["result"]
    assert frame.columns == result["columns"]
    assert frame.dtypes == [tuple(pair) for pair in result["dtypes"]]
    assert sorted(repr(tuple(row)) for row in frame.collect()) == sorted(result["rows"])


def test_grouped_sum_noargs_keeps_narrow_widths_logical_width_1(
    narrow_mixed: object,
) -> None:
    """pins: logical-width-1/C-013 — cell grouped_sum_noargs."""
    _assert_cell_frame(narrow_mixed.groupBy("g").sum(), "grouped_sum_noargs")


def test_grouped_avg_noargs_keeps_narrow_widths_logical_width_1(
    narrow_mixed: object,
) -> None:
    """pins: logical-width-1/C-013 — cell grouped_avg_noargs."""
    _assert_cell_frame(narrow_mixed.groupBy("g").avg(), "grouped_avg_noargs")


def test_grouped_mean_noargs_keeps_narrow_widths_logical_width_1(
    narrow_mixed: object,
) -> None:
    """pins: logical-width-1/C-013 — cell grouped_mean_noargs."""
    _assert_cell_frame(narrow_mixed.groupBy("g").mean(), "grouped_mean_noargs")


def test_grouped_min_noargs_keeps_narrow_widths_logical_width_1(
    narrow_mixed: object,
) -> None:
    """pins: logical-width-1/C-013 — cell grouped_min_noargs."""
    _assert_cell_frame(narrow_mixed.groupBy("g").min(), "grouped_min_noargs")


def test_grouped_max_noargs_keeps_narrow_widths_logical_width_1(
    narrow_mixed: object,
) -> None:
    """pins: logical-width-1/C-013 — cell grouped_max_noargs."""
    _assert_cell_frame(narrow_mixed.groupBy("g").max(), "grouped_max_noargs")


def test_describe_narrow_matches_spark_logical_width_1(narrow_mixed: object) -> None:
    """pins: logical-width-1/C-014 — cell describe_narrow (regression guard)."""
    _assert_cell_frame(narrow_mixed.describe(), "describe_narrow")


def test_summary_narrow_matches_spark_logical_width_1(narrow_mixed: object) -> None:
    """pins: logical-width-1/C-014 — cell summary_narrow (regression guard)."""
    _assert_cell_frame(
        narrow_mixed.select("sh", "ti", "f").summary("count", "min", "max"),
        "summary_narrow",
    )


def test_fillna_float_keeps_narrow_widths_logical_width_1(spark: ReparkSession) -> None:
    """pins: logical-width-1/C-014 — cell na_fill_float_col (regression guard)."""
    frame = spark.createDataFrame([(None, None)], "f float, sh smallint")
    _assert_cell_frame(frame.fillna(1.7), "na_fill_float_col")


def test_replace_narrow_matches_spark_logical_width_1(spark: ReparkSession) -> None:
    """pins: logical-width-1/C-014 — cell na_replace_narrow (regression guard)."""
    frame = spark.createDataFrame([(1, 1.5)], "sh smallint, f float")
    _assert_cell_frame(frame.replace(1, 9), "na_replace_narrow")
