"""DOOR-CONVERGE-1 pins: both doors answer PySpark 4.1.2 with one kernel.

pins: door-converge-1/C-001, door-converge-1/C-002, door-converge-1/C-003,
door-converge-1/C-004, door-converge-1/C-005, door-converge-1/C-006,
door-converge-1/C-007, door-converge-1/C-008, door-converge-1/C-010,
door-converge-1/C-011, door-converge-1/C-012, door-converge-1/C-013,
door-converge-1/C-014
"""

from __future__ import annotations

import pyarrow
import pytest

from repark.errors import AnalysisException, PySparkException
from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812

_CHUNKED_X = (
    "eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4"
    "\r\n"
    "eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eA=="
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-door-converge-1").getOrCreate()
    yield session
    session.stop()


def _sql_field(spark: ReparkSession, query: str) -> tuple[pyarrow.DataType, bool, list]:
    """Return (arrow type, nullable, values) of the first column of a spark.sql answer."""
    table = spark.sql(query).to_arrow()
    field = table.schema.field(0)
    return field.type, field.nullable, table.column(0).to_pylist()


def test_c001_base64_pads_and_chunks_on_both_doors(spark: ReparkSession) -> None:
    """Oracle BL17-0..7, DIV-api-base64-long: padded, 76-char CRLF-chunked, both doors."""
    assert _sql_field(spark, "SELECT base64('Spark') AS b") == (
        pyarrow.string(),
        False,
        ["U3Bhcms="],
    )
    assert _sql_field(spark, "SELECT base64('A') AS b")[2] == ["QQ=="]
    assert _sql_field(spark, "SELECT base64('Apache') AS b")[2] == ["QXBhY2hl"]
    assert _sql_field(spark, "SELECT base64('') AS b")[2] == [""]
    assert _sql_field(spark, "SELECT base64(NULL) AS b") == (
        pyarrow.string(),
        True,
        [None],
    )
    assert _sql_field(spark, "SELECT base64(X'00FF') AS b")[2] == ["AP8="]
    chunked = _sql_field(spark, "SELECT base64(repeat('x', 100)) AS b")
    assert (chunked[0], chunked[2]) == (pyarrow.string(), [_CHUNKED_X])
    frame = spark.createDataFrame([("Spark",), ("A",)], ["s"])
    table = frame.select(F.base64("s").alias("b")).to_arrow()
    assert table.column("b").to_pylist() == ["U3Bhcms=", "QQ=="]
    long_frame = spark.createDataFrame([("x" * 100,)], ["s"])
    assert long_frame.select(F.base64("s").alias("b")).to_arrow().column("b").to_pylist() == [
        _CHUNKED_X
    ]


def test_c001_unbase64_lenient_decode_on_both_doors(spark: ReparkSession) -> None:
    """Oracle DIV-unbase64-0..3: binary out, unpadded input, skips CRLF, '!!' -> b''."""
    assert _sql_field(spark, "SELECT unbase64('U3Bhcms=') AS v") == (
        pyarrow.binary(),
        False,
        [b"Spark"],
    )
    assert _sql_field(spark, "SELECT unbase64('U3Bhcms') AS v")[2] == [b"Spark"]
    assert _sql_field(spark, "SELECT unbase64('!!') AS v")[2] == [b""]
    assert _sql_field(spark, "SELECT cast(unbase64('eHh4\\r\\neHh4') as string) AS v")[2] == [
        "xxxxxx"
    ]
    frame = spark.createDataFrame([("U3Bhcms=",), ("U3Bhcms",), ("!!",)], ["s"])
    assert frame.select(F.unbase64("s").alias("v")).to_arrow().column("v").to_pylist() == [
        b"Spark",
        b"Spark",
        b"",
    ]


def test_l001_unbase64_strict_endings_on_both_doors(spark: ReparkSession) -> None:
    """Oracle C6-unb64-*: Java MIME decoder refuses bad endings, stays lenient."""
    for query, fragment in (
        ("SELECT unbase64('QQ==QQ') AS v", "incorrect ending byte at 5"),
        ("SELECT unbase64('U3Bhcms=QQ') AS v", "incorrect ending byte at 9"),
        ("SELECT unbase64('QQ=') AS v", "wrong 4-byte ending unit"),
        ("SELECT unbase64('=QQ') AS v", "wrong 4-byte ending unit"),
        ("SELECT unbase64('Q') AS v", "Last unit does not have enough valid bits"),
    ):
        with pytest.raises(PySparkException, match=fragment):
            spark.sql(query).collect()
    assert _sql_field(spark, "SELECT unbase64('QR') AS v")[2] == [b"A"]
    assert _sql_field(spark, "SELECT unbase64('QQQ') AS v")[2] == [b"A\x04"]
    assert _sql_field(spark, "SELECT unbase64('U3Bh cms=') AS v")[2] == [b"Spark"]
    frame = spark.createDataFrame([("QQ==QQ",)], ["s"])
    with pytest.raises(PySparkException, match="incorrect ending byte"):
        frame.select(F.unbase64("s").alias("v")).collect()


def test_c002_hypot_registers_rescaled_on_both_doors(spark: ReparkSession) -> None:
    """Oracle BL16-0..8: f64::hypot rescaled answers, NULL propagates, inf over NaN."""
    rescaled = _sql_field(
        spark,
        "SELECT hypot(CAST('1e200' AS DOUBLE), CAST('1e200' AS DOUBLE)) AS h",
    )
    assert (rescaled[0], rescaled[2]) == (pyarrow.float64(), [1.414213562373095e200])
    assert _sql_field(spark, "SELECT hypot(CAST(3 AS DOUBLE), CAST(4 AS DOUBLE)) AS h") == (
        pyarrow.float64(),
        False,
        [5.0],
    )
    assert _sql_field(spark, "SELECT hypot(CAST(NULL AS DOUBLE), CAST(1 AS DOUBLE)) AS h") == (
        pyarrow.float64(),
        True,
        [None],
    )
    assert _sql_field(
        spark, "SELECT hypot(CAST('Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE)) AS h"
    ) == (pyarrow.float64(), True, [float("inf")])
    assert _sql_field(
        spark,
        "SELECT hypot(CAST('1.7976931348623157E308' AS DOUBLE), "
        "CAST('1.7976931348623157E308' AS DOUBLE)) AS h",
    )[2] == [float("inf")]
    frame = spark.createDataFrame([(1.0,)], "x double")
    table = frame.select(F.hypot(F.lit(1e200), F.lit(1e200)).alias("h")).to_arrow()
    assert table.column("h").to_pylist() == [1.414213562373095e200]


def test_c003_abs_integer_min_raises_on_the_sql_door(spark: ReparkSession) -> None:
    """Oracle DIV-abs-0..2: ANSI ARITHMETIC_OVERFLOW on every signed minimum."""
    for query in (
        "SELECT abs(CAST(-128 AS TINYINT)) AS v",
        "SELECT abs(CAST(-32768 AS SMALLINT)) AS v",
        "SELECT abs(CAST(-2147483648 AS INT)) AS v",
        "SELECT abs(CAST(-9223372036854775808 AS BIGINT)) AS v",
        "SELECT abs(-2147483648) AS v",
    ):
        with pytest.raises(PySparkException, match="ARITHMETIC_OVERFLOW"):
            spark.sql(query).collect()
    assert _sql_field(spark, "SELECT abs(CAST(-5 AS TINYINT)) AS v") == (
        pyarrow.int8(),
        False,
        [5],
    )
    assert _sql_field(spark, "SELECT abs(CAST(NULL AS INT)) AS v") == (
        pyarrow.int32(),
        True,
        [None],
    )
    spark.createDataFrame([(-2147483648,)], "x int").createOrReplaceTempView("mins")
    with pytest.raises(PySparkException, match="ARITHMETIC_OVERFLOW"):
        spark.sql("SELECT abs(x) AS v FROM mins").collect()


def test_c004_size_null_array_is_null_int_on_both_doors(spark: ReparkSession) -> None:
    """Oracle DIV-size-0..3, DIV-api-size-null: sizeOfNull=false semantics, int out."""
    assert _sql_field(spark, "SELECT size(array(1, 2)) AS v") == (
        pyarrow.int32(),
        False,
        [2],
    )
    assert _sql_field(spark, "SELECT size(CAST(NULL AS ARRAY<INT>)) AS v") == (
        pyarrow.int32(),
        True,
        [None],
    )
    assert _sql_field(spark, "SELECT size(map(1, 2)) AS v") == (
        pyarrow.int32(),
        False,
        [1],
    )
    assert _sql_field(spark, "SELECT cardinality(CAST(NULL AS ARRAY<INT>)) AS v") == (
        pyarrow.int32(),
        True,
        [None],
    )
    frame = spark.createDataFrame([([1, 2],), (None,)], "a array<int>")
    table = frame.select(F.size("a").alias("v")).to_arrow()
    assert table.schema.field("v").type == pyarrow.int32()
    assert table.schema.field("v").nullable is True
    assert table.column("v").to_pylist() == [2, None]


def test_c005_array_contains_three_valued_on_both_doors(spark: ReparkSession) -> None:
    """Oracle DIV-array_contains-0..3, DIV-api-contains: NULL blocks the decision."""
    assert _sql_field(spark, "SELECT array_contains(array(1, NULL), 2) AS v") == (
        pyarrow.bool_(),
        True,
        [None],
    )
    assert _sql_field(spark, "SELECT array_contains(array(1, NULL), 1) AS v") == (
        pyarrow.bool_(),
        True,
        [True],
    )
    assert _sql_field(spark, "SELECT array_contains(CAST(NULL AS ARRAY<INT>), 1) AS v") == (
        pyarrow.bool_(),
        True,
        [None],
    )
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.NULL_TYPE"):
        spark.sql("SELECT array_contains(array(1, 2), NULL) AS v").collect()
    frame = spark.createDataFrame([([1, None],)], "a array<int>")
    table = frame.select(F.array_contains("a", 2).alias("v")).to_arrow()
    assert table.column("v").to_pylist() == [None]


def test_l002_array_contains_coerces_to_tightest_common_type(spark: ReparkSession) -> None:
    """Oracle C6-ac-*: needle and element widen together, never needle down.

    Spark answers these literal-haystack legs non-null (cell C6-ac-double-hit,
    batch-7 N7-31); RePark reports nullable=True because the array constructor's
    declared containsNull is DataFusion's unconditionally-nullable element —
    recorded divergence ARRAY-LITERAL-CONTAINSNULL-1, owner DOOR-CONVERGE-2.
    """
    assert _sql_field(
        spark,
        "SELECT array_contains(array(1,2), CAST(3 AS DOUBLE)/CAST(2 AS DOUBLE)) AS v",
    ) == (pyarrow.bool_(), True, [False])
    assert _sql_field(spark, "SELECT array_contains(array(1,2), CAST(2 AS DOUBLE)) AS v") == (
        pyarrow.bool_(),
        True,
        [True],
    )
    assert _sql_field(
        spark,
        "SELECT array_contains(array(1,2), CAST(2147483648 AS BIGINT)) AS v",
    ) == (pyarrow.bool_(), True, [False])
    assert _sql_field(spark, "SELECT array_contains(array(1,2), CAST(1 AS BIGINT)) AS v") == (
        pyarrow.bool_(),
        True,
        [True],
    )
    assert _sql_field(spark, "SELECT array_contains(array(1.5, 2.5), 2.5) AS v") == (
        pyarrow.bool_(),
        True,
        [True],
    )
    assert _sql_field(spark, "SELECT array_contains(array(array(1), array(2)), array(2)) AS v") == (
        pyarrow.bool_(),
        True,
        [True],
    )
    frame = spark.createDataFrame([([1, 2],)], "a array<int>")
    assert frame.select(F.array_contains("a", 1.5).alias("v")).to_arrow().column(
        "v"
    ).to_pylist() == [False]
    assert frame.select(F.array_contains("a", F.lit(2147483648)).alias("v")).to_arrow().column(
        "v"
    ).to_pylist() == [False]


def test_l004_array_contains_empty_untyped_array_answers_false(
    spark: ReparkSession,
) -> None:
    """Oracle C6-ac-empty-untyped: array() + int needle is False non-null.

    Spark answers non-null (cell C6-ac-empty-untyped); RePark reports
    nullable=True for the same ARRAY-LITERAL-CONTAINSNULL-1 reason as test_l002.
    """
    assert _sql_field(spark, "SELECT array_contains(array(), 1) AS v") == (
        pyarrow.bool_(),
        True,
        [False],
    )


def test_l003_array_contains_diff_types_refuses_on_both_doors(
    spark: ReparkSession,
) -> None:
    """Oracle C6-ac-string-needle/-int-in-strings/-api-ac-string/-string-off."""
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES"):
        spark.sql("SELECT array_contains(array(1,2), '1') AS v").collect()
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES"):
        spark.sql("SELECT array_contains(array('1','2'), 1) AS v").collect()
    frame = spark.createDataFrame([([1, 2],)], "a array<int>")
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES"):
        frame.select(F.array_contains("a", "1").alias("v")).collect()


def test_l003_array_contains_diff_types_refuses_when_ansi_off() -> None:
    """Oracle C6-ac-string-off: the refusal is planning-time, ANSI-independent."""
    ansi_off = (
        ReparkSession.builder.appName("door-converge-1-ac-off")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    try:
        with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES"):
            ansi_off.sql("SELECT array_contains(array(1,2), '1') AS v").collect()
        with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES"):
            ansi_off.sql("SELECT array_contains(array('1','2'), 1) AS v").collect()
    finally:
        ansi_off.stop()


def test_c006_count_aggregates_non_null_bigint_on_both_doors(spark: ReparkSession) -> None:
    """Oracle BL18-sql/-api/-sql-empty: approx_count_distinct + regr_count non-null."""
    table = spark.sql(
        "SELECT approx_count_distinct(i) AS a, regr_count(l, i) AS r, count(i) AS c "
        "FROM (SELECT 1 AS i, 2.0 AS l UNION ALL SELECT 2, 3.0) q"
    ).to_arrow()
    for name in ("a", "r", "c"):
        field = table.schema.field(name)
        assert (field.type, field.nullable) == (pyarrow.int64(), False), name
    assert table.to_pylist() == [{"a": 2, "r": 2, "c": 2}]
    empty = spark.sql(
        "SELECT approx_count_distinct(i) AS a, regr_count(l, i) AS r "
        "FROM (SELECT 1 AS i, 2.0 AS l WHERE false) q"
    ).to_arrow()
    assert empty.to_pylist() == [{"a": 0, "r": 0}]
    for name in ("a", "r"):
        assert empty.schema.field(name).nullable is False, name
    frame = spark.createDataFrame([(1, 2.0), (2, 3.0)], "i int, l double")
    api = frame.select(
        F.approx_count_distinct("i").alias("a"), F.regr_count("l", "i").alias("r")
    ).to_arrow()
    for name in ("a", "r"):
        field = api.schema.field(name)
        assert (field.type, field.nullable) == (pyarrow.int64(), False), name
    assert api.to_pylist() == [{"a": 2, "r": 2}]


def test_c007_ascii_codepoint_and_binary_length_on_both_doors(spark: ReparkSession) -> None:
    """Oracle DIV-ascii-0..3, DIV-length-0..3, DIV-api-*: codepoint, bytes for binary."""
    assert _sql_field(spark, "SELECT ascii('é') AS v") == (pyarrow.int32(), False, [233])
    assert _sql_field(spark, "SELECT ascii('€x') AS v")[2] == [8364]
    assert _sql_field(spark, "SELECT ascii('') AS v")[2] == [0]
    assert _sql_field(spark, "SELECT ascii(NULL) AS v") == (pyarrow.int32(), True, [None])
    assert _sql_field(spark, "SELECT length('héllo') AS v") == (
        pyarrow.int32(),
        False,
        [5],
    )
    assert _sql_field(spark, "SELECT length(X'C3A9') AS v") == (
        pyarrow.int32(),
        False,
        [2],
    )
    assert _sql_field(spark, "SELECT character_length(X'0102') AS v") == (
        pyarrow.int32(),
        False,
        [2],
    )
    frame = spark.createDataFrame([("é", b"\xc3\xa9")], ["s", "b"])
    api = frame.select(F.ascii("s").alias("a"), F.length("b").alias("v")).to_arrow()
    assert api.column("a").to_pylist() == [233]
    assert api.column("v").to_pylist() == [2]


def test_c008_bin_rint_refuse_boolean_on_the_sql_door(spark: ReparkSession) -> None:
    """Oracle BL6-sql-0..1: BOOLEAN refuses with DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE."""
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"):
        spark.sql("SELECT bin(true) AS v").collect()
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"):
        spark.sql("SELECT rint(true) AS v").collect()
    assert _sql_field(spark, "SELECT bin(1) AS v") == (pyarrow.string(), False, ["1"])
    assert _sql_field(spark, "SELECT rint(CAST(2.5 AS DOUBLE)) AS v") == (
        pyarrow.float64(),
        True,
        [2.0],
    )


def test_l007_abs_wraps_signed_minima_when_ansi_off() -> None:
    """Oracle C6-abs-off-*, C6-api-abs-off-tiny: ANSI off wraps, width kept."""
    ansi_off = (
        ReparkSession.builder.appName("door-converge-1-abs-off")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    try:
        for query, arrow_type, want in (
            ("SELECT abs(CAST(-128 AS TINYINT)) AS v", pyarrow.int8(), [-128]),
            ("SELECT abs(CAST(-32768 AS SMALLINT)) AS v", pyarrow.int16(), [-32768]),
            (
                "SELECT abs(CAST(-2147483648 AS INT)) AS v",
                pyarrow.int32(),
                [-2147483648],
            ),
            (
                "SELECT abs(-9223372036854775808L) AS v",
                pyarrow.int64(),
                [-9223372036854775808],
            ),
            ("SELECT abs(CAST(-5 AS TINYINT)) AS v", pyarrow.int8(), [5]),
        ):
            assert _sql_field(ansi_off, query) == (arrow_type, False, want), query
        frame = ansi_off.createDataFrame([(-128,)], "x tinyint")
        table = frame.select(F.abs("x").alias("v")).to_arrow()
        assert table.schema.field("v").type == pyarrow.int8()
        assert table.schema.field("v").nullable is True
        assert table.column("v").to_pylist() == [-128]
    finally:
        ansi_off.stop()


def test_element_at_alias_1_resolves_the_dedicated_binding(spark: ReparkSession) -> None:
    """ELEMENT-AT-ALIAS-1: element_at keeps array indexing, not map_extract."""
    assert _sql_field(spark, "SELECT element_at(array(10,20,30), 2) AS v") == (
        pyarrow.int32(),
        True,
        [20],
    )
    frame = spark.createDataFrame([([10, 20, 30],)], "a array<int>")
    table = frame.select(F.element_at("a", 3).alias("v")).to_arrow()
    assert table.column("v").to_pylist() == [30]
