"""Divergence pins for the EX-25 F.* long-tail (a) batch.

Registry §7 rows EX-FN-1..19.
"""

from __future__ import annotations

import datetime
from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException, UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-ex25-functions-a").getOrCreate()
    yield session
    session.stop()


def test_arrays_zip_names_its_fields_by_position(spark: ReparkSession) -> None:
    """arrays_zip zips with NULL fill; the field names are positional (FNP9-ARRAYS-ZIP-NAMES-1)."""
    frame = spark.createDataFrame([([1, 2], ["x"])], "a ARRAY<INT>, b ARRAY<STRING>")
    table = frame.select(F.arrays_zip("a", "b").alias("v")).toArrow()
    assert table.column("v").to_pylist() == [[{"0": 1, "1": "x"}, {"0": 2, "1": None}]]
    assert [field.name for field in table.schema.field("v").type.value_type] == ["0", "1"]


def test_posexplode_pair_answers(spark: ReparkSession) -> None:
    """posexplode and posexplode_outer emit pos/col rows (EX-FN-2 fixed, fnp-gen-1)."""
    frame = spark.createDataFrame([([1, 2],), (None,), ([],)], "a ARRAY<INT>")
    assert frame.select(F.posexplode("a")).toArrow().to_pylist() == [
        {"pos": 0, "col": 1},
        {"pos": 1, "col": 2},
    ]
    assert frame.select(F.posexplode_outer("a")).toArrow().to_pylist() == [
        {"pos": 0, "col": 1},
        {"pos": 1, "col": 2},
        {"pos": None, "col": None},
        {"pos": None, "col": None},
    ]


def test_encode_decode_charset_refuses(spark: ReparkSession) -> None:
    """encode/decode refuse charset codecs; Spark encodes UTF-8/US-ASCII (EX-FN-3)."""
    frame = spark.createDataFrame([("AB",)], "s STRING")
    with pytest.raises(PySparkException, match="no built-in encoding"):
        frame.select(F.encode("s", "utf-8")).collect()
    with pytest.raises(PySparkException, match="no built-in encoding"):
        frame.select(F.decode(F.unbase64(F.lit("QUI=")), "utf-8")).collect()


def test_expr_column_reference_refuses(spark: ReparkSession) -> None:
    """expr with a column reference defers binding (EX-FN-4); use without the column refuses.

    Construction succeeds since FNP-4B (the C-003 deferred contract — Spark binds the
    reference at use); selecting against a frame with no ``a`` raises ``No field named a``.
    """
    column = F.expr("a + 1")
    frame = spark.createDataFrame([(1,)], "b long")
    with pytest.raises(AnalysisException, match="No field named a"):
        frame.select(column.alias("v")).to_arrow()


def test_format_number_refuses() -> None:
    """format_number refuses; Spark renders grouped decimals (EX-FN-5)."""
    with pytest.raises(UnsupportedOperationException, match="format_number"):
        F.format_number("x", 2)


def test_from_csv_answers(spark: ReparkSession) -> None:
    """from_csv parses the row struct (EX-FN-6 fixed, fnp-gen-1)."""
    frame = spark.createDataFrame([("1,hello",), ("2,",), (None,)], "line STRING")
    assert frame.select(F.from_csv("line", "a INT, b STRING").alias("v")).toArrow().to_pylist() == [
        {"v": {"a": 1, "b": "hello"}},
        {"v": {"a": 2, "b": None}},
        {"v": None},
    ]


def test_hash_refuses() -> None:
    """hash refuses; Spark answers the Murmur3 ints (EX-FN-7)."""
    with pytest.raises(UnsupportedOperationException, match=r"functions\.hash"):
        F.hash("n")


def test_json_tuple_answers(spark: ReparkSession) -> None:
    """json_tuple projects the string fields (EX-FN-8 fixed, fnp-gen-1)."""
    frame = spark.createDataFrame([('{"a": 1, "b": 2}',), ("{bad",), (None,)], "line STRING")
    assert frame.select(F.json_tuple("line", "a", "b")).toArrow().to_pylist() == [
        {"c0": "1", "c1": "2"},
        {"c0": None, "c1": None},
        {"c0": None, "c1": None},
    ]


def test_make_timestamp_answers(spark: ReparkSession) -> None:
    """make_timestamp builds the timestamp from parts (EX-FN-10, FIXED FNP-11A)."""
    frame = spark.createDataFrame(
        [(2014, 12, 28, 6, 30, 45)], "y INT, mo INT, d INT, h INT, mi INT, s INT"
    )
    table = frame.select(F.make_timestamp("y", "mo", "d", "h", "mi", "s").alias("v")).toArrow()
    assert table.column("v").to_pylist() == [
        datetime.datetime(2014, 12, 28, 6, 30, 45, tzinfo=datetime.UTC)
    ]


def test_months_between_answers(spark: ReparkSession) -> None:
    """months_between answers the month distance (EX-FN-11, FIXED FNP-11A)."""
    frame = spark.createDataFrame([("2024-02-29", "2024-01-31")], "e STRING, s STRING")
    table = frame.select(F.months_between("e", "s").alias("v")).toArrow()
    assert table.column("v").to_pylist() == [1.0]


def test_single_node_ids_refuse() -> None:
    """monotonically_increasing_id and spark_partition_id refuse (EX-FN-12)."""
    with pytest.raises(UnsupportedOperationException, match="monotonically_increasing_id"):
        F.monotonically_increasing_id()
    with pytest.raises(UnsupportedOperationException, match="spark_partition_id"):
        F.spark_partition_id()


def test_input_file_name_refuses() -> None:
    """input_file_name refuses; Spark answers the read path (EX-FN-13)."""
    with pytest.raises(UnsupportedOperationException, match="input_file_name"):
        F.input_file_name()


def test_raise_error_refuses() -> None:
    """raise_error refuses at build; Spark raises USER_RAISED_EXCEPTION (EX-FN-14)."""
    with pytest.raises(UnsupportedOperationException, match="raise_error"):
        F.raise_error("boom")


def test_replace_lit_spelling_refuses() -> None:
    """replace takes a plain-string search; Spark takes lit/column (EX-FN-15)."""
    with pytest.raises(TypeError, match="bytes-like object"):
        F.replace("s", F.lit("a"), F.lit("X"))


def test_replace_dollar_arm_answers_backslash(spark: ReparkSession) -> None:
    """replace with $1 in the replacement answers a backslash; Spark is literal (EX-FN-15)."""
    frame = spark.createDataFrame([("aaa",)], "s STRING")
    rows = frame.select(F.replace("s", "a", "$1").alias("v")).collect()
    assert [row["v"] for row in rows] == ["\\" * 3]


def test_schema_of_csv_answers(spark: ReparkSession) -> None:
    """schema_of_csv infers the struct (EX-FN-16 fixed, fnp-gen-1)."""
    frame = spark.createDataFrame([("x",)], "s STRING")
    assert frame.select(F.schema_of_csv("1,hello").alias("v")).toArrow().to_pylist() == [
        {"v": "STRUCT<_c0: INT, _c1: STRING>"}
    ]


def test_sentences_refuses() -> None:
    """sentences refuses; Spark nests words by sentence (EX-FN-17)."""
    with pytest.raises(UnsupportedOperationException, match="sentences"):
        F.sentences("s")


def test_split_refuses() -> None:
    """split refuses; Spark cuts on the pattern (EX-FN-18)."""
    with pytest.raises(UnsupportedOperationException, match=r"functions\.split"):
        F.split("s", ",")


def test_make_interval_string_form(spark: ReparkSession) -> None:
    """make_interval casts to Spark's spelled-out text (EX-FN-19, FIXED FNP-11A)."""
    frame = spark.createDataFrame(
        [(1, 2, 1, 3, 4, 5, 6)], "y INT, mo INT, w INT, d INT, h INT, mi INT, s INT"
    )
    rows = frame.select(
        F.make_interval("y", "mo", "w", "d", "h", "mi", "s").cast("string").alias("v")
    ).collect()
    assert [row["v"] for row in rows] == ["1 years 2 months 10 days 4 hours 5 minutes 6 seconds"]
