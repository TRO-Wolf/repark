"""Divergence pins for the EX-25 F.* long-tail (a) batch.

Registry §7 rows EX-FN-1..19.
"""

from __future__ import annotations

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
    """arrays_zip zips with NULL fill; the field names are positional (FNP9-ARRAYS-ZIP-NAMES-1).

    pins: fnp-9-collections-json/C-006, C-008
    """
    frame = spark.createDataFrame([([1, 2], ["x"])], "a ARRAY<INT>, b ARRAY<STRING>")
    table = frame.select(F.arrays_zip("a", "b").alias("v")).toArrow()
    assert table.column("v").to_pylist() == [[{"0": 1, "1": "x"}, {"0": 2, "1": None}]]
    assert [field.name for field in table.schema.field("v").type.value_type] == ["0", "1"]


def test_posexplode_pair_refuses() -> None:
    """posexplode and posexplode_outer refuse; Spark emits pos/col rows (EX-FN-2)."""
    with pytest.raises(UnsupportedOperationException, match="posexplode"):
        F.posexplode("a")
    with pytest.raises(UnsupportedOperationException, match="posexplode_outer"):
        F.posexplode_outer("a")


def test_encode_decode_charset_refuses(spark: ReparkSession) -> None:
    """encode/decode refuse charset codecs; Spark encodes UTF-8/US-ASCII (EX-FN-3)."""
    frame = spark.createDataFrame([("AB",)], "s STRING")
    with pytest.raises(PySparkException, match="no built-in encoding"):
        frame.select(F.encode("s", "utf-8")).collect()
    with pytest.raises(PySparkException, match="no built-in encoding"):
        frame.select(F.decode(F.unbase64(F.lit("QUI=")), "utf-8")).collect()


def test_expr_column_reference_refuses() -> None:
    """expr with a column reference refuses; Spark binds it (EX-FN-4)."""
    with pytest.raises(AnalysisException, match="No field named a"):
        F.expr("a + 1")


def test_format_number_refuses() -> None:
    """format_number refuses; Spark renders grouped decimals (EX-FN-5)."""
    with pytest.raises(UnsupportedOperationException, match="format_number"):
        F.format_number("x", 2)


def test_from_csv_refuses() -> None:
    """from_csv refuses; Spark parses the row struct (EX-FN-6)."""
    with pytest.raises(UnsupportedOperationException, match="from_csv"):
        F.from_csv("line", "a INT, b STRING")


def test_hash_refuses() -> None:
    """hash refuses; Spark answers the Murmur3 ints (EX-FN-7)."""
    with pytest.raises(UnsupportedOperationException, match=r"functions\.hash"):
        F.hash("n")


def test_json_tuple_refuses() -> None:
    """json_tuple refuses; Spark projects the string fields (EX-FN-8)."""
    with pytest.raises(UnsupportedOperationException, match="json_tuple"):
        F.json_tuple("line", "a", "b")


def test_moment_aggregates_refuse() -> None:
    """kurtosis, skewness and mode refuse; Spark aggregates them (EX-FN-9)."""
    with pytest.raises(UnsupportedOperationException, match="kurtosis"):
        F.kurtosis("x")
    with pytest.raises(UnsupportedOperationException, match="skewness"):
        F.skewness("x")
    with pytest.raises(UnsupportedOperationException, match="mode"):
        F.mode("x")


def test_make_timestamp_refuses() -> None:
    """make_timestamp refuses; Spark builds the timestamp (EX-FN-10)."""
    with pytest.raises(UnsupportedOperationException, match="make_timestamp"):
        F.make_timestamp("y", "mo", "d", "h", "mi", "s")


def test_months_between_refuses() -> None:
    """months_between refuses; Spark answers the month distance (EX-FN-11)."""
    with pytest.raises(UnsupportedOperationException, match="months_between"):
        F.months_between("e", "s")


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


def test_schema_of_csv_refuses() -> None:
    """schema_of_csv refuses; Spark infers the struct (EX-FN-16).

    schema_of_json left this row when FNP-9/10 built the kernel.
    pins: fnp-9-collections-json/C-003
    """
    with pytest.raises(UnsupportedOperationException, match="schema_of_csv"):
        F.schema_of_csv("line")


def test_sentences_refuses() -> None:
    """sentences refuses; Spark nests words by sentence (EX-FN-17)."""
    with pytest.raises(UnsupportedOperationException, match="sentences"):
        F.sentences("s")


def test_split_refuses() -> None:
    """split refuses; Spark cuts on the pattern (EX-FN-18)."""
    with pytest.raises(UnsupportedOperationException, match=r"functions\.split"):
        F.split("s", ",")


def test_make_interval_string_form(spark: ReparkSession) -> None:
    """make_interval casts to DataFusion's terse form; Spark spells units out (EX-FN-19)."""
    frame = spark.createDataFrame(
        [(1, 2, 1, 3, 4, 5, 6)], "y INT, mo INT, w INT, d INT, h INT, mi INT, s INT"
    )
    rows = frame.select(
        F.make_interval("y", "mo", "w", "d", "h", "mi", "s").cast("string").alias("v")
    ).collect()
    assert [row["v"] for row in rows] == ["14 mons 10 days 4 hours 5 mins 6.000000000 secs"]
