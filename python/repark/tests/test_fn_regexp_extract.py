from __future__ import annotations

import pytest

from repark.spark import functions as F  # noqa: N812

PAIRS = r"(\d+)-(\d+)"
PAIRS_SQL = r"(\\d+)-(\\d+)"


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fn-regexp-extract").getOrCreate()


def _frame():
    return _session().createDataFrame([("100-200",), ("abc",), ("",), (None,)], "s string")


@pytest.mark.parametrize(("idx", "want"), [(1, "100"), (2, "200"), (0, "100-200")])
def test_extract_group_index(idx: int, want: str) -> None:
    """Groups 1/2, whole-match 0; no match is ''. pins: fn-regexp-extract-1/C-002"""
    got = (
        _frame()
        .select(F.regexp_extract("s", F.lit(PAIRS), idx).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert got == [want, "", "", None]


def test_extract_idx_defaults_to_group_one() -> None:
    """Omitted idx is group 1 on both doors. pins: fn-regexp-extract-1/C-002"""
    got = (
        _frame()
        .select(F.regexp_extract("s", F.lit(PAIRS)).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert got == ["100", "", "", None]
    door = (
        _session()
        .sql(f"SELECT regexp_extract('100-200', '{PAIRS_SQL}') AS r")
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert door == ["100"]


def test_extract_null_in_null_out() -> None:
    """NULL str/regexp/idx yields NULL. pins: fn-regexp-extract-1/C-002"""
    spark = _session()
    null_str = (
        spark.sql("SELECT CAST(NULL AS STRING) AS s")
        .select(F.regexp_extract("s", F.lit(PAIRS), 1).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert null_str == [None]
    null_re = (
        _frame()
        .select(F.regexp_extract("s", F.lit(None).cast("string"), 1).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert null_re == [None, None, None, None]
    null_idx = (
        _frame()
        .select(F.regexp_extract("s", F.lit(PAIRS), F.lit(None).cast("int")).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert null_idx == [None, None, None, None]
    door = (
        spark.sql("SELECT regexp_extract('abc', '([0-9]+)', CAST(NULL AS INT)) AS r")
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert door == [None]


@pytest.mark.parametrize("idx", [3, -1])
def test_extract_bad_group_index_raises_sparks_condition(idx: int) -> None:
    """Out-of-range idx raises REGEX_GROUP_INDEX. pins: fn-regexp-extract-1/C-002"""
    with pytest.raises(Exception, match="REGEX_GROUP_INDEX") as sql_caught:
        _session().sql(f"SELECT regexp_extract('100-200', '{PAIRS_SQL}', {idx}) AS r").collect()
    assert "`regexp_extract` is invalid" in str(sql_caught.value)
    with pytest.raises(Exception, match="REGEX_GROUP_INDEX") as facade_caught:
        _frame().select(F.regexp_extract("s", F.lit(PAIRS), idx).alias("r")).collect()
    assert "`regexp_extract` is invalid" in str(facade_caught.value)


def test_extract_group_one_of_groupless_pattern_raises() -> None:
    """Group 1 of a groupless pattern reads between 0 and 0. pins: fn-regexp-extract-1/C-002"""
    with pytest.raises(Exception, match="between 0 and 0, but got 1"):
        _session().sql("SELECT regexp_extract('abc', '[a-z]+', 1) AS r").collect()


@pytest.mark.parametrize(
    ("value", "pattern", "idx"),
    [("abc", PAIRS, 3), ("abc", PAIRS, -1), ("ABC", "[a-z]+", 1)],
)
def test_extract_nomatch_bad_index_returns_empty(value: str, pattern: str, idx: int) -> None:
    """Non-matching input answers '' for any idx. pins: fn-regexp-extract-1/C-002"""
    facade = (
        _session()
        .createDataFrame([(value,)], "s string")
        .select(F.regexp_extract("s", F.lit(pattern), idx).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert facade == [""]
    sql_pattern = pattern.replace("\\", "\\\\")
    door = (
        _session()
        .sql(f"SELECT regexp_extract('{value}', '{sql_pattern}', {idx}) AS r")
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert door == [""]


@pytest.mark.parametrize(("value", "want"), [("alpha", "alpha"), ("fox", "")])
def test_extract_posix_alpha_is_java_union(value: str, want: str) -> None:
    """POSIX class follows the Java union on both doors. pins: fn-regexp-extract-1/C-002"""
    facade = (
        _session()
        .createDataFrame([(value,)], "s string")
        .select(F.regexp_extract("s", F.lit("([[:alpha:]]+)"), 1).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert facade == [want]
    door = (
        _session()
        .sql(f"SELECT regexp_extract('{value}', '([[:alpha:]]+)', 1) AS r")
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert door == [want]


def test_extract_unicode_letter_class() -> None:
    """\\p{L} matches letters on both doors. pins: fn-regexp-extract-1/C-002"""
    facade = (
        _session()
        .range(1)
        .select(F.regexp_extract(F.lit("alpha"), "(\\p{L}+)", 1).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert facade == ["alpha"]
    door = (
        _session()
        .sql(r"SELECT regexp_extract('alpha', '(\\p{L}+)', 1) AS r")
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert door == ["alpha"]


def test_extract_java_lookbehind_is_loud() -> None:
    """Lookbehind has no engine support and refuses. pins: fn-regexp-extract-1/C-002"""
    with pytest.raises(Exception, match="invalid regular expression"):
        _session().sql("SELECT regexp_extract('foobar', '(?<=foo)bar', 0) AS r").collect()
    with pytest.raises(Exception, match="invalid regular expression"):
        _session().range(1).select(
            F.regexp_extract(F.lit("foobar"), "(?<=foo)bar", 0).alias("r")
        ).collect()


def test_extract_edge_strings() -> None:
    """Non-ASCII, empty input, empty pattern, idle group. pins: fn-regexp-extract-1/C-002"""
    spark = _session()
    cases = [
        (r"SELECT regexp_extract('ünï', '(\\w+)', 0) AS r", "n"),
        ("SELECT regexp_extract('', '(a*)', 1) AS r", ""),
        ("SELECT regexp_extract('abc', '', 0) AS r", ""),
        ("SELECT regexp_extract('ac', '(a)(b)?', 2) AS r", ""),
    ]
    for query, want in cases:
        assert spark.sql(query).toArrow().column("r").to_pylist() == [want]


def test_extract_accepts_column_and_str_args() -> None:
    """Bare strs are column name and literal per slot. pins: fn-regexp-extract-1/C-002"""
    got = (
        _frame()
        .select(F.regexp_extract("s", "(\\d+)-(\\d+)", 1).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert got == ["100", "", "", None]
    column_idx = (
        _frame()
        .select(F.regexp_extract("s", F.lit(PAIRS), F.lit(1)).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()
    )
    assert column_idx == ["100", "", "", None]


def test_extract_doors_agree() -> None:
    """Facade and SQL door reach the same kernel. pins: fn-regexp-extract-1/C-002"""
    spark = _session()
    frame = _frame()
    frame.createOrReplaceTempView("fn_extract_v")
    facade = frame.select(F.regexp_extract("s", F.lit(PAIRS), 1).alias("r")).toArrow()
    door = spark.sql(f"SELECT regexp_extract(s, '{PAIRS_SQL}', 1) AS r FROM fn_extract_v").toArrow()
    assert facade.column("r").to_pylist() == door.column("r").to_pylist()
    assert facade.schema.field("r").type == door.schema.field("r").type


def test_extract_returns_nullable_string() -> None:
    """Utf8, nullable when an input is. pins: fn-regexp-extract-1/C-002"""
    table = _frame().select(F.regexp_extract("s", F.lit(PAIRS), 1).alias("r")).toArrow()
    assert str(table.schema.field("r").type) == "string"
    assert table.schema.field("r").nullable is True
