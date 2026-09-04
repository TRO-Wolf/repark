"""FN-TRIM-CHARS-1: trim/ltrim/rtrim two-arg charset overloads (registry §7)."""

import pytest

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812


def test_fn_trim_two_arg_charset() -> None:
    """FN-TRIM-CHARS-1: trim('xxSparkxx', 'x') is 'Spark'. pins: fn-fix-2-string-rows/C-003"""
    repark = ReparkSession.builder.appName("fn-trim-chars").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("xxSparkxx",), ("  Spark  ",), ("abSparkba",), ("üüSparküü",), ("",), (None,)],
            ["s"],
        )
        trimmed = [
            row[0] for row in frame.select(F.trim(F.col("s"), F.lit("x")).alias("r")).collect()
        ]
        assert trimmed == ["Spark", "  Spark  ", "abSparkba", "üüSparküü", "", None]
        both = [
            row[0]
            for row in repark.range(1)
            .select(F.trim(F.lit("xxSparkxx"), F.lit("x")).alias("t"))
            .collect()
        ]
        assert both == ["Spark"]
        left = [
            row[0]
            for row in repark.range(1)
            .select(F.ltrim(F.lit("xxSparkxx"), F.lit("x")).alias("t"))
            .collect()
        ]
        assert left == ["Sparkxx"]
        right = [
            row[0]
            for row in repark.range(1)
            .select(F.rtrim(F.lit("xxSparkxx"), F.lit("x")).alias("t"))
            .collect()
        ]
        assert right == ["xxSpark"]
        whitespace = [
            row[0]
            for row in repark.range(1).select(F.trim(F.lit("  Spark  ")).alias("t")).collect()
        ]
        assert whitespace == ["Spark"]
        charset = [
            row[0]
            for row in repark.range(1)
            .select(F.trim(F.lit("abSparkba"), F.lit("ab")).alias("t"))
            .collect()
        ]
        assert charset == ["Spark"]
        unicode_trim = [
            row[0]
            for row in repark.range(1)
            .select(F.trim(F.lit("üüSparküü"), F.lit("ü")).alias("t"))
            .collect()
        ]
        assert unicode_trim == ["Spark"]
    finally:
        repark.stop()


@pytest.mark.parametrize("op", ["trim", "ltrim", "rtrim"])
def test_fn_trim_empty_charset_is_noop(op: str) -> None:
    """FN-TRIM-CHARS-1: empty trim set keeps 'abc'. pins: fn-fix-2-ctrl-1-controls/C-002"""
    repark = ReparkSession.builder.appName("fn-trim-empty").master("local[1]").getOrCreate()
    try:
        func = {"trim": F.trim, "ltrim": F.ltrim, "rtrim": F.rtrim}[op]
        values = [
            row[0]
            for row in repark.range(1).select(func(F.lit("abc"), F.lit("")).alias("t")).collect()
        ]
        assert values == ["abc"]
    finally:
        repark.stop()


@pytest.mark.parametrize("op", ["ltrim", "rtrim"])
def test_fn_trim_null_charset_is_null(op: str) -> None:
    """FN-TRIM-CHARS-1: NULL trim set gives NULL. pins: fn-fix-2-ctrl-1-controls/C-002"""
    repark = ReparkSession.builder.appName("fn-trim-null-side").master("local[1]").getOrCreate()
    try:
        func = {"ltrim": F.ltrim, "rtrim": F.rtrim}[op]
        values = [
            row[0]
            for row in repark.range(1)
            .select(func(F.lit("abc"), F.lit(None).cast("string")).alias("t"))
            .collect()
        ]
        assert values == [None]
        keyword = {"ltrim": "LEADING", "rtrim": "TRAILING"}[op]
        sql_values = [
            row[0] for row in repark.sql(f"SELECT TRIM({keyword} NULL FROM 'abc')").collect()
        ]
        assert sql_values == [None]
    finally:
        repark.stop()


def test_fn_trim_empty_and_null_charset_sql() -> None:
    """FN-TRIM-CHARS-1: BOTH '' keeps; NULL gives NULL. pins: fn-fix-2-ctrl-1-controls/C-002"""
    repark = ReparkSession.builder.appName("fn-trim-empty-sql").master("local[1]").getOrCreate()
    try:
        empty = [row[0] for row in repark.sql("SELECT TRIM(BOTH '' FROM 'abc')").collect()]
        assert empty == ["abc"]
        null_charset = [row[0] for row in repark.sql("SELECT TRIM(BOTH NULL FROM 'abc')").collect()]
        assert null_charset == [None]
        null_func = [
            row[0]
            for row in repark.range(1)
            .select(F.trim(F.lit("abc"), F.lit(None).cast("string")).alias("t"))
            .collect()
        ]
        assert null_func == [None]
    finally:
        repark.stop()
