"""FN-TRIM-CHARS-1: trim/ltrim/rtrim two-arg charset overloads (registry §7)."""

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
