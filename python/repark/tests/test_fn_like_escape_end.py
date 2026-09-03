"""Pin FN-LIKE-ESCEND-1: a pattern ending in the escape char answers False today."""

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812


def test_like_pattern_ending_in_escape_answers_false() -> None:
    """FN-LIKE-ESCEND-1 today: like('ab', 'ab\\\\') is False; Spark raises ESC_AT_THE_END."""
    repark = ReparkSession.builder.appName("fn-like-escend").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,)], ["n"])
        values = [
            row["r"]
            for row in frame.select(F.like(F.lit("ab"), F.lit("ab\\")).alias("r")).collect()
        ]
        assert values == [False]
    finally:
        repark.stop()


def test_like_escaped_backslash_control_is_true() -> None:
    """Control: like('a\\\\b', 'a\\\\\\\\b') is True on repark and on Spark 4.1.2."""
    repark = (
        ReparkSession.builder.appName("fn-like-escend-control").master("local[1]").getOrCreate()
    )
    try:
        frame = repark.createDataFrame([(1,)], ["n"])
        values = [
            row["r"]
            for row in frame.select(F.like(F.lit("a\\b"), F.lit("a\\\\b")).alias("r")).collect()
        ]
        assert values == [True]
    finally:
        repark.stop()
