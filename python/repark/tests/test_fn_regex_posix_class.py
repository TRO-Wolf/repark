"""Pin FN-REGEX-POSIX-1: POSIX ``[[:alpha:]]`` counts letters today; Spark does not."""

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812

_POSIX_ALPHA_FRAME: list[tuple[str]] = [("a1b2 Ünï_9",), ("foo",), ("aabbaa",)]


def test_regexp_count_posix_alpha_counts_letters() -> None:
    """FN-REGEX-POSIX-1 today: regexp_count is [3, 3, 6]; Spark 4.1.2 answers [1, 0, 4]."""
    repark = ReparkSession.builder.appName("fn-regex-posix-count").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(_POSIX_ALPHA_FRAME, ["s"])
        counted = frame.select(F.regexp_count(F.col("s"), F.lit("[[:alpha:]]")).alias("c"))
        values = [row["c"] for row in counted.collect()]
        assert values == [3, 3, 6]
    finally:
        repark.stop()


def test_rlike_posix_alpha_matches_every_row() -> None:
    """FN-REGEX-POSIX-1 today: rlike is all True; Spark 4.1.2 answers [True, False, True]."""
    repark = ReparkSession.builder.appName("fn-regex-posix-rlike").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(_POSIX_ALPHA_FRAME, ["s"])
        values = [
            row["m"]
            for row in frame.select(F.rlike(F.col("s"), F.lit("[[:alpha:]]")).alias("m")).collect()
        ]
        assert values == [True, True, True]
    finally:
        repark.stop()
