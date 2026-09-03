"""Pin FN-ELT-1: out-of-range ``elt`` answers NULL today; Spark raises."""

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812


def test_elt_index_three_answers_null() -> None:
    """FN-ELT-1 today: index 3 is NULL; Spark 4.1.2 raises INVALID_ARRAY_INDEX."""
    repark = ReparkSession.builder.appName("fn-elt-3").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,)], ["n"])
        values = [
            row["r"]
            for row in frame.select(F.elt(F.lit(3), F.lit("a"), F.lit("b")).alias("r")).collect()
        ]
        assert values == [None]
    finally:
        repark.stop()


def test_elt_index_zero_answers_null() -> None:
    """FN-ELT-1 today: index 0 is NULL; Spark 4.1.2 raises INVALID_ARRAY_INDEX."""
    repark = ReparkSession.builder.appName("fn-elt-0").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,)], ["n"])
        values = [
            row["r"]
            for row in frame.select(F.elt(F.lit(0), F.lit("a"), F.lit("b")).alias("r")).collect()
        ]
        assert values == [None]
    finally:
        repark.stop()
