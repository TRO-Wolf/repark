"""FN-ELT-1: out-of-range elt raises INVALID_ARRAY_INDEX under ANSI."""

import pytest

from repark.errors import PySparkException
from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812


def test_elt_index_three_raises_invalid_array_index() -> None:
    """FN-ELT-1: index 3 raises INVALID_ARRAY_INDEX. pins: fn-fix-2-string-rows/C-003"""
    repark = ReparkSession.builder.appName("fn-elt-3").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,)], ["n"])
        with pytest.raises(PySparkException, match="INVALID_ARRAY_INDEX"):
            frame.select(F.elt(F.lit(3), F.lit("a"), F.lit("b")).alias("r")).collect()
        in_range = [
            row["r"]
            for row in frame.select(F.elt(F.lit(1), F.lit("a"), F.lit("b")).alias("r")).collect()
        ]
        assert in_range == ["a"]
        second = [
            row["r"]
            for row in frame.select(F.elt(F.lit(2), F.lit("a"), F.lit("b")).alias("r")).collect()
        ]
        assert second == ["b"]
        null_index = [
            row["r"]
            for row in frame.select(F.elt(F.lit(None), F.lit("a"), F.lit("b")).alias("r")).collect()
        ]
        assert null_index == [None]
    finally:
        repark.stop()


def test_elt_index_zero_raises_invalid_array_index() -> None:
    """FN-ELT-1: index 0 raises INVALID_ARRAY_INDEX. pins: fn-fix-2-string-rows/C-003"""
    repark = ReparkSession.builder.appName("fn-elt-0").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,)], ["n"])
        with pytest.raises(PySparkException, match="INVALID_ARRAY_INDEX"):
            frame.select(F.elt(F.lit(0), F.lit("a"), F.lit("b")).alias("r")).collect()
        with pytest.raises(PySparkException, match="INVALID_ARRAY_INDEX"):
            frame.select(F.elt(F.lit(-1), F.lit("a"), F.lit("b")).alias("r")).collect()
    finally:
        repark.stop()


@pytest.mark.parametrize("index", [3, 0, -1])
def test_elt_out_of_range_returns_null_with_ansi_off(index: int) -> None:
    """FN-ELT-1: ANSI-off elt of index 3, 0, or -1 is NULL. pins: fn-fix-2-ctrl-1-controls/C-002"""
    repark = (
        ReparkSession.builder.appName("fn-elt-ansi-off")
        .master("local[1]")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    try:
        frame = repark.createDataFrame([(1,)], ["n"])
        values = [
            row["r"]
            for row in frame.select(
                F.elt(F.lit(index), F.lit("a"), F.lit("b")).alias("r")
            ).collect()
        ]
        assert values == [None]
        null_index = [
            row["r"]
            for row in frame.select(
                F.elt(F.lit(None).cast("int"), F.lit("a"), F.lit("b")).alias("r")
            ).collect()
        ]
        assert null_index == [None]
    finally:
        repark.stop()
