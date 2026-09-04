"""FN-LIKE-ESCEND-1: a pattern ending in the escape char raises ESC_AT_THE_END."""

import pytest

from repark.errors import AnalysisException
from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812


def test_like_pattern_ending_in_escape_raises() -> None:
    """FN-LIKE-ESCEND-1: trailing escape raises ESC_AT_THE_END. pins: fn-fix-2-string-rows/C-003"""
    repark = ReparkSession.builder.appName("fn-like-escend").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,)], ["n"])
        with pytest.raises(AnalysisException, match=r"INVALID_FORMAT\.ESC_AT_THE_END"):
            frame.select(F.like(F.lit("ab"), F.lit("ab\\")).alias("r")).collect()
        with pytest.raises(AnalysisException, match="42601"):
            repark.sql(r"SELECT 'ab' LIKE 'ab\\'").collect()
    finally:
        repark.stop()


def test_like_escaped_backslash_control_is_true() -> None:
    """Control: like('a\\\\b', 'a\\\\\\\\b') is True. pins: fn-fix-2-string-rows/C-003"""
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
