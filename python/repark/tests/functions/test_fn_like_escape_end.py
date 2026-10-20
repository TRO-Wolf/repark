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


def test_like_escape_at_end_explicit_escape_raises() -> None:
    """FN-LIKE-ESCEND-1: 'a%' LIKE 'a\\' ESCAPE '\\' raises. pins: fn-fix-2-ctrl-1-controls/C-002"""
    repark = (
        ReparkSession.builder.appName("fn-like-escend-explicit").master("local[1]").getOrCreate()
    )
    try:
        with pytest.raises(AnalysisException, match=r"INVALID_FORMAT\.ESC_AT_THE_END"):
            repark.sql(r"SELECT 'a%' LIKE 'a\\' ESCAPE '\\'").collect()
        with pytest.raises(AnalysisException, match="42601"):
            repark.range(1).select(F.like(F.lit("a%"), F.lit("a\\"))).collect()
    finally:
        repark.stop()


def test_like_escape_at_end_raises_with_ansi_off() -> None:
    """FN-LIKE-ESCEND-1: ANSI-off trailing escape raises. pins: fn-fix-2-ctrl-1-controls/C-002"""
    repark = (
        ReparkSession.builder.appName("fn-like-escend-ansi-off")
        .master("local[1]")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    try:
        with pytest.raises(AnalysisException, match=r"INVALID_FORMAT\.ESC_AT_THE_END"):
            repark.range(1).select(F.like(F.lit("ab"), F.lit("ab\\"))).collect()
        with pytest.raises(AnalysisException, match="42601"):
            repark.sql(r"SELECT 'ab' LIKE 'ab\\'").collect()
        with pytest.raises(AnalysisException, match="42601"):
            repark.sql(r"SELECT 'a%' LIKE 'a\\' ESCAPE '\\'").collect()
    finally:
        repark.stop()
