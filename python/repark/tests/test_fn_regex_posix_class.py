"""FN-REGEX-POSIX-1: POSIX ``[[:alpha:]]`` is a Java union bracket."""

import pytest

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812

_POSIX_ALPHA_FRAME: list[tuple[str] | tuple[None]] = [
    ("a1b2 Ünï_9",),
    ("foo",),
    ("aabbaa",),
    ("",),
    (None,),
]


def test_regexp_count_posix_alpha_is_java_union() -> None:
    """FN-REGEX-POSIX-1: regexp_count is [1, 0, 4]. pins: fn-fix-2-string-rows/C-003"""
    repark = ReparkSession.builder.appName("fn-regex-posix-count").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(_POSIX_ALPHA_FRAME, ["s"])
        counted = frame.select(F.regexp_count(F.col("s"), F.lit("[[:alpha:]]")).alias("c"))
        values = [row["c"] for row in counted.collect()]
        assert values == [1, 0, 4, 0, None]
    finally:
        repark.stop()


def test_rlike_posix_alpha_is_java_union() -> None:
    """FN-REGEX-POSIX-1: rlike is [True, False, True]. pins: fn-fix-2-string-rows/C-003"""
    repark = ReparkSession.builder.appName("fn-regex-posix-rlike").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(_POSIX_ALPHA_FRAME, ["s"])
        values = [
            row["m"]
            for row in frame.select(F.rlike(F.col("s"), F.lit("[[:alpha:]]")).alias("m")).collect()
        ]
        assert values == [True, False, True, False, None]
        replaced = [
            row["r"]
            for row in frame.select(
                F.regexp_replace(F.col("s"), F.lit("[[:alpha:]]"), F.lit("#")).alias("r")
            ).collect()
        ]
        assert replaced == ["#1b2 Ünï_9", "foo", "##bb##", "", None]
    finally:
        repark.stop()


@pytest.mark.parametrize("value", ["x", "fox"])
def test_bracket_posix_class_with_extra_literal_matches(value: str) -> None:
    """FN-REGEX-POSIX-1: '[[:alpha:]x]' matches 'x', 'fox'. pins: fn-fix-2-ctrl-1-controls/C-002"""
    repark = ReparkSession.builder.appName("fn-regex-bracket").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(value,)], ["s"])
        rlike_values = [
            row["m"]
            for row in frame.select(F.rlike(F.col("s"), F.lit("[[:alpha:]x]")).alias("m")).collect()
        ]
        assert rlike_values == [True]
        like_values = [
            row["m"]
            for row in frame.select(
                F.regexp_like(F.col("s"), F.lit("[[:alpha:]x]")).alias("m")
            ).collect()
        ]
        assert like_values == [True]
        sql_values = [
            row[0] for row in repark.sql(f"SELECT regexp_like('{value}', '[[:alpha:]x]')").collect()
        ]
        assert sql_values == [True]
    finally:
        repark.stop()
