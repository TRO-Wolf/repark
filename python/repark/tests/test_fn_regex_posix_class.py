"""FN-REGEX-POSIX-1: POSIX ``[[:alpha:]]`` is a Java union bracket."""

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
