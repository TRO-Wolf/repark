"""Demonstrate the ``F.*`` regular-expression family on a small local frame.

pins: ex-5-functions-strings-b-regex/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.regexp",
    "F.rlike",
    "F.regexp_like",
    "F.regexp_count",
    "F.regexp_replace",
    "F.regexp_substr",
    "F.regexp_instr",
    "F.regexp_extract_all",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Check the match predicates, the counts, the rewrites, the first matches, and the groups."""
    repark = ReparkSession.builder.appName("ex-regex").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([("Spark SQL",), ("aaa",), ("abc123",), (None,)], ["s"])
        rows = frame.select(
            F.regexp(F.col("s"), F.lit("S.*k")).alias("regexp"),
            F.rlike(F.col("s"), F.lit("^S")).alias("rlike"),
            F.rlike(F.col("s"), F.lit("\\d")).alias("rlike_digit"),
            F.regexp_like(F.col("s"), F.lit("S.*k")).alias("regexp_like"),
            F.regexp_count(F.col("s"), F.lit("a")).alias("count_a"),
            F.regexp_count(F.col("s"), F.lit("[0-9]")).alias("count_digit"),
            F.regexp_replace(F.col("s"), "[a-z]", "*").alias("masked"),
            F.regexp_replace(F.col("s"), "\\w+", "N").alias("words"),
            F.regexp_substr(F.col("s"), F.lit("[a-z]+")).alias("first_word"),
            F.regexp_substr(F.col("s"), F.lit("[0-9]+")).alias("digits"),
            F.regexp_instr(F.col("s"), F.lit("a")).alias("pos_a"),
            F.regexp_instr(F.col("s"), F.lit("z")).alias("pos_none"),
        ).collect()
        checked = (
            ("regexp", [True, False, False, None]),
            ("rlike", [True, False, False, None]),
            ("rlike_digit", [False, False, True, None]),
            ("regexp_like", [True, False, False, None]),
            ("count_a", [1, 3, 1, None]),
            ("count_digit", [0, 0, 3, None]),
            ("masked", ["S**** SQL", "***", "***123", None]),
            ("words", ["N N", "N", "N", None]),
            ("first_word", ["park", "aaa", "abc", None]),
            ("digits", [None, None, "123", None]),
            ("pos_a", [3, 1, 1, None]),
            ("pos_none", [0, 0, 0, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        one = repark.createDataFrame([(None,)], "s STRING")
        groups = one.select(
            F.regexp_extract_all(F.lit("a1b2c3"), F.lit("([a-z])([0-9])"), 1).alias("groups1"),
            F.regexp_extract_all(F.lit("a1b2c3"), F.lit("([a-z])([0-9])"), 2).alias("groups2"),
            F.regexp_extract_all(F.lit("aaa"), F.lit("z"), 0).alias("no_groups"),
        ).collect()
        checked = (
            ("groups1", [["a", "b", "c"]]),
            ("groups2", [["1", "2", "3"]]),
            ("no_groups", [[]]),
        )
        for name, expected in checked:
            values = [row[name] for row in groups]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
