"""Demonstrate the deprecated PySpark alias names against their modern siblings."""

from __future__ import annotations

import warnings

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.approxCountDistinct",
    "F.shiftLeft",
    "F.shiftRight",
    "F.shiftRightUnsigned",
    "F.toDegrees",
    "F.toRadians",
]

EXPECTED_WARNINGS: frozenset[str] = frozenset(
    {
        "Deprecated in 2.1, use approx_count_distinct instead.",
        "Deprecated in 3.2, use shiftleft instead.",
        "Deprecated in 3.2, use shiftright instead.",
        "Deprecated in 3.2, use shiftrightunsigned instead.",
        "Deprecated in 2.1, use degrees instead.",
        "Deprecated in 2.1, use radians instead.",
    }
)


def main() -> None:
    """Match each alias to its modern name and to Spark's FutureWarning text."""
    repark = ReparkSession.builder.appName("ex-deprecated-aliases").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(1, -8, 180.0), (2, 7, 90.0), (2, None, None)], "g int, i int, deg double"
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            aliased = frame.select(
                F.shiftLeft("i", 1).alias("l"),
                F.shiftRight("i", 1).alias("r"),
                F.shiftRightUnsigned("i", 1).alias("u"),
                F.toDegrees("deg").alias("d"),
                F.toRadians("deg").alias("rad"),
            ).collect()
            counted = (
                frame.groupBy("g").agg(F.approxCountDistinct("i").alias("n")).orderBy("g").collect()
            )
        modern = frame.select(
            F.shiftleft("i", 1).alias("l"),
            F.shiftright("i", 1).alias("r"),
            F.shiftrightunsigned("i", 1).alias("u"),
            F.degrees("deg").alias("d"),
            F.radians("deg").alias("rad"),
        ).collect()
        if [row.asDict() for row in aliased] != [row.asDict() for row in modern]:
            raise SystemExit(f"alias rows {aliased!r} != modern rows {modern!r}")
        expected_counts = (
            frame.groupBy("g").agg(F.approx_count_distinct("i").alias("n")).orderBy("g").collect()
        )
        if [row["n"] for row in counted] != [row["n"] for row in expected_counts]:
            raise SystemExit(f"approxCountDistinct {counted!r} != {expected_counts!r}")
        seen = {str(item.message) for item in caught if item.category is FutureWarning}
        if seen != EXPECTED_WARNINGS:
            raise SystemExit(f"warnings {sorted(seen)!r} != {sorted(EXPECTED_WARNINGS)!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
