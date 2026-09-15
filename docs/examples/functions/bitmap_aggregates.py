"""Demonstrate the ``bitmap_construct_agg`` / ``bitmap_or_agg`` / ``bitmap_and_agg`` aggregates."""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.bitmap_construct_agg",
    "F.bitmap_or_agg",
    "F.bitmap_and_agg",
]


def main() -> None:
    """Run the measured construct, fold, and identity arms."""
    repark = ReparkSession.builder.appName("ex-bitmap-aggregates").master("local[1]").getOrCreate()
    try:
        positions = repark.createDataFrame([(1,), (2,), (3,), (32767,), (None,)], "x BIGINT")
        counts = [
            row["count"]
            for row in positions.select(
                F.bitmap_count(F.bitmap_construct_agg(F.bitmap_bit_position(F.col("x")))).alias(
                    "count"
                )
            ).collect()
        ]
        print(f"F.bitmap_construct_agg: {counts!r}")
        if counts != [4]:
            raise SystemExit(f"F.bitmap_construct_agg {counts!r} != [4]")

        marks = repark.createDataFrame([(1, 1), (2, 1), (2, 2), (3, 2)], "x BIGINT, g BIGINT")
        inner = marks.groupBy("g").agg(
            F.bitmap_construct_agg(F.bitmap_bit_position(F.col("x"))).alias("b")
        )
        folded = [
            (row["o"], row["a"])
            for row in inner.agg(
                F.bitmap_count(F.bitmap_or_agg(F.col("b"))).alias("o"),
                F.bitmap_count(F.bitmap_and_agg(F.col("b"))).alias("a"),
            ).collect()
        ]
        print(f"F.bitmap_or_agg / F.bitmap_and_agg: {folded!r}")
        if folded != [(3, 1)]:
            raise SystemExit(f"F.bitmap_or_agg / F.bitmap_and_agg {folded!r} != [(3, 1)]")

        nulls = repark.createDataFrame([(None,)], "b BINARY")
        identities = [
            (
                row["o"],
                row["a"],
            )
            for row in nulls.select(
                F.bitmap_count(F.bitmap_or_agg(F.col("b"))).alias("o"),
                F.bitmap_count(F.bitmap_and_agg(F.col("b"))).alias("a"),
            ).collect()
        ]
        print(f"all-NULL identities: {identities!r}")
        if identities != [(0, 32768)]:
            raise SystemExit(f"all-NULL identities {identities!r} != [(0, 32768)]")

        empty = repark.createDataFrame([], "b BINARY")
        empties = [
            (
                row["o"],
                row["a"],
            )
            for row in empty.select(
                F.bitmap_count(F.bitmap_or_agg(F.col("b"))).alias("o"),
                F.bitmap_count(F.bitmap_and_agg(F.col("b"))).alias("a"),
            ).collect()
        ]
        print(f"empty identities: {empties!r}")
        if empties != [(0, 32768)]:
            raise SystemExit(f"empty identities {empties!r} != [(0, 32768)]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
