"""Demonstrate ``F.first``, ``F.last`` and aliases ``F.first_value``/``F.last_value``.

pins: ex-12-functions-aggregates-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession, Window

COVERS: list[str] = ["F.first", "F.last", "F.first_value", "F.last_value", "F.col"]


def main() -> None:
    """Take each ordered window's first and last value; the aliases answer identically."""
    repark = ReparkSession.builder.appName("ex-first-last").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1),
                ("a", 2),
                ("a", 3),
                ("a", None),
                ("b", 4),
                ("b", 6),
            ],
            ["k", "v"],
        )
        window = (
            Window.partitionBy("k")
            .orderBy(F.col("v").asc_nulls_last())
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        windowed = frame.select(
            "k",
            F.first("v").over(window).alias("first_value"),
            F.last("v").over(window).alias("last_value"),
            F.first_value("v").over(window).alias("first_alias"),
            F.last_value("v").over(window).alias("last_alias"),
        )
        answers = sorted(tuple(row) for row in windowed.distinct().collect())
        if answers != [("a", 1, None, 1, None), ("b", 4, 6, 4, 6)]:
            raise SystemExit(f"F.first/F.last window answers {answers!r} != the ordered window's")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
