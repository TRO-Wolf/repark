"""Demonstrate the ``F.*`` array element access names on a small local frame.

pins: ex-8-functions-arrays/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.element_at",
    "F.try_element_at",
    "F.get",
    "F.slice",
    "F.array_contains",
    "F.col",
]


def main() -> None:
    """Read elements one-based, tried, zero-based, as a window, and test membership."""
    repark = ReparkSession.builder.appName("ex-array-elements").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([([10, 20, 30],), ([None, 5],), (None,)], ["a"])
        rows = frame.select(
            F.col("a"),
            F.element_at(F.col("a"), 1).alias("first"),
            F.element_at(F.col("a"), -1).alias("last"),
            F.element_at(F.col("a"), 2).alias("second"),
            F.try_element_at(F.col("a"), 1).alias("try_first"),
            F.try_element_at(F.col("a"), 10).alias("try_oob"),
            F.get(F.col("a"), 0).alias("get_first"),
            F.get(F.col("a"), 2).alias("get_third"),
            F.get(F.col("a"), 9).alias("get_oob"),
            F.slice(F.col("a"), 2, 2).alias("mid"),
            F.slice(F.col("a"), -2, 2).alias("tail"),
            F.array_contains(F.col("a"), F.lit(5)).alias("contained"),
        ).collect()
        checked = (
            ("first", [10, None, None]),
            ("last", [30, 5, None]),
            ("second", [20, 5, None]),
            ("try_first", [10, None, None]),
            ("try_oob", [None, None, None]),
            ("get_first", [10, None, None]),
            ("get_third", [30, None, None]),
            ("get_oob", [None, None, None]),
            ("mid", [[20, 30], [5], None]),
            ("tail", [[20, 30], [None, 5], None]),
            ("contained", [False, True, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} gave {values!r}, expected {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
