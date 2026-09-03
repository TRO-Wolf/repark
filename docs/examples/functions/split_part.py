"""Demonstrate the ``F.*`` delimiter-driven extraction names on a small local frame.

pins: ex-5-functions-strings-b-regex/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.split_part", "F.substring_index", "F.col", "F.lit"]


def main() -> None:
    """Check parts by one-based and negative index, and the left/right substring_index counts."""
    repark = ReparkSession.builder.appName("ex-split-part").master("local[1]").getOrCreate()
    try:
        commas = repark.createDataFrame([("one,two,three",), ("a,b",), (None,)], ["s"])
        parts = commas.select(
            F.split_part(F.col("s"), F.lit(","), F.lit(1)).alias("p1"),
            F.split_part(F.col("s"), F.lit(","), F.lit(2)).alias("p2"),
            F.split_part(F.col("s"), F.lit(","), F.lit(3)).alias("p3"),
            F.split_part(F.col("s"), F.lit(","), F.lit(-1)).alias("pm1"),
        ).collect()
        checked = (
            ("p1", ["one", "a", None]),
            ("p2", ["two", "b", None]),
            ("p3", ["three", "", None]),
            ("pm1", ["three", "b", None]),
        )
        for name, expected in checked:
            values = [row[name] for row in parts]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        dots = repark.createDataFrame([("www.apache.org",), ("a.b",), ("nodot",), (None,)], ["s"])
        counts = dots.select(
            F.substring_index(F.col("s"), ".", 2).alias("left2"),
            F.substring_index(F.col("s"), ".", -1).alias("right1"),
            F.substring_index(F.col("s"), ".", 0).alias("zero"),
            F.substring_index(F.col("s"), ".", 4).alias("over"),
        ).collect()
        checked = (
            ("left2", ["www.apache", "a.b", "nodot", None]),
            ("right1", ["org", "b", "nodot", None]),
            ("zero", ["", "", "", None]),
            ("over", ["www.apache.org", "a.b", "nodot", None]),
        )
        for name, expected in checked:
            values = [row[name] for row in counts]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
