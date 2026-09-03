"""Demonstrate the ``F.*`` position-driven slice names on a small local frame.

pins: ex-5-functions-strings-b-regex/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.substr", "F.substring", "F.overlay", "F.col", "F.lit"]


def main() -> None:
    """Check the slice spellings against one another and overlay's two arities, NULLs included."""
    repark = ReparkSession.builder.appName("ex-slice").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("Spark",), ("SQL",), ("hello world",), ("café",), ("",), (None,)], ["s"]
        )
        rows = frame.select(
            F.substr(F.col("s"), F.lit(2), F.lit(3)).alias("substr"),
            F.substr(F.col("s"), F.lit(-3), F.lit(2)).alias("substr_neg"),
            F.substring(F.col("s"), 2, 3).alias("substring"),
            F.overlay(F.col("s"), F.lit("XY"), 2, 3).alias("overlay"),
            F.overlay(F.col("s"), F.lit("XY"), 2).alias("overlay_default"),
        ).collect()
        checked = (
            ("substr", ["par", "QL", "ell", "afé", "", None]),
            ("substr_neg", ["ar", "SQ", "rl", "af", "", None]),
            ("substring", ["par", "QL", "ell", "afé", "", None]),
            ("overlay", ["SXYk", "SXY", "hXYo world", "cXY", "XY", None]),
            ("overlay_default", ["SXYrk", "SXY", "hXYlo world", "cXYé", "XY", None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
