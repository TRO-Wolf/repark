"""Demonstrate the edge-cutting pair ``F.left`` and ``F.right``.

pins: ex-4-functions-strings-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.left", "F.right", "F.col", "F.lit"]


def main() -> None:
    """Check both edges at a positive width, and the empty answer at a negative one."""
    repark = ReparkSession.builder.appName("ex-edges").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("Spark",), ("Apache",), ("aPACHE",), ("",), (None,)], ["s"]
        )
        rows = frame.select(
            F.left(F.col("s"), F.lit(3)).alias("left_3"),
            F.left(F.col("s"), F.lit(-3)).alias("left_neg3"),
            F.right(F.col("s"), F.lit(3)).alias("right_3"),
            F.right(F.col("s"), F.lit(-3)).alias("right_neg3"),
        ).collect()
        checked = (
            ("left_3", ["Spa", "Apa", "aPA", "", None]),
            ("left_neg3", ["", "", "", "", None]),
            ("right_3", ["ark", "che", "CHE", "", None]),
            ("right_neg3", ["", "", "", "", None]),
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
