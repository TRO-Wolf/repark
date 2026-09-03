"""Demonstrate ``F.explode`` and ``F.explode_outer``, one row per array element.

pins: ex-8-functions-arrays/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.explode", "F.explode_outer", "F.col"]


def main() -> None:
    """Fan arrays out to rows; the outer spelling keeps the empty and NULL rows."""
    repark = ReparkSession.builder.appName("ex-explode").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([([1, 2], "r1"), (None, "r2"), ([], "r3")], ["a", "tag"])
        exploded = frame.select(F.col("tag"), F.explode(F.col("a")).alias("v")).collect()
        values = [(row["tag"], row["v"]) for row in exploded]
        print(f"F.explode: {values!r}")
        if values != [("r1", 1), ("r1", 2)]:
            raise SystemExit(f"F.explode gave {values!r}, expected [('r1', 1), ('r1', 2)]")
        outer = frame.select(F.col("tag"), F.explode_outer(F.col("a")).alias("v")).collect()
        values = [(row["tag"], row["v"]) for row in outer]
        print(f"F.explode_outer: {values!r}")
        if values != [("r1", 1), ("r1", 2), ("r2", None), ("r3", None)]:
            raise SystemExit(
                f"F.explode_outer gave {values!r}, "
                "expected [('r1', 1), ('r1', 2), ('r2', None), ('r3', None)]"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
