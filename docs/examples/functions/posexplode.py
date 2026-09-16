"""Demonstrate ``F.posexplode`` and ``F.posexplode_outer``, position plus element per row.

pins: fnp-gen-1/C-002
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.posexplode", "F.posexplode_outer", "F.col"]


def main() -> None:
    """Fan arrays out to pos/col rows; the outer spelling keeps NULL and empty rows."""
    repark = ReparkSession.builder.appName("ex-posexplode").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([([1, 2], "r1"), (None, "r2"), ([], "r3")], ["a", "tag"])
        exploded = frame.select(F.col("tag"), F.posexplode(F.col("a"))).collect()
        values = [(row["tag"], row["pos"], row["col"]) for row in exploded]
        print(f"F.posexplode: {values!r}")
        if values != [("r1", 0, 1), ("r1", 1, 2)]:
            raise SystemExit(f"F.posexplode gave {values!r}, expected [('r1', 0, 1), ('r1', 1, 2)]")
        outer = frame.select(F.col("tag"), F.posexplode_outer(F.col("a"))).collect()
        values = [(row["tag"], row["pos"], row["col"]) for row in outer]
        print(f"F.posexplode_outer: {values!r}")
        expected = [("r1", 0, 1), ("r1", 1, 2), ("r2", None, None), ("r3", None, None)]
        if values != expected:
            raise SystemExit(f"F.posexplode_outer gave {values!r}, expected {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
