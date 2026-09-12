"""Demonstrate ``F.stack``, Spark's n-row unpivot of a value list.

pins: perf-unpivot-1/C-004
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.stack", "F.lit"]


def main() -> None:
    """Stack four literals into two rows of two columns."""
    repark = ReparkSession.builder.appName("ex-stack").master("local[1]").getOrCreate()
    try:
        frame = repark.range(1).select(
            F.stack(F.lit(2), F.lit(1), F.lit(2), F.lit(3), F.lit(4)).alias("x", "y")
        )
        values = [(row["x"], row["y"]) for row in frame.collect()]
        print(f"F.stack: {values!r}")
        if values != [(1, 2), (3, 4)]:
            raise SystemExit(f"F.stack gave {values!r}, expected [(1, 2), (3, 4)]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
