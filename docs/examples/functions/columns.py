"""Demonstrate ``F.column``, the constructor spelling that agrees with ``F.col``.

pins: ex-10-functions-null-cond-misc/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.column", "F.col"]


def main() -> None:
    """Check ``F.column`` against ``F.col`` on values, NULL included."""
    repark = ReparkSession.builder.appName("ex-columns").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,), (2,), (None,)], ["x"])
        rows = frame.select(
            F.column("x").alias("built"),
            F.col("x").alias("reference"),
        ).collect()
        built = [row["built"] for row in rows]
        reference = [row["reference"] for row in rows]
        print(f"F.column: {built!r}")
        if built != [1, 2, None]:
            raise SystemExit(f"F.column gave {built!r}, expected [1, 2, None]")
        if built != reference:
            raise SystemExit(f"F.column gave {built!r}, F.col gave {reference!r}; must agree")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
