"""Demonstrate the ``F.bit_and`` / ``F.bit_or`` / ``F.bit_xor`` aggregate folds.

pins: ex-13-functions-aggregates-b-stats/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.bit_and", "F.bit_or", "F.bit_xor", "F.col"]


def main() -> None:
    """Check the three bitwise folds over each group, NULL inputs skipped."""
    repark = ReparkSession.builder.appName("ex-bit-aggregates").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(1, 5), (1, 3), (1, 12), (1, None), (2, 7), (2, None)],
            ["g", "b"],
        )
        rows = (
            frame.groupBy("g")
            .agg(
                F.bit_and(F.col("b")).alias("bit_and"),
                F.bit_or(F.col("b")).alias("bit_or"),
                F.bit_xor(F.col("b")).alias("bit_xor"),
            )
            .orderBy("g")
            .collect()
        )
        checked = (
            ("bit_and", [0, 7]),
            ("bit_or", [15, 7]),
            ("bit_xor", [10, 7]),
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
