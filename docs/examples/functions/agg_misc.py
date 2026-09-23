"""Demonstrate FNP-AGG-1 slice (d) grouping_id on a small cube.

pins: fnp-agg-1/C-007
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.grouping_id",
]


def main() -> None:
    """Pin cube grouping_id bitmask rows."""
    repark = ReparkSession.builder.appName("ex-agg-misc").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 10, 1.5, "x"),
                ("a", 20, 2.5, "y"),
                ("a", 30, -4.0, "x"),
                ("a", None, None, None),
                ("b", 5, 0.0, "z"),
                ("b", None, None, None),
            ],
            ["k", "v", "d", "s"],
        )
        cube_rows = frame.cube("k").agg(F.grouping_id().alias("gid")).orderBy("k").collect()
        cube_expected = [(None, 1), ("a", 0), ("b", 0)]
        cube_values = [(row["k"], row["gid"]) for row in cube_rows]
        print(f"F.grouping_id: {cube_values!r}")
        if cube_values != cube_expected:
            raise SystemExit(f"F.grouping_id produced {cube_values!r}, expected {cube_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
