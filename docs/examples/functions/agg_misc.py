"""Demonstrate the FNP-AGG-1 step-2 aggregates on a small grouped frame.

pins: fnp-agg-1/C-007
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.any_value",
    "F.col",
    "F.kurtosis",
    "F.max_by",
    "F.min_by",
    "F.mode",
    "F.product",
    "F.skewness",
]


def main() -> None:
    """Pin grouped any_value, max_by, min_by, product, kurtosis, skewness, mode."""
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
        rows = (
            frame.groupBy("k")
            .agg(
                F.any_value(F.col("v")).alias("any_v"),
                F.max_by("s", "v").alias("max_s"),
                F.min_by("s", "d").alias("min_s"),
                F.product("v").alias("prod"),
                F.kurtosis("v").alias("kurt"),
                F.skewness("d").alias("skew"),
                F.mode("s").alias("mode_s"),
            )
            .orderBy("k")
            .collect()
        )
        checked = (
            ("any_v", [10, 5]),
            ("max_s", ["x", "z"]),
            ("min_s", ["x", "z"]),
            ("prod", [6000.0, 5.0]),
            ("kurt", [-1.5, None]),
            ("skew", [-0.642723256123866, None]),
            ("mode_s", ["x", "z"]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} produced {values!r}, expected {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
