"""Demonstrate Spark's nine ``F.regr_*`` linear-regression aggregates on a grouped frame.

pins: ex-13-functions-aggregates-b-stats/C-001
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.regr_avgx",
    "F.regr_avgy",
    "F.regr_count",
    "F.regr_intercept",
    "F.regr_r2",
    "F.regr_slope",
    "F.regr_sxx",
    "F.regr_sxy",
    "F.regr_syy",
    "F.col",
]


def main() -> None:
    """Check the nine regression aggregates over (y, x) pairs and a one-pair group."""
    repark = ReparkSession.builder.appName("ex-regression").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                (1, 1.0, 2.0),
                (1, 2.0, 4.0),
                (1, 3.0, 5.0),
                (1, 4.0, 10.0),
                (1, None, 3.0),
                (2, 1.0, 2.0),
            ],
            ["g", "y", "x"],
        )
        rows = (
            frame.groupBy("g")
            .agg(
                F.regr_count(F.col("y"), F.col("x")).alias("regr_count"),
                F.regr_avgx(F.col("y"), F.col("x")).alias("regr_avgx"),
                F.regr_avgy(F.col("y"), F.col("x")).alias("regr_avgy"),
                F.regr_slope(F.col("y"), F.col("x")).alias("regr_slope"),
                F.regr_intercept(F.col("y"), F.col("x")).alias("regr_intercept"),
                F.regr_r2(F.col("y"), F.col("x")).alias("regr_r2"),
                F.regr_sxx(F.col("y"), F.col("x")).alias("regr_sxx"),
                F.regr_sxy(F.col("y"), F.col("x")).alias("regr_sxy"),
                F.regr_syy(F.col("y"), F.col("x")).alias("regr_syy"),
            )
            .orderBy("g")
            .collect()
        )
        counts = [row["regr_count"] for row in rows]
        print(f"F.regr_count: {counts!r}")
        if counts != [4, 1]:
            raise SystemExit(f"F.regr_count values {counts!r} != [4, 1]")
        checked = (
            ("regr_avgx", [5.25, 2.0]),
            ("regr_avgy", [2.5, 1.0]),
            ("regr_slope", [0.35971223021582727, None]),
            ("regr_intercept", [0.6115107913669069, None]),
            ("regr_r2", [0.8992805755395683, None]),
            ("regr_sxx", [34.75000000000001, 0.0]),
            ("regr_sxy", [12.5, 0.0]),
            ("regr_syy", [5.0, 0.0]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if len(values) != len(expected):
                raise SystemExit(f"F.{name} produced {len(values)} values")
            for value, want in zip(values, expected, strict=True):
                if value is None or want is None:
                    if value is not want:
                        raise SystemExit(f"F.{name} gave {value!r}, expected {want!r}")
                elif not math.isclose(value, want, rel_tol=1e-12):
                    raise SystemExit(f"F.{name} gave {value!r}, expected {want!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
