"""Demonstrate the ``F.*`` dispersion aggregates on a small grouped frame.

pins: ex-13-functions-aggregates-b-stats/C-001
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.std",
    "F.stddev",
    "F.stddev_pop",
    "F.stddev_samp",
    "F.var_pop",
    "F.var_samp",
    "F.variance",
    "F.col",
]


def main() -> None:
    """Check sample versus population dispersion and the one-value group answers."""
    repark = ReparkSession.builder.appName("ex-dispersion").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(1, 2.0), (1, 4.0), (1, 6.0), (1, 8.0), (1, None), (2, 3.0), (2, None)],
            ["g", "v"],
        )
        rows = (
            frame.groupBy("g")
            .agg(
                F.std(F.col("v")).alias("std"),
                F.stddev(F.col("v")).alias("stddev"),
                F.stddev_pop(F.col("v")).alias("stddev_pop"),
                F.stddev_samp(F.col("v")).alias("stddev_samp"),
                F.var_pop(F.col("v")).alias("var_pop"),
                F.var_samp(F.col("v")).alias("var_samp"),
                F.variance(F.col("v")).alias("variance"),
            )
            .orderBy("g")
            .collect()
        )
        checked = (
            ("std", [2.581988897471611, None]),
            ("stddev", [2.581988897471611, None]),
            ("stddev_pop", [2.23606797749979, 0.0]),
            ("stddev_samp", [2.581988897471611, None]),
            ("var_pop", [5.0, 0.0]),
            ("var_samp", [6.666666666666667, None]),
            ("variance", [6.666666666666667, None]),
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
        if [row["std"] for row in rows] != [row["stddev"] for row in rows]:
            raise SystemExit("F.std is Spark's stddev spelling and must equal F.stddev")
        if [row["stddev"] for row in rows] != [row["stddev_samp"] for row in rows]:
            raise SystemExit("F.stddev is the sample spelling and must equal F.stddev_samp")
        if [row["variance"] for row in rows] != [row["var_samp"] for row in rows]:
            raise SystemExit("F.variance is the sample spelling and must equal F.var_samp")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
