"""Demonstrate the ``F.*`` two-column agreement aggregates on a small grouped frame.

pins: ex-13-functions-aggregates-b-stats/C-001
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.corr", "F.covar_pop", "F.covar_samp", "F.col"]


def main() -> None:
    """Check correlation and covariance on paired input, one NULL pair included."""
    repark = ReparkSession.builder.appName("ex-covariance").master("local[1]").getOrCreate()
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
                F.corr(F.col("y"), F.col("x")).alias("corr"),
                F.covar_pop(F.col("y"), F.col("x")).alias("covar_pop"),
                F.covar_samp(F.col("y"), F.col("x")).alias("covar_samp"),
            )
            .orderBy("g")
            .collect()
        )
        checked = (
            ("corr", [0.9483040522636019, None]),
            ("covar_pop", [3.125, 0.0]),
            ("covar_samp", [4.166666666666667, None]),
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
