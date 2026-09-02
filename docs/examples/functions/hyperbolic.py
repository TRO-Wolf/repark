"""Demonstrate the ``F.*`` hyperbolic family and the domains of its inverses.

pins: ex-2-functions-math-bitwise/C-002
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.sinh", "F.cosh", "F.tanh", "F.asinh", "F.acosh", "F.atanh", "F.col"]


def main() -> None:
    """Check the six curves on symmetric input, then the inverse domains at their edges."""
    repark = ReparkSession.builder.appName("ex-hyperbolic").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(0.0,), (1.0,), (-1.0,), (2.0,), (-2.0,), (None,)], ["x"])
        rows = frame.select(
            F.col("x"),
            F.sinh(F.col("x")).alias("sinh"),
            F.cosh(F.col("x")).alias("cosh"),
            F.tanh(F.col("x")).alias("tanh"),
            F.asinh(F.col("x")).alias("asinh"),
            F.acosh(F.col("x")).alias("acosh"),
            F.atanh(F.col("x")).alias("atanh"),
        ).collect()
        checked = (
            (
                "sinh",
                [
                    0.0,
                    1.1752011936438014,
                    -1.1752011936438014,
                    3.626860407847019,
                    -3.626860407847019,
                    None,
                ],
            ),
            (
                "cosh",
                [
                    1.0,
                    1.543080634815244,
                    1.543080634815244,
                    3.7621956910836314,
                    3.7621956910836314,
                    None,
                ],
            ),
            (
                "tanh",
                [
                    0.0,
                    0.7615941559557649,
                    -0.7615941559557649,
                    0.9640275800758169,
                    -0.9640275800758169,
                    None,
                ],
            ),
            (
                "asinh",
                [
                    0.0,
                    0.8813735870195429,
                    -0.8813735870195428,
                    1.4436354751788103,
                    -1.4436354751788099,
                    None,
                ],
            ),
            (
                "acosh",
                [math.nan, 0.0, math.nan, 1.3169578969248166, math.nan, None],
            ),
            (
                "atanh",
                [0.0, math.inf, -math.inf, math.nan, math.nan, None],
            ),
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
                elif isinstance(want, float) and math.isnan(want):
                    if not (isinstance(value, float) and math.isnan(value)):
                        raise SystemExit(f"F.{name} gave {value!r}, expected NaN")
                elif (
                    isinstance(want, float) and math.isinf(want) and value != want
                ) or not math.isclose(value, want, rel_tol=1e-12):
                    raise SystemExit(f"F.{name} gave {value!r}, expected {want!r}")
        if rows[1]["sinh"] != rows[2]["sinh"] * -1.0 or rows[3]["sinh"] != rows[4]["sinh"] * -1.0:
            raise SystemExit(f"F.sinh should be odd: {[row['sinh'] for row in rows]!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
