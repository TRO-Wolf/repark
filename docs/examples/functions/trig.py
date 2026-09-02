"""Demonstrate the ``F.*`` trigonometry family on a small local frame.

pins: ex-2-functions-math-bitwise/C-002
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.sin",
    "F.cos",
    "F.tan",
    "F.asin",
    "F.acos",
    "F.atan",
    "F.atan2",
    "F.cot",
    "F.csc",
    "F.sec",
    "F.degrees",
    "F.radians",
    "F.pi",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Check the ratios, their inverses, the degree round trip, and NULL on every name."""
    repark = ReparkSession.builder.appName("ex-trig").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(0.0,), (1.0,), (-1.0,), (0.5,), (2.0,), (None,)], ["x"])
        rows = frame.select(
            F.col("x"),
            F.sin(F.col("x")).alias("sin"),
            F.cos(F.col("x")).alias("cos"),
            F.tan(F.col("x")).alias("tan"),
            F.asin(F.col("x")).alias("asin"),
            F.acos(F.col("x")).alias("acos"),
            F.atan(F.col("x")).alias("atan"),
            F.atan2(F.col("x"), F.lit(1.0)).alias("atan2_x_1"),
            F.atan2(F.lit(1.0), F.col("x")).alias("atan2_1_x"),
            F.atan2(F.lit(0.0), F.lit(0.0)).alias("atan2_00"),
            F.cot(F.col("x")).alias("cot"),
            F.csc(F.col("x")).alias("csc"),
            F.sec(F.col("x")).alias("sec"),
            F.degrees(F.col("x")).alias("degrees"),
            F.radians(F.col("x")).alias("radians"),
            F.pi().alias("pi"),
            F.degrees(F.pi()).alias("degrees_pi"),
            F.radians(F.lit(90.0)).alias("radians_90"),
        ).collect()
        checked = (
            (
                "sin",
                [
                    0.0,
                    0.8414709848078965,
                    -0.8414709848078965,
                    0.479425538604203,
                    0.9092974268256817,
                    None,
                ],
            ),
            (
                "cos",
                [
                    1.0,
                    0.5403023058681398,
                    0.5403023058681398,
                    0.8775825618903728,
                    -0.4161468365471424,
                    None,
                ],
            ),
            (
                "tan",
                [
                    0.0,
                    1.5574077246549023,
                    -1.5574077246549023,
                    0.5463024898437905,
                    -2.185039863261519,
                    None,
                ],
            ),
            (
                "asin",
                [
                    0.0,
                    1.5707963267948966,
                    -1.5707963267948966,
                    0.5235987755982989,
                    math.nan,
                    None,
                ],
            ),
            (
                "acos",
                [
                    1.5707963267948966,
                    0.0,
                    3.141592653589793,
                    1.0471975511965979,
                    math.nan,
                    None,
                ],
            ),
            (
                "atan",
                [
                    0.0,
                    0.7853981633974483,
                    -0.7853981633974483,
                    0.4636476090008061,
                    1.1071487177940904,
                    None,
                ],
            ),
            (
                "atan2_x_1",
                [
                    0.0,
                    0.7853981633974483,
                    -0.7853981633974483,
                    0.4636476090008061,
                    1.1071487177940904,
                    None,
                ],
            ),
            (
                "atan2_1_x",
                [
                    1.5707963267948966,
                    0.7853981633974483,
                    2.356194490192345,
                    1.1071487177940904,
                    0.4636476090008061,
                    None,
                ],
            ),
            ("atan2_00", [0.0] * 6),
            (
                "cot",
                [
                    math.inf,
                    0.6420926159343306,
                    -0.6420926159343306,
                    1.830487721712452,
                    -0.45765755436028577,
                    None,
                ],
            ),
            (
                "csc",
                [
                    math.inf,
                    1.1883951057781212,
                    -1.1883951057781212,
                    2.085829642933488,
                    1.0997501702946164,
                    None,
                ],
            ),
            (
                "sec",
                [
                    1.0,
                    1.8508157176809255,
                    1.8508157176809255,
                    1.139493927324549,
                    -2.402997961722381,
                    None,
                ],
            ),
            (
                "degrees",
                [
                    0.0,
                    57.29577951308232,
                    -57.29577951308232,
                    28.64788975654116,
                    114.59155902616465,
                    None,
                ],
            ),
            (
                "radians",
                [
                    0.0,
                    0.017453292519943295,
                    -0.017453292519943295,
                    0.008726646259971648,
                    0.03490658503988659,
                    None,
                ],
            ),
            ("pi", [3.141592653589793] * 6),
            ("degrees_pi", [180.0] * 6),
            ("radians_90", [1.5707963267948966] * 6),
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
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
