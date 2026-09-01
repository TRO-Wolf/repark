"""Take roots of a column, and measure a length without writing the squares out.

``F.sqrt`` and ``F.cbrt`` are the square and cube roots, and they part company on
negative input: a real square root of a negative number does not exist, so the
answer is the float NaN, while a cube root is defined everywhere and keeps the
sign of its argument. NULL input gives NULL from both, which is not the same
thing as NaN — NULL means "no value here", NaN means "a value, and it is not a
number".

``F.hypot`` is the third name here because it is the one a reader reaches for
next: it is the Euclidean length of the two legs it is given, the same number as
``F.sqrt(a * a + b * b)``, spelled the way the geometry reads. This script
asserts the two agree on the classic right triangles.
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.sqrt", "F.cbrt", "F.hypot", "F.col"]


def main() -> None:
    """Contrast the two roots on signed input, then check hypot against the long form."""
    repark = ReparkSession.builder.appName("ex-roots").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(9.0,), (-8.0,), (0.0,), (None,)], ["x"])
        rows = frame.select(
            F.col("x"),
            F.sqrt(F.col("x")).alias("root2"),
            F.cbrt(F.col("x")).alias("root3"),
        ).collect()
        for row in rows:
            print(f"x={row['x']!r:>6}  sqrt={row['root2']!r:<20} cbrt={row['root3']!r}")
        squares = [row["root2"] for row in rows]
        cubes = [row["root3"] for row in rows]
        if squares[0] != 3.0 or squares[2] != 0.0:
            raise SystemExit(f"F.sqrt on 9.0 and 0.0 gave {squares[0]!r}, {squares[2]!r}")
        if not math.isnan(squares[1]):
            raise SystemExit(f"F.sqrt of a negative should be NaN, got {squares[1]!r}")
        if squares[3] is not None or cubes[3] is not None:
            raise SystemExit(f"a NULL row should stay NULL: {squares[3]!r}, {cubes[3]!r}")
        if abs(cubes[0] - 9.0 ** (1.0 / 3.0)) > 1e-12 or cubes[1] != -2.0 or cubes[2] != 0.0:
            raise SystemExit(f"F.cbrt values {cubes!r} unexpected")

        legs = repark.createDataFrame([(3.0, 4.0), (5.0, 12.0), (0.0, 0.0)], ["a", "b"])
        pairs = legs.select(
            F.hypot(F.col("a"), F.col("b")).alias("hypot"),
            F.sqrt(F.col("a") * F.col("a") + F.col("b") * F.col("b")).alias("long_form"),
        ).collect()
        for row in pairs:
            print(f"hypot={row['hypot']!r:>6}  sqrt(a*a + b*b)={row['long_form']!r}")
        lengths = [row["hypot"] for row in pairs]
        if lengths != [5.0, 13.0, 0.0]:
            raise SystemExit(f"F.hypot on the classic triples gave {lengths!r}")
        if lengths != [row["long_form"] for row in pairs]:
            raise SystemExit("F.hypot and sqrt(a*a + b*b) disagreed on ordinary input")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
