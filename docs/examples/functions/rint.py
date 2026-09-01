"""Round a column to the nearest whole number, with ties going to the even one.

``F.rint`` gets its own example because of one rule: a value exactly half way
between two integers does not round up, it rounds to whichever neighbour is
even. So ``0.5`` and ``1.5`` both land on the same side of their gap — ``0.0``
and ``2.0`` — and ``2.5`` goes back down to ``2.0``. Rounding half up would send
those three to ``1.0``, ``2.0`` and ``3.0`` instead. The rule exists because
always rounding ties upward biases a column's sum; alternating between the
neighbours does not.

The result is a float, not an integer — ``rint`` moves a value onto a whole
number, it does not change the column's type — and it keeps the sign, so
``-0.5`` rounds to negative zero. NULL stays NULL.
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.rint", "F.col"]


def main() -> None:
    """Round the halfway cases, the ordinary cases, and a NULL."""
    repark = ReparkSession.builder.appName("ex-rint").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(0.5,), (1.5,), (2.5,), (-1.5,), (2.4,), (2.6,), (-0.5,), (None,)], ["v"]
        )
        rows = frame.select(F.col("v"), F.rint(F.col("v")).alias("rounded")).collect()
        for row in rows:
            print(f"v={row['v']!r:>6}  rint={row['rounded']!r}")
        rounded = [row["rounded"] for row in rows]
        if rounded[:6] != [0.0, 2.0, 2.0, -2.0, 2.0, 3.0]:
            raise SystemExit(f"F.rint gave {rounded[:6]!r}; ties should go to the even neighbour")
        if rounded[6] != 0.0 or math.copysign(1.0, rounded[6]) != -1.0:
            raise SystemExit(f"rint(-0.5) should be negative zero, got {rounded[6]!r}")
        if rounded[7] is not None:
            raise SystemExit(f"a NULL row should stay NULL, got {rounded[7]!r}")
        if not isinstance(rounded[0], float):
            raise SystemExit(f"F.rint answers in floats, got {type(rounded[0]).__name__}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
