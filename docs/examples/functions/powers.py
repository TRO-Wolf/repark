"""Raise a column to a power, two spellings of it, and the natural exponential.

``F.pow`` and ``F.power`` are aliases: the same operation under two names, so
that SQL's ``power(a, b)`` and the shorter form both work. They are separate
callable objects rather than one object bound twice, so the alias relation is
demonstrated here the way it actually matters — identical output on identical
input, column for column.

``F.exp`` belongs with them because it is the same idea with the base fixed:
``exp(x)`` is ``e`` raised to ``x``, and this script checks it against
``pow(e, x)`` to the last few bits. The result is a float in every case, so the
two routes agree to a tolerance rather than exactly — floating point does not
promise that two different roads to a number arrive at the same bit pattern.
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.pow", "F.power", "F.exp", "F.col", "F.lit"]


def main() -> None:
    """Show the alias pair agreeing, then exp against pow with base e."""
    repark = ReparkSession.builder.appName("ex-powers").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(2.0,), (9.0,), (-3.0,), (None,)], ["base"])
        rows = frame.select(
            F.col("base"),
            F.pow(F.col("base"), F.lit(2.0)).alias("squared"),
            F.power(F.col("base"), F.lit(2.0)).alias("squared_alias"),
            F.pow(F.col("base"), F.lit(0.5)).alias("root"),
        ).collect()
        for row in rows:
            print(
                f"base={row['base']!r:>6}  pow(base, 2)={row['squared']!r:>6}  "
                f"power(base, 2)={row['squared_alias']!r:>6}  pow(base, 0.5)={row['root']!r}"
            )
        squared = [row["squared"] for row in rows]
        if squared != [row["squared_alias"] for row in rows]:
            raise SystemExit("F.pow and F.power are aliases and must agree column for column")
        if squared[:2] != [4.0, 81.0] or squared[3] is not None:
            raise SystemExit(f"F.pow squares gave {squared!r}")
        if rows[1]["root"] != 3.0:
            raise SystemExit(f"a 0.5 exponent is a square root: {rows[1]['root']!r}")

        exponents = repark.createDataFrame([(0.0,), (1.0,), (2.0,), (-3.0,)], ["x"])
        against_e = exponents.select(
            F.col("x"),
            F.exp(F.col("x")).alias("exp"),
            F.pow(F.exp(F.lit(1.0)), F.col("x")).alias("e_to_the_x"),
        ).collect()
        for row in against_e:
            print(f"x={row['x']!r:>6}  exp(x)={row['exp']!r:<22} pow(e, x)={row['e_to_the_x']!r}")
        if against_e[0]["exp"] != 1.0:
            raise SystemExit(f"exp(0) should be 1.0, got {against_e[0]['exp']!r}")
        for row in against_e:
            if abs(row["exp"] - row["e_to_the_x"]) > 1e-12 * max(1.0, abs(row["exp"])):
                raise SystemExit(f"exp and pow(e, x) disagreed at x = {row['x']!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
