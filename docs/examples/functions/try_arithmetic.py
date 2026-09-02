"""Demonstrate the ``F.*`` try-arithmetic names, which answer NULL instead of raising.

pins: ex-2-functions-math-bitwise/C-002
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.try_add", "F.try_subtract", "F.try_multiply", "F.try_divide", "F.col"]


def main() -> None:
    """Check overflow and divide-by-zero against NULL, with ordinary input unchanged."""
    repark = ReparkSession.builder.appName("ex-try-arithmetic").master("local[1]").getOrCreate()
    try:
        ints = repark.createDataFrame(
            [(2147483647, 1), (1073741827, 2), (-2147483648, 1), (6, 3), (None, 3)], ["a", "b"]
        )
        rows = ints.select(
            F.col("a"),
            F.col("b"),
            F.try_add(F.col("a").cast("int"), F.col("b").cast("int")).alias("added"),
            F.try_subtract(F.col("a").cast("int"), F.col("b").cast("int")).alias("subtracted"),
            F.try_multiply(F.col("a").cast("int"), F.col("b").cast("int")).alias("multiplied"),
            F.try_divide(F.col("a").cast("int"), F.col("b").cast("int")).alias("divided"),
        ).collect()
        for row in rows:
            print(
                f"a={row['a']!r:>12}  b={row['b']!r:>2}  "
                f"add={row['added']!r:>12}  sub={row['subtracted']!r:>12}  "
                f"mul={row['multiplied']!r:>12}  div={row['divided']!r:>14}"
            )
        values = [row["added"] for row in rows]
        if values != [None, 1073741829, -2147483647, 9, None]:
            raise SystemExit(f"F.try_add gave {values!r}; overflow answers NULL")
        values = [row["subtracted"] for row in rows]
        if values != [2147483646, 1073741825, None, 3, None]:
            raise SystemExit(f"F.try_subtract gave {values!r}; overflow answers NULL")
        values = [row["multiplied"] for row in rows]
        if values != [2147483647, None, -2147483648, 18, None]:
            raise SystemExit(f"F.try_multiply gave {values!r}; overflow answers NULL")
        values = [row["divided"] for row in rows]
        if values != [2147483647.0, 536870913.5, -2147483648.0, 2.0, None]:
            raise SystemExit(f"F.try_divide gave {values!r}; integer division answers float")

        quotients = repark.createDataFrame(
            [(1.0, 0.0), (6.0, 3.0), (1.5, 0.5), (None, 2.0)], ["a", "b"]
        )
        rows = quotients.select(
            F.col("a"), F.col("b"), F.try_divide(F.col("a"), F.col("b")).alias("divided")
        ).collect()
        for row in rows:
            print(f"a={row['a']!r:>5}  b={row['b']!r:>5}  div={row['divided']!r}")
        values = [row["divided"] for row in rows]
        if values != [None, 2.0, 3.0, None]:
            raise SystemExit(f"F.try_divide gave {values!r}; a zero divisor answers NULL")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
