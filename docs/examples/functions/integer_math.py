"""Demonstrate the ``F.*`` integer helpers: factorial, pmod, greatest, least, width bucket.

pins: ex-2-functions-math-bitwise/C-002
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.factorial",
    "F.pmod",
    "F.greatest",
    "F.least",
    "F.width_bucket",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Check each helper on the rows that show its rule, NULL handling included."""
    repark = ReparkSession.builder.appName("ex-integer-math").master("local[1]").getOrCreate()
    try:
        numbers = repark.createDataFrame([(0,), (5,), (10,), (None,)], ["n"])
        rows = numbers.select(F.col("n"), F.factorial(F.col("n")).alias("factorial")).collect()
        for row in rows:
            print(f"n={row['n']!r:>4}  factorial={row['factorial']!r}")
        values = [row["factorial"] for row in rows]
        if values != [1, 120, 3628800, None]:
            raise SystemExit(f"F.factorial gave {values!r}")

        pairs = repark.createDataFrame([(7, 3), (-7, 3), (0, 3), (None, 3), (7, None)], ["a", "b"])
        rows = pairs.select(
            F.col("a"), F.col("b"), F.pmod(F.col("a"), F.col("b")).alias("pmod")
        ).collect()
        for row in rows:
            print(f"a={row['a']!r:>4}  b={row['b']!r:>4}  pmod={row['pmod']!r}")
        values = [row["pmod"] for row in rows]
        if values != [1, 2, 0, None, None]:
            raise SystemExit(f"F.pmod gave {values!r}; a positive divisor answers non-negative")

        spread = repark.createDataFrame(
            [(1, 5, 3), (None, 4, 2), (None, None, None)], ["a", "b", "c"]
        )
        rows = spread.select(
            F.col("a"),
            F.col("b"),
            F.col("c"),
            F.greatest(F.col("a"), F.col("b"), F.col("c")).alias("greatest"),
            F.least(F.col("a"), F.col("b"), F.col("c")).alias("least"),
        ).collect()
        for row in rows:
            print(
                f"a={row['a']!r:>4}  b={row['b']!r:>4}  c={row['c']!r:>4}  "
                f"greatest={row['greatest']!r:>4}  least={row['least']!r:>4}"
            )
        values = [row["greatest"] for row in rows]
        if values != [5, 4, None]:
            raise SystemExit(f"F.greatest gave {values!r}; NULLs are skipped, all-NULL is NULL")
        values = [row["least"] for row in rows]
        if values != [1, 2, None]:
            raise SystemExit(f"F.least gave {values!r}; NULLs are skipped, all-NULL is NULL")

        buckets = repark.createDataFrame([(3.5,), (0.0,), (10.0,), (-1.0,), (None,)], ["x"])
        rows = buckets.select(
            F.col("x"),
            F.width_bucket(F.col("x"), F.lit(0.0), F.lit(10.0), F.lit(5)).alias("bucket"),
        ).collect()
        for row in rows:
            print(f"x={row['x']!r:>5}  bucket={row['bucket']!r}")
        values = [row["bucket"] for row in rows]
        if values != [2, 1, 6, 0, None]:
            raise SystemExit(
                f"F.width_bucket gave {values!r}; the max lands one past the last bucket"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
