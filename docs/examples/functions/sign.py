"""Read the sign off a column, and apply or leave it alone."""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.sign", "F.signum", "F.negative", "F.positive", "F.col"]


def main() -> None:
    """Check the alias pair, the unary pair, and NULL on both."""
    repark = ReparkSession.builder.appName("ex-sign").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(2.5,), (-1.5,), (0.0,), (None,)], ["v"])
        rows = frame.select(
            F.col("v"),
            F.signum(F.col("v")).alias("signum"),
            F.sign(F.col("v")).alias("sign"),
            F.negative(F.col("v")).alias("negative"),
            F.positive(F.col("v")).alias("positive"),
        ).collect()
        for row in rows:
            print(
                f"v={row['v']!r:>6}  signum={row['signum']!r:>6}  sign={row['sign']!r:>6}  "
                f"negative={row['negative']!r:>6}  positive={row['positive']!r:>6}"
            )
        signs = [row["signum"] for row in rows]
        if signs != [1.0, -1.0, 0.0, None]:
            raise SystemExit(f"F.signum gave {signs!r}, expected [1.0, -1.0, 0.0, None]")
        if signs != [row["sign"] for row in rows]:
            raise SystemExit("F.sign and F.signum are aliases and must agree row for row")
        negated = [row["negative"] for row in rows]
        if negated != [-2.5, 1.5, 0.0, None]:
            raise SystemExit(f"F.negative gave {negated!r}")
        if [row["positive"] for row in rows] != [2.5, -1.5, 0.0, None]:
            raise SystemExit("F.positive should hand every value back unchanged")

        counts = repark.createDataFrame([(-3,), (0,), (7,)], ["n"])
        integral = counts.select(
            F.signum(F.col("n")).alias("signum"),
            F.negative(F.col("n")).alias("negative"),
            F.positive(F.col("n")).alias("positive"),
        ).collect()
        for row in integral:
            print(
                f"signum={row['signum']!r:>6}  negative={row['negative']!r:>4}  "
                f"positive={row['positive']!r:>4}"
            )
        if [row["negative"] for row in integral] != [3, 0, -7]:
            raise SystemExit("F.negative should keep an integer column integral")
        if [row["signum"] for row in integral] != [-1.0, 0.0, 1.0]:
            raise SystemExit("F.signum answers in floats even on an integer column")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
