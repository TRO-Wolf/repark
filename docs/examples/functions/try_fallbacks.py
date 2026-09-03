"""Demonstrate the ``F.try_mod`` and ``F.try_to_number`` NULL fallbacks.

pins: ex-11-functions-hash-url-random/C-001
"""

from __future__ import annotations

from decimal import Decimal

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.try_mod", "F.try_to_number", "F.col", "F.lit"]


def main() -> None:
    """Check the modulo-by-zero and format-mismatch edges answering NULL."""
    repark = ReparkSession.builder.appName("ex-try-fallbacks").master("local[1]").getOrCreate()
    try:
        mods = repark.createDataFrame([(6, 3), (7, 0), (-7, 3), (None, 3)], ["a", "b"])
        rows = mods.select(F.col("a"), F.col("b"), F.try_mod(F.col("a"), F.col("b")).alias("m"))
        values = [row["m"] for row in rows.collect()]
        print(f"F.try_mod: {values!r}")
        if values != [0, None, -1, None]:
            raise SystemExit(f"F.try_mod values {values!r} != [0, None, -1, None]")

        numbers = repark.createDataFrame([("123.45",), ("abc",), (None,)], ["s"])
        rows = numbers.select(
            F.col("s"),
            F.try_to_number(F.col("s"), F.lit("999.99")).alias("two_digits"),
            F.try_to_number(F.col("s"), F.lit("999")).alias("no_decimals"),
        )
        got = [(row["s"], row["two_digits"], row["no_decimals"]) for row in rows.collect()]
        print(f"F.try_to_number: {got!r}")
        expected = [
            ("123.45", Decimal("123.45"), None),
            ("abc", None, None),
            (None, None, None),
        ]
        if got != expected:
            raise SystemExit(f"F.try_to_number values {got!r} != {expected!r}")

        literals = repark.createDataFrame([(1,)], ["i"])
        row = literals.select(
            F.try_to_number(F.lit("$123"), F.lit("$999")).alias("dollars"),
            F.try_to_number(F.lit(None), F.lit("999.99")).alias("null_input"),
        ).collect()[0]
        print(f"F.try_to_number literal row: {row!r}")
        if row["dollars"] != Decimal("123"):
            raise SystemExit(f"F.try_to_number gave {row['dollars']!r}, expected Decimal('123')")
        if row["null_input"] is not None:
            raise SystemExit(f"F.try_to_number gave {row['null_input']!r}, expected None")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
