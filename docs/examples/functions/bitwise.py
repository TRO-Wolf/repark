"""Demonstrate the ``F.*`` integer bit family: negations, popcount, bit reads, and shifts.

pins: ex-10-functions-null-cond-misc/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.negate",
    "F.bitwiseNOT",
    "F.bitwise_not",
    "F.bit_count",
    "F.bit_get",
    "F.getbit",
    "F.shiftleft",
    "F.shiftright",
    "F.shiftrightunsigned",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Check each operation on the values that show its rule, NULLs included."""
    repark = ReparkSession.builder.appName("ex-bitwise").master("local[1]").getOrCreate()
    try:
        signed = repark.createDataFrame([(5,), (-5,), (0,), (None,)], ["x"])
        rows = signed.select(
            F.col("x"),
            F.negate(F.col("x")).alias("negate"),
            F.bitwiseNOT(F.col("x")).alias("bitwisenot"),
            F.bitwise_not(F.col("x")).alias("bitwise_not"),
        ).collect()
        negate = [row["negate"] for row in rows]
        bitwisenot = [row["bitwisenot"] for row in rows]
        bitwise_not = [row["bitwise_not"] for row in rows]
        print(f"F.negate: {negate!r}")
        if negate != [-5, 5, 0, None]:
            raise SystemExit(f"F.negate gave {negate!r}, expected [-5, 5, 0, None]")
        print(f"F.bitwiseNOT: {bitwisenot!r}")
        if bitwisenot != [-6, 4, -1, None]:
            raise SystemExit(f"F.bitwiseNOT gave {bitwisenot!r}, expected [-6, 4, -1, None]")
        if bitwise_not != bitwisenot:
            raise SystemExit(
                f"F.bitwise_not gave {bitwise_not!r}, F.bitwiseNOT gave {bitwisenot!r}; must agree"
            )

        counted = repark.createDataFrame([(5,), (255,), (0,), (None,)], ["x"])
        values = [
            row["count"] for row in counted.select(F.bit_count(F.col("x")).alias("count")).collect()
        ]
        print(f"F.bit_count: {values!r}")
        if values != [2, 8, 0, None]:
            raise SystemExit(f"F.bit_count gave {values!r}, expected [2, 8, 0, None]")

        bits = repark.createDataFrame([(5,), (2,), (None,)], ["x"])
        rows = bits.select(
            F.col("x"),
            F.bit_get(F.col("x"), F.lit(0)).alias("bit_get_0"),
            F.bit_get(F.col("x"), F.lit(1)).alias("bit_get_1"),
            F.bit_get(F.col("x"), F.lit(2)).alias("bit_get_2"),
            F.getbit(F.col("x"), F.lit(0)).alias("getbit_0"),
            F.getbit(F.col("x"), F.lit(1)).alias("getbit_1"),
            F.getbit(F.col("x"), F.lit(2)).alias("getbit_2"),
        ).collect()
        checked = (
            ("bit_get_0", [1, 0, None]),
            ("bit_get_1", [0, 1, None]),
            ("bit_get_2", [1, 0, None]),
            ("getbit_0", [1, 0, None]),
            ("getbit_1", [0, 1, None]),
            ("getbit_2", [1, 0, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} gave {values!r}, expected {expected!r}")

        shifted = repark.createDataFrame([(2,), (1,), (-8,), (None,)], ["x"])
        rows = shifted.select(
            F.col("x"),
            F.shiftleft(F.col("x"), 3).alias("shiftleft_3"),
            F.shiftright(F.col("x"), 1).alias("shiftright_1"),
            F.shiftrightunsigned(F.col("x"), 1).alias("shiftrightunsigned_1"),
        ).collect()
        checked = (
            ("shiftleft_3", [16, 8, -64, None]),
            ("shiftright_1", [1, 0, -4, None]),
            ("shiftrightunsigned_1", [1, 0, 9223372036854775804, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} gave {values!r}, expected {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
