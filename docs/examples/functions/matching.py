"""Demonstrate the ``F.*`` string-matching names on a small local frame.

pins: ex-4-functions-strings-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.contains",
    "F.startswith",
    "F.endswith",
    "F.instr",
    "F.locate",
    "F.levenshtein",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Check the boolean tests, the 1-based positions, and the edit distance."""
    repark = ReparkSession.builder.appName("ex-matching").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("Spark", "x"), ("Apache", "abc"), ("aPACHE", " xx "), ("", ""), (None, None)],
            ["s", "t"],
        )
        rows = frame.select(
            F.contains(F.col("s"), F.lit("par")).alias("contains_par"),
            F.startswith(F.col("s"), F.lit("Sp")).alias("startswith_Sp"),
            F.endswith(F.col("s"), F.lit("rk")).alias("endswith_rk"),
            F.instr(F.col("s"), F.lit("par")).alias("instr_par"),
            F.locate("x", F.col("t")).alias("locate_x"),
            F.levenshtein(F.col("s"), F.col("t")).alias("levenshtein_st"),
            F.levenshtein(F.lit("kitten"), F.lit("sitting")).alias("levenshtein_ks"),
        ).collect()
        checked = (
            ("contains_par", [True, False, False, False, None]),
            ("startswith_Sp", [True, False, False, False, None]),
            ("endswith_rk", [True, False, False, False, None]),
            ("instr_par", [2, 0, 0, 0, None]),
            ("locate_x", [1, 0, 2, 0, None]),
            ("levenshtein_st", [5, 5, 6, 0, None]),
            ("levenshtein_ks", [3] * 5),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
