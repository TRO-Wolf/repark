"""Demonstrate the ``F.*`` logarithm names and the constant ``F.e``.

pins: ex-2-functions-math-bitwise/C-002
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.ln", "F.log", "F.log10", "F.log2", "F.e", "F.col"]


def main() -> None:
    """Check the base spellings against one another on positive input, NULLs included."""
    repark = ReparkSession.builder.appName("ex-logs").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(1.0,), (2.718281828459045,), (10.0,), (8.0,), (100.0,), (0.5,), (None,)], ["x"]
        )
        rows = frame.select(
            F.col("x"),
            F.ln(F.col("x")).alias("ln"),
            F.log(F.col("x")).alias("log"),
            F.log(2.0, F.col("x")).alias("log_2"),
            F.log10(F.col("x")).alias("log10"),
            F.log2(F.col("x")).alias("log2"),
            F.e().alias("e"),
            F.ln(F.e()).alias("ln_e"),
        ).collect()
        checked = (
            (
                "ln",
                [
                    0.0,
                    1.0,
                    2.302585092994046,
                    2.0794415416798357,
                    4.605170185988092,
                    -0.6931471805599453,
                    None,
                ],
            ),
            (
                "log",
                [
                    0.0,
                    1.0,
                    2.302585092994046,
                    2.0794415416798357,
                    4.605170185988092,
                    -0.6931471805599453,
                    None,
                ],
            ),
            (
                "log_2",
                [
                    0.0,
                    1.4426950408889634,
                    3.3219280948873626,
                    3.0,
                    6.643856189774725,
                    -1.0,
                    None,
                ],
            ),
            (
                "log10",
                [
                    0.0,
                    0.4342944819032518,
                    1.0,
                    0.9030899869919435,
                    2.0,
                    -0.3010299956639812,
                    None,
                ],
            ),
            (
                "log2",
                [
                    0.0,
                    1.4426950408889634,
                    3.3219280948873626,
                    3.0,
                    6.643856189774725,
                    -1.0,
                    None,
                ],
            ),
            ("e", [2.718281828459045] * 7),
            ("ln_e", [1.0] * 7),
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
                elif not math.isclose(value, want, rel_tol=1e-12):
                    raise SystemExit(f"F.{name} gave {value!r}, expected {want!r}")
        if [row["ln"] for row in rows] != [row["log"] for row in rows]:
            raise SystemExit("F.log(x) with one argument is F.ln(x) and must agree exactly")
        if [row["log_2"] for row in rows] != [row["log2"] for row in rows]:
            raise SystemExit("F.log(2.0, x) is F.log2(x) and must agree exactly")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
