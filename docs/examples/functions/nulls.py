"""Demonstrate the ``F.*`` NULL tests and NULL substitutions on small local frames.

pins: ex-10-functions-null-cond-misc/C-001
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.isnull",
    "F.isnotnull",
    "F.equal_null",
    "F.coalesce",
    "F.ifnull",
    "F.nvl",
    "F.nvl2",
    "F.nullif",
    "F.nullifzero",
    "F.zeroifnull",
    "F.nanvl",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Check each test and substitution on rows carrying NULLs, NaN included."""
    repark = ReparkSession.builder.appName("ex-nulls").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(1, 2.5, "a"), (0, None, None), (None, 4.0, None), (3, None, "a")], ["i", "d", "s"]
        )
        rows = frame.select(
            F.col("i"),
            F.col("d"),
            F.col("s"),
            F.isnull(F.col("i")).alias("isnull_i"),
            F.isnotnull(F.col("i")).alias("isnotnull_i"),
            F.isnull(F.col("d")).alias("isnull_d"),
            F.isnotnull(F.col("d")).alias("isnotnull_d"),
            F.coalesce(F.col("i"), F.lit(-1)).alias("coalesce_i"),
            F.coalesce(F.col("d"), F.lit(-1.0)).alias("coalesce_d"),
            F.ifnull(F.col("d"), F.lit(-1.0)).alias("ifnull_d"),
            F.nvl(F.col("i"), F.lit(-1)).alias("nvl_i"),
            F.nvl2(F.col("d"), F.col("i"), F.lit(-99)).alias("nvl2_d_i"),
            F.nullif(F.col("i"), F.lit(0)).alias("nullif_i_0"),
            F.nullif(F.col("i"), F.lit(1)).alias("nullif_i_1"),
            F.nullifzero(F.col("i")).alias("nullifzero_i"),
            F.zeroifnull(F.col("i")).alias("zeroifnull_i"),
            F.nanvl(F.col("d"), F.lit(-1.0)).alias("nanvl_d"),
            F.equal_null(F.col("s"), F.lit("a")).alias("equal_null_s_a"),
        ).collect()
        checked = (
            ("isnull_i", [False, False, True, False]),
            ("isnotnull_i", [True, True, False, True]),
            ("isnull_d", [False, True, False, True]),
            ("isnotnull_d", [True, False, True, False]),
            ("coalesce_i", [1, 0, -1, 3]),
            ("coalesce_d", [2.5, -1.0, 4.0, -1.0]),
            ("ifnull_d", [2.5, -1.0, 4.0, -1.0]),
            ("nvl_i", [1, 0, -1, 3]),
            ("nvl2_d_i", [1, -99, None, -99]),
            ("nullif_i_0", [1, None, None, 3]),
            ("nullif_i_1", [None, 0, None, 3]),
            ("nullifzero_i", [1, None, None, 3]),
            ("zeroifnull_i", [1, 0, 0, 3]),
            ("nanvl_d", [2.5, None, 4.0, None]),
            ("equal_null_s_a", [True, False, False, True]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} gave {values!r}, expected {expected!r}")

        pairs = repark.createDataFrame(
            [(None, None), ("a", None), ("a", "a"), (None, "a")], ["a", "b"]
        )
        values = [
            row["equal_null_ab"]
            for row in pairs.select(
                F.equal_null(F.col("a"), F.col("b")).alias("equal_null_ab")
            ).collect()
        ]
        print(f"F.equal_null: {values!r}")
        if values != [True, False, True, False]:
            raise SystemExit(
                f"F.equal_null gave {values!r}, expected [True, False, True, False]; "
                "two NULLs compare equal"
            )

        nans = repark.createDataFrame([(0.0,)], ["x"])
        nan_rows = nans.select(
            F.isnull(F.lit(float("nan"))).alias("isnull_nan"),
            F.nanvl(F.lit(float("nan")), F.lit(-1.0)).alias("nanvl_nan"),
            F.coalesce(F.lit(float("nan")), F.lit(-1.0)).alias("coalesce_nan"),
        ).collect()
        print(
            f"NaN literals: isnull={nan_rows[0]['isnull_nan']!r} "
            f"nanvl={nan_rows[0]['nanvl_nan']!r} coalesce={nan_rows[0]['coalesce_nan']!r}"
        )
        if nan_rows[0]["isnull_nan"] is not False:
            raise SystemExit(f"F.isnull on NaN gave {nan_rows[0]['isnull_nan']!r}, expected False")
        if nan_rows[0]["nanvl_nan"] != -1.0:
            raise SystemExit(f"F.nanvl on NaN gave {nan_rows[0]['nanvl_nan']!r}, expected -1.0")
        coalesce_nan = nan_rows[0]["coalesce_nan"]
        if not (isinstance(coalesce_nan, float) and math.isnan(coalesce_nan)):
            raise SystemExit(f"F.coalesce on NaN gave {coalesce_nan!r}, expected NaN passthrough")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
