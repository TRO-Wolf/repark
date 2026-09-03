"""Demonstrate ``F.when`` conditional values and ``F.assert_true``'s enforced condition.

pins: ex-10-functions-null-cond-misc/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.when", "F.assert_true", "F.col"]


def main() -> None:
    """Choose values with ``F.when``, then enforce a condition with ``F.assert_true``."""
    repark = ReparkSession.builder.appName("ex-conditional").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,), (2,), (None,)], ["x"])
        rows = frame.select(
            F.when(F.col("x") % 2 == 0, "even")
            .when(F.col("x").isNull(), "missing")
            .otherwise("odd")
            .alias("when_chain"),
            F.when(F.col("x") > 1, "big").alias("when_bare"),
        ).collect()
        chain = [row["when_chain"] for row in rows]
        print(f"F.when chain: {chain!r}")
        if chain != ["odd", "even", "missing"]:
            raise SystemExit(f"F.when chain gave {chain!r}, expected ['odd', 'even', 'missing']")
        bare = [row["when_bare"] for row in rows]
        print(f"F.when bare: {bare!r}")
        if bare != [None, "big", None]:
            raise SystemExit(
                f"F.when without otherwise gave {bare!r}, expected [None, 'big', None]"
            )

        passing = repark.createDataFrame([(1,), (2,)], ["x"])
        values = [
            row["ok"]
            for row in passing.select(F.assert_true(F.col("x") >= 0).alias("ok")).collect()
        ]
        print(f"F.assert_true pass: {values!r}")
        if values != [None, None]:
            raise SystemExit(
                f"F.assert_true on a true condition gave {values!r}, expected [None, None]"
            )

        failing = repark.createDataFrame([(1,)], ["x"])
        try:
            failing.select(F.assert_true(F.col("x") > 2, "x must exceed 2").alias("ok")).collect()
        except Exception as error:
            if "x must exceed 2" not in str(error):
                raise SystemExit(f"F.assert_true raised without its message: {error!r}") from error
        else:
            raise SystemExit("F.assert_true on a false condition did not raise")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
