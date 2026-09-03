"""Demonstrate ``F.concat`` and ``F.concat_ws`` and their two NULL contracts.

pins: ex-4-functions-strings-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.concat", "F.concat_ws", "F.col", "F.lit"]


def main() -> None:
    """Check that concat propagates NULL while concat_ws skips it."""
    repark = ReparkSession.builder.appName("ex-concat").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("Spark", "x"), ("Apache", "abc"), ("aPACHE", " xx "), ("", ""), (None, None)],
            ["s", "t"],
        )
        rows = frame.select(
            F.concat(F.col("s"), F.lit("!")).alias("concat_lit"),
            F.concat(F.col("s"), F.col("t")).alias("concat_st"),
            F.concat_ws("-", F.col("s"), F.col("t")).alias("concat_ws_st"),
        ).collect()
        checked = (
            ("concat_lit", ["Spark!", "Apache!", "aPACHE!", "!", None]),
            ("concat_st", ["Sparkx", "Apacheabc", "aPACHE xx ", "", None]),
            ("concat_ws_st", ["Spark-x", "Apache-abc", "aPACHE- xx ", "-", ""]),
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
