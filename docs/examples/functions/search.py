"""Demonstrate the ``F.*`` exact-location names on a small local frame.

pins: ex-5-functions-strings-b-regex/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.position", "F.find_in_set", "F.col", "F.lit"]


def main() -> None:
    """Check the one-based positions, the not-found zeros, and the comma-list membership index."""
    repark = ReparkSession.builder.appName("ex-search").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("Spark",), ("SQL",), ("hello world",), ("café",), ("",), (None,)], ["s"]
        )
        rows = frame.select(
            F.position(F.lit("SQL"), F.col("s")).alias("pos_sql"),
            F.position(F.lit("l"), F.col("s")).alias("pos_l"),
            F.position(F.col("s"), F.lit("Spark SQL")).alias("pos_in"),
            F.find_in_set(F.col("s"), F.lit("a,b,SQL")).alias("in_set"),
            F.find_in_set(F.col("s"), F.lit("a,b,c")).alias("in_set_plain"),
        ).collect()
        checked = (
            ("pos_sql", [0, 1, 0, 0, 0, None]),
            ("pos_l", [0, 0, 3, 0, 0, None]),
            ("pos_in", [1, 7, 0, 0, 1, None]),
            ("in_set", [0, 3, 0, 0, 0, None]),
            ("in_set_plain", [0, 0, 0, 0, 0, None]),
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
