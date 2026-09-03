"""Demonstrate the ``F.*`` array builders and counters on a small local frame.

pins: ex-8-functions-arrays/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.array",
    "F.array_repeat",
    "F.sequence",
    "F.size",
    "F.cardinality",
    "F.array_size",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Build arrays from columns and integer ranges, then count them three ways."""
    repark = ReparkSession.builder.appName("ex-arrays").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,), (2,), (None,)], ["x"])
        rows = frame.select(
            F.col("x"),
            F.array(F.col("x"), F.lit(7)).alias("arrayed"),
            F.array(F.col("x")).alias("single"),
            F.array_repeat(F.col("x"), F.lit(2)).alias("repeated"),
            F.sequence(F.lit(1), F.col("x")).alias("up"),
        ).collect()
        checked = (
            ("arrayed", [[1, 7], [2, 7], [None, 7]]),
            ("single", [[1], [2], [None]]),
            ("repeated", [[1, 1], [2, 2], [None, None]]),
            ("up", [[1], [1, 2], None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} gave {values!r}, expected {expected!r}")
        stepped = frame.select(
            F.sequence(F.lit(1), F.lit(5), F.lit(2)).alias("step_up"),
            F.sequence(F.lit(5), F.lit(1), F.lit(-2)).alias("step_down"),
        ).collect()
        ups = [row["step_up"] for row in stepped]
        print(f"F.sequence stepped: {ups!r}")
        if ups != [[1, 3, 5]] * 3:
            raise SystemExit(f"F.sequence(1, 5, 2) gave {ups!r}, expected [[1, 3, 5]] * 3")
        downs = [row["step_down"] for row in stepped]
        print(f"F.sequence stepped down: {downs!r}")
        if downs != [[5, 3, 1]] * 3:
            raise SystemExit(f"F.sequence(5, 1, -2) gave {downs!r}, expected [[5, 3, 1]] * 3")
        arrays = repark.createDataFrame([([10, 20, 30],), ([None, 5],), (None,)], ["a"])
        counted = arrays.select(
            F.size(F.col("a")).alias("size"),
            F.cardinality(F.col("a")).alias("cardinality"),
            F.array_size(F.col("a")).alias("array_size"),
        ).collect()
        sizes = [row["size"] for row in counted]
        if sizes != [3, 2, None]:
            raise SystemExit(f"F.size gave {sizes!r}, expected [3, 2, None]")
        for name in ("cardinality", "array_size"):
            values = [row[name] for row in counted]
            print(f"F.{name}: {values!r}")
            if values != [3, 2, None]:
                raise SystemExit(f"F.{name} gave {values!r}, expected [3, 2, None]")
        if sizes != [row["cardinality"] for row in counted] or sizes != [
            row["array_size"] for row in counted
        ]:
            raise SystemExit("F.size, F.cardinality and F.array_size must agree exactly")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
