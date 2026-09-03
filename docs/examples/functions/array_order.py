"""Demonstrate the ``F.*`` names that order an array and read it back.

pins: ex-8-functions-arrays/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.sort_array",
    "F.array_max",
    "F.array_min",
    "F.array_join",
    "F.shuffle",
    "F.col",
]


def main() -> None:
    """Sort and join arrays, read their extremes, and permute one to shape."""
    repark = ReparkSession.builder.appName("ex-array-order").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([([3, 1, 2],), ([2, None, 1],), (None,)], ["a"])
        rows = frame.select(
            F.col("a"),
            F.sort_array(F.col("a")).alias("ascending"),
            F.array_max(F.col("a")).alias("largest"),
            F.array_min(F.col("a")).alias("smallest"),
            F.array_join(F.col("a"), ",").alias("joined"),
        ).collect()
        checked = (
            ("ascending", [[1, 2, 3], [None, 1, 2], None]),
            ("largest", [3, 2, None]),
            ("smallest", [1, 1, None]),
            ("joined", ["3,1,2", "2,1", None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} gave {values!r}, expected {expected!r}")
        plain = repark.createDataFrame([([3, 1, 2],), ([10, 5, 8],), (None,)], ["a"])
        descending = [
            row["descending"]
            for row in plain.select(F.sort_array(F.col("a"), False).alias("descending")).collect()
        ]
        print(f"F.sort_array descending: {descending!r}")
        if descending != [[3, 2, 1], [10, 8, 5], None]:
            raise SystemExit(f"F.sort_array(a, False) gave {descending!r}")
        shuffled_rows = plain.select(F.col("a"), F.shuffle(F.col("a")).alias("shuffled")).collect()
        for row in shuffled_rows:
            original = row["a"]
            shuffled = row["shuffled"]
            if original is None:
                if shuffled is not None:
                    raise SystemExit(f"F.shuffle gave {shuffled!r} for a NULL array")
                continue
            if len(shuffled) != len(original) or sorted(shuffled) != sorted(original):
                raise SystemExit(f"F.shuffle gave {shuffled!r} for {original!r}")
        print("F.shuffle: permutations of the input, shape checked")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
