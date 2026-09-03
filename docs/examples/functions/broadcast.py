"""Demonstrate ``F.broadcast``: the join hint marking the small side of a join.

pins: ex-10-functions-null-cond-misc/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.broadcast", "F.col"]


def main() -> None:
    """Join with the hint marked and check the hinted result equals the plain join."""
    repark = ReparkSession.builder.appName("ex-broadcast").master("local[1]").getOrCreate()
    try:
        left = repark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["k", "v"])
        right = repark.createDataFrame([(1, "x"), (3, "y")], ["k", "w"])
        hinted = [
            tuple(row) for row in left.join(F.broadcast(right), "k").orderBy(F.col("k")).collect()
        ]
        plain = [tuple(row) for row in left.join(right, "k").orderBy(F.col("k")).collect()]
        print(f"F.broadcast join: {hinted!r}")
        if hinted != [(1, "a", "x"), (3, "c", "y")]:
            raise SystemExit(
                f"The hinted join gave {hinted!r}, expected [(1, 'a', 'x'), (3, 'c', 'y')]"
            )
        if hinted != plain:
            raise SystemExit(f"The hinted join {hinted!r} disagrees with the plain join {plain!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
