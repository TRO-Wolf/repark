"""Demonstrate the ``F.*`` ordering family: six spellings that place NULLs first or last.

pins: ex-10-functions-null-cond-misc/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.asc",
    "F.asc_nulls_first",
    "F.asc_nulls_last",
    "F.desc",
    "F.desc_nulls_first",
    "F.desc_nulls_last",
    "F.col",
]


def main() -> None:
    """Order one NULL-bearing column six ways and check each arrangement."""
    repark = ReparkSession.builder.appName("ex-sort-order").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(2,), (None,), (1,), (None,)], ["x"])
        checked = (
            ("asc", F.asc(F.col("x")), [None, None, 1, 2]),
            ("asc_nulls_first", F.asc_nulls_first(F.col("x")), [None, None, 1, 2]),
            ("asc_nulls_last", F.asc_nulls_last(F.col("x")), [1, 2, None, None]),
            ("desc", F.desc(F.col("x")), [2, 1, None, None]),
            ("desc_nulls_first", F.desc_nulls_first(F.col("x")), [None, None, 2, 1]),
            ("desc_nulls_last", F.desc_nulls_last(F.col("x")), [2, 1, None, None]),
        )
        for name, ordering, expected in checked:
            values = [row[0] for row in frame.orderBy(ordering).collect()]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} ordered {values!r}, expected {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
