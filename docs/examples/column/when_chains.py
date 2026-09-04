"""Branch on conditions with a chained ``when`` ladder closed by ``otherwise``.

pins: ex-17-column-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["Column.when", "Column.otherwise"]


def main() -> None:
    """Run the measured CASE ladder answer on one local frame."""
    repark = ReparkSession.builder.appName("ex-col-when").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1, 10.0),
                ("a", 2, 20.0),
                ("a", 2, 30.0),
                ("a", 3, 40.0),
                ("b", 1, 50.0),
                ("b", 2, None),
            ],
            ["g", "k", "v"],
        )
        chain = F.when(frame.k == 1, "one")
        chain = chain.when(frame.k == 2, "two")
        cased = chain.otherwise("other")
        labelled = frame.select(frame.k, cased.alias("w"))
        if labelled.columns != ["k", "w"]:
            raise SystemExit(f"Column.otherwise columns {labelled.columns!r} != ['k', 'w']")
        rows = sorted(labelled.collect(), key=tuple)
        expected = [
            (1, "one"),
            (1, "one"),
            (2, "two"),
            (2, "two"),
            (2, "two"),
            (3, "other"),
        ]
        if rows != expected:
            raise SystemExit(f"Column.when rows {rows!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
