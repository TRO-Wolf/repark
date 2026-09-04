"""Repartition one frame and prove every partitioning call keeps the row multiset.

pins: ex-18-dataframe-c/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.repartition",
    "DataFrame.repartitionById",
    "DataFrame.repartitionByRange",
]


def main() -> None:
    """Run the measured repartition answers: the same rows and count under every call."""
    repark = ReparkSession.builder.appName("ex-df-repartition").master("local[1]").getOrCreate()
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
        expected = {
            ("a", 1, 10.0),
            ("a", 2, 20.0),
            ("a", 2, 30.0),
            ("a", 3, 40.0),
            ("b", 1, 50.0),
            ("b", 2, None),
        }
        spread = frame.repartition(2)
        spread_rows = set(spread.collect())
        if spread_rows != expected:
            raise SystemExit(f"DataFrame.repartition rows {spread_rows!r} != {expected!r}")
        spread_count = spread.count()
        if spread_count != 6:
            raise SystemExit(f"DataFrame.repartition count {spread_count!r} != 6")
        ranged = frame.repartitionByRange(2, "k")
        ranged_rows = set(ranged.collect())
        if ranged_rows != expected:
            raise SystemExit(f"DataFrame.repartitionByRange rows {ranged_rows!r} != {expected!r}")
        ranged_count = ranged.count()
        if ranged_count != 6:
            raise SystemExit(f"DataFrame.repartitionByRange count {ranged_count!r} != 6")
        by_id = frame.repartitionById(2, F.lit(0))
        by_id_rows = set(by_id.collect())
        if by_id_rows != expected:
            raise SystemExit(f"DataFrame.repartitionById rows {by_id_rows!r} != {expected!r}")
        by_id_count = by_id.count()
        if by_id_count != 6:
            raise SystemExit(f"DataFrame.repartitionById count {by_id_count!r} != 6")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
