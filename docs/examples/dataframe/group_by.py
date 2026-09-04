"""Group rows by key and aggregate: the three groupBy spellings.

pins: ex-16-dataframe-b/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.groupBy",
    "DataFrame.group_by",
    "DataFrame.groupby",
]


def main() -> None:
    """Run the measured grouping answers: count, expression agg, and dict agg."""
    repark = ReparkSession.builder.appName("ex-df-b-group-by").master("local[1]").getOrCreate()
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
        counted = frame.groupBy("g").count()
        assert counted.columns == ["g", "count"]
        assert set(counted.collect()) == {("a", 4), ("b", 2)}

        totaled = frame.group_by("g").agg(F.sum("v"), F.count(F.lit(1)))
        assert totaled.columns == ["g", "sum(v)", "count(1)"]
        assert set(totaled.collect()) == {("a", 100.0, 4), ("b", 50.0, 2)}

        peak = frame.groupby("g").agg({"v": "max"})
        assert peak.columns == ["g", "max(v)"]
        assert set(peak.collect()) == {("a", 40.0), ("b", 50.0)}
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
