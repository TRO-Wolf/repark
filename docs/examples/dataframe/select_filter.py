"""Demonstrate a ``select`` then ``filter`` / ``where`` chain on a local frame.

pins: ex-0-example-drift-gate/C-002, C-008, C-010
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import SparkSession

COVERS: list[str] = [
    "DataFrame.select",
    "DataFrame.filter",
    "DataFrame.where",
    "DataFrame.collect",
]


def main() -> None:
    """Keep rows where ``n > 1`` after selecting two columns."""
    spark = SparkSession.builder.appName("ex-select-filter").master("local[1]").getOrCreate()
    try:
        frame = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["n", "label"])
        selected = frame.select("n", "label")
        filtered = selected.filter(F.col("n") > 1)
        via_where = selected.where(F.col("n") > 1)
        rows = filtered.collect()
        where_rows = via_where.collect()
        labels = [row["label"] for row in rows]
        if labels != ["b", "c"]:
            raise SystemExit(f"filter labels {labels!r} != ['b', 'c']")
        if [row["label"] for row in where_rows] != labels:
            raise SystemExit("where and filter disagreed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
