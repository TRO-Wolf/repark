"""Demonstrate ``F.listagg`` and ``F.string_agg``, which join a group's values into one string.

pins: ex-12-functions-aggregates-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.listagg", "F.string_agg", "F.col"]


def main() -> None:
    """Join every group's values with a delimiter, over the whole frame and per group."""
    repark = ReparkSession.builder.appName("ex-strings-agg").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1, "x"),
                ("a", 2, "y"),
                ("a", 3, "x"),
                ("a", None, None),
                ("b", 4, "z"),
                ("b", 6, "z"),
            ],
            ["k", "v", "s"],
        )
        joined = frame.select(F.listagg("s", ",").alias("joined")).collect()[0]["joined"]
        if sorted(joined.split(",")) != ["x", "x", "y", "z", "z"]:
            raise SystemExit(f"F.listagg value {joined!r} sorted != ['x', 'x', 'y', 'z', 'z']")
        aggregated = frame.groupBy("k").agg(F.listagg(F.col("s"), "-").alias("joined"))
        rows = sorted(aggregated.collect(), key=lambda row: row["k"])
        joined_by_group = [sorted(row["joined"].split("-")) for row in rows]
        if joined_by_group != [["x", "x", "y"], ["z", "z"]]:
            raise SystemExit(
                f"F.listagg values {joined_by_group!r} != [['x', 'x', 'y'], ['z', 'z']]"
            )
        alias_value = frame.select(F.string_agg("s", ",").alias("joined")).collect()[0]["joined"]
        if sorted(alias_value.split(",")) != sorted(joined.split(",")):
            raise SystemExit(
                f"F.string_agg value {alias_value!r} sorted != F.listagg {joined!r} sorted"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
