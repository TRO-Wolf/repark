"""Combine frames: by-position union, the historical alias, and by-name unions.

pins: ex-19-dataframe-d-window/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.union",
    "DataFrame.unionAll",
    "DataFrame.unionByName",
    "DataFrame.union_by_name",
]


def main() -> None:
    """Run the measured union answers with the duplicate row kept."""
    repark = ReparkSession.builder.appName("ex-df-d-set-ops").master("local[1]").getOrCreate()
    try:
        left = repark.createDataFrame([("a", 1), ("b", 2)], ["g", "k"])
        right = repark.createDataFrame([("c", 3), ("a", 1)], ["g", "k"])
        unioned = sorted(tuple(row) for row in left.union(right).collect())
        unioned_expected = [("a", 1), ("a", 1), ("b", 2), ("c", 3)]
        if unioned != unioned_expected:
            raise SystemExit(f"DataFrame.union rows {unioned!r} != {unioned_expected!r}")
        aliased = sorted(tuple(row) for row in left.unionAll(right).collect())
        if aliased != unioned_expected:
            raise SystemExit(f"DataFrame.unionAll rows {aliased!r} != {unioned_expected!r}")

        by_name = repark.createDataFrame([("a", 1)], ["g", "k"])
        reordered = repark.createDataFrame([(2, "b")], ["k", "g"])
        named = by_name.unionByName(reordered)
        named_columns = named.columns
        named_columns_expected = ["g", "k"]
        if named_columns != named_columns_expected:
            raise SystemExit(
                f"DataFrame.unionByName columns {named_columns!r} != {named_columns_expected!r}"
            )
        named_rows = sorted(tuple(row) for row in named.collect())
        named_rows_expected = [("a", 1), ("b", 2)]
        if named_rows != named_rows_expected:
            raise SystemExit(
                f"DataFrame.unionByName rows {named_rows!r} != {named_rows_expected!r}"
            )

        missing = by_name.unionByName(repark.createDataFrame([(2,)], ["k"]), True)
        missing_columns = missing.columns
        if missing_columns != named_columns_expected:
            raise SystemExit(
                f"DataFrame.unionByName columns {missing_columns!r} != {named_columns_expected!r}"
            )
        missing_rows = sorted(
            (tuple(row) for row in missing.collect()),
            key=lambda row: (row[0] is None, row[0]),
        )
        missing_rows_expected = [("a", 1), (None, 2)]
        if missing_rows != missing_rows_expected:
            raise SystemExit(
                f"DataFrame.unionByName rows {missing_rows!r} != {missing_rows_expected!r}"
            )

        snake = by_name.union_by_name(reordered)
        snake_rows = sorted(tuple(row) for row in snake.collect())
        if snake_rows != named_rows_expected:
            raise SystemExit(
                f"DataFrame.union_by_name rows {snake_rows!r} != {named_rows_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
