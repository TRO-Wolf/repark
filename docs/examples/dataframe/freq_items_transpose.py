"""Read the frequent-item table and transpose a frame on an index column.

pins: df-rust-3/C-001, C-003
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.freqItems",
    "DataFrame.transpose",
    "DataFrameStatFunctions.freqItems",
]


def main() -> None:
    """Run freqItems on both doors and transpose on a local frame."""
    repark = (
        ReparkSession.builder.appName("ex-df-freqitems-transpose").master("local[1]").getOrCreate()
    )
    try:
        frame = repark.createDataFrame(
            [(1, 50.0), (2, 20.0), (3, 40.0), (1, 10.0), (2, 30.0)],
            ["k", "v"],
        )
        frequent = frame.freqItems(["k", "v"])
        expected_columns = ["k_freqItems", "v_freqItems"]
        if frequent.columns != expected_columns:
            raise SystemExit(
                f"DataFrame.freqItems columns {frequent.columns!r} != {expected_columns!r}"
            )
        frequent_row = frequent.collect()[0]
        if sorted(frequent_row["k_freqItems"]) != [1, 2, 3]:
            raise SystemExit(f"DataFrame.freqItems k {frequent_row['k_freqItems']!r} != [1, 2, 3]")
        if sorted(frequent_row["v_freqItems"]) != [10.0, 20.0, 30.0, 40.0, 50.0]:
            raise SystemExit(
                f"DataFrame.freqItems v {frequent_row['v_freqItems']!r} != the five v values"
            )
        stat_row = frame.stat.freqItems(["k"]).collect()[0]
        if sorted(stat_row["k_freqItems"]) != [1, 2, 3]:
            raise SystemExit(
                f"DataFrameStatFunctions.freqItems k {stat_row['k_freqItems']!r} != [1, 2, 3]"
            )

        table = repark.createDataFrame([("a", 1, 10.0), ("b", 2, 20.0)], ["idx", "x", "y"])
        transposed = table.transpose("idx")
        if transposed.columns != ["key", "a", "b"]:
            raise SystemExit(
                f"DataFrame.transpose columns {transposed.columns!r} != ['key', 'a', 'b']"
            )
        rows = [tuple(row) for row in transposed.collect()]
        if rows != [("x", 1, 2), ("y", 10.0, 20.0)]:
            raise SystemExit(
                f"DataFrame.transpose rows {rows!r} != [('x', 1, 2), ('y', 10.0, 20.0)]"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
