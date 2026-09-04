"""Take rows from both ends of one ordered frame.

pins: ex-18-dataframe-c/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.take", "DataFrame.tail"]


def main() -> None:
    """Run the measured take and tail answers on one sorted frame."""
    repark = ReparkSession.builder.appName("ex-df-take-tail").master("local[1]").getOrCreate()
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
        ordered = frame.sort("k", "v")
        head = ordered.take(2)
        head_values = [tuple(row) for row in head]
        head_expected = [("a", 1, 10.0), ("b", 1, 50.0)]
        if head_values != head_expected:
            raise SystemExit(f"DataFrame.take rows {head_values!r} != {head_expected!r}")
        last = ordered.tail(2)
        tail_values = [tuple(row) for row in last]
        tail_expected = [("a", 2, 30.0), ("a", 3, 40.0)]
        if tail_values != tail_expected:
            raise SystemExit(f"DataFrame.tail rows {tail_values!r} != {tail_expected!r}")
        empty = ordered.tail(0)
        if empty != []:
            raise SystemExit(f"DataFrame.tail rows {empty!r} != []")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
