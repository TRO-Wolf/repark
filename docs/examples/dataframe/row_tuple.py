"""Count and locate values in a collected Row with the tuple protocol.

pins: row-tuple-1/C-001, C-002
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = ["Row.count", "Row.index"]


def main() -> None:
    """Run the measured Row.count and Row.index answers on one collected row."""
    repark = ReparkSession.builder.appName("ex-df-row-tuple").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1, 2, 1)], ["a", "b", "c"])
        row = frame.first()
        count_value = row.count(1)
        if count_value != 2:
            raise SystemExit(f"Row.count {count_value!r} != 2")
        index_value = row.index(1, 1)
        if index_value != 2:
            raise SystemExit(f"Row.index {index_value!r} != 2")
        try:
            row.index(9)
        except ValueError as error:
            if str(error) != "tuple.index(x): x not in tuple":
                raise SystemExit(f"Row.index message {error!s}") from error
        else:
            raise SystemExit("Row.index(9) did not raise")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
