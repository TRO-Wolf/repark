"""Apply a Python callable per row or batch, and read observed aggregate metrics.

pins: df-surface-b-1/C-001, C-002, C-004, C-005
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import Observation, ReparkSession
from repark.spark.row import Row

COVERS: list[str] = [
    "DataFrame.foreach",
    "DataFrame.foreachPartition",
    "DataFrame.observe",
    "Observation.get",
]

_SEEN: list[int] = []
_PARTS: list[list[int]] = []


def _record_row(row: Row) -> None:
    """Append one row's ``a`` value for the foreach example."""
    _SEEN.append(int(row.a))


def _record_partition(iterator: object) -> None:
    """Append one partition's ``a`` values for the foreachPartition example."""
    _PARTS.append([int(row.a) for row in iterator])


def main() -> None:
    """Run foreach, foreachPartition, and observe against one local frame."""
    repark = ReparkSession.builder.appName("ex-df-foreach-observe").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([("x", 1, 2), ("y", 3, 4)], ["key", "a", "b"])
        _SEEN.clear()
        foreach_result = frame.foreach(_record_row)
        if foreach_result is not None:
            raise SystemExit(f"DataFrame.foreach {foreach_result!r} != None")
        if _SEEN != [1, 3]:
            raise SystemExit(f"DataFrame.foreach rows {_SEEN!r} != [1, 3]")
        _PARTS.clear()
        partition_result = frame.foreachPartition(_record_partition)
        if partition_result is not None:
            raise SystemExit(f"DataFrame.foreachPartition {partition_result!r} != None")
        flattened = [value for part in _PARTS for value in part]
        if flattened != [1, 3]:
            raise SystemExit(f"DataFrame.foreachPartition rows {flattened!r} != [1, 3]")
        observation = Observation("m")
        observed = frame.observe(observation, F.count(F.lit(1)).alias("c"), F.sum("a").alias("s"))
        if observed.columns != ["key", "a", "b"]:
            raise SystemExit(f"DataFrame.observe columns {observed.columns!r}")
        observed.collect()
        got = observation.get
        expected = {"c": 2, "s": 4}
        if got != expected:
            raise SystemExit(f"Observation.get {got!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
