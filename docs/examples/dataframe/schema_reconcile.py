"""Reconcile a frame to a target schema and stamp field metadata.

pins: df-surface-a-1/C-001, C-002
"""

from __future__ import annotations

import warnings

from repark.spark import ReparkSession
from repark.spark.types import IntegerType, LongType, StringType, StructField, StructType

COVERS: list[str] = ["DataFrame.to", "DataFrame.withMetadata", "DataFrame.registerTempTable"]


def main() -> None:
    """Run the measured to / withMetadata / registerTempTable answers."""
    repark = (
        ReparkSession.builder.appName("ex-df-schema-reconcile").master("local[1]").getOrCreate()
    )
    try:
        frame = repark.createDataFrame([("x", 1, 2), ("y", 3, 4)], "key string, a int, b int")
        reconciled = frame.to(
            StructType([StructField("b", LongType()), StructField("key", StringType())])
        )
        if reconciled.columns != ["b", "key"]:
            raise SystemExit(f"to columns {reconciled.columns!r}")
        if [row.asDict() for row in reconciled.collect()] != [
            {"b": 2, "key": "x"},
            {"b": 4, "key": "y"},
        ]:
            raise SystemExit("to rows differ")
        stamped = frame.withMetadata("a", {"k": "v"})
        if stamped.columns != ["key", "a", "b"]:
            raise SystemExit(f"withMetadata columns {stamped.columns!r}")
        if stamped.schema["a"].metadata != {"k": "v"}:
            raise SystemExit(f"withMetadata metadata {stamped.schema['a'].metadata!r}")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            result = frame.registerTempTable("rtt_ex")
        if result is not None:
            raise SystemExit("registerTempTable did not return None")
        if repark.table("rtt_ex").count() != 2:
            raise SystemExit("registerTempTable view did not read back")
        narrow = frame.to(
            StructType([StructField("a", IntegerType()), StructField("a2", IntegerType())])
        )
        if narrow.columns != ["a", "a2"]:
            raise SystemExit(f"to fill columns {narrow.columns!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
