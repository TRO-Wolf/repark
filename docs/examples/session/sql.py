"""Demonstrate the native ``repark.sql`` door and ``ReparkSession`` construction.

pins: ex-0-example-drift-gate/C-002, C-008, C-010
"""

from __future__ import annotations

import repark as repark_native
from repark.spark import ReparkSession

COVERS: list[str] = [
    "repark.sql",
    "SparkSession.builder",
    "SparkSession.Builder.appName",
    "SparkSession.Builder.getOrCreate",
    "SparkSession.createDataFrame",
    "SparkSession.stop",
]


def main() -> None:
    """Run one native SQL select and one ``ReparkSession`` DataFrame collect."""
    native_rows = repark_native.sql("SELECT 1 AS x").collect()
    if native_rows[0]["x"] != 1:
        raise SystemExit(f"repark.sql row {native_rows!r} != 1")
    repark = ReparkSession.builder.appName("ex-session").master("local[1]").getOrCreate()
    try:
        rows = repark.createDataFrame([(2,)], ["y"]).collect()
        if rows[0]["y"] != 2:
            raise SystemExit(f"createDataFrame row {rows!r} != 2")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
