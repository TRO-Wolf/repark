"""Demonstrate the native ``repark.sql`` door and Spark session construction.

pins: ex-0-example-drift-gate/C-002, C-008, C-010
"""

from __future__ import annotations

import repark
from repark.spark import SparkSession

COVERS: list[str] = [
    "repark.sql",
    "SparkSession.builder",
    "SparkSession.Builder.appName",
    "SparkSession.Builder.getOrCreate",
    "SparkSession.createDataFrame",
    "SparkSession.stop",
]


def main() -> None:
    """Run one native SQL select and one Spark-session DataFrame collect."""
    native_rows = repark.sql("SELECT 1 AS x").collect()
    if native_rows[0]["x"] != 1:
        raise SystemExit(f"repark.sql row {native_rows!r} != 1")
    spark = SparkSession.builder.appName("ex-session").master("local[1]").getOrCreate()
    try:
        rows = spark.createDataFrame([(2,)], ["y"]).collect()
        if rows[0]["y"] != 2:
            raise SystemExit(f"createDataFrame row {rows!r} != 2")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
