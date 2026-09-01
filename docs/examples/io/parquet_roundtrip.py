"""Demonstrate a local Parquet write then read round trip.

pins: ex-0-example-drift-gate/C-002, C-008, C-010
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import SparkSession

COVERS: list[str] = [
    "DataFrame.write",
    "DataFrameWriter.mode",
    "DataFrameWriter.parquet",
    "SparkSession.read",
    "DataFrameReader.parquet",
]


def main() -> None:
    """Write two rows to a temp directory and read them back."""
    spark = SparkSession.builder.appName("ex-parquet").master("local[1]").getOrCreate()
    try:
        source = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        target = Path("parquet_out")
        source.write.mode("overwrite").parquet(str(target))
        restored = spark.read.parquet(str(target)).collect()
        pairs = sorted((row["id"], row["name"]) for row in restored)
        if pairs != [(1, "a"), (2, "b")]:
            raise SystemExit(f"round trip {pairs!r} != [(1, 'a'), (2, 'b')]")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
