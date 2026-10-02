"""Flag every occurrence of a repeated value (repark-only ``Column.is_duplicated``).

pins: polars-is-duplicated-1/C-001
"""

from __future__ import annotations

from repark import functions as F  # noqa: N812
from repark import polars as rp
from repark.spark import ReparkSession

COVERS: list[str] = ["Column.is_duplicated"]


def main() -> None:
    """Run the polars-shaped duplicate mask on one local frame, both doors."""
    repark = ReparkSession.builder.appName("ex-col-is-duplicated").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(1,), (2,), (2,), (None,), (None,), (3,), (1,)],
            ["c"],
        )
        expected_filter = [(1,), (2,), (2,), (None,), (None,), (1,)]
        expected_mask = [True, True, True, True, True, False, True]
        key = F.col("c")
        spark_rows = [tuple(row) for row in frame.filter(key.is_duplicated()).collect()]
        if spark_rows != expected_filter:
            raise SystemExit(f"Column.is_duplicated filter {spark_rows!r} != {expected_filter!r}")
        polars_key = rp.col("c")
        polars_rows = [
            tuple(row) for row in frame.pl.filter(polars_key.is_duplicated()).spark.collect()
        ]
        if polars_rows != expected_filter:
            raise SystemExit(f"rp is_duplicated filter {polars_rows!r} != {expected_filter!r}")
        mask = [row[0] for row in frame.select(key.is_duplicated().alias("d")).collect()]
        if mask != expected_mask:
            raise SystemExit(f"Column.is_duplicated mask {mask!r} != {expected_mask!r}")
        widened = frame.withColumns({"d": key.is_duplicated()})
        if [row[1] for row in widened.collect()] != expected_mask:
            raise SystemExit("Column.is_duplicated withColumns mask mismatch")
        if frame.select(key.is_duplicated()).columns != ["is_duplicated(c)"]:
            raise SystemExit("Column.is_duplicated default projection name mismatch")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
