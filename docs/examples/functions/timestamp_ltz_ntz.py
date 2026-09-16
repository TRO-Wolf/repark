"""Demonstrate LTZ and NTZ timestamp parsing plus the tolerant try form."""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.to_timestamp_ltz", "F.to_timestamp_ntz", "F.try_to_timestamp"]


def main() -> None:
    """Check zone-aware and zone-free parsing with and without a Java pattern."""
    builder = (
        ReparkSession.builder.appName("ex-timestamp-ltz-ntz")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
    )
    repark = builder.getOrCreate()
    try:
        frame = repark.createDataFrame([("2016-12-31 00:12:00",), ("garbage",), (None,)], ["s"])
        tolerant = frame.select(
            F.try_to_timestamp("s").cast("string").alias("ts"),
            F.try_to_timestamp("s", F.lit("yyyy-MM-dd HH:mm:ss")).cast("string").alias("fmt"),
        ).collect()
        rows = [row["ts"] for row in tolerant]
        print(f"F.try_to_timestamp: {rows!r}")
        if rows != ["2016-12-31 00:12:00", None, None]:
            raise SystemExit(f"try_to_timestamp rows {rows!r}")
        if [row["fmt"] for row in tolerant] != rows:
            raise SystemExit("try_to_timestamp with a pattern disagrees")
        defaulted = repark.createDataFrame([("2016-12-31 00:12:00",)], ["s"])
        row = defaulted.select(
            F.to_timestamp_ltz("s").cast("string").alias("ltz"),
            F.to_timestamp_ntz("s").alias("ntz"),
            F.to_timestamp("s").cast("string").alias("plain"),
        ).collect()[0]
        print(f"F.to_timestamp_ltz: {[row['ltz']]!r}")
        if row["ltz"] != "2016-12-31 00:12:00" or row["ltz"] != row["plain"]:
            raise SystemExit(f"to_timestamp_ltz ltz {row['ltz']!r} plain {row['plain']!r}")
        print(f"F.to_timestamp_ntz: {[row['ntz']]!r}")
        if row["ntz"] != datetime.datetime(2016, 12, 31, 0, 12, 0):
            raise SystemExit(f"to_timestamp_ntz ntz {row['ntz']!r}")
        patterned = repark.createDataFrame([("31/12/2016 10:30",)], ["s"])
        shaped = patterned.select(
            F.to_timestamp_ltz("s", F.lit("dd/MM/yyyy HH:mm")).cast("string").alias("ltz"),
            F.to_timestamp_ntz("s", F.lit("dd/MM/yyyy HH:mm")).alias("ntz"),
        ).collect()[0]
        if shaped["ltz"] != "2016-12-31 10:30:00":
            raise SystemExit(f"to_timestamp_ltz pattern ltz {shaped['ltz']!r}")
        if shaped["ntz"] != datetime.datetime(2016, 12, 31, 10, 30, 0):
            raise SystemExit(f"to_timestamp_ntz pattern ntz {shaped['ntz']!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
