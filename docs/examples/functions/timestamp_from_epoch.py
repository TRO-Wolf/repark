"""Demonstrate the ``F.*`` timestamp construction from epoch counts.

pins: ex-7-functions-datetime-b/C-001
"""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.timestamp_seconds",
    "F.timestamp_millis",
    "F.timestamp_micros",
    "F.unix_seconds",
    "F.col",
]


def main() -> None:
    """Check each epoch unit into its instant, and the seconds round trip."""
    repark = (
        ReparkSession.builder.appName("ex-timestamp-from-epoch").master("local[1]").getOrCreate()
    )
    try:
        seconds = repark.createDataFrame([(0,), (1577836800,), (-1,), (None,)], ["n"])
        rows = seconds.select(F.timestamp_seconds(F.col("n")).alias("v")).collect()
        values = [row["v"] for row in rows]
        if values != [
            datetime.datetime(1970, 1, 1, 0, 0),
            datetime.datetime(2020, 1, 1, 0, 0),
            datetime.datetime(1969, 12, 31, 23, 59, 59),
            None,
        ]:
            raise SystemExit(f"F.timestamp_seconds values {values!r} != the epoch instant list")

        millis = repark.createDataFrame([(0,), (1234567890123,), (-1,), (None,)], ["n"])
        rows = millis.select(F.timestamp_millis(F.col("n")).alias("v")).collect()
        values = [row["v"] for row in rows]
        if values != [
            datetime.datetime(1970, 1, 1, 0, 0),
            datetime.datetime(2009, 2, 13, 23, 31, 30, 123000),
            datetime.datetime(1969, 12, 31, 23, 59, 59, 999000),
            None,
        ]:
            raise SystemExit(f"F.timestamp_millis values {values!r} != the millisecond instants")

        micros = repark.createDataFrame([(0,), (1234567890123456,), (None,)], ["n"])
        rows = micros.select(F.timestamp_micros(F.col("n")).alias("v")).collect()
        values = [row["v"] for row in rows]
        if values != [
            datetime.datetime(1970, 1, 1, 0, 0),
            datetime.datetime(2009, 2, 13, 23, 31, 30, 123456),
            None,
        ]:
            raise SystemExit(f"F.timestamp_micros values {values!r} != the microsecond instants")

        rows = seconds.select(F.unix_seconds(F.timestamp_seconds(F.col("n"))).alias("v")).collect()
        values = [row["v"] for row in rows]
        if values != [0, 1577836800, -1, None]:
            raise SystemExit(
                f"the F.unix_seconds round trip gave {values!r} != [0, 1577836800, -1, None]"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
