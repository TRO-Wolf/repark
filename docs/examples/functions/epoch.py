"""Demonstrate the ``F.*`` epoch conversions between calendar values and counts since 1970.

pins: ex-7-functions-datetime-b/C-001
"""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.unix_date",
    "F.unix_seconds",
    "F.unix_millis",
    "F.unix_micros",
    "F.date_from_unix_date",
    "F.from_unixtime",
    "F.col",
]


def main() -> None:
    """Check calendar-to-epoch counts, the inverse date build, and the string render."""
    repark = ReparkSession.builder.appName("ex-epoch").master("local[1]").getOrCreate()
    try:
        dates = repark.createDataFrame(
            [("1970-01-01",), ("1970-01-02",), ("1969-12-31",), ("2024-02-29",), (None,)], ["d"]
        )
        rows = dates.select(F.unix_date(F.col("d").cast("date")).alias("v")).collect()
        values = [row["v"] for row in rows]
        if values != [0, 1, -1, 19782, None]:
            raise SystemExit(f"F.unix_date values {values!r} != [0, 1, -1, 19782, None]")

        days = repark.createDataFrame([(0,), (1,), (-1,), (19000,), (None,)], ["n"])
        rows = days.select(F.date_from_unix_date(F.col("n")).alias("v")).collect()
        values = [row["v"] for row in rows]
        if values != [
            datetime.date(1970, 1, 1),
            datetime.date(1970, 1, 2),
            datetime.date(1969, 12, 31),
            datetime.date(2022, 1, 8),
            None,
        ]:
            raise SystemExit(f"F.date_from_unix_date values {values!r} != the epoch day list")

        stamps = repark.createDataFrame(
            [
                ("1970-01-01 00:00:00",),
                ("2020-01-01 00:00:00",),
                ("1969-12-31 23:59:59",),
                (None,),
            ],
            ["ts"],
        )
        rows = stamps.select(
            F.unix_seconds(F.col("ts").cast("timestamp")).alias("seconds"),
            F.unix_millis(F.col("ts").cast("timestamp")).alias("millis"),
            F.unix_micros(F.col("ts").cast("timestamp")).alias("micros"),
        ).collect()
        values = [row["seconds"] for row in rows]
        if values != [0, 1577836800, -1, None]:
            raise SystemExit(f"F.unix_seconds values {values!r} != [0, 1577836800, -1, None]")
        values = [row["millis"] for row in rows]
        if values != [0, 1577836800000, -1000, None]:
            raise SystemExit(f"F.unix_millis values {values!r} != [0, 1577836800000, -1000, None]")
        values = [row["micros"] for row in rows]
        if values != [0, 1577836800000000, -1000000, None]:
            raise SystemExit(
                f"F.unix_micros values {values!r} != [0, 1577836800000000, -1000000, None]"
            )

        counts = repark.createDataFrame([(0,), (86400,), (31536001,), (-86400,), (None,)], ["s"])
        rows = counts.select(F.from_unixtime(F.col("s")).alias("v")).collect()
        values = [row["v"] for row in rows]
        if values != [
            "1970-01-01 00:00:00",
            "1970-01-02 00:00:00",
            "1971-01-01 00:00:01",
            "1969-12-31 00:00:00",
            None,
        ]:
            raise SystemExit(f"F.from_unixtime values {values!r} != the rendered UTC strings")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
