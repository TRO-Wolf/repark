"""Demonstrate the session zone and the ``F.*`` UTC offset renders.

pins: ex-7-functions-datetime-b/C-001
"""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.from_utc_timestamp", "F.to_utc_timestamp", "F.current_timezone", "F.col"]


def main() -> None:
    """Check both offset renders, the Tokyo zone, and the session zone answer."""
    repark = ReparkSession.builder.appName("ex-utc-offsets").master("local[1]").getOrCreate()
    try:
        instants = repark.createDataFrame(
            [("2020-01-01 12:00:00",), ("2020-07-01 12:00:00",), (None,)], ["ts"]
        )
        rows = instants.select(
            F.from_utc_timestamp(F.col("ts").cast("timestamp"), "America/New_York").alias(
                "from_utc"
            ),
            F.to_utc_timestamp(F.col("ts").cast("timestamp"), "America/New_York").alias("to_utc"),
        ).collect()
        values = [row["from_utc"] for row in rows]
        if values != [
            datetime.datetime(2020, 1, 1, 7, 0),
            datetime.datetime(2020, 7, 1, 8, 0),
            None,
        ]:
            raise SystemExit(f"F.from_utc_timestamp values {values!r} != the New York wall times")
        values = [row["to_utc"] for row in rows]
        if values != [
            datetime.datetime(2020, 1, 1, 17, 0),
            datetime.datetime(2020, 7, 1, 16, 0),
            None,
        ]:
            raise SystemExit(f"F.to_utc_timestamp values {values!r} != the UTC instants")

        tokyo = repark.createDataFrame([("2020-01-01 00:00:00",), (None,)], ["ts"])
        rows = tokyo.select(
            F.from_utc_timestamp(F.col("ts").cast("timestamp"), "Asia/Tokyo").alias("v")
        ).collect()
        values = [row["v"] for row in rows]
        if values != [datetime.datetime(2020, 1, 1, 9, 0), None]:
            raise SystemExit(f"F.from_utc_timestamp Tokyo values {values!r} != [09:00, None]")

        rows = instants.select(F.current_timezone().alias("zone")).collect()
        values = [row["zone"] for row in rows]
        if values != ["UTC", "UTC", "UTC"]:
            raise SystemExit(f"F.current_timezone values {values!r} != ['UTC'] * 3")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
