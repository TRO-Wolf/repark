"""Demonstrate the current date and timestamp spellings on a small local frame.

pins: ex-6-functions-datetime-a/C-001
"""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.curdate",
    "F.current_date",
    "F.currentDate",
    "F.current_timestamp",
    "F.currentTimestamp",
    "F.now",
]


def main() -> None:
    """Check the six spellings answer one session date and one session timestamp."""
    repark = ReparkSession.builder.appName("ex-current-datetime").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,), (2,)], ["x"])
        rows = frame.select(
            F.curdate().alias("curdate"),
            F.current_date().alias("current_date"),
            F.currentDate().alias("currentDate"),
            F.current_timestamp().alias("current_timestamp"),
            F.currentTimestamp().alias("currentTimestamp"),
            F.now().alias("now"),
        ).collect()
        for name in ["curdate", "current_date", "currentDate"]:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if not all(type(value) is datetime.date for value in values):
                raise SystemExit(f"F.{name} gave a non-date value: {values!r}")
            if [row["curdate"] for row in rows] != values:
                raise SystemExit("F.curdate, F.current_date and F.currentDate must agree exactly")
        for name in ["current_timestamp", "currentTimestamp", "now"]:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if not all(type(value) is datetime.datetime for value in values):
                raise SystemExit(f"F.{name} gave a non-timestamp value: {values!r}")
            if [row["current_timestamp"] for row in rows] != values:
                raise SystemExit(
                    "F.current_timestamp, F.currentTimestamp and F.now must agree exactly"
                )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
