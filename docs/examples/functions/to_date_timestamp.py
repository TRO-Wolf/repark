"""Demonstrate ``F.to_date``, ``F.to_timestamp`` and the ``try_`` door that answers NULL.

pins: ex-7-functions-datetime-b/C-001
"""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.to_date", "F.to_timestamp", "F.try_to_date", "F.col"]


def main() -> None:
    """Parse well-formed calendar strings with to_date and to_timestamp, NULL with try_to_date."""
    repark = ReparkSession.builder.appName("ex-to-datetime").master("local[1]").getOrCreate()
    try:
        well_formed = repark.createDataFrame(
            [("2020-01-02",), ("2020-01-02 13:45:00",), (None,)], ["s"]
        )
        rows = well_formed.select(
            F.to_date(F.col("s")).alias("d"),
            F.to_timestamp(F.col("s")).alias("ts"),
        ).collect()
        values = [row["d"] for row in rows]
        if values != [datetime.date(2020, 1, 2), datetime.date(2020, 1, 2), None]:
            raise SystemExit(f"F.to_date values {values!r} != the parsed date list")
        values = [row["ts"] for row in rows]
        if values != [
            datetime.datetime(2020, 1, 2, 0, 0),
            datetime.datetime(2020, 1, 2, 13, 45),
            None,
        ]:
            raise SystemExit(f"F.to_timestamp values {values!r} != the parsed instant list")

        malformed = repark.createDataFrame([("2020-01-02",), ("not-a-date",), (None,)], ["s"])
        rows = malformed.select(F.try_to_date(F.col("s")).alias("v")).collect()
        values = [row["v"] for row in rows]
        if values != [datetime.date(2020, 1, 2), None, None]:
            raise SystemExit(f"F.try_to_date values {values!r} != [date, None, None]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
