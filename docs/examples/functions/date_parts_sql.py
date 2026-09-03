"""Demonstrate the SQL field extraction trio ``date_part``, ``datepart`` and ``extract``.

pins: ex-6-functions-datetime-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.date_part", "F.datepart", "F.extract", "F.col", "F.lit"]


def main() -> None:
    """Check the field spellings answer one value from a date and a timestamp hour."""
    repark = ReparkSession.builder.appName("ex-date-parts-sql").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("2024-01-31", "2024-03-15 13:45:30.123456"),
                ("2024-02-29", "2023-12-31 23:59:59"),
                ("2023-11-15", "2024-02-29 00:15:00"),
                ("2023-12-31", "2023-06-30 06:30:00"),
                (None, None),
            ],
            ["ds", "tss"],
        )
        date_column = F.col("ds").cast("date")
        timestamp_column = F.col("tss").cast("timestamp")
        rows = frame.select(
            date_column.alias("d"),
            timestamp_column.alias("ts"),
            F.date_part(F.lit("year"), date_column).alias("date_part_year"),
            F.date_part(F.lit("hour"), timestamp_column).alias("date_part_hour"),
            F.datepart(F.lit("year"), date_column).alias("datepart_year"),
            F.extract(F.lit("year"), date_column).alias("extract_year"),
        ).collect()
        checked = (
            ("date_part_year", [2024, 2024, 2023, 2023, None]),
            ("date_part_hour", [13, 23, 0, 6, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        if [row["datepart_year"] for row in rows] != [row["date_part_year"] for row in rows]:
            raise SystemExit("F.datepart is F.date_part and must agree exactly")
        if [row["extract_year"] for row in rows] != [row["date_part_year"] for row in rows]:
            raise SystemExit("F.extract is F.date_part and must agree exactly")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
