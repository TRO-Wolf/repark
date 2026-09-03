"""Demonstrate rendering a date as text with format patterns and name shorthands.

pins: ex-6-functions-datetime-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.date_format", "F.dayname", "F.monthname", "F.col"]


def main() -> None:
    """Check ISO and slashed renderings of one date plus the weekday and month names."""
    repark = ReparkSession.builder.appName("ex-date-format").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("2024-01-31",),
                ("2024-02-29",),
                ("2023-11-15",),
                ("2023-12-31",),
                (None,),
            ],
            ["ds"],
        )
        date_column = F.col("ds").cast("date")
        rows = frame.select(
            date_column.alias("d"),
            F.date_format(date_column, "yyyy-MM-dd").alias("iso"),
            F.date_format(date_column, "dd/MM/yyyy").alias("slashed"),
            F.dayname(date_column).alias("dayname"),
            F.monthname(date_column).alias("monthname"),
        ).collect()
        checked = (
            ("iso", ["2024-01-31", "2024-02-29", "2023-11-15", "2023-12-31", None]),
            ("slashed", ["31/01/2024", "29/02/2024", "15/11/2023", "31/12/2023", None]),
            ("dayname", ["Wed", "Thu", "Wed", "Sun", None]),
            ("monthname", ["Jan", "Feb", "Nov", "Dec", None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
