"""Demonstrate measuring the distance between two dates with ``datediff``.

pins: ex-6-functions-datetime-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.date_diff", "F.datediff", "F.col"]


def main() -> None:
    """Check end-minus-start day counts, negative when end precedes start, NULL kept."""
    repark = ReparkSession.builder.appName("ex-date-difference").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("2024-01-31", "2024-03-10"),
                ("2024-02-29", "2023-11-05"),
                ("2023-11-15", "2024-01-20"),
                ("2023-12-31", "2025-01-15"),
                (None, None),
            ],
            ["ds", "d2s"],
        )
        date_column = F.col("ds").cast("date")
        end_column = F.col("d2s").cast("date")
        rows = frame.select(
            date_column.alias("d"),
            end_column.alias("d2"),
            F.date_diff(end_column, date_column).alias("date_diff"),
            F.datediff(end_column, date_column).alias("datediff"),
        ).collect()
        expected = [39, -116, 66, 381, None]
        for name in ["date_diff", "datediff"]:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        if [row["date_diff"] for row in rows] != [row["datediff"] for row in rows]:
            raise SystemExit("F.datediff is F.date_diff and must agree exactly")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
