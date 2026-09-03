"""Demonstrate the ``F.*`` partition transforms through ``writeTo(...).partitionedBy(...)``.

pins: ex-7-functions-datetime-b/C-001
"""

from __future__ import annotations

import datetime
import tempfile

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.years", "F.months", "F.days", "F.bucket", "F.col"]


def main() -> None:
    """Create one transform-partitioned table per name and read the rows back."""
    repark = (
        ReparkSession.builder.appName("ex-partition-transforms").master("local[1]").getOrCreate()
    )
    with tempfile.TemporaryDirectory(prefix="ex-partition-transforms-") as warehouse:
        try:
            repark.register_memory_catalog("local", warehouse)
            repark.sql("CREATE NAMESPACE local.lns")

            years_frame = repark.sql(
                "SELECT * FROM (VALUES (DATE '2024-03-15', 1), (DATE '2025-06-01', 2))"
                " AS t(event_date, id)"
            )
            years_frame.writeTo("local.lns.years_t").partitionedBy(
                F.years(F.col("event_date"))
            ).create()
            rows = repark.sql("SELECT * FROM local.lns.years_t ORDER BY id").collect()
            values = [tuple(row) for row in rows]
            if values != [(datetime.date(2024, 3, 15), 1), (datetime.date(2025, 6, 1), 2)]:
                raise SystemExit(f"F.years partitioned rows {values!r} != the two dated rows")

            months_frame = repark.sql(
                "SELECT * FROM (VALUES (DATE '2024-03-15', 1), (DATE '2024-06-01', 2))"
                " AS t(event_date, id)"
            )
            months_frame.writeTo("local.lns.months_t").partitionedBy(
                F.months(F.col("event_date"))
            ).create()
            rows = repark.sql("SELECT * FROM local.lns.months_t ORDER BY id").collect()
            values = [tuple(row) for row in rows]
            if values != [(datetime.date(2024, 3, 15), 1), (datetime.date(2024, 6, 1), 2)]:
                raise SystemExit(f"F.months partitioned rows {values!r} != the two dated rows")

            days_frame = repark.sql(
                "SELECT * FROM (VALUES (DATE '2024-03-15', 1), (DATE '2024-06-01', 2))"
                " AS t(event_date, id)"
            )
            days_frame.writeTo("local.lns.days_t").partitionedBy(
                F.days(F.col("event_date"))
            ).create()
            rows = repark.sql("SELECT * FROM local.lns.days_t ORDER BY id").collect()
            values = [tuple(row) for row in rows]
            if values != [(datetime.date(2024, 3, 15), 1), (datetime.date(2024, 6, 1), 2)]:
                raise SystemExit(f"F.days partitioned rows {values!r} != the two dated rows")

            ids = repark.sql(
                "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (55, 'd'), (89, 'e'))"
                " AS t(id, name)"
            )
            ids.writeTo("local.lns.bucket_t").partitionedBy(F.bucket(4, F.col("id"))).create()
            rows = repark.sql("SELECT * FROM local.lns.bucket_t ORDER BY id").collect()
            values = [tuple(row) for row in rows]
            if values != [(1, "a"), (2, "b"), (3, "c"), (55, "d"), (89, "e")]:
                raise SystemExit(f"F.bucket partitioned rows {values!r} != the five id rows")
        finally:
            repark.stop()


if __name__ == "__main__":
    main()
