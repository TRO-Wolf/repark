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
    """Create one transform-partitioned table per name and check rows and partition values."""
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
            rows = repark.sql(
                "SELECT partition.event_date_year AS year_value FROM local.lns.years_t.files"
                " ORDER BY year_value"
            ).collect()
            values = [tuple(row) for row in rows]
            if values != [(54,), (55,)]:
                raise SystemExit(f"F.years partition values {values!r} != [(54,), (55,)]")

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
            rows = repark.sql(
                "SELECT partition.event_date_month AS month_value FROM local.lns.months_t.files"
                " ORDER BY month_value"
            ).collect()
            values = [tuple(row) for row in rows]
            if values != [(650,), (653,)]:
                raise SystemExit(f"F.months partition values {values!r} != [(650,), (653,)]")

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
            rows = repark.sql(
                "SELECT partition.event_date_day AS day_value FROM local.lns.days_t.files"
                " ORDER BY day_value"
            ).collect()
            values = [tuple(row) for row in rows]
            if values != [(datetime.date(2024, 3, 15),), (datetime.date(2024, 6, 1),)]:
                raise SystemExit(
                    f"F.days partition values {values!r}"
                    f" != [(datetime.date(2024, 3, 15),), (datetime.date(2024, 6, 1),)]"
                )

            ids = repark.sql(
                "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (55, 'd'), (89, 'e'))"
                " AS t(id, name)"
            )
            ids.writeTo("local.lns.bucket_t").partitionedBy(F.bucket(4, F.col("id"))).create()
            rows = repark.sql("SELECT * FROM local.lns.bucket_t ORDER BY id").collect()
            values = [tuple(row) for row in rows]
            if values != [(1, "a"), (2, "b"), (3, "c"), (55, "d"), (89, "e")]:
                raise SystemExit(f"F.bucket partitioned rows {values!r} != the five id rows")
            rows = repark.sql(
                "SELECT partition.id_bucket AS bucket_value FROM local.lns.bucket_t.files"
                " ORDER BY bucket_value"
            ).collect()
            values = [tuple(row) for row in rows]
            if values != [(0,), (1,), (3,)]:
                raise SystemExit(f"F.bucket partition values {values!r} != [(0,), (1,), (3,)]")
        finally:
            repark.stop()


if __name__ == "__main__":
    main()
