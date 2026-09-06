"""Demonstrate month-end shifts, interval arithmetic, epoch seconds, and Spark's TIME refusal."""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.add_months",
    "F.make_interval",
    "F.unix_timestamp",
    "F.to_unix_timestamp",
    "F.try_to_time",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Run the measured month-end, interval, epoch-seconds, and TIME-refusal arms."""
    repark = ReparkSession.builder.appName("ex-dates-more").master("local[1]").getOrCreate()
    try:
        month_starts = repark.createDataFrame(
            [("2024-01-31",), ("2024-02-29",), ("2023-12-15",), (None,)], "d STRING"
        ).select(F.to_date("d").alias("d"))
        rows = month_starts.select(
            F.add_months("d", 1).alias("plus_one"),
            F.add_months("d", -2).alias("minus_two"),
        ).collect()
        checked = (
            (
                "plus_one",
                [
                    datetime.date(2024, 2, 29),
                    datetime.date(2024, 3, 29),
                    datetime.date(2024, 1, 15),
                    None,
                ],
            ),
            (
                "minus_two",
                [
                    datetime.date(2023, 11, 30),
                    datetime.date(2023, 12, 29),
                    datetime.date(2023, 10, 15),
                    None,
                ],
            ),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")

        day = repark.createDataFrame([("2024-01-15",)], "d STRING").select(
            F.to_date("d").alias("d")
        )
        rows = day.select(
            (
                F.col("d")
                + F.make_interval(
                    F.lit(1), F.lit(2), F.lit(0), F.lit(3), F.lit(0), F.lit(0), F.lit(0)
                )
            ).alias("shifted")
        ).collect()
        values = [row["shifted"] for row in rows]
        print(f"F.make_interval date shift: {values!r}")
        if values != [datetime.date(2025, 3, 18)]:
            raise SystemExit(f"F.make_interval date shift {values!r} != [2025-03-18]")

        stamp = repark.createDataFrame([("2024-01-15 10:30:05",)], "t STRING").select(
            F.to_timestamp("t").alias("t")
        )
        rows = stamp.select(
            (
                F.col("t")
                + F.make_interval(
                    F.lit(0), F.lit(0), F.lit(0), F.lit(0), F.lit(4), F.lit(5), F.lit(6)
                )
            ).alias("shifted")
        ).collect()
        values = [row["shifted"] for row in rows]
        print(f"F.make_interval timestamp shift: {values!r}")
        if values != [datetime.datetime(2024, 1, 15, 14, 35, 11)]:
            raise SystemExit(f"F.make_interval timestamp shift {values!r} != [2024-01-15 14:35:11]")

        stamps = repark.createDataFrame(
            [("2024-06-15 12:00:00",), ("1970-01-01 00:00:00",), (None,)],
            "s STRING",
        )
        rows = stamps.select(
            F.unix_timestamp("s").alias("from_str"),
            F.to_unix_timestamp("s").alias("alias"),
            F.unix_timestamp(F.to_timestamp("s")).alias("from_ts"),
        ).collect()
        checked = (
            ("from_str", [1718452800, 0, None]),
            ("alias", [1718452800, 0, None]),
            ("from_ts", [1718452800, 0, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.unix_timestamp {name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.unix_timestamp {name} {values!r} != {expected!r}")

        now_rows = repark.range(3).select(F.unix_timestamp().alias("now")).collect()
        now_values = [row["now"] for row in now_rows]
        print(f"F.unix_timestamp(): {now_values!r}")
        if len(now_values) != 3 or len(set(now_values)) != 1:
            raise SystemExit(f"F.unix_timestamp() {now_values!r} is not one epoch across rows")
        if not isinstance(now_values[0], int) or now_values[0] <= 1_700_000_000:
            raise SystemExit(f"F.unix_timestamp() {now_values[0]!r} is not a current epoch int")

        try:
            repark.range(1).select(F.try_to_time(F.lit("12:34:56")).alias("v")).collect()
        except Exception as error:
            print(f"F.try_to_time raises: {error}")
            if "UNSUPPORTED_TIME_TYPE" not in str(error):
                raise SystemExit(
                    f"F.try_to_time raised without UNSUPPORTED_TIME_TYPE: {error}"
                ) from error
        else:
            raise SystemExit("F.try_to_time('12:34:56') did not raise")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
