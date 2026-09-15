"""Demonstrate the temporal constructors, intervals and timestamp arithmetic."""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.make_timestamp_ltz",
    "F.make_timestamp_ntz",
    "F.try_make_timestamp",
    "F.try_make_timestamp_ltz",
    "F.try_make_timestamp_ntz",
    "F.make_ym_interval",
    "F.try_make_interval",
    "F.convert_timezone",
    "F.localtimestamp",
    "F.timestamp_add",
    "F.timestamp_diff",
]


def main() -> None:
    """Check valid and invalid parts, try forms, and hour and day arithmetic."""
    builder = ReparkSession.builder.appName("ex-temporal-constructors").master("local[1]")
    repark = builder.getOrCreate()
    try:
        parts = repark.createDataFrame(
            [(2014, 12, 28, 6, 30, 45), (2019, 13, 1, 10, 11, 12)],
            "y int, mo int, d int, h int, mi int, s int",
        )
        cols = [F.col(name) for name in ("y", "mo", "d", "h", "mi", "s")]
        naive_rows = parts.select(F.try_make_timestamp_ntz(*cols).alias("ntz")).collect()
        naive = [row["ntz"] for row in naive_rows]
        if naive != [datetime.datetime(2014, 12, 28, 6, 30, 45), None]:
            raise SystemExit(f"try_make_timestamp_ntz rows {naive!r}")
        valid = parts.limit(1)
        built = valid.select(
            F.make_timestamp_ntz(*cols).alias("ntz"),
            F.make_timestamp_ltz(*cols).alias("ltz"),
            F.try_make_timestamp(*cols).alias("try_ts"),
            F.try_make_timestamp_ltz(*cols).alias("try_ltz"),
        ).collect()[0]
        if built["ntz"] != datetime.datetime(2014, 12, 28, 6, 30, 45):
            raise SystemExit(f"make_timestamp_ntz {built['ntz']!r}")
        if built["try_ts"] != built["ltz"] or built["try_ltz"] != built["ltz"]:
            raise SystemExit(f"try forms differ from make_timestamp_ltz: {built!r}")
        start = F.make_timestamp_ntz(F.lit(2024), F.lit(1), F.lit(1), F.lit(0), F.lit(0), F.lit(0))
        end = F.make_timestamp_ntz(F.lit(2024), F.lit(1), F.lit(2), F.lit(1), F.lit(0), F.lit(0))
        arith = valid.select(
            F.timestamp_diff("HOUR", start, end).alias("hours"),
            F.timestamp_add("DAY", F.lit(1), start).alias("next_day"),
            F.convert_timezone(F.lit("UTC"), F.lit("UTC"), start).alias("same"),
            F.localtimestamp().isNotNull().alias("has_now"),
        ).collect()[0]
        if arith["hours"] != 25:
            raise SystemExit(f"timestamp_diff hours {arith['hours']!r} != 25")
        if arith["next_day"] != datetime.datetime(2024, 1, 2, 0, 0, 0):
            raise SystemExit(f"timestamp_add {arith['next_day']!r}")
        if arith["same"] != datetime.datetime(2024, 1, 1, 0, 0, 0) or arith["has_now"] is not True:
            raise SystemExit(
                f"convert_timezone / localtimestamp {arith['same']!r} {arith['has_now']!r}"
            )
        overflow = valid.select(
            F.try_make_interval(F.lit(2147483647), F.lit(12)).isNull().alias("overflow_is_null")
        ).collect()[0]["overflow_is_null"]
        if overflow is not True:
            raise SystemExit("try_make_interval overflow did not answer NULL")
        interval_frame = valid.select(F.make_ym_interval(F.lit(1), F.lit(2)).alias("ym"))
        if interval_frame.columns != ["ym"]:
            raise SystemExit(f"make_ym_interval columns {interval_frame.columns!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
