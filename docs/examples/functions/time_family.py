"""Demonstrate the TIME family: current_time answers, the rest refuse like Spark."""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession
from repark.spark.column import Column

COVERS: list[str] = [
    "F.make_time",
    "F.to_time",
    "F.time_diff",
    "F.time_trunc",
    "F.current_time",
    "F.typeof",
    "F.lit",
]


def _refuses(repark: ReparkSession, label: str, column: Column) -> None:
    """Run one TIME refusal and check Spark's UNSUPPORTED_TIME_TYPE text."""
    frame = repark.createDataFrame([(1,)], ["x"])
    try:
        frame.select(column.alias("v")).collect()
    except Exception as error:
        print(f"{label} raises: {error}")
        if "[UNSUPPORTED_TIME_TYPE]" not in str(error):
            raise SystemExit(f"{label} raised without UNSUPPORTED_TIME_TYPE: {error}") from error
    else:
        raise SystemExit(f"{label} did not raise")


def main() -> None:
    """Check current_time and typeof answer while the TIME builders refuse."""
    builder = (
        ReparkSession.builder.appName("ex-time-family")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
    )
    repark = builder.getOrCreate()
    try:
        now = (
            repark.createDataFrame([(1,)], ["x"])
            .select(
                F.current_time().alias("now"),
                F.current_time(3).alias("now3"),
                F.typeof(F.lit("a")).alias("name"),
                F.typeof(F.current_time()).alias("typename"),
            )
            .collect()[0]
        )
        print(f"F.current_time: {now['now']!r}")
        if now["now"] is None or now["now3"] is None:
            raise SystemExit(f"current_time answered NULL: {now!r}")
        if now["name"] != "string" or now["typename"] != "time(6)":
            raise SystemExit(f"typeof disagrees: {now!r}")
        _refuses(repark, "F.make_time", F.make_time(F.lit(6), F.lit(30), F.lit(45)))
        _refuses(repark, "F.to_time", F.to_time(F.lit("10:30")))
        _refuses(
            repark,
            "F.time_diff",
            F.time_diff(F.lit("HOUR"), F.lit(datetime.time(8, 30)), F.lit(datetime.time(12, 30))),
        )
        _refuses(
            repark,
            "F.time_trunc",
            F.time_trunc(F.lit("HOUR"), F.lit(datetime.time(12, 34, 56))),
        )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
