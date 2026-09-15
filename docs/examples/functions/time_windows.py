"""Demonstrate time windows, window event times, and session gaps."""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.window",
    "F.window_time",
    "F.session_window",
]


def main() -> None:
    """Bucket three rows into ten-minute windows and five-minute sessions."""
    builder = ReparkSession.builder.appName("ex-time-windows").master("local[1]")
    repark = builder.getOrCreate()
    try:
        rows = [
            (1, datetime.datetime(2024, 1, 1, 10, 7, 30)),
            (2, datetime.datetime(2024, 1, 1, 10, 12, 0)),
            (3, datetime.datetime(2024, 1, 1, 10, 31, 0)),
        ]
        frame = repark.createDataFrame(rows, "id int, ts timestamp")
        buckets = (
            frame.groupBy(F.window("ts", "10 minutes"))
            .agg(F.count("*").alias("c"))
            .select(F.window_time("window").alias("wt"), "c")
            .orderBy("wt")
            .collect()
        )
        bucketed = [(row["wt"], row["c"]) for row in buckets]
        expected = [
            (datetime.datetime(2024, 1, 1, 10, 9, 59, 999999), 1),
            (datetime.datetime(2024, 1, 1, 10, 19, 59, 999999), 1),
            (datetime.datetime(2024, 1, 1, 10, 39, 59, 999999), 1),
        ]
        if bucketed != expected:
            raise SystemExit(f"window buckets {bucketed!r} != {expected!r}")
        sessions = (
            frame.groupBy(F.session_window("ts", "5 minutes"))
            .agg(F.count("*").alias("c"))
            .collect()
        )
        counts = sorted(row["c"] for row in sessions)
        if counts != [1, 2]:
            raise SystemExit(f"session counts {counts!r} != [1, 2]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
