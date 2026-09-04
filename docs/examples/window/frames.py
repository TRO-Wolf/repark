"""Frame window specs: ``rowsBetween`` / ``rangeBetween`` as statics and on a built spec.

pins: ex-20-window-catalog/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession, Window

COVERS: list[str] = [
    "Window.rowsBetween",
    "Window.rows_between",
    "Window.rangeBetween",
    "Window.range_between",
    "WindowSpec.rowsBetween",
    "WindowSpec.rows_between",
    "WindowSpec.rangeBetween",
    "WindowSpec.range_between",
]

WIN_ROWS = [
    ("a", 1, 10.0),
    ("a", 2, 20.0),
    ("a", 3, 30.0),
    ("b", 4, 50.0),
    ("b", 5, 60.0),
    ("b", 6, 70.0),
]

WHOLE_EXPECTED = [
    ("a", 1, 10.0, 240.0),
    ("a", 2, 20.0, 240.0),
    ("a", 3, 30.0, 240.0),
    ("b", 4, 50.0, 240.0),
    ("b", 5, 60.0, 240.0),
    ("b", 6, 70.0, 240.0),
]

RUNNING_EXPECTED = [
    ("a", 1, 10.0, 10.0),
    ("a", 2, 20.0, 30.0),
    ("a", 3, 30.0, 60.0),
    ("b", 4, 50.0, 50.0),
    ("b", 5, 60.0, 110.0),
    ("b", 6, 70.0, 180.0),
]

CUMULATIVE_EXPECTED = [
    ("a", 1, 10.0, 10.0),
    ("a", 2, 20.0, 30.0),
    ("a", 3, 30.0, 60.0),
    ("b", 4, 50.0, 110.0),
    ("b", 5, 60.0, 170.0),
    ("b", 6, 70.0, 240.0),
]

WIDE_EXPECTED = [
    ("a", 1, 10.0, 60.0),
    ("a", 2, 20.0, 60.0),
    ("a", 3, 30.0, 60.0),
    ("b", 4, 50.0, 180.0),
    ("b", 5, 60.0, 180.0),
    ("b", 6, 70.0, 180.0),
]

SLIDING_EXPECTED = [
    ("a", 1, 10.0, 15.0),
    ("a", 2, 20.0, 20.0),
    ("a", 3, 30.0, 25.0),
    ("b", 4, 50.0, 55.0),
    ("b", 5, 60.0, 60.0),
    ("b", 6, 70.0, 65.0),
]


def main() -> None:
    """Run the measured frame answers: whole-frame, running, wide-range, and sliding windows."""
    repark = ReparkSession.builder.appName("ex-win-frames").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(WIN_ROWS, ["g", "k", "v"])

        whole = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        totals = sorted(
            tuple(row) for row in frame.withColumn("t", F.sum("v").over(whole)).collect()
        )
        totals_expected = WHOLE_EXPECTED
        if totals != totals_expected:
            raise SystemExit(f"Window.rowsBetween whole {totals!r} != {totals_expected!r}")

        snake_whole = Window.rows_between(Window.unbounded_preceding, Window.unbounded_following)
        snake_totals = sorted(
            tuple(row) for row in frame.withColumn("t", F.sum("v").over(snake_whole)).collect()
        )
        if snake_totals != totals_expected:
            raise SystemExit(f"Window.rows_between whole {snake_totals!r} != {totals_expected!r}")

        global_range = Window.rangeBetween(Window.unboundedPreceding, Window.currentRow).orderBy(
            "k"
        )
        cumulative = sorted(
            tuple(row) for row in frame.withColumn("cs", F.sum("v").over(global_range)).collect()
        )
        cumulative_expected = CUMULATIVE_EXPECTED
        if cumulative != cumulative_expected:
            raise SystemExit(
                f"Window.rangeBetween cumulative {cumulative!r} != {cumulative_expected!r}"
            )

        snake_global = Window.range_between(Window.unbounded_preceding, Window.current_row)
        snake_cumulative = sorted(
            tuple(row)
            for row in frame.withColumn("cs", F.sum("v").over(snake_global.order_by("k"))).collect()
        )
        if snake_cumulative != cumulative_expected:
            raise SystemExit(
                f"Window.range_between cumulative {snake_cumulative!r} != {cumulative_expected!r}"
            )

        spec = Window.partitionBy("g").orderBy("k")
        running = sorted(
            tuple(row)
            for row in frame.withColumn(
                "rt",
                F.sum("v").over(spec.rangeBetween(Window.unboundedPreceding, Window.currentRow)),
            ).collect()
        )
        running_expected = RUNNING_EXPECTED
        if running != running_expected:
            raise SystemExit(f"WindowSpec.rangeBetween running {running!r} != {running_expected!r}")

        wide = sorted(
            tuple(row)
            for row in frame.withColumn("rs", F.sum("v").over(spec.rangeBetween(-5, 5))).collect()
        )
        wide_expected = WIDE_EXPECTED
        if wide != wide_expected:
            raise SystemExit(f"WindowSpec.rangeBetween wide {wide!r} != {wide_expected!r}")

        sliding = sorted(
            tuple(row)
            for row in frame.withColumn("ma", F.avg("v").over(spec.rowsBetween(-1, 1))).collect()
        )
        sliding_expected = SLIDING_EXPECTED
        if sliding != sliding_expected:
            raise SystemExit(f"WindowSpec.rowsBetween sliding {sliding!r} != {sliding_expected!r}")

        snake_spec = Window.partition_by("g").order_by("k")
        snake_running = sorted(
            tuple(row)
            for row in frame.withColumn(
                "rt",
                F.sum("v").over(snake_spec.range_between(Window.unbounded_preceding, 0)),
            ).collect()
        )
        if snake_running != running_expected:
            raise SystemExit(
                f"WindowSpec.range_between running {snake_running!r} != {running_expected!r}"
            )

        snake_sliding = sorted(
            tuple(row)
            for row in frame.withColumn(
                "ma", F.avg("v").over(snake_spec.rows_between(-1, 1))
            ).collect()
        )
        if snake_sliding != sliding_expected:
            raise SystemExit(
                f"WindowSpec.rows_between sliding {snake_sliding!r} != {sliding_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
