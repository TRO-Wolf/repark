"""Frame bounds: the ``Window`` constants and the running/trailing frames they set.

pins: ex-20-window-catalog/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession, Window

COVERS: list[str] = [
    "Window.currentRow",
    "Window.current_row",
    "Window.unboundedPreceding",
    "Window.unbounded_preceding",
    "Window.unboundedFollowing",
    "Window.unbounded_following",
]

WIN_ROWS = [
    ("a", 1, 10.0),
    ("a", 2, 20.0),
    ("a", 3, 30.0),
    ("b", 4, 50.0),
    ("b", 5, 60.0),
    ("b", 6, 70.0),
]

RUNNING_EXPECTED = [
    ("a", 1, 10.0, 10.0),
    ("a", 2, 20.0, 30.0),
    ("a", 3, 30.0, 60.0),
    ("b", 4, 50.0, 50.0),
    ("b", 5, 60.0, 110.0),
    ("b", 6, 70.0, 180.0),
]

TRAILING_EXPECTED = [
    ("a", 1, 10.0, 60.0),
    ("a", 2, 20.0, 50.0),
    ("a", 3, 30.0, 30.0),
    ("b", 4, 50.0, 180.0),
    ("b", 5, 60.0, 130.0),
    ("b", 6, 70.0, 70.0),
]


def main() -> None:
    """Run the measured bound answers: constant values, the running and trailing frames."""
    repark = ReparkSession.builder.appName("ex-win-bounds").master("local[1]").getOrCreate()
    try:
        bounds = (Window.currentRow, Window.unboundedPreceding, Window.unboundedFollowing)
        bounds_expected = (0, -9223372036854775808, 9223372036854775807)
        if bounds != bounds_expected:
            raise SystemExit(f"Window constants {bounds!r} != {bounds_expected!r}")

        snake_bounds = (Window.current_row, Window.unbounded_preceding, Window.unbounded_following)
        if snake_bounds != bounds_expected:
            raise SystemExit(f"Window snake constants {snake_bounds!r} != {bounds_expected!r}")

        running = (
            Window.partitionBy("g")
            .orderBy("k")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        frame = repark.createDataFrame(WIN_ROWS, ["g", "k", "v"])
        running_rows = sorted(
            tuple(row) for row in frame.withColumn("cs", F.sum("v").over(running)).collect()
        )
        running_expected = RUNNING_EXPECTED
        if running_rows != running_expected:
            raise SystemExit(
                f"Window.unboundedPreceding running {running_rows!r} != {running_expected!r}"
            )

        spec = Window.partitionBy("g").orderBy("k")
        trailing = sorted(
            tuple(row)
            for row in frame.withColumn(
                "ts",
                F.sum("v").over(spec.rowsBetween(Window.currentRow, Window.unboundedFollowing)),
            ).collect()
        )
        trailing_expected = TRAILING_EXPECTED
        if trailing != trailing_expected:
            raise SystemExit(
                f"Window.unboundedFollowing trailing {trailing!r} != {trailing_expected!r}"
            )

        snake_spec = Window.partition_by("g").order_by("k")
        snake_trailing = sorted(
            tuple(row)
            for row in frame.withColumn(
                "ts",
                F.sum("v").over(
                    snake_spec.rows_between(Window.current_row, Window.unbounded_following)
                ),
            ).collect()
        )
        if snake_trailing != trailing_expected:
            raise SystemExit(f"Window snake trailing {snake_trailing!r} != {trailing_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
