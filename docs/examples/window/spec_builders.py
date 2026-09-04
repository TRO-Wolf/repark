"""Build window specs with the static ``Window`` builders and the chained ``WindowSpec`` forms.

pins: ex-20-window-catalog/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession, Window

COVERS: list[str] = [
    "Window.partitionBy",
    "Window.partition_by",
    "Window.orderBy",
    "Window.order_by",
    "WindowSpec.partitionBy",
    "WindowSpec.partition_by",
    "WindowSpec.orderBy",
    "WindowSpec.order_by",
]

WIN_ROWS = [
    ("a", 1, 10.0),
    ("a", 2, 20.0),
    ("a", 3, 30.0),
    ("b", 4, 50.0),
    ("b", 5, 60.0),
    ("b", 6, 70.0),
]

NUMBERED_EXPECTED = [
    ("a", 1, 10.0, 1),
    ("a", 2, 20.0, 2),
    ("a", 3, 30.0, 3),
    ("b", 4, 50.0, 1),
    ("b", 5, 60.0, 2),
    ("b", 6, 70.0, 3),
]

CUMULATIVE_EXPECTED = [
    ("a", 1, 10.0, 10.0),
    ("a", 2, 20.0, 30.0),
    ("a", 3, 30.0, 60.0),
    ("b", 4, 50.0, 110.0),
    ("b", 5, 60.0, 170.0),
    ("b", 6, 70.0, 240.0),
]

RANK_DESC_EXPECTED = [
    ("a", 1, 10.0, 3),
    ("a", 2, 20.0, 2),
    ("a", 3, 30.0, 1),
    ("b", 4, 50.0, 3),
    ("b", 5, 60.0, 2),
    ("b", 6, 70.0, 1),
]


def main() -> None:
    """Run the measured spec-builder answers: per-partition numbering and global cumulative sum."""
    repark = ReparkSession.builder.appName("ex-win-spec-builders").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(WIN_ROWS, ["g", "k", "v"])

        spec = Window.partitionBy("g").orderBy("k")
        numbered = sorted(
            tuple(row) for row in frame.withColumn("rn", F.row_number().over(spec)).collect()
        )
        numbered_expected = NUMBERED_EXPECTED
        if numbered != numbered_expected:
            raise SystemExit(f"Window.partitionBy row_number {numbered!r} != {numbered_expected!r}")

        snake_spec = Window.partition_by("g").order_by("k")
        snake_numbered = sorted(
            tuple(row) for row in frame.withColumn("rn", F.row_number().over(snake_spec)).collect()
        )
        if snake_numbered != numbered_expected:
            raise SystemExit(
                f"Window.partition_by row_number {snake_numbered!r} != {numbered_expected!r}"
            )

        global_spec = Window.orderBy("k")
        cumulative = sorted(
            tuple(row) for row in frame.withColumn("cs", F.sum("v").over(global_spec)).collect()
        )
        cumulative_expected = CUMULATIVE_EXPECTED
        if cumulative != cumulative_expected:
            raise SystemExit(f"Window.orderBy cumulative {cumulative!r} != {cumulative_expected!r}")

        snake_global = Window.order_by("k")
        snake_cumulative = sorted(
            tuple(row) for row in frame.withColumn("cs", F.sum("v").over(snake_global)).collect()
        )
        if snake_cumulative != cumulative_expected:
            raise SystemExit(
                f"Window.order_by cumulative {snake_cumulative!r} != {cumulative_expected!r}"
            )

        base = Window.partitionBy("g")
        ranked = sorted(
            tuple(row)
            for row in frame.withColumn(
                "rk", F.rank().over(base.orderBy(F.col("k").desc()))
            ).collect()
        )
        ranked_expected = RANK_DESC_EXPECTED
        if ranked != ranked_expected:
            raise SystemExit(f"WindowSpec.orderBy rank {ranked!r} != {ranked_expected!r}")

        snake_base = Window.partition_by("g")
        snake_ranked = sorted(
            tuple(row)
            for row in frame.withColumn(
                "rk", F.rank().over(snake_base.order_by(F.col("k").desc()))
            ).collect()
        )
        if snake_ranked != ranked_expected:
            raise SystemExit(f"WindowSpec.order_by rank {snake_ranked!r} != {ranked_expected!r}")

        ordered = Window.orderBy("k")
        swapped = sorted(
            tuple(row)
            for row in frame.withColumn(
                "rn", F.row_number().over(ordered.partitionBy("g"))
            ).collect()
        )
        swapped_expected = NUMBERED_EXPECTED
        if swapped != swapped_expected:
            raise SystemExit(
                f"WindowSpec.partitionBy row_number {swapped!r} != {swapped_expected!r}"
            )

        snake_ordered = Window.order_by("k")
        snake_swapped = sorted(
            tuple(row)
            for row in frame.withColumn(
                "rn", F.row_number().over(snake_ordered.partition_by("g"))
            ).collect()
        )
        if snake_swapped != swapped_expected:
            raise SystemExit(
                f"WindowSpec.partition_by row_number {snake_swapped!r} != {swapped_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
