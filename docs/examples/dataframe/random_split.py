"""Split a frame into weighted buckets with randomSplit.

pins: ex-16-dataframe-b/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.randomSplit",
    "DataFrame.random_split",
]


def main() -> None:
    """Run the measured split answers: two parts, every row placed exactly once."""
    repark = ReparkSession.builder.appName("ex-df-b-random-split").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(1,), (2,), (3,), (4,), (5,), (6,)],
            ["n"],
        )
        parts = frame.randomSplit([0.5, 0.5])
        part_count = len(parts)
        part_count_expected = 2
        if part_count != part_count_expected:
            raise SystemExit(
                f"DataFrame.randomSplit parts {part_count!r} != {part_count_expected!r}"
            )
        first_names = parts[0].columns
        first_names_expected = ["n"]
        if first_names != first_names_expected:
            raise SystemExit(
                f"DataFrame.randomSplit columns {first_names!r} != {first_names_expected!r}"
            )
        second_names = parts[1].columns
        if second_names != first_names_expected:
            raise SystemExit(
                f"DataFrame.randomSplit columns {second_names!r} != {first_names_expected!r}"
            )
        placed = parts[0].count() + parts[1].count()
        placed_expected = 6
        if placed != placed_expected:
            raise SystemExit(f"DataFrame.randomSplit placed rows {placed!r} != {placed_expected!r}")

        snake_parts = frame.random_split([0.5, 0.5])
        snake_count = len(snake_parts)
        if snake_count != part_count_expected:
            raise SystemExit(
                f"DataFrame.random_split parts {snake_count!r} != {part_count_expected!r}"
            )
        snake_placed = snake_parts[0].count() + snake_parts[1].count()
        if snake_placed != placed_expected:
            raise SystemExit(
                f"DataFrame.random_split placed rows {snake_placed!r} != {placed_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
