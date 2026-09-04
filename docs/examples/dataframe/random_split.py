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
        if part_count != 2:
            raise SystemExit(f"DataFrame.randomSplit parts {part_count!r} != 2")
        first_names = parts[0].columns
        if first_names != ["n"]:
            raise SystemExit(f"DataFrame.randomSplit columns {first_names!r} != ['n']")
        second_names = parts[1].columns
        if second_names != ["n"]:
            raise SystemExit(f"DataFrame.randomSplit columns {second_names!r} != ['n']")
        placed = parts[0].count() + parts[1].count()
        if placed != 6:
            raise SystemExit(f"DataFrame.randomSplit placed rows {placed!r} != 6")

        snake_parts = frame.random_split([0.5, 0.5])
        snake_count = len(snake_parts)
        if snake_count != 2:
            raise SystemExit(f"DataFrame.random_split parts {snake_count!r} != 2")
        snake_placed = snake_parts[0].count() + snake_parts[1].count()
        if snake_placed != 6:
            raise SystemExit(f"DataFrame.random_split placed rows {snake_placed!r} != 6")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
