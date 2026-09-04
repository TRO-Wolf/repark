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
        assert len(parts) == 2
        assert parts[0].columns == ["n"]
        assert parts[1].columns == ["n"]
        assert parts[0].count() + parts[1].count() == 6

        snake_parts = frame.random_split([0.5, 0.5])
        assert len(snake_parts) == 2
        assert snake_parts[0].count() + snake_parts[1].count() == 6
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
